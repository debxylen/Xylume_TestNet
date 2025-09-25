from utils import *
from tx_sign import *
from tx_decode import *
from dag import *
import traceback
import pickle
import os
import sys
import json
import time
from readerwriterlock.rwlock import RWLockFair
from copy import deepcopy
from threading import Thread, Condition, Lock
from dotenv import load_dotenv
load_dotenv()

with open("./config.json", "r") as f:
    constants = json.load(f)

CHAIN_ID = constants["CHAIN_ID"]
MINT_ADDRESS = constants["MINT_ADDRESS"]
NODE_ADDRESS = constants["NODE_ADDRESS"]
NULL_ADDRESS = constants["NULL_ADDRESS"]
DECIMALS = constants["DECIMALS"]
u = 10**DECIMALS
INITIAL_SUPPLY = constants["INITIAL_SUPPLY"] * u
MINER_FEE_SHARE = constants["MINER_FEE_SHARE"]
NETWORK_MINER = constants["NETWORK_MINER"]
TOKEN_CONTRACT = constants["TOKEN_CONTRACT"]

# This variable holds the sacred integer of destiny.
# Without it, the universe may collapse.
legendary_number = 6934  # Chosen by the ancient gods of computation

## To Do:
## - Better error logging (printing function name along with traceback, etc)
## - Integrate and deploy XEVM into production.
## - Batch transactions and atomic bundles
## - Transaction dependencies
## - USDX
## - Consider native token wrappers.
## - Programmable wallets
## - Mining pools
## - Multi-sig
## - RPC or Interaction-wrapping native contract
## - Domain system

class Core:
    def __init__(self, p2p, ws):
        self.dag_lock = RWLockFair()
        if os.path.isfile('dag'):
            self.load_dag()
            self.genesis = self.dag.nodes[list(self.dag.nodes)[0]]['transaction']
        else:
            self.genesis = Transaction(sender=NULL_ADDRESS, recipient=MINT_ADDRESS, amount=INITIAL_SUPPLY, gas=0, parents=[])
            self.dag = DAG(self.genesis)
            self.save_dag()
        self.tokens_lock = RWLockFair()
        if os.path.isfile('tokens.json'):
            self.load_tokens()
        else:
            self.tokens = {}
            self.save_tokens()

        self.mempool = []
        self.picked = []
        self.funnel = []
        self.raw_txs = {}
        self.mempool_lock = RWLockFair()
        self.picked_lock = RWLockFair()
        self.funnel_cn = Condition()
        self.raw_txs_lock = Lock()

        self.p2p = p2p
        self.p2p.process_received = self.process_received
        self.ws = ws
        self.last_speed = self.load_speed() # in nanoseconds
        self.last_miner_request = 0
        self.last_received_timestamp_ns = 0
        
        self.start_funnel()
        self.start_retry()
        self.mine_passive()
        self.ws.start()

    def add_pending(self, sender, recipient, amount: int, gas, nonce = None, data = '0x'):
        """Add a new transaction to the mempool."""
        sender, recipient = sender.lower(), recipient.lower()
        amount, gas = int(amount), int(gas)
        if nonce == None:
            nonce = self.get_transaction_count(sender)
        transaction = Transaction(sender, recipient, amount, gas, [], nonce, data) # pending tx, diff from normal ones
        with self.mempool_lock.gen_wlock(): self.mempool.append(transaction)
        return transaction

    def remove_bad_nodes(self):
        with self.dag_lock.gen_rlock(): node_list = list(self.dag.nodes) # snapshot
        nodes_to_remove = [i for i in node_list if not self.dag.nodes[i].get('transaction')]

        with self.dag_lock.gen_wlock():
            for i in nodes_to_remove:
                self.dag.remove_node(i)

    def get_balance(self, address, mode='latest'):
        address = address.lower()

        # Confirmed balance from DAG
        with self.dag_lock.gen_rlock():
            nodes_snapshot = list(self.dag.nodes.values())
        confirmed_balance = sum(
            tx.juice for node in nodes_snapshot
            if (tx := node.get("transaction"))
            and tx.recipient == address
            and tx.juice > 0
        )

        if mode == 'latest':
            return confirmed_balance

        pending_in = 0
        pending_out = 0

        # Include mempool
        with self.mempool_lock.gen_rlock():
            mempool_snapshot = list(self.mempool)
        for tx in mempool_snapshot:
            if tx.recipient == address:
                pending_in += tx.amount
            if tx.sender == address:
                pending_out += tx.amount + tx.gas

        # Include picked
        with self.picked_lock.gen_rlock():
            picked_snapshot = list(self.picked)
        for tx in picked_snapshot:
            if tx.recipient == address:
                pending_in += tx.amount
            if tx.sender == address:
                pending_out += tx.amount + tx.gas

        # Include funnel
        with self.funnel_lock.gen_rlock():
            funnel_snapshot = list(self.funnel)
        for tx in funnel_snapshot:
            if tx.recipient == address:
                pending_in += tx.amount
            if tx.sender == address:
                pending_out += tx.amount + tx.gas

        return confirmed_balance + pending_in - pending_out

    def get_transaction_count(self, sender_address, mode='latest'):
        sender_address = sender_address.lower()
        count = 0

        # Confirmed transactions from DAG
        with self.dag_lock.gen_rlock():
            nodes_snapshot = list(self.dag.nodes.values())
        for node in nodes_snapshot:
            if (tx := node.get("transaction")) and tx.sender.lower() == sender_address:
                count += 1

        if mode.lower() == 'latest':
            return count

        # Include mempool
        with self.mempool_lock.gen_rlock():
            mempool_snapshot = list(self.mempool)
        for tx in mempool_snapshot:
            if tx.sender.lower() == sender_address:
                count += 1

        # Include picked
        with self.picked_lock.gen_rlock():
            picked_snapshot = list(self.picked)
        for tx in picked_snapshot:
            if tx.sender.lower() == sender_address:
                count += 1

        # Include funnel
        with self.funnel_lock.gen_rlock():
            funnel_snapshot = list(self.funnel)
        for tx in funnel_snapshot:
            if tx.sender.lower() == sender_address:
                count += 1

        return count

    def get_tx_by_number(self, n: int):
        with self.dag_lock.gen_rlock(): nodes_snapshot = dict(self.dag.nodes)
        nodes_list = list(nodes_snapshot)
        return nodes_snapshot[nodes_list[n]].get('transaction') if len(nodes_list) > n else None

    def get_tx_by_hash(self, tx_hash):
        with self.dag_lock.gen_rlock(): nodes_snapshot = dict(self.dag.nodes)
        if tx_hash not in nodes_snapshot:
            return None
        txnode = nodes_snapshot[tx_hash]
        return txnode.get('transaction', None)

    def generate_job(self, node_miner = False):
        """Generate a new mining job."""
        if not node_miner: self.last_miner_request = time.time()
        if len(self.mempool) == 0:
            return 'NO_JOB'
        with self.mempool_lock.gen_wlock(): tx = self.mempool.pop(0)
        with self.picked_lock.gen_wlock(): self.picked.append({"tx": tx, "timestamp": time.time_ns()})
        job = {"transactions": [tx if node_miner else tx.__json__()]}
        return job

    def start_retry(self):
        def _retry():
            while True:
                time.sleep(0.5)

                now = time.time_ns()
                expired = []

                with self.picked_lock.gen_rlock():
                    for p in self.picked:
                        if now - p["timestamp"] >= 2 * 1e9:
                            expired.append(p)

                expired.sort(key=lambda p: p["tx"].timestamp) # ensures proper ordering, no nonce mismatches
                for p in expired:
                    tx = p["tx"]

                    parents = self.find_valid_parents(tx)
                    if not parents:
                        continue

                    if not self.simulate_contract(tx):
                        continue

                    # send to funnel
                    with self.funnel_cn:
                        self.funnel.append({
                            "tx": tx,
                            "parents": parents,
                            "miner": NETWORK_MINER
                        })
                        self.funnel_cn.notify()

                    with self.picked_lock.gen_wlock():
                        try:
                            self.picked.remove(p)
                        except:
                            pass

        Thread(target=_retry, daemon=True).start()

    def start_funnel(self):
        def _funnel():
            while True:
                with self.funnel_cn:
                    if not self.funnel:
                        self.funnel_cn.wait()
                
                    # sort funnel based on timestamp
                    self.funnel.sort(key=lambda x: x["tx"].timestamp)
                    tx_entry = self.funnel[0]
                    tx_ts = tx_entry["tx"].timestamp

                with self.picked_lock.gen_rlock():
                    # hold off if earlier-picked txs exist
                    if any(p["tx"].timestamp < tx_ts for p in self.picked):
                        continue

                with self.funnel_cn:
                    if not self.funnel or self.funnel[0]["tx"].timestamp != tx_ts:
                        continue  # race: funnel changed, retry
                    tx_entry = self.funnel.pop(0)

                tx = tx_entry["tx"]
                if tx.nonce != self.get_transaction_count(tx.sender, "latest"):
                    continue

                parents = tx_entry["parents"]
                miner = tx_entry["miner"]

                finalizedtx, noderewardtx, minerrewardtx = self.finalize_and_broadcast(
                    tx, parents, NODE_ADDRESS, miner, MINER_FEE_SHARE
                )

                self.handle_peer_voting(finalizedtx, parents, tx.gas, NODE_ADDRESS, miner, MINER_FEE_SHARE)

        Thread(target=_funnel, daemon=True).start()

    def _process_contract_interaction(self, tx, simulate=False):
        if type(tx) == dict:
            sender = tx["sender"].lower()
            recipient = tx["recipient"].lower()
            nonce = tx["nonce"]
            data = tx["data"]
        else:
            sender = tx.sender.lower()
            recipient = tx.recipient.lower()
            nonce = tx.nonce
            data = tx.data

        with self.tokens_lock.gen_rlock():
            token_exists = recipient in self.tokens

        if token_exists:
            if len(data) != 138:
                if simulate: return False
                raise Exception('Invalid data length in token tx.')

            method_id = data[:10]
            if method_id == "0xa9059cbb":  # transfer(address,uint256)
                token_recipient = '0x' + data[10:74][-40:]
                token_amount = int(data[74:], 16)

                with self.tokens_lock.gen_rlock():
                    sender_balance = self.tokens[recipient]["balances"].get(sender, 0)

                if sender_balance < token_amount:
                    if simulate: return False
                    raise Exception('Insufficient token balance.')
                if simulate: return True

                with self.tokens_lock.gen_wlock():
                    self.tokens[recipient]["balances"][sender] -= token_amount
                    self.tokens[recipient]["balances"][token_recipient] = \
                        self.tokens[recipient]["balances"].get(token_recipient, 0) + token_amount
                self.save_tokens()

        elif recipient == TOKEN_CONTRACT.lower():
            f_params = hex_to_string(data).split(" ")
            f_id = f_params[0]

            if f_id == "createToken":
                authority = f_params[1].lower()
                initial_mint_address = f_params[2].lower()
                initial_mint_amount = int(f_params[3])
                symbol = f_params[4]
                name = ' '.join(f_params[5:])
                token_address = string_to_hex_with_prefix(f'{nonce}token{sender}').lower()[:42]

                with self.tokens_lock.gen_rlock():
                    if token_address in self.tokens:
                        if simulate: return False
                        raise Exception('Token address collision')
                if simulate: return True

                with self.tokens_lock.gen_wlock():
                    self.tokens[token_address] = {
                        "authority": authority,
                        "symbol": symbol,
                        "name": name,
                        "balances": {
                            initial_mint_address: initial_mint_amount
                        }
                    }
                self.save_tokens()

            elif f_id == "mintToken":
                token_address = f_params[1].lower()
                mint_address = f_params[2].lower()
                mint_amount = int(f_params[3])

                with self.tokens_lock.gen_rlock():
                    if self.tokens.get(token_address, {}).get("authority") != sender:
                        if simulate: return False
                        raise Exception('Not token authority')
                if simulate: return True

                with self.tokens_lock.gen_wlock():
                    self.tokens[token_address]["balances"][mint_address] = \
                        self.tokens[token_address]["balances"].get(mint_address, 0) + mint_amount
                self.save_tokens()

        return True

    def simulate_contract(self, tx):
        return self._process_contract_interaction(tx, simulate=True)

    def find_valid_parents(self, tx, parent_hashes=None):
        juice_needed = int(tx.amount + tx.gas)
        parentstotaljuice = 0
        parents = []

        if parent_hashes:
            for parent_hash in parent_hashes:
                if parentstotaljuice >= juice_needed:
                    break
                parent = self.get_tx_by_hash(parent_hash)
                if not parent:
                    continue
                if parent.recipient.lower() == tx.sender.lower() and parent.juice > 0:
                    parents.append(parent)
                    parentstotaljuice += parent.juice
        else:
            with self.dag_lock.gen_rlock(): nodes_snapshot = dict(self.dag.nodes)
            for ptxn, node in nodes_snapshot.items():
                ptx = node.get('transaction', None)
                if ptx and ptx.recipient.lower() == tx.sender.lower():
                    if juice_needed <= 0:
                        break
                    juice = int(ptx.juice)
                    if juice > 0:
                        parents.append(ptx)
                        juice_needed -= juice

            if juice_needed > 0:
                return None

        return parents

    def try_process_contract(self, tx):
        if tx.data and tx.data != '0x':
            try:
                self._process_contract_interaction(tx)
                return True
            except Exception as e:
                return False
        return True

    def finalize_and_broadcast(self, tx, parents, node_addr, miner_addr, miner_share): # broadcast to ws, not peers
        sender = tx.sender.lower()
        recipient = tx.recipient.lower()
        amount = int(tx.amount)
        fee = int(tx.gas)

        with self.dag_lock.gen_wlock():
            finalizedtx = self.dag.add_transaction(sender, recipient, amount, fee, parents, tx.nonce, tx.data)
            noderewardtx = self.dag.add_transaction(sender, node_addr, fee, 0, parents, finalizedtx.nonce + 1)
        nodenonce = self.get_transaction_count(NODE_ADDRESS, "latest")

        with self.dag_lock.gen_wlock():
            minerrewardtx = self.dag.add_transaction(node_addr, miner_addr, fee * miner_share, 0, [noderewardtx], nodenonce)

        self.last_speed = finalizedtx.timestamp - tx.timestamp
        self.remove_bad_nodes()
        self.save_dag()
        self.save_speed()

        for tx_ in [finalizedtx, noderewardtx, minerrewardtx]:
            self.ws.broadcast_tx(tx_)

        return finalizedtx, noderewardtx, minerrewardtx

    def handle_peer_voting(self, finalizedtx, parents, fee, node_addr, miner, miner_share):
        if len(self.p2p.peer_sockets) == 0:
            return
        with self.raw_txs_lock: raw_tx = self.raw_txs[finalizedtx.hash]
        response = self.p2p.broadcast({
            "raw_tx": raw_tx,
            "timestamp": finalizedtx.timestamp,
            "gas": finalizedtx.gas,
            "parent_hashes": [p.hash for p in parents],
            "node": node_addr,
            "miner": miner,
            "miner_share": miner_share,
        })
        agree = response.count(True) + 1 # include our own agree
        disagree = response.count(False)
        if disagree >= agree:
            pct = (disagree / (agree + disagree)) * 100
            print(f"{pct}% ({disagree}) disagree? Sounds like a Sybil skill issue. Or, we fcked up big time... But last I checked, we weren't a democracy. Welcome to the DAG, lil' tx {finalizedtx.hash}.")

    def submit_mined(self, mined_txs: dict, miner):
        if not mined_txs:
            return False, "No transactions submitted."
        try:
            n_valid = 0
            for tx_hash, parent_hashes in mined_txs.items():
                picked_entry = None
                with self.picked_lock.gen_rlock(): picked_entry = next((p for p in self.picked if p["tx"].hash == tx_hash), None)
                if not picked_entry:
                    continue # not in picked list

                tx = picked_entry["tx"]

                if not tx:
                    continue

                with self.funnel_cn: 
                    if any(f["tx"].hash == tx.hash for f in self.funnel): 
                        continue  # already in funnel

                parents = self.find_valid_parents(tx, parent_hashes)
                if not parents:
                    continue

                if not self.simulate_contract(tx):
                    continue

                with self.picked_lock.gen_wlock(): self.picked = [p for p in self.picked if p["tx"].hash != tx.hash]
                with self.funnel_cn:
                    self.funnel.append({
                        "tx": tx,
                        "parents": parents,
                        "miner": miner
                    })
                    self.funnel_cn.notify()

                n_valid += 1

            if n_valid:
                return True, f"{n_valid}/{len(mined_txs)} transactions successfully validated."
            else:
                return False, "None of the transactions were successfully validated."
        except Exception as e:
            print(traceback.format_exc())
            return False, str(e)

    def process_received(self, data):
        try:
            raw_tx = data["raw_tx"]
            decoded_tx = tx_decode(raw_tx)

            if int(decoded_tx['chainId']) != CHAIN_ID:
                return False

            if not verify_sign(decoded_tx):
                return False

            sender = decoded_tx['from_']
            recipient = decoded_tx['to']
            amount = int(decoded_tx['value'])
            gas = data["gas"]
            nonce = int(decoded_tx['nonce'])
            data_field = decoded_tx['data']
            if not data_field.startswith("0x"):
                data_field = "0x" + data_field

            # build tx object
            tx = Transaction(
                sender=sender,
                recipient=recipient,
                amount=amount,
                gas=gas,
                parents=[],
                nonce=nonce,
                data=data_field
            )

            if (int(data["timestamp"]) < self.last_received_timestamp_ns) or (int(data["timestamp"]) > time.time_ns()):
                return False
            tx.timestamp = int(data["timestamp"])

            # validations
            if amount < 0:
                return False
            if self.get_balance(sender, 'pending') < amount + gas:
                return False
            if decoded_tx['gas'] < gas or decoded_tx['gasPrice'] != 1:
                return False

            parent_hashes = data["parent_hashes"]
            node_addr = data["node"]
            miner = data["miner"]
            miner_share = data["miner_share"]

            with self.funnel_cn: 
                if any(f["tx"].hash == tx.hash for f in self.funnel):
                    return False  # already in funnel

            parents = self.find_valid_parents(tx, parent_hashes)
            if not parents:
                return False

            if not self.simulate_contract(tx):
                return False

            self.last_received_timestamp_ns = tx.timestamp
            with self.funnel_cn:
                self.funnel.append({
                    "tx": tx,
                    "parents": parents,
                    "miner": miner
                })
                self.funnel_cn.notify()
            return True

        except Exception:
            print(traceback.format_exc())
            return None

    def mine(self, tx):
        try:
            with self.funnel_cn: 
                if any(f["tx"].hash == tx.hash for f in self.funnel):
                    return False  # already in funnel

            with self.picked_lock.gen_rlock():
                if not any(p["tx"].hash == tx.hash for p in self.picked):
                    return False # not in picked list

            parents = self.find_valid_parents(tx)
            if not parents:
                return False

            if not self.simulate_contract(tx):
                with self.picked_lock.gen_wlock(): self.picked = [p for p in self.picked if p["tx"].hash != tx.hash] # drop
                return False

            with self.picked_lock.gen_wlock(): self.picked = [p for p in self.picked if p["tx"].hash != tx.hash]
            with self.funnel_cn:
                self.funnel.append({
                    "tx": tx,
                    "parents": parents,
                    "miner": NETWORK_MINER
                })
                self.funnel_cn.notify()

            return True

        except JuiceNotEnough:
            return False
        except Exception:
            print(traceback.format_exc())
            return False

    def mine_passive(self):
        def _mine_passive():
            active = False
            while True:
                time.sleep(0.1)  # breather

                if (time.time() - self.last_miner_request) <= 4:
                    if active:
                        print("Node-Miner Deactivated.")
                        active = False
                    continue
                else:
                    if not active:
                        print("Node-Miner Activated.")
                        active = True

                job = self.generate_job(node_miner=True)
                if job == 'NO_JOB':
                    continue
                for tx in job["transactions"]:
                    mined = self.mine(tx)

        self.passive_miner_thread = Thread(target=_mine_passive, daemon=True)
        self.passive_miner_thread.start()


    def compact_dag(self, fields):
        compacted = []
        with self.dag_lock.gen_rlock(): nodes_snapshot = dict(self.dag.nodes)
        for tx_hash, node in nodes_snapshot.items():
            tx = node.get('transaction', None)
            if not tx: continue # bad node in dag, somehow not cleaned up
            _tx_json = tx.__json__()
            tx_json = {}
            for field in fields:
                tx_json[field.lower()] = _tx_json.get(field.lower(), None)
            compacted.append(tx_json)
        return compacted

    def get_fee(self, tx = None):
        base_gas = 0.000069
        with self.mempool_lock.gen_rlock(): 
            if len(self.mempool) == 0:
                return base_gas * u
            first_tx_time = self.mempool[0].timestamp
            last_tx_time = self.mempool[-1].timestamp
        time_diff = (last_tx_time - first_tx_time) / 1_000_000_000  # Convert ns to seconds
        tps = (len(self.mempool) / time_diff) if time_diff > 0 else 0 # TPS (Transactions per second)

        boost_gas = base_gas * (tps / 10_000) # boost gas will be as much as base if tps is at 10000
        total_gas = base_gas + boost_gas
        total_gas = total_gas * u
        return total_gas

    def find_pending(self, sender, nonce):
        with self.mempool_lock.gen_rlock():
            for tx in self.mempool:
                if tx.sender.lower() == sender.lower() and int(tx.nonce) == int(nonce):
                    return tx
        return None

    def send_raw_transaction(self, raw_tx):
        try:
            tx_dict = tx_decode(raw_tx)

            if not int(tx_dict['chainId']) == CHAIN_ID:
                return {"error": f"Invalid transaction: chainId {tx_dict['chainId']} doesn't match {CHAIN_ID}."}

            if not verify_sign(tx_dict):
                return {'error': 'Invalid transaction. The signature verification failed, indicating the transaction was not signed by the sender. The reconstructed address does not match the provided address.'}

            sender = tx_dict['from_']
            recipient = tx_dict['to']
            amount = int(tx_dict['value'])
            fee = self.get_fee()
            nonce = int(tx_dict['nonce'])
            txcount = self.get_transaction_count(sender.lower(), "pending")
            data = tx_dict['data']
            if not data.startswith("0x"):
                data = "0x" + data

            if amount < 0:
                return {'error': 'Invalid amount. Amount must not be negative.'}

            if (tx_dict['gas'] < fee) or (tx_dict['gasPrice'] != 1):
                return {'error': f'Invalid gas values provided. Gas units required: {fee}, Gas Price: 1 wei'}

            if self.get_balance(sender, 'pending') < amount + fee:
                return {'error': f"Not enough balance."}

            to_replace = self.find_pending(sender, nonce)
            if to_replace:
                try:
                    with self.mempool_lock.gen_wlock(): self.mempool.remove(to_replace)
                    txh = self.add_pending(sender, recipient, amount, fee, nonce, data).hash
                    return {'transactionHash': txh}
                except ValueError:
                    return {'error': 'Transaction not found or already mined.'}

            if not nonce == txcount:
                return {'error': f'Invalid nonce provided: Given {nonce}, Expected {txcount}'}

            tokens_snapshot = self.tokens.copy()

            if recipient.lower() in tokens_snapshot:
                if len(data) != 138:
                    return {'error': 'Invalid data length. Must be 138 characters.'}
                method_id = data[:10]
                if method_id == "0xa9059cbb":
                    token_recipient = '0x' + data[10:74][-40:]
                    token_amount = int(data[74:], 16)
                    if tokens_snapshot[recipient.lower()]["balances"].get(sender.lower(), 0) < token_amount:
                        return {'error': f'Insufficient token balance.'}

            if recipient.lower() == TOKEN_CONTRACT.lower():
                f_params = hex_to_string(data).split(" ")
                f_id = f_params[0]
                if f_id == "createToken":
                    token_address = string_to_hex_with_prefix(f'{nonce}token{sender.lower()}').lower()[:42]
                    if token_address in tokens_snapshot:
                        return {'error': f'A token with the address {token_address} already exists.'}
                if f_id == "mintToken":
                    token_address = f_params[1].lower()
                    if token_address not in tokens_snapshot:
                        return {'error': f'Token address {token_address} not found.'}
                    if tokens_snapshot[token_address].get("authority") != sender.lower():
                        return {'error': f'Only the token authority can mint tokens.'}

            tx = self.add_pending(sender, recipient, amount, fee, nonce, data)
            txh = tx.hash
            with self.raw_txs_lock: self.raw_txs[txh] = raw_tx
            self.last_received_timestamp_ns = tx.timestamp
            return {'transactionHash': txh}

        except Exception as e:
            print(f"Error processing transaction: {traceback.format_exc()}")
            return {'error': 'Error 3469: Gas? Time? Luck? The universe isn’t sure, but something went sideways.'}

    def get_transaction_receipt(self, tx_hash):
        """Get the transaction receipt for a specific transaction."""
        tx = self.get_tx_by_hash(tx_hash)
        if tx == None:
            return {}
        return {
            'blockHash': tx.hash,
            'blockNumber': hex(list(self.dag.nodes).index(tx.hash)),
            'contractAddress': None,
            'cumulativeGasUsed': hex(tx.gas),
            'effectiveGasPrice': hex(1),
            'from': tx.sender,
            'gasUsed': hex(tx.gas),
            'status': hex(1),
            'to': tx.recipient,
            'transactionHash': tx.hash,
            'transactionIndex': hex(0), # since we dont have blocks, 1 tx = 1 tx = 1 block for evm compatibility
            'type': 0,
            'amount': hex(tx.amount),
            'juiceLeft': hex(tx.juice),
            'logs': [],
        }

    def load_dag(self):
        """Load the DAG object from a pickle file."""
        try:
            with open('dag', 'rb') as f:
                self.dag = pickle.load(f)
        except Exception as e:
            print(traceback.format_exc())
            sys.exit(1)

    def save_dag(self):
        """Save the DAG object to a file using pickle."""
        try:
            with self.dag_lock.gen_rlock():
                dag_snapshot = deepcopy(self.dag)
            with open('dag', 'wb') as f:
                pickle.dump(dag_snapshot, f)
        except Exception as e:
            print(traceback.format_exc())

    def save_speed(self):
        try:
            with open('data.json', 'r') as f:
                _data = json.load(f)
            _data["last_speed_ns"] = self.last_speed
            with open('data.json', 'w') as f:
                json.dump(_data, f, indent=2)
        except Exception as e:
            print(traceback.format_exc())

    def load_speed(self):
        try:
            with open('data.json', 'r') as f:
                _data = json.load(f)
            return _data["last_speed_ns"]
        except Exception as e:
            print(traceback.format_exc())


    def save_tokens(self):
        try:
            with self.tokens_lock.gen_rlock():
                tokens_snapshot = deepcopy(self.tokens)
            with open('tokens.json', 'w') as f:
                json.dump(tokens_snapshot, f, indent=2)
        except Exception as e:
            print(traceback.format_exc())

    def load_tokens(self):
        try:
            with open('tokens.json', 'r') as f:
                self.tokens = json.load(f)
        except Exception as e:
            print(traceback.format_exc())
