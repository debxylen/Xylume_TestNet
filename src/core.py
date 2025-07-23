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
from threading import Thread
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

class Core:
    def __init__(self, p2p, ws):
        if os.path.isfile('dag'):
            self.load_dag()
            self.genesis = self.dag.nodes[list(self.dag.nodes)[0]]['transaction']
        else:
            self.genesis = Transaction(sender=NULL_ADDRESS, recipient=MINT_ADDRESS, amount=INITIAL_SUPPLY, gas=0, parents=[])
            self.dag = DAG(self.genesis)
            self.save_dag()
        if os.path.isfile('tokens.json'):
            self.load_tokens()
        else:
            self.tokens = {}
            self.save_tokens()
        self.mempool = []
        self.p2p = p2p
        self.p2p.process_received = self.process_received
        self.ws = ws
        self.last_speed = self.load_speed() # in nanoseconds
        self.last_miner_request = 0
        self.mine_passive() # start passive mining thread
        self.ws.start()

    def add_pending(self, sender, recipient, amount: int, gas, nonce = None, data = '0x'):
        """Add a new transaction to the mempool."""
        sender, recipient = sender.lower(), recipient.lower()
        amount, gas = int(amount), int(gas)
        if nonce == None:
            nonce = self.get_transaction_count(sender)
        transaction = Transaction(sender, recipient, amount, gas, [], nonce, data) # pending tx, diff from normal ones
        self.mempool.append(transaction)
        return transaction

    def remove_bad_nodes(self):
        for i in list(self.dag.nodes):
            if not self.dag.nodes[i].get('transaction'):
                self.dag.remove_node(i)

    def get_balance(self, address, mode = 'latest'):
        address = address.lower()
        confirmed_balance = sum(
            tx.juice for node in self.dag.nodes.values()
            if (tx := node.get("transaction"))
            and tx.recipient == address
            and tx.juice > 0
        )
        if mode == 'latest': return confirmed_balance

        mempool_balance = (
            sum(tx.amount for tx in self.mempool if tx.recipient == address)
            - sum(tx.amount + tx.gas for tx in self.mempool if tx.sender == address)
        )
        return confirmed_balance + mempool_balance

    def get_transaction_count(self, sender_address, mode = 'latest'):
        """Counts the number of transactions in the DAG with a specific sender address."""
        count = 0
        for tx_hash in self.dag.nodes:
            transaction = self.get_tx_by_hash(tx_hash)
            if transaction:
                if transaction.sender == sender_address.lower():
                    count += 1
        if mode.lower() == 'latest': return count # stop, dont count pending if mode is latest
        for pending_tx in self.mempool:
            if pending_tx.sender == sender_address.lower():
                count += 1
        return count

    def get_tx_by_number(self, n: int):
        return self.dag.nodes[list(self.dag.nodes)[n]]['transaction'] if len(self.dag.nodes) > n else None

    def get_tx_by_hash(self, tx_hash):
        if not tx_hash in list(self.dag.nodes): return None
        txnode = self.dag.nodes[tx_hash]
        return txnode.get('transaction', None)

    def generate_job(self):
        """Generate a new mining job."""
        self.last_miner_request = time.time()
        if len(self.mempool) == 0:
            return 'NO_JOB'
        transaction_to_mine = self.mempool[0] # 1 tx max per job
        job = {"transactions": [transaction_to_mine.__json__()]}
        return job

    def retry_picked_loop(self):
        def _retry():
            while True:
                time.sleep(0.5)
                now = time.time()
                expired = []

                for txh, meta in list(self.picked.items()):
                    if now - meta["timestamp"] >= 2:  # timeout in seconds
                        expired.append(txh)

                for txh in expired:
                    try:
                        self.mempool.append(self.picked[txh]["tx"])
                    except Exception as e:
                        print(f"Error re-adding tx {txh}: {e}")
                    del self.picked[txh]
                    print(f"⏱️ Re-added expired tx {txh} back to mempool.")

        Thread(target=_retry, daemon=True).start()

    def _process_contract_interaction(self, tx):
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

        if recipient in self.tokens:
            if len(data) != 138:
                raise Exception('Invalid data length in token tx.')
            method_id = data[:10]
            if method_id == "0xa9059cbb":
                token_recipient = '0x' + data[10:74][-40:]
                token_amount = int(data[74:], 16)
                if self.tokens[recipient]["balances"].get(sender, 0) < token_amount:
                    raise Exception('Insufficient token balance.')
                self.tokens[recipient]["balances"][sender] -= token_amount
                self.tokens[recipient]["balances"][token_recipient] = self.tokens[recipient]["balances"].get(token_recipient, 0) + token_amount
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
                if token_address in self.tokens:
                    raise Exception('Token address collision')
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
                if self.tokens.get(token_address, {}).get("authority") != sender:
                    raise Exception('Not token authority')
                self.tokens[token_address]["balances"][mint_address] = self.tokens[token_address]["balances"].get(mint_address, 0) + mint_amount
                self.save_tokens()

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
                    return None
                if parent.recipient.lower() == tx.sender.lower() and parent.juice > 0:
                    parents.append(parent)
                    parentstotaljuice += parent.juice
                else:
                    return None
        else:
            for ptxn in self.dag.nodes:
                ptx = self.dag.nodes[ptxn].get('transaction', None)
                if ptx and ptx.recipient.lower() == tx.sender.lower():
                    juice = int(ptx.juice)
                    if juice_needed <= 0:
                        break
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

        finalizedtx = self.dag.add_transaction(sender, recipient, amount, fee, parents, tx.nonce, tx.data)
        noderewardtx = self.dag.add_transaction(sender, node_addr, fee, 0, parents, finalizedtx.nonce + 1)
        minerrewardtx = self.dag.add_transaction(node_addr, miner_addr, fee * miner_share, 0, [noderewardtx], self.get_transaction_count(NODE_ADDRESS, "latest"))

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
        response = self.p2p.broadcast({
            "tx": finalizedtx.__json__(),
            "parent_hashes": [p.hash for p in parents],
            "node": node_addr,
            "miner": miner,
            "miner_share": miner_share
        })
        agree = response.count(True)
        disagree = response.count(False)
        if disagree > agree:
            pct = (disagree / (agree + disagree)) * 100
            print(f"{pct}% ({disagree}) disagree? Sounds like a Sybil skill issue... Welcome to the DAG, lil' tx {finalizedtx.hash}.")

    def submit_mined(self, mined_txs: dict, miner):
        try:
            reward_txs = []
            for tx_hash, parent_hashes in mined_txs.items():
                tx = next((t for t in self.mempool if t.hash == tx_hash), None)
                if not tx:
                    continue

                parents = self.find_valid_parents(tx, parent_hashes)
                if not parents:
                    continue

                if tx.nonce != self.get_transaction_count(tx.sender, "latest"):
                    continue

                if not self.try_process_contract(tx):
                    continue

                try:
                    self.mempool.remove(tx)
                except ValueError:
                    pass

                finalizedtx, noderewardtx, minerrewardtx = self.finalize_and_broadcast(
                    tx, parents, NODE_ADDRESS, miner, MINER_FEE_SHARE
                )

                self.handle_peer_voting(finalizedtx, parents, tx.gas, NODE_ADDRESS, miner, MINER_FEE_SHARE)

                reward_txs.append(minerrewardtx)

            return True, reward_txs

        except Exception as e:
            print(traceback.format_exc())
            return False, str(e)

    def process_received(self, data):
        try:
            tx_json = data["tx"]
            parent_hashes = data["parent_hashes"]
            node_addr = data["node"]
            miner = data["miner"]
            miner_share = data["miner_share"]

            tx = Transaction(
                sender=tx_json["sender"],
                recipient=tx_json["recipient"],
                amount=tx_json["amount"],
                gas=tx_json["gas"],
                parents=[],
                nonce=tx_json["nonce"],
                data=tx_json.get("data", "0x")
            )
            tx.timestamp = int(tx_json.get("timestamp", time.time())) * 1_000_000 # convert seconds to ns, as Transaction(...) uses ns, json uses s
            parents = self.find_valid_parents(tx, parent_hashes)
            if not parents:
                return False

            if not self.try_process_contract(tx):
                return False

            finalizedtx, noderewardtx, minerrewardtx = self.finalize_and_broadcast(
                tx, parents, node_addr, miner, miner_share
            )

            return True

        except Exception:
            print(traceback.format_exc())
            return None


    def mine(self, tx):
        try:
            parents = self.find_valid_parents(tx)
            if not parents:
                return False

            expected_nonce = self.get_transaction_count(tx.sender, "latest")
            if tx.nonce != expected_nonce:
                return False

            if not self.try_process_contract(tx):
                return 'drop'

            finalizedtx, noderewardtx, minerrewardtx = self.finalize_and_broadcast(
                tx, parents, NODE_ADDRESS, NETWORK_MINER, MINER_FEE_SHARE
            )

            self.handle_peer_voting(finalizedtx, parents, tx.gas, NODE_ADDRESS, NETWORK_MINER, MINER_FEE_SHARE)
            return True

        except JuiceNotEnough:
            return False
        except Exception:
            print(traceback.format_exc())
            return False

    def compact_dag(self, fields):
        compacted = []
        for tx_hash in self.dag.nodes:
            tx = self.dag.nodes[tx_hash].get('transaction', None)
            if not tx: continue # bad node in dag, somehow not cleaned up
            _tx_json = tx.__json__()
            tx_json = {}
            for field in fields:
                tx_json[field.lower()] = _tx_json.get(field.lower(), None)
            compacted.append(tx_json)
        return compacted

    def get_fee(self, tx = None):
        base_gas = 0.000069
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
        for tx in self.mempool:
            if tx.sender.lower() == sender.lower() and int(tx.nonce) == int(nonce):
                return tx
        return None

    def mine_passive(self):
        def _mine_passive():
            active = False
            while True:
                time.sleep(0.1)  # avoid oofing cpu
                if (time.time() - self.last_miner_request) <= 4:
                    if active:
                        print("Network Miner Deactivated.")
                        active = False
                    continue
                else:
                    if not active:
                        print("Network Miner Activated.")
                        active = True

                for tx in list(self.mempool):
                    mined = self.mine(tx)
                    if mined in [True, 'drop']:
                        try:
                            self.mempool.remove(tx)
                        except:
                            pass  # already gone?!
                    else:
                        continue

        self.passive_miner_thread = Thread(target=_mine_passive, daemon=True)
        self.passive_miner_thread.start()

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
                    self.mempool.remove(to_replace)
                    txh = self.add_pending(sender, recipient, amount, fee, nonce, data).hash
                    return {'transactionHash': txh}
                except ValueError:
                    return {'error': 'Transaction not found or already mined.'}

            if not nonce == txcount:
                return {'error': f'Invalid nonce provided: Given {nonce}, Expected {txcount}'}

            if recipient.lower() in self.tokens:
                if len(data) != 138:
                    return {'error': 'Invalid data length. Must be 138 characters.'}
                method_id = data[:10]
                if method_id == "0xa9059cbb":
                    token_recipient = '0x' + data[10:74][-40:]
                    token_amount = int(data[74:], 16)
                    if self.tokens[recipient.lower()]["balances"].get(sender.lower(), 0) < token_amount:
                        return {'error': f'Insufficient token balance.'}

            if recipient.lower() == TOKEN_CONTRACT.lower():
                f_params = hex_to_string(data).split(" ")
                f_id = f_params[0]
                if f_id == "createToken":
                    token_address = string_to_hex_with_prefix(f'{nonce}token{sender.lower()}').lower()[:42]
                    if token_address in self.tokens:
                        return {'error': f'A token with the address {token_address} already exists.'}
                if f_id == "mintToken":
                    token_address = f_params[1].lower()
                    if token_address not in self.tokens:
                        return {'error': f'Token address {token_address} not found.'}
                    if self.tokens[token_address]["authority"] != sender.lower():
                        return {'error': f'Only the token authority can mint tokens.'}

            txh = self.add_pending(sender, recipient, amount, fee, nonce, data).hash
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
            with open('dag', 'wb') as f:
                pickle.dump(self.dag, f)
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
            with open('tokens.json', 'w') as f:
                json.dump(self.tokens, f, indent=2)
        except Exception as e:
            print(traceback.format_exc())

    def load_tokens(self):
        try:
            with open('tokens.json', 'r') as f:
                self.tokens = json.load(f)
        except Exception as e:
            print(traceback.format_exc())
