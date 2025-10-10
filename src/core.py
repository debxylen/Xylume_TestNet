from revert_exceptions import *
from utils import *
# from tx_sign import *
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
## - Remove preflights from most places except eth_sRT and process_received [entry point of transactions]
## - Transactions failed during runtime should be included in the DAG, also spend gas.
## - Indexing, etc for faster operations, eliminating O(n)+ times.
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
            self.genesis = Transaction(sender=NULL_ADDRESS, recipient=MINT_ADDRESS, amount=INITIAL_SUPPLY, gas=0, parents=[], nonce=0, data="0x", status=1, logs=[])
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

    def add_pending(self, sender, recipient, amount: int, gas, nonce = None, data = "0x"):
        """Add a new transaction to the mempool."""
        sender, recipient = sender.lower(), recipient.lower()
        amount, gas = int(amount), int(gas)
        if nonce == None:
            nonce = self.get_transaction_count(sender)
        transaction = Transaction(sender, recipient, amount, gas, [], nonce, data, 1, []) # pending tx, diff from normal ones
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
            and tx.status == 1
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
        with self.funnel_cn:
            funnel_snapshot = list(self.funnel)
        for _tx in funnel_snapshot:
            tx = _tx["tx"]
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
            if (tx := node.get("transaction")) and tx.sender.lower() == sender_address: # failed transactions also add to nonce
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
        with self.funnel_cn:
            funnel_snapshot = list(self.funnel)
        for _tx in funnel_snapshot:
            tx = _tx["tx"]
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

    def start_retry(self): # retries transactions unmined for >= 2s
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

                    # send to funnel
                    with self.funnel_cn:
                        self.funnel.append({
                            "tx": tx,
                            "parents": parents,
                            "miner": NETWORK_MINER,
                            "source": "self",
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

                if (tx_entry["source"] != "peer") and (finalizedtx, noderewardtx, minerrewardtx):
                    self.handle_peer_voting(finalizedtx, parents, tx.gas, NODE_ADDRESS, miner, MINER_FEE_SHARE)

        Thread(target=_funnel, daemon=True).start()

    def process_contract(self, tx, simulate=False, return_message=False, return_revert_fn=False):
        """simulate = False -> bool
               True = Successful execution
               False = Execution failure
           simulate = True -> bool
               True = Successful preflight
               False = Deterministic/static errors
               None = Runtime/state-dependent errors
        """
        logs = []
        written = False # whether self.tokens was modified
        changes = []
        try:
            if isinstance(tx, dict):
                sender = tx["sender"].lower()
                recipient = tx["recipient"].lower()
                nonce = tx["nonce"]
                data = tx["data"]
            else:
                sender = tx.sender.lower()
                recipient = tx.recipient.lower()
                nonce = tx.nonce
                data = tx.data

            if (not data.strip()) or (data == '0x'):
                return (True, logs, blank) if return_message else True
            
            with self.tokens_lock.gen_rlock():
                token_match = recipient in self.tokens

            if token_match:
                if len(data) != 138:
                    logs.append('Invalid data length')
                    if simulate: return (False, logs) if return_message else False
                    raise ExecutionError('Invalid data length')

                method_id = data[:10]
                if method_id == "0xa9059cbb":  # transfer(address,uint256)
                    try:
                        token_recipient = '0x' + data[10:74][-40:]
                        token_amount = int(data[74:], 16)
                    except Exception as e:
                        logs.append('Invalid parameters')
                        if simulate: return (False, logs) if return_message else False
                        raise ExecutionError('Invalid parameters')

                    with self.tokens_lock.gen_rlock():
                        sender_balance = self.tokens[recipient]["balances"].get(sender, 0)

                    if sender_balance < token_amount:
                        logs.append('Insufficient token balance')
                        if simulate: return (None, logs) if return_message else None
                        raise ExecutionError('Insufficient token balance.')

                    if not simulate:
                        with self.tokens_lock.gen_wlock():
                            self.tokens[recipient]["balances"][sender] -= token_amount
                            self.tokens[recipient]["balances"][token_recipient] = self.tokens[recipient]["balances"].get(token_recipient, 0) + token_amount
                        written = True
                        changes.append(("sub_token", recipient, sender, token_amount))
                        changes.append(("add_token", recipient, token_recipient, token_amount))
                    
                else:
                    logs.append(f'Unknown function {method_id}')
                    if simulate: return (None, logs) if return_message else None
                    raise ExecutionError(f'Unknown function {method_id}')

            elif recipient == TOKEN_CONTRACT.lower():
                try:
                    f_params = hex_to_string(data).split(" ")
                    f_id = f_params[0]
                except Exception as e:
                    logs.append('Malformed data')
                    if simulate: return (False, logs) if return_message else False
                    raise ExecutionError('Malformed data')

                if f_id == "createToken":
                    try:
                        if len(f_params) < 6:
                            logs.append('Invalid data length')
                            if simulate: return (False, logs) if return_message else False
                            raise ExecutionError('Invalid data length')
                        authority = f_params[1].lower()
                        initial_mint_address = f_params[2].lower()
                        initial_mint_amount = int(f_params[3])
                        symbol = f_params[4]
                        name = ' '.join(f_params[5:])
                    except Exception as e:
                        logs.append('Invalid parameters')
                        if simulate: return (False, logs) if return_message else False
                        raise ExecutionError('Invalid parameters')
                    token_address = string_to_hex_with_prefix(f'{nonce}token{sender}').lower()[:42]

                    with self.tokens_lock.gen_rlock():
                        if token_address in self.tokens:
                            logs.append('Token address collision')
                            if simulate: return (False, logs) if return_message else False
                            # deterministic, because token addresses are immutable
                            raise ExecutionError('Token address collision')

                    if not simulate:
                        with self.tokens_lock.gen_wlock():
                            self.tokens[token_address] = {
                                "authority": authority,
                                "symbol": symbol,
                                "name": name,
                                "balances": {
                                    initial_mint_address: initial_mint_amount
                                }
                            }
                        written = True
                        changes.append(("create_token", token_address))

                elif f_id == "mintToken":
                    try:
                        if len(f_params) < 4:
                            logs.append('Invalid data length')
                            if simulate: return (False, logs) if return_message else False
                            raise ExecutionError('Invalid data length')
                        token_address = f_params[1].lower()
                        mint_address = f_params[2].lower()
                        mint_amount = int(f_params[3])
                    except Exception as e:
                        logs.append('Invalid parameters')
                        if simulate: return (False, logs) if return_message else False
                        raise ExecutionError('Invalid parameters')

                    with self.tokens_lock.gen_rlock():
                        token = self.tokens.get(token_address)
                        if not token:
                            logs.append('Token not found')
                            if simulate: return (None, logs) if return_message else None
                            raise ExecutionError('Token not found')
                        if token.get("authority") != sender:
                            logs.append('Not token authority')
                            if simulate: return (None, logs) if return_message else None
                            raise ExecutionError('Not token authority')

                    if not simulate:
                        with self.tokens_lock.gen_wlock():
                            self.tokens[token_address]["balances"][mint_address] = self.tokens[token_address]["balances"].get(mint_address, 0) + mint_amount
                        written = True
                        changes.append(("add_token", token_address, mint_address, mint_amount))

                else:
                    logs.append(f'Unknown function {f_id}')
                    if simulate: return (None, logs) if return_message else None
                    raise ExecutionError(f'Unknown function {f_id}')
                
            else:
                # call to a non-contract
                pass

            if written and not simulate:
                self.save_tokens()
            if return_revert_fn and written:
                def revert_fn():
                    for op in reversed(changes):
                        if op[0] == "add_token":
                            self.tokens[op[1]]["balances"][op[2]] -= op[3]
                        elif op[0] == "sub_token":
                            self.tokens[op[1]]["balances"][op[2]] += op[3]
                        elif op[0] == "create_token":
                            del self.tokens[op[1]]
                return True, logs, revert_fn
            return (True, logs, blank) if return_message else True
        except ExecutionError as e:
            # rollback
            for op in reversed(changes):
                if op[0] == "add_token":
                    self.tokens[op[1]]["balances"][op[2]] -= op[3]
                elif op[0] == "sub_token":
                    self.tokens[op[1]]["balances"][op[2]] += op[3]
                elif op[0] == "create_token":
                    del self.tokens[op[1]]
            logs.append('Transaction reverted due to ExecutionError.')
            return (False, logs, blank) if return_message else False
        except Exception as e:
            print(traceback.format_exc())
            # rollback
            for op in reversed(changes):
                if op[0] == "add_token":
                    self.tokens[op[1]]["balances"][op[2]] -= op[3]
                elif op[0] == "sub_token":
                    self.tokens[op[1]]["balances"][op[2]] += op[3]
                elif op[0] == "create_token":
                    del self.tokens[op[1]]
            logs.append('Transaction reverted due to an unexpected error.')
            logs.append(f"{e.__class__.__name__}: {e}")
            return (False, logs, blank) if return_message else False

    def find_valid_parents(self, tx, parent_hashes=None):
        juice_needed = int(tx.amount + tx.gas)
        parentstotaljuice = 0
        parents = []

        if parent_hashes:
            for parent_hash in parent_hashes:
                if parentstotaljuice >= juice_needed:
                    break
                parent = self.get_tx_by_hash(parent_hash)
                if parent and parent.recipient.lower() == tx.sender.lower() and parent.juice > 0 and parent.status == 1:
                    parents.append(parent)
                    parentstotaljuice += parent.juice
        else:
            with self.dag_lock.gen_rlock(): nodes_snapshot = dict(self.dag.nodes)
            for ptxn, node in nodes_snapshot.items():
                ptx = node.get('transaction', None)
                if ptx and ptx.recipient.lower() == tx.sender.lower() and ptx.status == 1:
                    if juice_needed <= 0:
                        break
                    juice = int(ptx.juice)
                    if juice > 0:
                        parents.append(ptx)
                        juice_needed -= juice

            if juice_needed > 0:
                return None

        return parents

    def finalize_and_broadcast(self, tx, parents, node_addr, miner_addr, miner_share):
        try:
            sender = tx.sender.lower()
            recipient = tx.recipient.lower()
            amount = int(tx.amount)
            fee = int(tx.gas)

            # Process contract
            contract_interaction = self.process_contract(tx, simulate=False, return_message=True, return_revert_fn=True)
            result, logs, revert = contract_interaction

            # Determine finalized tx status
            status = 1 if result else 0
            fee_status = 1
            
            # Determine total juice available from parents
            full_deducts, partial_parent, partial_amount, total_found_juice = self.dag.select_parents(parents, amount + fee)
            
            # If not enough juice for amount, finalized tx fails
            if total_found_juice < (amount + fee):
                status = 0
                if total_found_juice < fee:
                    fee_status = 0

            nodenonce = self.get_transaction_count(NODE_ADDRESS, "latest")
            finalizedtx = noderewardtx = minerrewardtx = None
            final_parents = full_deducts + ([partial_parent] if partial_parent else [])

            with self.dag_lock.gen_wlock():
                # Add finalized tx (may fail, status=0)
                if status == 0:
                    finalizedtx = self.dag.add_transaction(
                        sender, recipient, amount, fee, [], tx.nonce, tx.data, status=status, logs=logs,
                    )
                else:
                    finalizedtx = self.dag.add_transaction(
                        sender, recipient, amount, fee, final_parents, tx.nonce, tx.data, status=status, logs=logs,
                        check_juice=False, full_deducts=full_deducts, partial_parent=partial_parent, partial_amount=partial_amount
                    )

                if fee_status == 0:
                    noderewardtx = self.dag.add_transaction(
                        sender, node_addr, fee, 0, [], finalizedtx.nonce + 1, data="0x", status=fee_status,
                        logs=[f'Node reward transaction for {finalizedtx.hash}']
                    )

                    minerrewardtx = self.dag.add_transaction(
                        node_addr, miner_addr, fee * miner_share, 0, [], nodenonce, data="0x", status=fee_status,
                        logs=[f'Miner reward transaction for {finalizedtx.hash}']
                    )
                else:
                    noderewardtx = self.dag.add_transaction(
                        sender, node_addr, fee, 0, final_parents, finalizedtx.nonce + 1, data="0x", status=fee_status,
                        logs=[f'Node reward transaction for {finalizedtx.hash}'],
                    )

                    minerrewardtx = self.dag.add_transaction(
                        node_addr, miner_addr, fee * miner_share, 0, [noderewardtx], nodenonce, data="0x", status=fee_status,
                        logs=[f'Miner reward transaction for {finalizedtx.hash}'],
                    )

            self.last_speed = finalizedtx.timestamp - tx.timestamp
            self.remove_bad_nodes()
            self.save_dag()
            self.save_speed()

            # Broadcast whatever txs were successfully added
            for tx_ in [finalizedtx, noderewardtx, minerrewardtx]:
                if tx_:
                    self.ws.broadcast_tx(tx_)

            return finalizedtx, noderewardtx, minerrewardtx
        except:
            print(traceback.format_exc())
            return None, None, None

    def handle_peer_voting(self, finalizedtx, parents, fee, node_addr, miner, miner_share):
        if len(self.p2p._peers) == 0:
            return
        with self.raw_txs_lock: raw_tx = self.raw_txs.get(finalizedtx.hash)
        if raw_tx == None:
            print(f"Raw TX for {finalizedtx.hash} seems lost... :(")
            raw_tx = 'lost'
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

                with self.picked_lock.gen_wlock(): self.picked = [p for p in self.picked if p["tx"].hash != tx.hash]
                with self.funnel_cn:
                    self.funnel.append({
                        "tx": tx,
                        "parents": parents,
                        "miner": miner,
                        "source": "self",
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
            if raw_tx == 'lost':
                ## tf do i even do now??
                return False
            decoded_tx = tx_decode(raw_tx)

            if int(decoded_tx['chainId']) != CHAIN_ID:
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
                data=data_field,
                status=1,
                logs=[],
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

            # transaction hasnt come through send_raw_transaction (which already has a preflight),
            # but through process_received (from other nodes, may or may not have preflighted)
            if self.process_contract(tx, simulate=True) == False:
                return False

            self.last_received_timestamp_ns = tx.timestamp
            with self.funnel_cn:
                self.funnel.append({
                    "tx": tx,
                    "parents": parents,
                    "miner": miner,
                    "source": "peer",
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

            with self.picked_lock.gen_wlock(): self.picked = [p for p in self.picked if p["tx"].hash != tx.hash]
            with self.funnel_cn:
                self.funnel.append({
                    "tx": tx,
                    "parents": parents,
                    "miner": NETWORK_MINER,
                    "source": "self",
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

    def send_raw_transaction(self, raw_tx, skip_preflight: bool = True):
        try:
            try:
                tx_dict = tx_decode(raw_tx)
            except rlp.exceptions.DecodingError:
                return {"error": f"Unsupported raw transaction: The raw transaction could not be decoded."}

            if not int(tx_dict['chainId']) == CHAIN_ID:
                return {"error": f"Invalid transaction: chainId {tx_dict['chainId']} doesn't match {CHAIN_ID}."}

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

            if (tx_dict['gas'] < fee):
                return {'error': f'Invalid gas values provided. Minimum gas units required: {fee}'}

            if self.get_balance(sender, 'pending') < amount + fee:
                return {'error': f"Not enough balance."}

            simulation = self.process_contract({
                            "sender": sender, 
                            "recipient": recipient, 
                            "amount": amount, 
                            "fee": fee, 
                            "nonce": nonce, 
                            "data": data
                        }, simulate=True, return_message=True)
            
            if skip_preflight: # only drops on deterministic errors
                if simulation[0] == False:
                    return {"error": f"Transaction rejected: {simulation[1]}"}
            else:
                if not simulation[0]:
                    return {"error": f"Preflight failed: {simulation[1]}"}

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
            'status': hex(tx.status),
            'to': tx.recipient,
            'transactionHash': tx.hash,
            'transactionIndex': "0x0", # since we dont have blocks, 1 tx = 1 tx = 1 block for evm compatibility
            'type': "0x0",
            'value': hex(tx.amount),
            'juiceLeft': hex(tx.juice),
            'logs': tx.logs,
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
