from utils import *
from tx_sign import *
from tx_decode import *
from dag import *
import pyfiglet
import traceback
import pickle
import os
import sys
import json
import time
from threading import Thread
from dotenv import load_dotenv
load_dotenv()

with open("config.json", "r") as f:
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
    def __init__(self, p2p):
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
        self.last_speed = self.load_speed() # in nanoseconds
        self.last_miner_request = 0
        self.mine_passive() # start passive mining thread
        os.system("cls" if os.name == "nt" else "clear")
        print(pyfiglet.figlet_format("Xylume TestNet", font="doom"))

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

    def get_balance(self, address):
        address = address.lower()
        balance = 0
        for i in self.dag.nodes:
            node = self.dag.nodes[i].get('transaction')
            if not node: continue
            if address.lower() == node.sender: balance -= node.amount
            if address.lower() == node.recipient: balance += node.amount
        return balance

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
        transactions_to_mine = self.mempool[:1] # 1 tx max per job
        job = {"transactions": [pendingtx.__json__() for pendingtx in transactions_to_mine]}
        return job

    def _process_contract_interaction(self, tx):
        sender = tx.sender.lower()
        recipient = tx.recipient.lower()
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
                token_address = string_to_hex_with_prefix(f'{tx.nonce}token{sender}').lower()[:42]
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

    def submit_mined(self, mined_txs: dict, miner):
        try:
            txs = []
            for tx_hash, parent_hashes in mined_txs.items():
                mined_txs[tx_hash] = set(parent_hashes)
                tx = next((tx for tx in self.mempool if tx.hash == tx_hash), None)
                if not tx:
                    return False, f"Error: Transaction {tx_hash} not found in mempool."
                if not mined_txs[tx_hash].issubset(self.dag.nodes):
                    return False, f"Error: A parent transaction was not found in DAG."
                txs.append(tx)

            txs.sort(key=lambda tx: [tx.hash for tx in self.mempool].index(tx.hash))

            noderewardtxs = []
            minerrewardtxs = []
            for tx in txs:
                reject_job = False
                parent_hashes = mined_txs[tx.hash]
                sender = tx.sender.lower()
                recipient = tx.recipient.lower()
                amount = int(tx.amount)
                fee = tx.gas

                try:
                    self.mempool.remove(tx)
                    parentstotaljuice = 0
                    parents = []

                    for parent_hash in parent_hashes:
                        if parentstotaljuice >= amount + fee:
                            break
                        parent = self.get_tx_by_hash(parent_hash)
                        if not parent or parent.recipient.lower() != sender or parent.juice <= 0:
                            reject_job = True
                            break
                        parents.append(parent)
                        parentstotaljuice += parent.juice

                    if reject_job:
                        raise RejectJob('Rejected Job.')

                except ValueError:
                    continue
                except RejectJob:
                    continue

                expected_nonce = self.get_transaction_count(sender, "latest")
                if not tx.nonce == expected_nonce:
                    continue

                if parentstotaljuice >= amount + fee:
                    try:
                        # Execute contract logic
                        if tx.data and (tx.data != '0x'):
                            try:
                                self._process_contract_interaction(tx)
                            except Exception as e:
                                print(f"Contract execution failed during mining: {e}")
                                continue  # skip tx

                        finalizedtx = self.dag.add_transaction(sender, recipient, amount, fee, parents, tx.nonce, tx.data)
                        noderewardtx = self.dag.add_transaction(sender, NODE_ADDRESS.lower(), fee, 0, parents, finalizedtx.nonce + 1)
                        noderewardtxs.append(noderewardtx)
                        minerrewardtx = self.dag.add_transaction(NODE_ADDRESS.lower(), miner.lower(), fee * MINER_FEE_SHARE, 0, [noderewardtx], self.get_transaction_count(NODE_ADDRESS, "latest"))
                        minerrewardtxs.append(minerrewardtx)

                        self.last_speed = finalizedtx.timestamp - tx.timestamp
                        self.remove_bad_nodes()
                        self.save_dag()
                        self.save_speed()

                        if len(self.p2p.peer_sockets) > 0:
                            nodesresponse = self.p2p.broadcast({"tx": tx, "parent_hashes": parent_hashes, "fee": fee, "miner": miner, "miner_share": MINER_FEE_SHARE})
                            agree = nodesresponse.count(True)
                            disagree = nodesresponse.count(False)
                            totalvotes = agree + disagree
                            if disagree > agree:
                                disagree_percent = (disagree / totalvotes) * 100
                                print(f"{disagree_percent}% ({disagree}) disagree? Sounds like a Sybil skill issue. Or, we fcked up big time... But last I checked, we weren't a democracy. Welcome to the DAG, lil' tx {finalizedtx.hash}.")

                    except JuiceNotEnough:
                        continue
                    except Exception:
                        print(traceback.format_exc())
                        continue

            return True, minerrewardtxs
        except Exception:
            print(traceback.format_exc())
            return False, str(e)

    def process_received(self, data):  # process validated txs received from other nodes
        try:
            tx, parent_hashes, fee, miner, miner_share = data["tx"], data["parent_hashes"], data["fee"], data["miner"], data["miner_share"]
            sender = tx["sender"].lower()  # tx will be in json as object cant be sent over network
            recipient = tx["recipient"].lower()
            amount = int(tx["amount"])

            parentstotaljuice = 0
            parents = []
            reject_job = False

            for parent_hash in parent_hashes:
                if parentstotaljuice >= amount + fee:
                    break
                parent = self.get_tx_by_hash(parent_hash)
                if not parent:
                    reject_job = True
                    break
                if parent.recipient.lower() == sender and parent.juice > 0:
                    parents.append(parent)
                    parentstotaljuice += parent.juice
                else:
                    reject_job = True
                    break

            if reject_job:
                return False

            if parentstotaljuice >= amount + fee:
                try:
                    # ⬇️ Process token/contract logic from peer-mined tx
                    if tx["data"]:
                        self._process_contract_interaction(tx)

                    finalizedtx = self.dag.add_transaction(sender, recipient, amount, parents, tx["nonce"])
                    noderewardtx = self.dag.add_transaction(sender, NODE_ADDRESS.lower(), fee, parents, finalizedtx.nonce + 1)
                    minerrewardtx = self.dag.add_transaction(NODE_ADDRESS.lower(), miner.lower(), fee * miner_share, [noderewardtx], self.get_transaction_count(NODE_ADDRESS, "latest"))
                    self.remove_bad_nodes()
                    self.save_dag()
                    self.last_speed = finalizedtx.timestamp - tx["timestamp"]
                    self.save_speed()
                    return True
                except JuiceNotEnough:
                    return False
                except Exception as e:
                    print(traceback.format_exc())
                    return None
            else:
                self.remove_bad_nodes()
                self.save_dag()
                return False
        except Exception as e:
            print(traceback.format_exc())
            return None

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
                time.sleep(0.5)  # avoid oofing cpu
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
                    parents = []
                    juice_needed = int(tx.amount + tx.gas)

                    try:
                        self.mempool.remove(tx)  # remove from mempool to avoid mining again
                    except:
                        continue  # tx already mined in meantime somehow

                    for ptxn in self.dag.nodes:
                        ptx = self.dag.nodes[ptxn].get('transaction', None)
                        if ptx:
                            if ptx.recipient == tx.sender:
                                juice = int(ptx.juice)
                                if juice_needed == 0:
                                    break
                                elif juice == 0:
                                    pass
                                elif juice >= juice_needed:
                                    parents.append(ptx)
                                    juice_needed = 0
                                elif juice > 0:
                                    parents.append(ptx)
                                    juice_needed -= juice
                    if not (juice_needed == 0):
                        continue  # weird case where not enough juice could be found. cosmic rarity, as we checked when tx was received

                    expected_nonce = self.get_transaction_count(tx.sender, "latest")
                    if not tx.nonce == expected_nonce:
                        continue  # nonce mismatch

                    try:
                        # ⬇️ PROCESS TOKEN / CONTRACT INTERACTIONS HERE
                        if tx.data and (tx.data != '0x'):
                            self._process_contract_interaction(tx)

                        finalizedtx = self.dag.add_transaction(tx.sender, tx.recipient, tx.amount, tx.gas, parents, tx.nonce, tx.data)
                        noderewardtx = self.dag.add_transaction(tx.sender, NODE_ADDRESS.lower(), tx.gas, 0, parents, finalizedtx.nonce + 1)
                        minerrewardtx = self.dag.add_transaction(NODE_ADDRESS.lower(), NETWORK_MINER.lower(), tx.gas * MINER_FEE_SHARE, 0, [noderewardtx], self.get_transaction_count(NODE_ADDRESS, "latest"))
                        self.last_speed = finalizedtx.timestamp - tx.timestamp
                        self.remove_bad_nodes()
                        self.save_dag()
                        self.save_speed()

                        if len(self.p2p.peer_sockets) > 0:
                            nodesresponse = self.p2p.broadcast({
                                "tx": tx,
                                "parent_hashes": [parent.hash for parent in parents],
                                "fee": tx.gas,
                                "miner": NETWORK_MINER,
                                "miner_share": MINER_FEE_SHARE
                            })

                            agree = nodesresponse.count(True)
                            disagree = nodesresponse.count(False)

                            totalvotes = agree + disagree

                            if disagree > agree:
                                disagree_percent = (disagree / totalvotes) * 100
                                print(f"{disagree_percent}% ({disagree}) disagree? Sounds like a Sybil skill issue. Or, we fcked up big time... But last I checked, we weren't a democracy. Welcome to the DAG, lil' tx {finalizedtx.hash}.")

                    except JuiceNotEnough:
                        continue
                    except Exception as e:
                        print(traceback.format_exc())
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

            if self.get_balance(sender) < amount + fee:
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