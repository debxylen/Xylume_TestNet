from revert_exceptions import *
import networkx as nx
import random
import time
from hashlib import blake2b

class Transaction:
    def __init__(self, sender: str, recipient: str, amount: int, gas: int, parents: list, nonce: int, data: str, status: int, logs: list):
        self.sender = sender.lower()
        self.recipient = recipient.lower()
        self.amount = int(amount)
        self.juice = int(amount)
        self.parents = [p if isinstance(p, str) else p.hash for p in parents if p]
        self.nonce = int(nonce)
        self.timestamp = time.time_ns()
        self.gas = gas
        self.data = data
        self.status = status
        self.logs = logs
        self.hash = "0x" + blake2b(f"{self.sender}{self.recipient}{self.amount}{self.nonce}{self.gas}{self.data}".encode(), digest_size=32).hexdigest()

    def __json__(self):
        return {
            "hash": self.hash,
            "nonce": hex(self.nonce or 0),
            "blockHash": self.hash,
            "blockNumber": None,
            "transactionIndex": "0x0",
            "from": self.sender,
            "to": self.recipient,
            "value": hex(self.amount or 0),
            "gas": hex(self.gas or 0),
            "gasPrice": hex(1),
            "input": self.data,
            "type": "0x0",
            "v": "0x364f",
            "r": "0x0",
            "s": "0x0",

            "maxFeePerGas": "0x0",
            "maxPriorityFeePerGas": "0x0",
            "chainId": "0x1b16",

            "timestamp": hex(int(self.timestamp / 1_000_000_000)),  # ns → s
            "parents": self.parents or [],
            "juice": self.juice or 0,
            "status": self.status or 1,
            "logs": self.logs or [],
        }

class DAG(nx.DiGraph):
    def __init__(self, genesis_tx):
        super().__init__()
        self.version = 3
        self.add_node(genesis_tx.hash, transaction=genesis_tx)

    def select_parents(self, parents, needed_amount):
        needed = int(needed_amount)
        full_deducts = []
        partial_parent = None
        partial_amount = 0
        
        for parent in parents:
            parent_tx = self.nodes[parent]['transaction'] if type(parent) == str else parent
            if parent_tx.juice <= 0: continue
            if parent_tx.juice > needed:
                partial_parent, partial_amount = parent_tx, needed
                needed = 0
                break
            elif parent_tx.juice <= needed:
                full_deducts.append(parent_tx)
                needed -= parent_tx.juice

        if needed != 0:
            return None, None, None, int(needed_amount) - needed
        
        return full_deducts, partial_parent, partial_amount, int(needed_amount) - needed
    
    def add_transaction(self, sender, recipient, amount, gas, parents, nonce, data, status, logs,
                        check_juice=True, full_deducts=None, partial_parent=None, partial_amount=None): # for confirmed tx

        # Failed tx: just add node, no juice deduction, no parent edges
        if status == 0:
            tx = Transaction(sender=sender, recipient=recipient, amount=amount, gas=gas, parents=[], nonce=nonce, data=data, status=status, logs=logs)
            self.add_node(tx.hash, transaction=tx)
            return tx

        # check_juice = False is supposed to be when a function already ran select_parents before this function.
        # this avoids double-running of that function, for performance.
        # otherwise, the script, normally, proceeds to check whether juice is sufficient or not
        if check_juice:
            full_deducts, partial_parent, partial_amount, _ = self.select_parents(parents, amount)
            if (full_deducts == partial_parent == partial_amount == None):
                raise JuiceNotEnough('Insufficient juice in parents.')

        # only the amount is deducted here, gas excluded
        # gas deduction is handled by separate transactions (sender → node, node → miner),

        final_parents = full_deducts + ([partial_parent] if partial_parent else [])
        tx = Transaction(sender=sender, recipient=recipient, amount=amount, gas=gas, parents=final_parents, nonce=nonce, data=data, status=status, logs=logs)
        tx_id = tx.hash
        self.add_node(tx_id, transaction=tx)
                
        for parent_tx in full_deducts:
            parent_tx.juice = 0
            self.add_edge(parent_tx.hash, tx_id)
        if partial_parent:
            partial_parent.juice -= partial_amount
            self.add_edge(partial_parent.hash, tx_id)

        return tx

    def tx_to_block(self, tx):
        block_json = {
            "hash": tx.hash,
            "parentHash": "0x" + "00" * 32,
            "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccc5a9f6b6b9959f2b11a0f6e4b1d2e1f2a3b4",
            "miner": "0x0000000000000000000000000000000000000000",
            "difficulty": "0x0",
            "totalDifficulty": "0x0",
            "extraData": "0x",
            "logsBloom": "0x" + "0" * 512,
            "transactionsRoot": "0x56e81f171bcc55a6ff8345e69d70669a7a2a2f7f9a3b520c1f74b7a5b5e4c1f3",
            "stateRoot": "0x" + "00" * 32,
            "receiptsRoot": "0x56e81f171bcc55a6ff8345e69d70669a7a2a2f7f9a3b520c1f74b7a5b5e4c1f3",
            "number": hex(list(self.nodes).index(tx.hash)),
            "nonce": hex(tx.nonce if tx.nonce else 0),
            "mixHash": "0x" + "00" * 32,
            "timestamp": hex(int(tx.timestamp / 1_000_000_000)),  # ns → s
            "gasUsed": hex(tx.gas),
            "gasLimit": hex(tx.gas),
            "size": "0x200",
            "transactions": [tx.hash],
            "uncles": []
        }
        return block_json

