from revert_exceptions import *
import networkx as nx
import random
import time
from hashlib import blake2b

class Transaction:
    def __init__(self, sender: str, recipient: str, amount: int, gas: int, parents: list = [], nonce: int = 0, data: str = '0x'):
        self.sender = sender.lower()
        self.recipient = recipient.lower()
        self.amount = int(amount)
        self.juice = int(amount)
        self.parents = parents
        self.nonce = int(nonce)
        self.timestamp = time.time_ns()
        self.gas = gas
        self.data = data
        self.hash = "0x" + blake2b(f"{self.sender}{self.recipient}{self.amount}{self.nonce}{self.gas}{self.data or '0x'}".encode(), digest_size=32).hexdigest()

    def __json__(self):
        return {
            "hash": self.hash,
            "sender": self.sender,
            "recipient": self.recipient,
            "amount": self.amount,
            "juice": self.juice,
            "parents": [parent.hash for parent in self.parents],
            "nonce": self.nonce,
            "timestamp": (self.timestamp/1000)/1000, # ns to s
            "gas": self.gas,
            "data": self.data,
        }
   
class DAG(nx.DiGraph):
    def __init__(self, genesis_tx):
        super().__init__()
        self.add_node(genesis_tx.hash, transaction=genesis_tx)
    
    def add_transaction(self, sender, recipient, amount, gas, parents, nonce, data='0x'): # for confirmed tx
        try: # try block to avoid partial execution
            tx = Transaction(sender=sender, recipient=recipient, amount=amount, gas=gas, parents=parents, nonce=nonce, data=data)
            tx_id = tx.hash
        
            self.add_node(tx_id, transaction=tx)
        
            juicetocollect = int(amount+gas)
            for parent in parents:
                parent_tx = self.nodes[parent]['transaction'] if type(parent)==str else parent
                if parent_tx.juice > 0:
                    if parent_tx.juice >= juicetocollect:
                        parent_tx.juice -= juicetocollect
                        juicetocollect = 0
                    elif parent_tx.juice < juicetocollect:
                        juicetocollect -= parent_tx.juice
                        parent_tx.juice = 0

                self.add_edge(parent, tx_id)

            if juicetocollect == 0: 
                return tx
            else: # raise error if juicetocollect is still not 0
                raise JuiceNotEnough("Not enough juice in parents.")

        except JuiceNotEnough as e:
            raise JuiceNotEnough("Not enough juice in parents.") # Handled in core

    def tx_to_block(self, tx):
        block_json = {
            "hash": tx.hash,
            "number": str(hex(list(self.nodes).index(tx.hash))),
            "nonce": str(hex(tx.nonce if tx.nonce else 0)),
            "timestamp": (tx.timestamp/1000)/1000, #ns to s
            "transactions": [tx.hash],
            "gasUsed": tx.gas,
            "gasLimit": tx.gas,
        }
        return block_json
