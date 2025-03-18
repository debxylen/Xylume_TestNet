from flask import Flask, request, jsonify
import traceback
import logging
import os
from flask_cors import CORS, cross_origin
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

class RPC(Flask):
    def __init__(self, core, chainid):
        super().__init__(__name__)
        logging.getLogger('werkzeug').setLevel(logging.WARNING)
        self.core = core
        self.CHAIN_ID = chainid
        CORS(self, origins="*")
        self.limiter = Limiter(get_remote_address, app=self, storage_uri="memory://")
        self.route('/', methods=['GET', 'POST'])(self.home)
        self.route('/rpc/', methods=['POST'])(self.handle_rpc)
        self.route('/mining/get', methods=['GET'])(cross_origin()(self.limiter.limit("1/second")(self.get_job)))
        self.route('/mining/submit', methods=['POST'])(cross_origin()(self.limiter.limit("1/2second")(self.submit_mined)))
    def home(self):
        return "Xylume TestNet is alive and working properly."

    def handle_rpc(self):
        data = request.get_json()
        try:
            method = data.get('method')
            if method == 'eth_chainId':
                return jsonify({'jsonrpc': '2.0', 'result': hex(self.CHAIN_ID), 'id': data.get('id')})

            if method == 'eth_blockNumber':
                return jsonify({'jsonrpc': '2.0', 'result': hex(len(self.core.dag.nodes) - 1), 'id': data.get('id')})

            if method == 'eth_getBlockByNumber':
                return self.handle_get_block_by_number(data)

            if method == 'eth_getTransactionByNumber':
                return self.handle_get_tx_by_number(data)

            if method == 'eth_getBalance':
                return self.handle_get_balance(data)

            if method == 'eth_getTransactionByHash':
                return self.handle_get_transaction_by_hash(data)

            if method == 'eth_getBlockByHash':
                return self.handle_get_transaction_by_hash(data) # same as tx by hash

            if method == 'eth_getCode':
                return jsonify({'jsonrpc': '2.0', 'result': '0x', 'id': data.get('id')})

            if method == 'eth_estimateGas':
                return self.handle_estimate_gas(data)

            if method == 'eth_gasPrice':
                return jsonify({'jsonrpc': '2.0', 'result': hex(1), 'id': data.get('id')})

            if method == 'eth_getTransactionCount':
                return self.handle_get_transaction_count(data)

            if method == 'eth_sendRawTransaction':
                return self.handle_send_raw_transaction(data)

            if method == 'net_version':
                return jsonify({'jsonrpc': '2.0', 'result': hex(self.CHAIN_ID), 'id': data.get('id')})

            if method == 'eth_getTransactionReceipt':
                return self.handle_get_transaction_receipt(data)

            if method == 'xyl_lastConfirmationSpeed':
                return self.handle_confirmation_speed(data)

            return jsonify({'jsonrpc': '2.0', 'error': {'code': -32601, 'message': 'Method not found'}, 'id': data.get('id')})
        
        except:
            print(traceback.format_exc())
            return jsonify({'jsonrpc': '2.0', 'error':  {'code': 3469, 'message': "Error 3469: Gas? Time? Luck? The universe isn’t sure, but something went sideways."}, 'id': data.get('id')})

    def handle_get_block_by_number(self, data):
        if 'latest' in data.get('params')[0]:
            block_number = len(self.core.dag.nodes) - 1
        else:
            block_number = int(data.get('params')[0], 16)
            tx = self.core.get_tx_by_number(block_number)
        if not tx:
            return jsonify({'jsonrpc': '2.0', 'result': None, 'id': data.get('id')})
        tx = self.core.dag.tx_to_block(tx)
        try:
            return jsonify({'jsonrpc': '2.0', 'result': tx, 'id': data.get('id')})
        except:
            print(tx)
            return jsonify({'jsonrpc': '2.0', 'result': None, 'id': data.get('id')})

    def handle_get_tx_by_number(self, data):
        if 'latest' in data.get('params')[0]:
            block_number = len(self.core.dag.nodes) - 1
        else:
            block_number = int(data.get('params')[0], 16)
            tx = self.core.get_tx_by_number(block_number)
        if not tx:
            return jsonify({'jsonrpc': '2.0', 'result': None, 'id': data.get('id')})
        tx = tx.__json__()
        try:
            return jsonify({'jsonrpc': '2.0', 'result': tx, 'id': data.get('id')})
        except:
            return jsonify({'jsonrpc': '2.0', 'result': None, 'id': data.get('id')})


    def handle_get_balance(self, data):
        address = str(data.get('params')[0])
        balance = self.core.get_balance(address)
        return jsonify({'jsonrpc': '2.0', 'result': hex(balance), 'id': data.get('id')})


    def handle_get_transaction_by_hash(self, data):
        tx_hash = data.get('params')[0]
        transaction = self.core.get_tx_by_hash(tx_hash)
        if not transaction:
            print('A TX was requested from hash', tx_hash, 'but not found.')
            return jsonify({'jsonrpc': '2.0', 'result': 'Not found.', 'id': data.get('id')})
        return jsonify({'jsonrpc': '2.0', 'result': transaction.__json__(), 'id': data.get('id')})


    def handle_estimate_gas(self, data):
        gasp = int(data['params'][0].get('gasPrice', '0x1'), 16)  # in wxei
        gasunits = self.core.get_fee()
        totalgas = int(gasunits * gasp)
        return jsonify({'jsonrpc': '2.0', 'result': hex(totalgas), 'id': data.get('id')})


    def handle_get_transaction_count(self, data):
        address = data.get('params')[0]
        mode = 'latest' if len(data.get('params'))==1 else data.get('params')[1]
        count = self.core.get_transaction_count(address, mode)
        return jsonify({'jsonrpc': '2.0', 'result': hex(count), 'id': data.get('id')})


    def handle_send_raw_transaction(self, data):
        raw_transaction = data.get('params')[0]
        tx_number = self.core.send_raw_transaction(raw_transaction)
        if "contractAddress" in tx_number:
            return jsonify({'jsonrpc': '2.0', 'result': tx_number["contractAddress"], 'id': data.get('id')})
        if "data" in tx_number:
            return jsonify({'jsonrpc': '2.0', 'result': tx_number["data"], 'id': data.get('id')})
        if "transactionHash" in tx_number:
            return jsonify({'jsonrpc': '2.0', 'result': tx_number["transactionHash"], 'id': data.get('id')})
        if "result" in tx_number:
            return jsonify({'jsonrpc': '2.0', 'result': tx_number["result"], 'id': data.get('id')})
        if "error" in tx_number:
            return jsonify({'jsonrpc': '2.0', 'error': {'code': 3469, 'message': tx_number["error"]}, 'id': data.get('id')})
        else:
            return jsonify({'jsonrpc': '2.0', 'error': {'code': 3469, 'message': 'Error 3469: Gas? Time? Luck? The universe isn’t sure, but something went sideways.'}, 'id': data.get('id')})


    def handle_get_transaction_receipt(self, data):
        transaction_id = data['params'][0]
        receipt = self.core.get_transaction_receipt(transaction_id)
        return jsonify({'jsonrpc': '2.0', 'result': receipt, 'id': data.get('id')})


    def handle_confirmation_speed(self, data):
        unit = 's' # default unit seconds
        if len(data['params']) > 0: # if a param is given
            unit = data['params'][0]
        if unit == 's': speed = self.core.last_speed / (10**9) # ns to s
        elif unit == 'ms': speed = self.core.last_speed / (10**6) # ns to ms
        elif unit == 'ns': speed = self.core.last_speed # ns
        else: speed = self.core.last_speed / (10**9) # default fallback s
        return jsonify({'jsonrpc': '2.0', 'result': speed, 'id': data.get('id')})


    def get_job(self):
        """Send a new mining job to the miner."""
        job = self.core.generate_job()
        return jsonify(job)


    def submit_mined(self):
        """Receive mined txs from a miner and validate it."""
        data = request.json
        miner_address = data.get('miner')
        mined_data = data.get('mined_data')

        result, reason = self.core.submit_mined(mined_data, miner_address)
        if result:
            return jsonify({'message': 'Job accepted.'}), 200
        else:
            return jsonify({'message': f'Job rejected: {reason}'}), 400
