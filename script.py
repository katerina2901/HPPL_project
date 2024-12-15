import logging
import threading
import concurrent.futures

from env import * 
from tqdm import tqdm
from clickhouse_connect import get_client
from bitcoinrpc.authproxy import AuthServiceProxy
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')


console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


file_handler = logging.FileHandler('app.log', encoding='utf-8')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def get_rpc_connection():
    rpc_url = f'http://{RPC_USER}:{RPC_PASSWORD}@{RPC_HOST}:{RPC_PORT}'
    return AuthServiceProxy(rpc_url)

def get_clickhouse_client():

    return get_client(host=clickhouse_host,
                                port=clickhouse_port,
                                username=clickhouse_user,
                                password=clickhouse_password)

utxo_set = {}
utxo_lock = threading.Lock()

@retry(
    stop=stop_after_attempt(2),  
    wait=wait_exponential(multiplier=1, min=1, max=3),  
    retry=retry_if_exception_type(Exception),  
)

def process_block(block_hash, rpc_connection, clickhouse_client):
    rpc_connection = get_rpc_connection()
    clickhouse_client = get_clickhouse_client()

    try:

        block = rpc_connection.getblock(block_hash, 2)
        transactions = block['tx']

        data_to_insert = []

        for tx in transactions:
            txid = tx['txid']
            vin = tx.get('vin', [])
            vout = tx.get('vout', [])
            num_inputs = len(vin)
            num_outputs = len(vout)
            shared_send = 1 if num_inputs > 2 and num_outputs > 2 else 0

            for input_tx in vin:
                if 'coinbase' in input_tx:
                    continue 

                prev_txid = input_tx['txid']
                vout_index = input_tx['vout']

                if prev_txid is None or vout_index is None:
                    logging.warning(f"Non correct data for tx {txid}")
                    continue

                with utxo_lock:
                    key = (prev_txid, vout_index)
                    if key in utxo_set:
                        address = utxo_set.pop(key)
                    else:
                        try:
                            prev_tx = rpc_connection.getrawtransaction(prev_txid, True)
                            prev_vout = prev_tx['vout'][vout_index]
                            script_pub_key = prev_vout['scriptPubKey']
                            address = extract_address_from_script(script_pub_key, txid)
                        except Exception as e:
                            logging.error(f"Error getting previous TX {prev_txid}: {e}")
                            address = None

                if address is None:
                    continue  

                data_to_insert.append((address, txid, 1, shared_send))

            for index, output_tx in enumerate(vout):
                script_pub_key = output_tx['scriptPubKey']
                address = extract_address_from_script(script_pub_key, txid)

                if address is None:
                    continue

                with utxo_lock:
                    utxo_set[(txid, index)] = address

                data_to_insert.append((address, txid, 0, shared_send))

        if data_to_insert:
                try:
                    clickhouse_client.insert(
                'all_transactions',
                data_to_insert,
                column_names=['address', 'txid', 'is_input', 'shared_send']
            )
                except Exception as e:
                    logging.error(f"Error insert data to Clickhouse: {e}")

    except Exception as e:
        logging.error(f"Error processing block {block_hash}: {e}")

def extract_address_from_script(script_pub_key, txid):
    script_type = script_pub_key.get('type', '')
    if script_type in ['pubkeyhash', 'scripthash', 'witness_v0_keyhash', 'witness_v0_scripthash']:
        address = script_pub_key.get('address', [])
        return address if address else None
    # elif script_type == 'pubkey':
    #     # вырезаем публичный ключ из asm
    #     asm = script_pub_key.get('asm', '')
    #     public_key = asm.split()[0]
    #     return public_key  
    else:
        logging.warning(f"Skip script_type {script_type} в {txid}")
        return None

def process_block_batch(batch):
    rpc_connection = get_rpc_connection()
    clickhouse_client = get_clickhouse_client()
    for block_hash in batch:
        process_block(block_hash, rpc_connection, clickhouse_client)

def main():
    try:
        rpc_connection = get_rpc_connection()
        block_count = rpc_connection.getblockcount()
        logging.info(f"Current count of blocks: {block_count}")

        start_height = 1
        end_height = 850001
        total_blocks_to_process = min(end_height, block_count)
        logging.info(f"Start processing {start_height} - {total_blocks_to_process} blocks.")

        block_hashes = []
        for height in tqdm(range(start_height, total_blocks_to_process + 1), desc="Getting hashes of the blocks"):
            try:
                block_hash = rpc_connection.getblockhash(height)
                block_hashes.append(block_hash)
            except Exception as e:
                logging.error(f"Error gettinf hash of the block {height}: {e}")


        batch_size = 10000
        block_batches = [block_hashes[i:i + batch_size] for i in range(0, len(block_hashes), batch_size)]

        max_workers = 8

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(process_block_batch, batch)
                for batch in block_batches
            ]
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Processing batches"):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error processing batch: {e}")

        logging.info("Finish processing")
    except Exception as e:
        logging.error(f"Error in main: {e}")


if __name__ == '__main__':
    main()
