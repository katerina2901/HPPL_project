# Parallel processing of Bitcoin blocks with ClickHouse integration

## Project description

The project focuses on collecting information on all bitcoin addresses that participate in all Bitcoin blockchain transactions. The main goal is to assemble a large and easily accessible database that allows for quick analytical queries of data on transactions and addresses participating in the Bitcoin blockchain. The project includes applying concurrency techniques (multithreading) using `concurrent.futures.ThreadPoolExecutor`, integrating with the full Bitcoin Core node via RPC, and loading large amounts of data into ClickHouse's high-performance columnar DBMS.

## Key Attributes
- Extracting data from a full bitcoin note via RPC.
- Parallel processing of more then 850k blocks.
- Use of `utxo_set` to reduce the number of repeated RPC calls.
- Batch insertion of data into ClickHouse to speed up loading.

## Technical Requirements
- Installed and synchronised full-format Bitcoin Core node.
- A running and available ClickHouse server.
- Python 3.9+ 

## Creating a table in the database:
```CREATE TABLE all_transactions (
    address String,
    txid String,
    is_input UInt8,
    shared_send UInt8
)
ENGINE = MergeTree
ORDER BY (address, txid);
```

## Installation

1. Clone the repository:
   ```bash
   git clone [repo](https://github.com/katerina2901/HPPL_project)
2. ```pip install -r requirements.txt```
3. Run Bitcoin node and Clickhouse server
4.   ```python3 script.py```

