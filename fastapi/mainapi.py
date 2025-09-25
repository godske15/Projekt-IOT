import asyncio
import os
import time 
from datetime import datetime, timedelta

import asyncpg

from typing import Union
from fastapi import FastAPI


os.environ['TZ'] = 'UTC'
time.tzset()

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}

@app.get("/qdbversion")
async def connect_to_questdb():
    conn = await asyncpg.connect(
        host='127.0.0.1',
        port=8812,
        user='admin',
        password='quest',
        database='qdb'
    )

    version = await conn.fetchval("SELECT version()")

    await conn.close()
    return {"QuestDB_version": version}

@app.get("/select_trades")
async def query_with_asyncpg():
    conn = await asyncpg.connect(
        host='127.0.0.1',
        port=8812,
        user='admin',
        password='quest',
        database='qdb'
    )

    # Fetch multiple rows
    rows = await conn.fetch("""
                            SELECT *
                            FROM cpp_trades
                            WHERE timestamp >= $1
                            ORDER BY timestamp DESC LIMIT 10
                            """, datetime.now() - timedelta(days=1))

    print(f"Fetched {len(rows)} rows")
    for row in rows:
        print(f"Timestamp: {row['timestamp']}, Symbol: {row['symbol']}, Price: {row['price']}")
        return {"Row name": row['symbol'], "Price": row['price']}

    # Fetch a single row
    single_row = await conn.fetchrow("""
                                     SELECT *
                                     FROM cpp_trades LIMIT -1
                                     """)
 
#if single_row:
#       print(f"Latest trade: {single_row['symbol']} at {single_row['price']}")

    # Fetch a single value
#   count = await conn.fetchval("SELECT count(*) FROM trades")
#   print(f"Total trades: {count}")
    
    await conn.close()
    return {"Row name": single_row['symbol'], "Price": single_row['price']}

