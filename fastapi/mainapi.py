import asyncio
import os
import time
from datetime import datetime, timedelta, timezone
import asyncpg
from typing import Union, Optional, List, Dict
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

os.environ['TZ'] = 'UTC'
time.tzset()

app = FastAPI()

pool = None

# --- Data model for incoming sensor data ---
class SensorMetric(BaseModel):
    node_id: str
    device_id: Optional[str] = None
    metric_name: str
    metric_value: Union[float, int, str]

class SensorDataIn(BaseModel):
    timestamp: int = Field(..., description="Unix nanoseconds timestamp")
    metrics: List[SensorMetric]

@app.on_event("startup")
async def startup():
    global pool

    questdb_host = os.getenv('QUESTDB_HOST', '127.0.0.1')
    questdb_port = int(os.getenv('QUESTDB_PORT', '8812'))

    print(f"Connecting to QuestDB at {questdb_host}:{questdb_port}")

    try:
        pool = await asyncpg.create_pool(
            host=questdb_host,
            port=questdb_port,
            user='admin',
            password='quest',
            database='qdb',
            min_size=5,
            max_size=20
        )
        print("✓ QuestDB connection pool created")

    except Exception as e:
        print(f"✗ Failed to connect to QuestDB: {e}")
        raise

        # --- Create necessary tables if they do not exist ---
    async with pool.acquire() as conn:
        try:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS sensor_data (
                    timestamp TIMESTAMP,
                    node_id SYMBOL,
                    device_id SYMBOL,
                    metric_name SYMBOL,
                    metric_value DOUBLE
                ) timestamp(timestamp);
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS node_data (
                    timestamp TIMESTAMP,
                    node_id SYMBOL,
                    metric_name SYMBOL,
                    metric_value DOUBLE,
                    is_alarm BOOLEAN
                ) timestamp(timestamp);
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS node_births (
                    timestamp TIMESTAMP,
                    node_id SYMBOL,
                    sequence_number INT
                ) timestamp(timestamp);
            """)

            print("✓ Tables ensured")
        except Exception as e:
            print(f"✗ Failed to create tables: {e}")
            raise

    print("=" * 50)
    print("FastAPI REST API Active!")
    print("- Ingestion & Read-only access to QuestDB")
    print("=" * 50)

@app.on_event("shutdown")
async def shutdown():
    global pool
    if pool:
        await pool.close()
        print("QuestDB pool closed")

@app.post("/ingest/sensor")
async def ingest_sensor_data(data: SensorDataIn):
    ts_datetime = datetime.fromtimestamp(data.timestamp / 1_000_000_000)
    async with pool.acquire() as conn:
        try:
            for m in data.metrics:
                await conn.execute("""
                    INSERT INTO sensor_data(timestamp, node_id, device_id, metric_name, metric_value)
                    VALUES($1, $2, $3, $4, $5)
                """, ts_datetime, m.node_id, m.device_id, m.metric_name, m.metric_value)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"DB insert failed: {e}")

    return {"status": "ok", "inserted_metrics": len(data.metrics)}


@app.get("/")
def read_root():
    return {
        "service": "IoT Data API",
        "status": "running",
        "description": "Read-only REST API for IoT data"
    }

@app.get("/health")
def health():
    return{"status": "healthy"}

@app.get("/db_health")
async def health_check():
    try:
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        db_status = True
    except:
        db_status = False
    
    return {
        "database": "connected" if db_status else "disconnected",
        "status": "healthy" if db_status else "unhealthy"
    }

@app.get("/qdbversion")
async def get_questdb_version():
    async with pool.acquire() as conn:
        version = await conn.fetchval("SELECT version()")
    return {"QuestDB_version": version}

@app.get("/sensor_data")
async def query_sensor_data(hours: int = 24, limit: int = 100, device_id: Optional[str] = None):
    """Get sensor data from DDATA messages"""
    async with pool.acquire() as conn:
        if device_id:
            rows = await conn.fetch("""
                SELECT timestamp, node_id, device_id, metric_name, metric_value
                FROM sensor_data
                WHERE timestamp >= dateadd('h', -$1, now())
                AND device_id = $3
                ORDER BY timestamp DESC 
                LIMIT $2
            """, hours, limit, device_id)
        else:
            rows = await conn.fetch("""
                SELECT timestamp, node_id, device_id, metric_name, metric_value
                FROM sensor_data
                WHERE timestamp >= dateadd('h', -$1, now())
                ORDER BY timestamp DESC 
                LIMIT $2
            """, hours, limit)
        
        return {
            "count": len(rows),
            "data": [dict(row) for row in rows]
        }

@app.get("/node_data")
async def query_node_data(hours: int = 24, limit: int = 100, node_id: Optional[str] = None):
    """Get node data from NDATA messages"""
    async with pool.acquire() as conn:
        if node_id:
            rows = await conn.fetch("""
                SELECT timestamp, node_id, metric_name, metric_value, is_alarm
                FROM node_data
                WHERE timestamp >= dateadd('h', -$1, now())
                AND node_id = $3
                ORDER BY timestamp DESC 
                LIMIT $2
            """, hours, limit, node_id)
        else:
            rows = await conn.fetch("""
                SELECT timestamp, node_id, metric_name, metric_value, is_alarm
                FROM node_data
                WHERE timestamp >= dateadd('h', -$1, now())
                ORDER BY timestamp DESC 
                LIMIT $2
            """, hours, limit)
        
        return {
            "count": len(rows),
            "data": [dict(row) for row in rows]
        }

@app.get("/alarms")
async def query_alarms(hours: int = 24):
    """Get all alarm events"""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT timestamp, node_id, metric_name, metric_value
            FROM node_data
            WHERE is_alarm = true 
            AND timestamp >= dateadd('h', -$1, now())
            ORDER BY timestamp DESC 
            LIMIT 100
        """, hours)
        
        return {
            "count": len(rows),
            "alarms": [dict(row) for row in rows]
        }

@app.get("/node_births")
async def query_node_births(limit: int = 20):
    """Get node birth events"""
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT timestamp, node_id, sequence_number
            FROM node_births
            ORDER BY timestamp DESC 
            LIMIT $1
        """, limit)
        
        return {
            "count": len(rows),
            "births": [dict(row) for row in rows]
        }

@app.get("/statistics")
async def get_statistics():
    """Get overall statistics"""
    async with pool.acquire() as conn:
        # Count total sensor readings
        sensor_count = await conn.fetchval("SELECT count(*) FROM sensor_data")
        
        # Count active nodes (births in last 24h)
        active_nodes = await conn.fetch("""
            SELECT DISTINCT node_id 
            FROM node_births 
            WHERE timestamp >= dateadd('h', -24, now())
        """)
        
        # Count recent alarms
        alarm_count = await conn.fetchval("""
            SELECT count(*) 
            FROM node_data 
            WHERE is_alarm = true 
            AND timestamp >= dateadd('h', -24, now())
        """)
        
        return {
            "total_sensor_readings": sensor_count,
            "active_nodes_24h": len(active_nodes),
            "alarms_24h": alarm_count,
            "nodes": [dict(row)['node_id'] for row in active_nodes]
        }