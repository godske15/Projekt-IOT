import asyncio
import os
import time
from fastapi import Query  
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

# Add these new endpoints for Grafana  

@app.get("/grafana/timeseries/sensor")  
async def grafana_sensor_timeseries(  
    metric_name: str,  
    node_id: Optional[str] = None,  
    device_id: Optional[str] = None,  
    from_ms: Optional[int] = Query(None, alias="from"),  
    to_ms: Optional[int] = Query(None, alias="to")  
):  
    """  
    Grafana-compatible time series endpoint for sensor data  
    Returns data in Grafana Simple JSON format  
    """  
    # Calculate time range  
    if from_ms and to_ms:  
        from_time = datetime.fromtimestamp(from_ms / 1000, tz=timezone.utc)  
        to_time = datetime.fromtimestamp(to_ms / 1000, tz=timezone.utc)  
    else:  
        to_time = datetime.now(timezone.utc)  
        from_time = to_time - timedelta(hours=24)  
    
    async with pool.acquire() as conn:  
        query = """  
            SELECT timestamp, metric_value  
            FROM sensor_data  
            WHERE metric_name = $1  
            AND timestamp >= $2  
            AND timestamp <= $3  
        """  
        params = [metric_name, from_time, to_time]  
        
        if node_id:  
            query += " AND node_id = $4"  
            params.append(node_id)  
            if device_id:  
                query += " AND device_id = $5"  
                params.append(device_id)  
        elif device_id:  
            query += " AND device_id = $4"  
            params.append(device_id)  
        
        query += " ORDER BY timestamp"  
        
        rows = await conn.fetch(query, *params)  
        
        # Format for Grafana: [[value, timestamp_ms], ...]  
        datapoints = [  
            [float(row['metric_value']), int(row['timestamp'].timestamp() * 1000)]  
            for row in rows  
        ]  
        
        target_name = f"{metric_name}"  
        if node_id:  
            target_name += f" ({node_id}"  
            if device_id:  
                target_name += f"/{device_id}"  
            target_name += ")"  
        
        return [{  
            "target": target_name,  
            "datapoints": datapoints  
        }]  

@app.get("/grafana/timeseries/node")  
async def grafana_node_timeseries(  
    metric_name: str,  
    node_id: Optional[str] = None,  
    from_ms: Optional[int] = Query(None, alias="from"),  
    to_ms: Optional[int] = Query(None, alias="to")  
):  
    """  
    Grafana-compatible time series endpoint for node data  
    """  
    if from_ms and to_ms:  
        from_time = datetime.fromtimestamp(from_ms / 1000, tz=timezone.utc)  
        to_time = datetime.fromtimestamp(to_ms / 1000, tz=timezone.utc)  
    else:  
        to_time = datetime.now(timezone.utc)  
        from_time = to_time - timedelta(hours=24)  
    
    async with pool.acquire() as conn:  
        query = """  
            SELECT timestamp, metric_value  
            FROM node_data  
            WHERE metric_name = $1  
            AND timestamp >= $2  
            AND timestamp <= $3  
        """  
        params = [metric_name, from_time, to_time]  
        
        if node_id:  
            query += " AND node_id = $4"  
            params.append(node_id)  
        
        query += " ORDER BY timestamp"  
        
        rows = await conn.fetch(query, *params)  
        
        datapoints = [  
            [float(row['metric_value']), int(row['timestamp'].timestamp() * 1000)]  
            for row in rows  
        ]  
        
        target_name = f"{metric_name}"  
        if node_id:  
            target_name += f" ({node_id})"  
        
        return [{  
            "target": target_name,  
            "datapoints": datapoints  
        }]  

@app.get("/grafana/table/latest")  
async def grafana_latest_readings(  
    node_id: Optional[str] = None,  
    limit: int = 100  
):  
    """  
    Latest sensor readings in table format for Grafana  
    """  
    async with pool.acquire() as conn:  
        if node_id:  
            rows = await conn.fetch("""  
                SELECT timestamp, node_id, device_id, metric_name, metric_value  
                FROM sensor_data  
                WHERE node_id = $1  
                ORDER BY timestamp DESC  
                LIMIT $2  
            """, node_id, limit)  
        else:  
            rows = await conn.fetch("""  
                SELECT timestamp, node_id, device_id, metric_name, metric_value  
                FROM sensor_data  
                ORDER BY timestamp DESC  
                LIMIT $1  
            """, limit)  
        
        return {  
            "columns": [  
                {"text": "Timestamp", "type": "time"},  
                {"text": "Node ID", "type": "string"},  
                {"text": "Device ID", "type": "string"},  
                {"text": "Metric Name", "type": "string"},  
                {"text": "Value", "type": "number"}  
            ],  
            "rows": [  
                [  
                    int(row['timestamp'].timestamp() * 1000),  
                    row['node_id'],  
                    row['device_id'],  
                    row['metric_name'],  
                    float(row['metric_value'])  
                ]  
                for row in rows  
            ]  
        }  

@app.get("/grafana/metrics/available")  
async def grafana_available_metrics():  
    """  
    Returns available metrics for Grafana variable queries  
    """  
    async with pool.acquire() as conn:  
        # Get unique metric names from sensor_data  
        sensor_metrics = await conn.fetch("""  
            SELECT DISTINCT metric_name   
            FROM sensor_data  
            ORDER BY metric_name  
        """)  
        
        # Get unique metric names from node_data  
        node_metrics = await conn.fetch("""  
            SELECT DISTINCT metric_name   
            FROM node_data  
            ORDER BY metric_name  
        """)  
        
        return {  
            "sensor_metrics": [row['metric_name'] for row in sensor_metrics],  
            "node_metrics": [row['metric_name'] for row in node_metrics]  
        }  

@app.get("/grafana/nodes/available")  
async def grafana_available_nodes():  
    """  
    Returns available node IDs for Grafana variable queries  
    """  
    async with pool.acquire() as conn:  
        nodes = await conn.fetch("""  
            SELECT DISTINCT node_id   
            FROM sensor_data  
            ORDER BY node_id  
        """)  
        
        return [row['node_id'] for row in nodes]  

@app.get("/grafana/devices/available")  
async def grafana_available_devices(node_id: Optional[str] = None):  
    """  
    Returns available device IDs for Grafana variable queries  
    """  
    async with pool.acquire() as conn:  
        if node_id:  
            devices = await conn.fetch("""  
                SELECT DISTINCT device_id   
                FROM sensor_data  
                WHERE node_id = $1  
                ORDER BY device_id  
            """, node_id)  
        else:  
            devices = await conn.fetch("""  
                SELECT DISTINCT device_id   
                FROM sensor_data  
                ORDER BY device_id  
            """)  
        
        return [row['device_id'] for row in devices if row['device_id']]  

@app.get("/grafana/alarms/timeseries")  
async def grafana_alarms_timeseries(  
    from_ms: Optional[int] = Query(None, alias="from"),  
    to_ms: Optional[int] = Query(None, alias="to")  
):  
    """  
    Alarm count over time for Grafana  
    """  
    if from_ms and to_ms:  
        from_time = datetime.fromtimestamp(from_ms / 1000, tz=timezone.utc)  
        to_time = datetime.fromtimestamp(to_ms / 1000, tz=timezone.utc)  
    else:  
        to_time = datetime.now(timezone.utc)  
        from_time = to_time - timedelta(hours=24)  
    
    async with pool.acquire() as conn:  
        rows = await conn.fetch("""  
            SELECT   
                timestamp,  
                COUNT(*) as alarm_count  
            FROM node_data  
            WHERE is_alarm = true  
            AND timestamp >= $1  
            AND timestamp <= $2  
            GROUP BY timestamp  
            ORDER BY timestamp  
        """, from_time, to_time)  
        
        datapoints = [  
            [int(row['alarm_count']), int(row['timestamp'].timestamp() * 1000)]  
            for row in rows  
        ]  
        
        return [{  
            "target": "Alarms",  
            "datapoints": datapoints  
        }]  
