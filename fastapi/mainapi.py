import asyncio
import os
import time
from fastapi import Query  
from datetime import datetime, timedelta, timezone
import asyncpg
from typing import Union, Optional, List, Dict
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import re

os.environ['TZ'] = 'UTC'
time.tzset()

app = FastAPI()

pool = None
created_tables = set()  # Cache til at holde styr på oprettede tabeller

# --- Data model for incoming Sparkplug B data ---
class Metric(BaseModel):
    name: str
    timestamp: int
    dataType: str
    value: Union[float, int, str, bool]

class SparkplugPayload(BaseModel):
    timestamp: int
    seq: int
    metrics: List[Metric]

def sanitize_table_name(metric_name: str) -> str:
    """
    Konverterer metric navn til et gyldigt tabel navn
    Eksempel: "Inputs/Indoor_temperature" -> "indoor_temperature"
    Eksempel: "Inputs/Outdoor_temperature" -> "outdoor_temperature"
    """
    # Fjern kun præfiks som "Inputs/", "Node Control/", etc.
    # Men behold resten af navnet intakt
    if '/' in metric_name:
        metric_name = metric_name.split('/', 1)[-1]  # Tag alt efter første '/'
    
    # Konverter til lowercase og erstat ugyldige tegn
    table_name = re.sub(r'[^a-z0-9_]', '_', metric_name.lower())
    
    return table_name

def get_column_type(data_type: str, value: any) -> str:
    """
    Bestemmer QuestDB kolonne type baseret på dataType eller værdi
    """
    if data_type:
        type_mapping = {
            "Float": "DOUBLE",
            "UInt64": "LONG",
            "Boolean": "BOOLEAN",
            "String": "STRING",
            "Int": "INT",
            "Double": "DOUBLE"
        }
        return type_mapping.get(data_type, "STRING")
    
    # Hvis ingen dataType, gæt baseret på værdi
    if isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, int):
        return "LONG"
    elif isinstance(value, float):
        return "DOUBLE"
    else:
        return "STRING"

async def ensure_table_exists(conn, table_name: str, column_type: str):
    """
    Opretter en tabel hvis den ikke eksisterer
    """
    if table_name in created_tables:
        return
    
    try:
        # Check om tabellen allerede eksisterer
        # QuestDB returnerer table_name (lowercase med underscore)
        exists = await conn.fetchval("""
            SELECT COUNT(*) 
            FROM tables() 
            WHERE table_name = $1
        """, table_name)
        
        if exists == 0:
            # Bestem kolonne navn baseret på type
            value_column = "status" if column_type == "STRING" else "value"
            
            create_query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    timestamp TIMESTAMP,
                    node_name SYMBOL,
                    device_name SYMBOL,
                    {value_column} {column_type}
                ) timestamp(timestamp) PARTITION BY DAY;
            """
            
            await conn.execute(create_query)
            print(f"✓ Tabel '{table_name}' oprettet med {value_column} kolonne ({column_type})")
        
        created_tables.add(table_name)
        
    except Exception as e:
        print(f"✗ Fejl ved oprettelse af tabel {table_name}: {e}")
        raise

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

    print("=" * 50)
    print("FastAPI REST API Active!")
    print("- Dynamic table creation enabled")
    print("- Ingestion & Read-only access to QuestDB")
    print("=" * 50)

@app.on_event("shutdown")
async def shutdown():
    global pool
    if pool:
        await pool.close()
        print("QuestDB pool closed")

@app.post("/ingest/nbirth/{group_id}/{node_id}")
async def ingest_nbirth(group_id: str, node_id: str, data: SparkplugPayload):
    """
    Håndterer NBIRTH beskeder - opretter kun tabeller, indsætter IKKE data
    Topic format: spBv1.0/{group_id}/NBIRTH/{node_id}
    """
    ts_datetime = datetime.fromtimestamp(data.timestamp)
    
    tables_created = 0
    
    async with pool.acquire() as conn:
        try:
            for m in data.metrics:
                metric_name = m.name
                
                # Spring over control og properties metrics
                if metric_name.startswith("Node Control/") or metric_name.startswith("Properties/"):
                    continue
                
                # Spring over bdSeq (det er allerede i payload level)
                if metric_name == "bdSeq":
                    continue
                
                # Håndter kun "Inputs/" metrics - OPRET KUN TABELLER
                if metric_name.startswith("Inputs/"):
                    table_name = sanitize_table_name(metric_name)
                    column_type = get_column_type(m.dataType, m.value)
                    
                    # Sørg for at tabellen eksisterer (men indsæt IKKE data)
                    await ensure_table_exists(conn, table_name, column_type)
                    tables_created += 1
                    
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Table creation failed: {e}")

    return {
        "status": "ok",
        "message": "Tables created, no data inserted",
        "group_id": group_id,
        "node_id": node_id,
        "sequence": data.seq,
        "tables_created": tables_created,
        "timestamp": ts_datetime.isoformat()
    }

@app.post("/ingest/ddata/{group_id}/{node_id}/{device_id}")
async def ingest_ddata(group_id: str, node_id: str, device_id: str, data: SparkplugPayload):
    """
    Håndterer DDATA beskeder (device data)
    Topic format: spBv1.0/{group_id}/DDATA/{node_id}/{device_id}
    """
    # Fjern timezone info for at matche QuestDB's forventninger
    ts_datetime = datetime.fromtimestamp(data.timestamp)
    
    inserted_count = 0
    
    async with pool.acquire() as conn:
        try:
            for m in data.metrics:
                table_name = sanitize_table_name(m.name)
                column_type = get_column_type(m.dataType, m.value)
                
                # Sørg for at tabellen eksisterer
                await ensure_table_exists(conn, table_name, column_type)
                
                # Bestem kolonne navn
                value_column = "status" if column_type == "STRING" else "value"
                
                # Indsæt data
                insert_query = f"""
                    INSERT INTO {table_name}(timestamp, node_name, device_name, {value_column})
                    VALUES($1, $2, $3, $4)
                """
                
                await conn.execute(
                    insert_query,
                    ts_datetime,
                    node_id,
                    device_id,
                    m.value
                )
                inserted_count += 1
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"DB insert failed: {e}")

    return {
        "status": "ok",
        "group_id": group_id,
        "node_id": node_id,
        "device_id": device_id,
        "sequence": data.seq,
        "inserted_metrics": inserted_count,
        "timestamp": ts_datetime.isoformat()
    }

@app.get("/")
def read_root():
    return {
        "service": "IoT Data API",
        "status": "running",
        "description": "Dynamic table creation REST API for IoT data"
    }

@app.get("/health")
def health():
    return {"status": "healthy"}

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

@app.get("/tables")
async def list_tables():
    """Vis alle oprettede tabeller"""
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT table_name FROM tables()")
        return {
            "tables": [dict(row)['table_name'] for row in rows],
            "count": len(rows)
        }

@app.get("/table/{table_name}")
async def query_table(table_name: str, hours: int = 24, limit: int = 100):
    """Query en specifik dynamisk oprettet tabel"""
    # Sanitize table name for sikkerhed
    safe_table_name = re.sub(r'[^a-z0-9_]', '', table_name.lower())
    
    async with pool.acquire() as conn:
        try:
            # Check om tabellen eksisterer (brug table_name)
            exists = await conn.fetchval("""
                SELECT COUNT(*) FROM tables() WHERE table_name = $1
            """, safe_table_name)
            
            if exists == 0:
                raise HTTPException(status_code=404, detail=f"Table '{safe_table_name}' not found")
            
            # Query tabellen
            rows = await conn.fetch(f"""
                SELECT * FROM {safe_table_name}
                WHERE timestamp >= dateadd('h', -{hours}, now())
                ORDER BY timestamp DESC
                LIMIT {limit}
            """)
            
            return {
                "table": safe_table_name,
                "count": len(rows),
                "data": [dict(row) for row in rows]
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.get("/statistics")
async def get_statistics():
    """Get overall statistics"""
    async with pool.acquire() as conn:
        # Count all tables
        all_tables = await conn.fetch("SELECT table_name FROM tables()")
        
        return {
            "total_tables": len(all_tables),
            "tables": [dict(row)['table_name'] for row in all_tables]
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