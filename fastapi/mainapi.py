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
    
    # Convert Sparkplug timestamp → naive UTC datetime (QuestDB requires naive)
    if data.timestamp > 1e12:  # milliseconds
        ts_datetime = datetime.utcfromtimestamp(data.timestamp / 1000)
    else:  # seconds
        ts_datetime = datetime.utcfromtimestamp(data.timestamp)

    
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
    # Convert Sparkplug timestamp → naive UTC datetime (QuestDB requires naive)
    if data.timestamp > 1e12:  # milliseconds
        ts_datetime = datetime.utcfromtimestamp(data.timestamp / 1000)
    else:  # seconds
        ts_datetime = datetime.utcfromtimestamp(data.timestamp)
    
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

# ==================== GRAFANA ENDPOINTS ====================  

@app.get("/grafana/tables/list")  
async def grafana_list_tables():  
    """  
    List all available tables (metrics) for Grafana  
    """  
    async with pool.acquire() as conn:  
        rows = await conn.fetch("""  
            SELECT table_name   
            FROM tables()   
            WHERE table_name NOT IN ('sys.column_versions_purge_log', 'telemetry', 'telemetry_config')  
            ORDER BY table_name  
        """)  
        return [row['table_name'] for row in rows]  

@app.get("/grafana/nodes/list")  
async def grafana_list_nodes():  
    """  
    List all available node names across all tables  
    """  
    async with pool.acquire() as conn:  
        # Get all tables first  
        tables = await conn.fetch("""  
            SELECT table_name   
            FROM tables()   
            WHERE table_name NOT IN ('sys.column_versions_purge_log', 'telemetry', 'telemetry_config')  
        """)  
        
        all_nodes = set()  
        for table in tables:  
            table_name = table['table_name']  
            try:  
                nodes = await conn.fetch(f"""  
                    SELECT DISTINCT node_name   
                    FROM {table_name}  
                """)  
                all_nodes.update([row['node_name'] for row in nodes])  
            except:  
                continue  
        
        return sorted(list(all_nodes))  

@app.get("/grafana/devices/list")  
async def grafana_list_devices(node_name: Optional[str] = None):  
    """  
    List all available device names  
    """  
    async with pool.acquire() as conn:  
        tables = await conn.fetch("""  
            SELECT table_name   
            FROM tables()   
            WHERE table_name NOT IN ('sys.column_versions_purge_log', 'telemetry', 'telemetry_config')  
        """)  
        
        all_devices = set()  
        for table in tables:  
            table_name = table['table_name']  
            try:  
                if node_name:  
                    devices = await conn.fetch(f"""  
                        SELECT DISTINCT device_name   
                        FROM {table_name}  
                        WHERE node_name = $1  
                    """, node_name)  
                else:  
                    devices = await conn.fetch(f"""  
                        SELECT DISTINCT device_name   
                        FROM {table_name}  
                    """)  
                all_devices.update([row['device_name'] for row in devices])  
            except:  
                continue  
        
        return sorted(list(all_devices))  

@app.get("/grafana/timeseries/{table_name}")  
async def grafana_timeseries(  
    table_name: str,  
    node_name: Optional[str] = None,  
    device_name: Optional[str] = None,  
    from_ms: Optional[int] = Query(None, alias="from"),  
    to_ms: Optional[int] = Query(None, alias="to")  
):  
    """  
    Get time series data from a specific dynamic table  
    Works with both 'value' and 'status' columns  
    """  
    # Sanitize table name  
    safe_table_name = re.sub(r'[^a-z0-9_]', '', table_name.lower())  
    
    # Calculate time range - FIXED: timezone-aware
    if from_ms and to_ms:  
        from_time = datetime.fromtimestamp(from_ms / 1000, tz=timezone.utc)  
        to_time = datetime.fromtimestamp(to_ms / 1000, tz=timezone.utc)  
    else:  
        to_time = datetime.now(timezone.utc)  
        from_time = to_time - timedelta(hours=6)  
    
    async with pool.acquire() as conn:  
        # Check if table exists  
        exists = await conn.fetchval("""  
            SELECT COUNT(*) FROM tables() WHERE table_name = $1  
        """, safe_table_name)  
        
        if exists == 0:  
            raise HTTPException(status_code=404, detail=f"Table '{safe_table_name}' not found")  
        
        # Check which column exists (value or status)  
        columns = await conn.fetch(f"""  
            SELECT column_name   
            FROM table_columns('{safe_table_name}')  
        """)  
        column_names = [row['column_name'] for row in columns]  
        
        value_column = 'value' if 'value' in column_names else 'status'  
        
        # Build query  
        query = f"""  
            SELECT timestamp, {value_column}, node_name, device_name  
            FROM {safe_table_name}  
            WHERE timestamp >= $1  
            AND timestamp <= $2  
        """  
        params = [from_time, to_time]  
        
        if node_name:  
            query += f" AND node_name = ${len(params) + 1}"  
            params.append(node_name)  
        if device_name:  
            query += f" AND device_name = ${len(params) + 1}"  
            params.append(device_name)  
        
        query += " ORDER BY timestamp"  
        
        rows = await conn.fetch(query, *params)  
        
        # Group by node_name and device_name to create multiple series  
        series_data = {}  
        for row in rows:  
            key = f"{row['node_name']}/{row['device_name']}"  
            if key not in series_data:  
                series_data[key] = []  
            
            # Handle both numeric and string values  
            try:  
                value = float(row[value_column])  
            except (ValueError, TypeError):  
                # For boolean or string values, convert to 1/0  
                value = 1 if row[value_column] else 0  
            
            timestamp_ms = int(row['timestamp'].timestamp() * 1000)  
            series_data[key].append([value, timestamp_ms])  
        
        # Format for Grafana  
        result = []  
        for series_name, datapoints in series_data.items():  
            result.append({  
                "target": f"{safe_table_name} - {series_name}",  
                "datapoints": datapoints  
            })  
        
        return result  

@app.get("/grafana/current/{table_name}")  
async def grafana_current_value(table_name: str):  
    """  
    Get the latest value from a specific table  
    Perfect for stat/gauge panels  
    """  
    safe_table_name = re.sub(r'[^a-z0-9_]', '', table_name.lower())  
    
    async with pool.acquire() as conn:  
        # Check if table exists  
        exists = await conn.fetchval("""  
            SELECT COUNT(*) FROM tables() WHERE table_name = $1  
        """, safe_table_name)  
        
        if exists == 0:  
            raise HTTPException(status_code=404, detail=f"Table '{safe_table_name}' not found")  
        
        # Check which column exists  
        columns = await conn.fetch(f"""  
            SELECT column_name   
            FROM table_columns('{safe_table_name}')  
        """)  
        column_names = [row['column_name'] for row in columns]  
        value_column = 'value' if 'value' in column_names else 'status'  
        
        # Get latest value  
        row = await conn.fetchrow(f"""  
            SELECT timestamp, {value_column}, node_name, device_name  
            FROM {safe_table_name}  
            ORDER BY timestamp DESC  
            LIMIT 1  
        """)  
        
        if not row:  
            return {"value": None}  
        
        # Try to convert to float  
        try:  
            value = float(row[value_column])  
        except (ValueError, TypeError):  
            value = 1 if row[value_column] else 0  
        
        return {  
            "value": value,  
            "timestamp": int(row['timestamp'].timestamp() * 1000),  
            "node_name": row['node_name'],  
            "device_name": row['device_name']  
        }  

@app.get("/grafana/table/all_latest")  
async def grafana_all_latest_values(limit: int = 100):  
    """  
    Get latest values from ALL tables  
    Good for overview table  
    """  
    async with pool.acquire() as conn:  
        # Get all tables  
        tables = await conn.fetch("""  
            SELECT table_name   
            FROM tables()   
            WHERE table_name NOT IN ('sys.column_versions_purge_log', 'telemetry', 'telemetry_config')  
            ORDER BY table_name  
        """)  
        
        all_data = []  
        
        for table in tables:  
            table_name = table['table_name']  
            try:  
                # Check which column exists  
                columns = await conn.fetch(f"""  
                    SELECT column_name   
                    FROM table_columns('{table_name}')  
                """)  
                column_names = [row['column_name'] for row in columns]  
                value_column = 'value' if 'value' in column_names else 'status'  
                
                # Get latest value  
                row = await conn.fetchrow(f"""  
                    SELECT timestamp, {value_column}, node_name, device_name  
                    FROM {table_name}  
                    ORDER BY timestamp DESC  
                    LIMIT 1  
                """)  
                
                if row:  
                    all_data.append({  
                        "metric": table_name,  
                        "value": row[value_column],  
                        "node": row['node_name'],  
                        "device": row['device_name'],  
                        "timestamp": row['timestamp'].isoformat()  
                    })  
            except Exception as e:  
                print(f"Error reading table {table_name}: {e}")  
                continue  
        
        return all_data[:limit]  

@app.get("/grafana/stats")  
async def grafana_system_stats():  
    """  
    System statistics for stat panels  
    """  
    async with pool.acquire() as conn:  
        # Count tables (metrics)  
        tables = await conn.fetch("""  
            SELECT COUNT(*) as count  
            FROM tables()   
            WHERE table_name NOT IN ('sys.column_versions_purge_log', 'telemetry', 'telemetry_config')  
        """)  
        
        # Get unique nodes  
        all_tables = await conn.fetch("""  
            SELECT table_name   
            FROM tables()   
            WHERE table_name NOT IN ('sys.column_versions_purge_log', 'telemetry', 'telemetry_config')  
        """)  
        
        all_nodes = set()  
        all_devices = set()  
        total_datapoints = 0  
        
        for table in all_tables:  
            table_name = table['table_name']  
            try:  
                nodes = await conn.fetch(f"SELECT DISTINCT node_name FROM {table_name}")  
                all_nodes.update([row['node_name'] for row in nodes])  
                
                devices = await conn.fetch(f"SELECT DISTINCT device_name FROM {table_name}")  
                all_devices.update([row['device_name'] for row in devices])  
                
                count = await conn.fetchval(f"""  
                    SELECT COUNT(*)   
                    FROM {table_name}  
                    WHERE timestamp >= dateadd('h', -1, now())  
                """)  
                total_datapoints += count or 0  
            except:  
                continue  
        
        return {  
            "total_metrics": tables[0]['count'],  
            "active_nodes": len(all_nodes),  
            "active_devices": len(all_devices),  
            "datapoints_last_hour": total_datapoints  
        }
