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
import traceback
import subprocess


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
    FIX: Bruger 'table_name' i tables (IKKE name eller tables())
    """
    if table_name in created_tables:
        return
    
    try:
        # Check om tabellen allerede eksisterer
        exists = await conn.fetchval("""
            SELECT COUNT(*) 
            FROM tables 
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
    Håndterer NBIRTH beskeder - opretter tabeller OG indsætter initialdata
    Topic format: spBv1.0/{group_id}/NBIRTH/{node_id}
    """
    ts_datetime = datetime.fromtimestamp(data.timestamp / 1000, tz=timezone.utc)
    
    tables_created = 0
    metrics_inserted = 0
    
    async with pool.acquire() as conn:
        try:
            for m in data.metrics:
                metric_name = m.name
                
                # Spring over control og properties metrics
                if metric_name.startswith("Node Control/") or metric_name.startswith("Properties/"):
                    continue
                
                # Spring over bdSeq
                if metric_name == "bdSeq":
                    continue
                
                # Håndter "Inputs/" metrics - OPRET TABELLER OG INDSÆT DATA
                if metric_name.startswith("Inputs/"):
                    table_name = sanitize_table_name(metric_name)
                    column_type = get_column_type(m.dataType, m.value)
                    
                    # Sørg for at tabellen eksisterer
                    await ensure_table_exists(conn, table_name, column_type)
                    tables_created += 1
                    
                    # Indsæt også data fra NBIRTH
                    value_column = "status" if column_type == "STRING" else "value"
                    
                    # Brug metric's timestamp hvis den findes, ellers payload timestamp
                    metric_ts = datetime.fromtimestamp(m.timestamp / 1000, tz=timezone.utc) if m.timestamp else ts_datetime
                    
                    insert_query = f"""
                        INSERT INTO {table_name}(timestamp, node_name, device_name, {value_column})
                        VALUES($1, $2, $3, $4)
                    """
                    
                    await conn.execute(
                        insert_query,
                        metric_ts,
                        node_id,
                        "node",  # NBIRTH er node-level
                        m.value
                    )
                    metrics_inserted += 1
                    
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"NBIRTH processing failed: {e}")

    return {
        "status": "ok",
        "message": "Tables created and initial data inserted",
        "group_id": group_id,
        "node_id": node_id,
        "sequence": data.seq,
        "tables_created": tables_created,
        "metrics_inserted": metrics_inserted,
        "timestamp": ts_datetime.isoformat()
    }

@app.post("/ingest/ddata/{group_id}/{node_id}/{device_id}")
async def ingest_ddata(group_id: str, node_id: str, device_id: str, data: SparkplugPayload):
    """
    Håndterer DDATA beskeder (device data)
    Topic format: spBv1.0/{group_id}/DDATA/{node_id}/{device_id}
    """
    ts_datetime = datetime.fromtimestamp(data.timestamp / 1000, tz=timezone.utc)
    
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
                
                # Brug metric's timestamp hvis den findes
                metric_ts = datetime.fromtimestamp(m.timestamp / 1000, tz=timezone.utc) if m.timestamp else ts_datetime
                
                # Indsæt data
                insert_query = f"""
                    INSERT INTO {table_name}(timestamp, node_name, device_name, {value_column})
                    VALUES($1, $2, $3, $4)
                """
                
                await conn.execute(
                    insert_query,
                    metric_ts,
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
        rows = await conn.fetch("SELECT table_name FROM tables")
        return {
            "tables": [dict(row)['table_name'] for row in rows],
            "count": len(rows)
        }

@app.get("/table/{table_name}")
async def query_table(table_name: str, hours: int = 24, limit: int = 100):
    """Query en specifik dynamisk oprettet tabel"""
    safe_table_name = re.sub(r'[^a-z0-9_]', '', table_name.lower())
    
    async with pool.acquire() as conn:
        try:
            # Check om tabellen eksisterer
            exists = await conn.fetchval("""
                SELECT COUNT(*) FROM tables WHERE table_name = $1
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
        all_tables = await conn.fetch("SELECT table_name FROM tables")
        
        return {
            "total_tables": len(all_tables),
            "tables": [dict(row)['table_name'] for row in all_tables]
        }

# ==================== DEBUG ENDPOINTS ====================

@app.get("/debug/tables/all")
async def debug_all_tables():
    """Vis alle tabeller med row counts"""
    async with pool.acquire() as conn:
        try:
            # Get all tables - QuestDB system query
            tables = await conn.fetch("""
                SELECT table_name 
                FROM tables 
                WHERE table_name NOT IN ('sys.column_versions_purge_log', 'telemetry', 'telemetry_config')
                ORDER BY table_name
            """)
            
            result = []
            for table in tables:
                table_name = table['table_name']
                try:
                    count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                    result.append({
                        "table": table_name,
                        "rows": count
                    })
                except:
                    result.append({
                        "table": table_name,
                        "rows": "error"
                    })
            
            return {
                "total_tables": len(result),
                "tables": result
            }
            
        except Exception as e:
            return {
                "error": str(e),
                "traceback": traceback.format_exc()
            }

@app.get("/debug/table/{table_name}/raw")
async def debug_table_raw(table_name: str, limit: int = 10):
    """Se RAW data fra en tabel - til debugging"""
    safe_table_name = re.sub(r'[^a-z0-9_]', '', table_name.lower())
    
    async with pool.acquire() as conn:
        try:
            # Check if table exists
            exists = await conn.fetchval("""
                SELECT COUNT(*) FROM tables WHERE table_name = $1
            """, safe_table_name)
            
            if exists == 0:
                return {"error": f"Table '{safe_table_name}' not found"}
            
            # Get table structure - "column" er SQL keyword, skal være i quotes
            columns = await conn.fetch(f"""
                SELECT "column", type
                FROM table_columns('{safe_table_name}')
            """)
            
            structure = [{"column": row['column'], "type": row['type']} for row in columns]
            
            # Get raw data
            rows = await conn.fetch(f"""
                SELECT * FROM {safe_table_name}
                ORDER BY timestamp DESC
                LIMIT {limit}
            """)
            
            data = []
            for row in rows:
                row_dict = dict(row)
                # Convert timestamp to readable format
                if 'timestamp' in row_dict and row_dict['timestamp']:
                    row_dict['timestamp_iso'] = row_dict['timestamp'].isoformat()
                    row_dict['timestamp_ms'] = int(row_dict['timestamp'].timestamp() * 1000)
                data.append(row_dict)
            
            return {
                "table": safe_table_name,
                "structure": structure,
                "row_count": len(data),
                "data": data
            }
            
        except Exception as e:
            return {
                "error": str(e),
                "error_type": type(e).__name__,
                "traceback": traceback.format_exc()
            }

@app.get("/debug/table/{table_name}/count")
async def debug_table_count(table_name: str):
    """Tæl rækker i en tabel"""
    safe_table_name = re.sub(r'[^a-z0-9_]', '', table_name.lower())
    
    async with pool.acquire() as conn:
        try:
            # Fix: QuestDB uses 'table_name' in tables
            exists = await conn.fetchval("""
                SELECT COUNT(*) FROM tables WHERE table_name = $1
            """, safe_table_name)
            
            if exists == 0:
                return {"error": f"Table '{safe_table_name}' not found"}
            
            total_count = await conn.fetchval(f"SELECT COUNT(*) FROM {safe_table_name}")
            
            last_24h = await conn.fetchval(f"""
                SELECT COUNT(*) FROM {safe_table_name}
                WHERE timestamp >= dateadd('h', -24, now())
            """)
            
            last_hour = await conn.fetchval(f"""
                SELECT COUNT(*) FROM {safe_table_name}
                WHERE timestamp >= dateadd('h', -1, now())
            """)
            
            # Get timestamp range
            oldest = await conn.fetchrow(f"""
                SELECT timestamp FROM {safe_table_name}
                ORDER BY timestamp ASC LIMIT 1
            """)
            
            newest = await conn.fetchrow(f"""
                SELECT timestamp FROM {safe_table_name}
                ORDER BY timestamp DESC LIMIT 1
            """)
            
            return {
                "table": safe_table_name,
                "total_rows": total_count,
                "last_24h": last_24h,
                "last_hour": last_hour,
                "oldest_timestamp": oldest['timestamp'].isoformat() if oldest else None,
                "newest_timestamp": newest['timestamp'].isoformat() if newest else None
            }
            
        except Exception as e:
            return {
                "error": str(e),
                "error_type": type(e).__name__,
                "traceback": traceback.format_exc()
            }

# ==================== CONTAINER LOGS ENDPOINTS ====================

@app.get("/logs/containers")
async def list_containers():
    """List all running containers"""
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            timeout=5
        )
        containers = [name.strip() for name in result.stdout.split('\n') if name.strip()]
        return {"containers": containers}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/logs/container/{container_name}")
async def get_container_logs(
    container_name: str,
    lines: int = 100,
    since: Optional[str] = None,  # Format: "10m", "1h", "2024-01-01"
    follow: bool = False
):
    """
    Get logs from a specific container
    
    Parameters:
    - container_name: Name of the container
    - lines: Number of lines to return (default 100)
    - since: Show logs since timestamp (e.g., "10m", "1h", "2024-01-01")
    - follow: Stream logs (not recommended for API)
    """
    try:
        cmd = ["docker", "logs", container_name, "--tail", str(lines)]
        
        if since:
            cmd.extend(["--since", since])
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10
        )
        
        # Kombiner stdout og stderr
        logs = result.stdout + result.stderr
        
        # Parse logs til struktureret format
        log_lines = []
        for line in logs.split('\n'):
            if line.strip():
                log_lines.append({
                    "timestamp": datetime.utcnow().isoformat(),
                    "container": container_name,
                    "message": line
                })
        
        return {
            "container": container_name,
            "total_lines": len(log_lines),
            "logs": log_lines
        }
        
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Log retrieval timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/logs/search")
async def search_logs(
    query: str,
    containers: Optional[str] = None,  # Comma-separated list
    lines: int = 100
):
    """
    Search logs across containers
    
    Parameters:
    - query: Search term
    - containers: Comma-separated list of container names (default: all)
    - lines: Number of lines to check per container
    """
    try:
        # Get list of containers
        if containers:
            container_list = [c.strip() for c in containers.split(',')]
        else:
            result = subprocess.run(
                ["docker", "ps", "--format", "{{.Names}}"],
                capture_output=True,
                text=True,
                timeout=5
            )
            container_list = [name.strip() for name in result.stdout.split('\n') if name.strip()]
        
        matches = []
        
        for container in container_list:
            try:
                result = subprocess.run(
                    ["docker", "logs", container, "--tail", str(lines)],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                
                logs = result.stdout + result.stderr
                
                for i, line in enumerate(logs.split('\n')):
                    if query.lower() in line.lower():
                        matches.append({
                            "container": container,
                            "line_number": i,
                            "message": line,
                            "timestamp": datetime.utcnow().isoformat()
                        })
            except:
                continue
        
        return {
            "query": query,
            "total_matches": len(matches),
            "matches": matches
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/logs/tail/{container_name}")
async def tail_container_logs(
    container_name: str,
    lines: int = 50
):
    """
    Get latest logs from container (for live monitoring)
    Returns simplified format for Grafana
    """
    try:
        result = subprocess.run(
            ["docker", "logs", container_name, "--tail", str(lines), "--timestamps"],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        logs = result.stdout + result.stderr
        
        # Format for Grafana Table
        log_entries = []
        for line in logs.split('\n'):
            if line.strip():
                # Docker timestamp format: 2024-01-01T12:00:00.000000000Z
                parts = line.split(' ', 1)
                if len(parts) == 2:
                    timestamp_str, message = parts
                    try:
                        # Parse Docker timestamp
                        ts = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                        log_entries.append({
                            "time": int(ts.timestamp() * 1000),
                            "container": container_name,
                            "message": message
                        })
                    except:
                        log_entries.append({
                            "time": int(datetime.utcnow().timestamp() * 1000),
                            "container": container_name,
                            "message": line
                        })
        
        return log_entries
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))