import time
import random
import paho.mqtt.client as mqtt
import pysparkplug

# Broker info - Change this to your server's IP address
broker = "localhost"  # Replace with server IP if on different machines
port = 1883
username = "edge1"
password = "edge1pass"

# Sparkplug B configuration
group_id = "factory"
edge_node_id = "edge1"
device_id = "tempSensor01"

# Sequence number for message ordering
seq_num = 0

def get_next_seq():
    global seq_num
    seq_num = (seq_num + 1) % 256  # Sparkplug B sequence numbers are 0-255
    return seq_num

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Edge device connected to MQTT broker")
    else:
        print(f"❌ Failed to connect, return code {rc}")

def on_publish(client, userdata, mid):
    print(f"📤 Message {mid} published successfully")

def send_nbirth(client):
    """Send Node Birth certificate"""
    # Create metrics for NBIRTH
    metrics = [
        pysparkplug.Metric(
            name="Node Control/Rebirth",
            datatype=pysparkplug.DataType.BOOLEAN,
            value=False,
            timestamp=pysparkplug.get_current_timestamp()
        ),
        pysparkplug.Metric(
            name="temperatur",
            datatype=pysparkplug.DataType.INT32,
            value=25,
            timestamp=pysparkplug.get_current_timestamp()
        )
    ]
    
    # Create NBIRTH message
    nbirth = pysparkplug.NBirth(
        timestamp=pysparkplug.get_current_timestamp(),
        seq=get_next_seq(),
        metrics=metrics
    )
    
    # Send NBIRTH
    topic = f"spBv1.0/{group_id}/NBIRTH/{edge_node_id}"
    client.publish(topic, nbirth.encode())
    print(f"🔥 NBIRTH sent to topic: {topic}")

def send_dbirth(client):
    """Send Device Birth certificate"""
    # Create metrics for DBIRTH
    metrics = [
        pysparkplug.Metric(
            name="temperatur",
            datatype=pysparkplug.DataType.INT32,
            value=25,
            timestamp=pysparkplug.get_current_timestamp()
        ),
        pysparkplug.Metric(
            name="humidity",
            datatype=pysparkplug.DataType.FLOAT,
            value=65.5,
            timestamp=pysparkplug.get_current_timestamp()
        )
    ]
    
    # Create DBIRTH message
    dbirth = pysparkplug.DBirth(
        timestamp=pysparkplug.get_current_timestamp(),
        seq=get_next_seq(),
        metrics=metrics
    )
    
    # Send DBIRTH
    topic = f"spBv1.0/{group_id}/DBIRTH/{edge_node_id}/{device_id}"
    client.publish(topic, dbirth.encode())
    print(f"🔥 DBIRTH sent to topic: {topic}")

def send_ddata(client, temperature, humidity):
    """Send Device Data"""
    # Create metrics for DDATA
    metrics = [
        pysparkplug.Metric(
            name="temperatur",
            datatype=pysparkplug.DataType.INT32,
            value=temperature,
            timestamp=pysparkplug.get_current_timestamp()
        ),
        pysparkplug.Metric(
            name="humidity",
            datatype=pysparkplug.DataType.FLOAT,
            value=humidity,
            timestamp=pysparkplug.get_current_timestamp()
        )
    ]
    
    # Create DDATA message
    ddata = pysparkplug.DData(
        timestamp=pysparkplug.get_current_timestamp(),
        seq=get_next_seq(),
        metrics=metrics
    )
    
    # Send DDATA
    topic = f"spBv1.0/{group_id}/DDATA/{edge_node_id}/{device_id}"
    client.publish(topic, ddata.encode())
    print(f"📊 DDATA sent - Temperature: {temperature}°C, Humidity: {humidity}%")

# Create MQTT client
client = mqtt.Client(client_id=edge_node_id)
client.username_pw_set(username, password)
client.on_connect = on_connect
client.on_publish = on_publish

try:
    print("🚀 Starting Edge Device...")
    client.connect(broker, port, 60)
    client.loop_start()  # Start the network loop in background
    
    # Wait a moment for connection
    time.sleep(1)
    
    # Send birth certificates
    send_nbirth(client)
    time.sleep(1)
    send_dbirth(client)
    time.sleep(2)
    
    # Send periodic data
    print("📡 Starting to send periodic sensor data...")
    for i in range(10):  # Send 10 data points
        # Simulate sensor readings
        temperature = random.randint(20, 30)
        humidity = round(random.uniform(40.0, 80.0), 1)
        
        send_ddata(client, temperature, humidity)
        time.sleep(5)  # Send data every 5 seconds
    
    print("✅ Data transmission complete")
    
except KeyboardInterrupt:
    print("\n⏹️  Stopping edge device...")
except Exception as e:
    print(f"❌ Error: {e}")
finally:
    client.loop_stop()
    client.disconnect()
    print("🔌 Edge device disconnected")
