import paho.mqtt.client as mqtt
import pysparkplug

# Server/Broker configuration
broker = "localhost"  # Change this to your server's IP if running on different machines
port = 1883
username = "server"
password = "serverpass"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Server connected to MQTT broker successfully")
        # Subscribe to all Sparkplug B messages
        client.subscribe("spBv1.0/+/+/+/+")
        print("📡 Subscribed to Sparkplug B topics")
    else:
        print(f"❌ Failed to connect to broker, return code {rc}")

def on_message(client, userdata, msg):
    try:
        print(f"\n📨 Received message on topic: {msg.topic}")
        
        # Parse topic to understand message type
        topic_parts = msg.topic.split("/")
        if len(topic_parts) >= 5:
            namespace = topic_parts[0]  # spBv1.0
            group_id = topic_parts[1]   # factory
            message_type = topic_parts[2]  # NBIRTH, NDATA, etc.
            edge_node_id = topic_parts[3]  # edge1
            device_id = topic_parts[4] if len(topic_parts) > 4 else None  # tempSensor01
            
            print(f"   Group: {group_id}")
            print(f"   Message Type: {message_type}")
            print(f"   Edge Node: {edge_node_id}")
            if device_id:
                print(f"   Device: {device_id}")
        
        # Decode the Sparkplug B payload
        if message_type == "NBIRTH":
            nbirth = pysparkplug.NBirth()
            nbirth.decode(msg.payload)
            print(f"   📊 NBirth - Sequence: {nbirth.seq}, Timestamp: {nbirth.timestamp}")
            print(f"   📈 Metrics ({len(nbirth.metrics)}):")
            for metric in nbirth.metrics:
                print(f"      - {metric.name}: {metric.value} ({metric.datatype.name})")
                
        elif message_type == "NDATA":
            ndata = pysparkplug.NData()
            ndata.decode(msg.payload)
            print(f"   📊 NData - Sequence: {ndata.seq}, Timestamp: {ndata.timestamp}")
            print(f"   📈 Metrics ({len(ndata.metrics)}):")
            for metric in ndata.metrics:
                print(f"      - {metric.name}: {metric.value} ({metric.datatype.name})")
                
        elif message_type == "DBIRTH":
            dbirth = pysparkplug.DBirth()
            dbirth.decode(msg.payload)
            print(f"   📊 DBirth - Sequence: {dbirth.seq}, Timestamp: {dbirth.timestamp}")
            print(f"   📈 Metrics ({len(dbirth.metrics)}):")
            for metric in dbirth.metrics:
                print(f"      - {metric.name}: {metric.value} ({metric.datatype.name})")
                
        elif message_type == "DDATA":
            ddata = pysparkplug.DData()
            ddata.decode(msg.payload)
            print(f"   📊 DData - Sequence: {ddata.seq}, Timestamp: {ddata.timestamp}")
            print(f"   📈 Metrics ({len(ddata.metrics)}):")
            for metric in ddata.metrics:
                print(f"      - {metric.name}: {metric.value} ({metric.datatype.name})")
        else:
            print(f"   ⚠️  Unknown message type: {message_type}")
            
    except Exception as e:
        print(f"❌ Error decoding message: {e}")
        print(f"   Raw payload length: {len(msg.payload)} bytes")

def on_disconnect(client, userdata, rc):
    print("🔌 Server disconnected from MQTT broker")

# Create MQTT client for server
client = mqtt.Client(client_id="sparkplug_server")
client.username_pw_set(username, password)

# Set callbacks
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

try:
    print("🚀 Starting Sparkplug B MQTT Server...")
    client.connect(broker, port, 60)
    
    print("🔄 Server listening for messages... (Press Ctrl+C to stop)")
    client.loop_forever()
    
except KeyboardInterrupt:
    print("\n⏹️  Stopping server...")
    client.disconnect()
    print("✅ Server stopped")
except Exception as e:
    print(f"❌ Error: {e}")
