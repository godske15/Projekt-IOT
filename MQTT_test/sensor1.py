import time
import paho.mqtt.client as mqtt
import pysparkplug

# Broker info
broker = "localhost"
port = 1883
username = "edge1"
password = "edge1pass"

client = mqtt.Client(client_id="edge1")
client.username_pw_set(username, password)
client.connect(broker, port, 60)

# Create Sparkplug B metric
metric = pysparkplug.Metric(
    name="temperatur",
    datatype=pysparkplug.DataType.INT32,
    value=25,
    timestamp=pysparkplug.get_current_timestamp()
)

# Create NBIRTH message
nbirth = pysparkplug.NBirth(
    timestamp=pysparkplug.get_current_timestamp(),
    seq=0,
    metrics=[metric]
)

# Send NBIRTH
topic = "spBv1.0/factory/NBIRTH/edge1/tempSensor01"
client.publish(topic, nbirth.encode())

print("NBIRTH sendt ✅")
time.sleep(2)
client.disconnect()
