import time
import paho.mqtt.client as mqtt
import pysparkplug as sparkplug

# Broker info
broker = "localhost"
port = 1883
username = "edge1"
password = "edge1pass"

client = mqtt.Client(client_id="edge1")
client.username_pw_set(username, password) # Husk at indsætte nyt
client.connect(broker, port, 60)

# Lav Sparkplug B payload
payload = sparkplug.getSparkplugPayload()
metric = payload.metrics.add()
metric.name = "temperatur"
metric.int_value = 25
metric.datatype = sparkplug.DataType.Int32

# Send NBIRTH
topic = "spBv1.0/factory/NBIRTH/edge1/tempSensor01"
client.publish(topic, payload.SerializeToString())

print("NBIRTH sendt ✅")
time.sleep(2)
client.disconnect()

