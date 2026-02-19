import time
import json
import random
import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883
TOPIC = "weather/ne/north"
STATUS_TOPIC = "weather/status/north"
SHADOW_TOPIC = "weather/shadow/north"

client = mqtt.Client()
client.will_set(STATUS_TOPIC, payload="offline", qos=1, retain=True)
client.connect(BROKER, PORT, 60)
client.publish(STATUS_TOPIC, "online", qos=1, retain=True)

print("Sensor in the north direction started publishing...")

while True:
    temp = round(random.uniform(20, 35), 2)
    hum = round(random.uniform(40, 90), 2)
    rain = round(random.uniform(0, 5), 2)
    wind = round(random.uniform(1, 10), 2)
    
    data = {
        "station": "north",
        "temperature": temp,
        "humidity": hum,
        "rainfall": rain,
        "wind_speed": wind
    }

    payload = json.dumps(data)
    client.publish(TOPIC, payload, qos=1)

    shadow={
        "station": "north",
        "temperature": temp,
        "humidity": hum,
        "rainfall": rain,
        "wind_speed": wind,
        "status": "online",
        "timestamp": time.time()
    }

    client.publish(SHADOW_TOPIC, json.dumps(shadow), qos=1, retain=True)
    print("Published: ", payload)
    print("Shadow updated")

    time.sleep(5)