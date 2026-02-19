import json
import paho.mqtt.client as mqtt
import time
import threading

BROKER = "localhost"
PORT = 1883
TOPICS = [
    ("weather/sw/south", 1),
    ("weather/sw/west", 1),
    ("weather/status/#", 1)
]

PUBLISH_TOPIC = "weather/edge/sw_stats"

sensor_data = []
sensor_status = {}

def on_connect(client, userdata, flags, rc):
    print("SW Edge connected:", rc)

    for topic, qos, in TOPICS:
        client.subscribe(topic)
        print("Subscribed to: ", topic)

def on_message(client, userdata, msg):

    global sensor_data, sensor_status

    topic = msg.topic
    payload = msg.payload.decode()


    if topic.startswith("weather/status"):

        sensor = topic.split("/")[-1]
        status = payload

        sensor_status[sensor] = status

        print("Status:", sensor, status)
        return


    data = json.loads(payload)

    station = data["station"]

    # Ignore offline sensors
    if sensor_status.get(station) == "offline":
        print("Ignoring offline sensor:", station)
        return


    sensor_data.append(data)

    if len(sensor_data) > 200:
        sensor_data.pop(0)

    print("Received:", data)


def aggregate_loop(client):
    global sensor_data
    
    while True:
        time.sleep(30)

        if not sensor_data:
            continue

        temps = [d["temperature"] for d in sensor_data]
        rain = [d["rainfall"] for d in sensor_data]
        avg_temp = sum(temps)/len(temps)
        total_rain = sum(rain)

        result = {
            "samples": len(sensor_data),
            "avg_temperature": round(avg_temp, 2),
            "total_rainfall": round(total_rain, 2)
        }

        payload = json.dumps(result)

        client.publish(PUBLISH_TOPIC, payload, qos=1)

        print("Published to cloud: ", payload)

        sensor_data = []
    
client = mqtt.Client()

client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER, PORT, 60)

t = threading.Thread(target=aggregate_loop, args=(client,))
t.daemon = True
t.start()

print("SW Edge Running...")

client.loop_forever()