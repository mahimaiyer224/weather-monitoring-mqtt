import json, time, threading, os
from flask import Flask, jsonify, send_from_directory
import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883

EDGE_TOPIC = "weather/edge/+"
STATUS_TOPIC = "weather/status/+"
SHADOW_TOPIC = "weather/shadow/+"


HTTP_PORT = 6000

app = Flask(__name__)

latest_data = {
    "ne": {},
    "sw": {},
    "last_updated": None
}

sensor_status = {}
sensor_shadows = {}

def on_connect(client, userdata, flags, rc):

    print("Cloud connected:", rc)

    client.subscribe(EDGE_TOPIC, qos=1)
    client.subscribe(STATUS_TOPIC, qos=1)
    client.subscribe(SHADOW_TOPIC, qos=1)

    print("Subscribed to all topics")


def on_message(client, userdata, msg):

    global latest_data, sensor_status, sensor_shadows

    topic = msg.topic
    payload = msg.payload.decode()

    data = json.loads(payload) if payload.startswith("{") else payload


    if topic.startswith("weather/edge"):

        if "ne" in topic:
            latest_data["ne"] = data

        elif "sw" in topic:
            latest_data["sw"] = data

        latest_data["last_updated"] = time.time()

        print("Edge stats:", topic)


    elif topic.startswith("weather/status"):

        sensor = topic.split("/")[-1]

        sensor_status[sensor] = payload

        print("Status:", sensor, payload)


    elif topic.startswith("weather/shadow"):

        sensor = topic.split("/")[-1]

        sensor_shadows[sensor] = data

        print("Shadow updated:", sensor)


def start_mqtt():
    client = mqtt.Client()

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER, PORT, 60)

    client.loop_forever()

@app.route("/global_stats", methods=["GET"])

def global_stats():
    ne = latest_data.get("ne", {})
    sw = latest_data.get("sw", {})

    if not ne or not sw:
        return jsonify({"message": "Waiting for edge data"})
    
    total_samples = ne["samples"] + sw["samples"]

    avg_temp = (
        ne["avg_temperature"] * ne["samples"] + 
        sw["avg_temperature"] * sw["samples"]
    ) / total_samples

    total_rain = ne["total_rainfall"] + sw["total_rainfall"]

    return jsonify({
        "total_samples": total_samples,
        "city_avg_temperature": round(avg_temp, 2),
        "city_total_rainfall": round(total_rain, 2),
        "last_updated": latest_data["last_updated"]
    })

@app.route("/dashboard")
def dashboard():
    return send_from_directory(
        os.path.dirname(__file__), "index.html"
    )
@app.route("/health")
def health():
    return jsonify(sensor_status)


@app.route("/shadows")
def shadows():
    return jsonify(sensor_shadows)

if __name__ == "__main__":
    t = threading.Thread(target=start_mqtt)
    t.daemon = True
    t.start()

    print("Cloud Server Running....")

    app.run(host="0.0.0.0", port=HTTP_PORT, debug=True)