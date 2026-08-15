import argparse
import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("[mqtt] connected, subscribing to sensors/#")
        client.subscribe("sensors/#")
    else:
        print(f"[mqtt] connection failed: {reason_code}")


def on_message(client, userdata, msg):
    print(f"[{msg.topic}] {msg.payload.decode()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MQTT test subscriber")
    parser.add_argument("--mqtt-host", type=str, default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    args = parser.parse_args()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(args.mqtt_host, args.mqtt_port, keepalive=60)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
