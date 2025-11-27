import json, time, random
from confluent_kafka import Producer
from datetime import datetime
import argparse
from datetime import datetime, timezone

parser = argparse.ArgumentParser()
parser.add_argument("--bootstrap", default="localhost:9092")
parser.add_argument("--topic", default="sensor-data")
parser.add_argument("--rate", type=float, default=1.0)
parser.add_argument("--devices", type=int, default=5)
args = parser.parse_args()

producer_conf = {'bootstrap.servers': args.bootstrap}
producer = Producer(producer_conf)

def gen_reading(device_id):
    base_temp = 60 + device_id * 2
    base_vib = 10 + device_id
    base_rpm = 1500 + device_id * 10

    if random.random() < 0.01:
        temp = base_temp + random.uniform(40, 70)
        vib = base_vib + random.uniform(60, 120)
    else:
        temp = base_temp + random.uniform(-3, 3)
        vib = base_vib + random.uniform(-2, 2)

    rpm = int(base_rpm + random.uniform(-100, 100))

    return {
        "machine_id": device_id,
        "temperature": round(temp, 2),
        "vibration": round(vib, 2),
        "rpm": rpm,
        
        "timestamp": datetime.now(timezone.utc).isoformat()

    }

def delivery_report(err, msg):
    if err:
        print("Delivery failed:", err)

print(f"Producing messages to topic {args.topic}...")

try:
    while True:
        start = time.time()
        for d in range(1, args.devices + 1):
            msg = json.dumps(gen_reading(d))
            producer.produce(args.topic, msg.encode("utf-8"), callback=delivery_report)
        producer.poll(0)   # triggers delivery callbacks

        elapsed = time.time() - start
        time.sleep(max(0, 1.0/args.rate - elapsed))

except KeyboardInterrupt:
    print("Stopping producer...")
finally:
    producer.flush()
