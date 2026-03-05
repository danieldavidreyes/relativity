import json
from confluent_kafka import Consumer

consumer = Consumer({
    'bootstrap.servers': 'kafka:29092',
    'group.id': 'model-server',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['enriched-metrics'])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Error: {msg.error()}")
        continue

    data = json.loads(msg.value().decode('utf-8'))
    print(data)