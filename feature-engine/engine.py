from confluent_kafka import Consumer
import json
import time
import logging
from collections import deque


METRICS = [
    'cpu_usage_percent',
    'cpu_iowait_percent',
    'mem_used_percent',
    'mem_swap_used_percent',
    'cpu_load_avg_1m',
    'disk_io_time_ms'
]


class featureEngine():
    def __init__(self, bootstrap_servers):
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'auto.offset.reset': 'earliest',
            'group.id': 'feature-engine-group',
        })
        self.consumer.subscribe(['raw-metrics'])
        self.buffers = {}

    def get_buffer(self, server_id):
        if server_id not in self.buffers:
            self.buffers[server_id] = {
                metric: deque(maxlen=30) for metric in METRICS
            }
        return self.buffers[server_id]

    def run(self):
        while True:
            message = self.consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                logging.error(f"Consumer error: {message.error()}")
                continue

            data = json.loads(message.value().decode('utf-8'))
            server_id = data['server_id']
            buffer = self.get_buffer(server_id)

            for metric in METRICS:
                buffer[metric].append(data[metric])

            print(f"[{server_id}] Buffer fill: {len(buffer['cpu_usage_percent'])}/30")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    bootstrap_servers = 'kafka:29092'
    engine = featureEngine(bootstrap_servers)
    engine.run()