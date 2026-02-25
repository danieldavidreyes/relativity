from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
import math
import json
import time
import logging
from collections import deque
import numpy as np

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
        data.pop('ground_truth', None)     
        self.kafka_producer = Producer({'bootstrap.servers': bootstrap_servers})
        self.tick = 0
        self.ewma = {} 
        admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
        topic_name = 'enriched-metrics'
        topic_list = [NewTopic(topic_name, num_partitions=1, replication_factor=1)]
        try:
            fs = admin_client.create_topics(topic_list)
            for topic, f in fs.items():
                try:
                    f.result()
                    print(f"Topic '{topic}' created successfully.")
                except Exception as e:
                    print(f"Error creating topic '{topic}': {e}")
        except Exception as e:
            print(f"Error creating topic: {e}")

    def get_buffer(self, server_id):
        if server_id not in self.buffers:
            self.buffers[server_id] = {
                metric: deque(maxlen=30) for metric in METRICS
            }
            self.ewma[server_id] = {
                metric: None for metric in METRICS
            }
        return self.buffers[server_id]

    def run(self):
        self.tick=0
        while True:
            self.tick+=1
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

            if len(buffer['cpu_usage_percent']) >= 30:
                for metric in METRICS:
                    data[f'{metric}_mean'] = sum(buffer[metric]) / 30
                    data[f'{metric}_std'] = (sum((x - data[f'{metric}_mean']) ** 2 for x in buffer[metric]) / 30) ** 0.5
                    data[f'{metric}_zscore'] = (data[f'{metric}'] - data[f'{metric}_mean']) / data[f'{metric}_std'] if data[f'{metric}_std'] != 0 else 0
                    if self.ewma[server_id][metric] is None:
                        self.ewma[server_id][metric] = data[metric]
                    else:
                        self.ewma[server_id][metric] = 0.2 * data[metric] + 0.8 * self.ewma[server_id][metric]
                    data[f'{metric}_ewma'] = self.ewma[server_id][metric]
                    data[f'{metric}_diff'] = data[f'{metric}'] - buffer[metric][-2]
                    counts, bin_edges = np.histogram(buffer[metric], bins=10)
                    probs = [count / 30 for count in counts]


                    data[f'{metric}_entropy'] = -sum(p * math.log2(p) for p in probs if p > 0)
                data['hour_sin']= math.sin(2 * math.pi * (data['timestamp'] % 86400) / 86400)
                data['hour_cos'] = math.cos(2 * math.pi * (data['timestamp'] % 86400) / 86400)
                self.kafka_producer.produce('enriched-metrics', json.dumps(data).encode('utf-8'))
                self.kafka_producer.flush()
                
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    bootstrap_servers = 'kafka:29092'
    engine = featureEngine(bootstrap_servers)
    engine.run()