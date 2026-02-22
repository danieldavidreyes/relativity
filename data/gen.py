import numpy as np
import time
import json
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

class DataGenerator:
    def __init__(self, kafka_bootstrap_servers="kafka:29092"):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.create_topic()
        self.kafka_producer = Producer({'bootstrap.servers': self.kafka_bootstrap_servers})
        self.tick = 0
        np.random.seed(22)
    def create_topic(self):
        admin_client = AdminClient({'bootstrap.servers': self.kafka_bootstrap_servers})
        topic_name = 'raw-metrics'
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

    def generate_data(self):
        while True:
            self.tick +=1
            cpu_usage_percent = np.random.normal(loc=25,scale=8)
            cpu_iowait_percent = np.random.normal(loc=2,scale=1.5)
            mem_used_percent = np.random.normal(loc=65,scale=3)
            mem_swap_used_percent = np.random.normal(loc=0.5,scale=0.3)
            cpu_load_avg_1m = np.random.normal(loc=1.8,scale=0.6)
            disk_io_time_ms = np.random.normal(loc=15,scale=8)
            timestamp = int(time.time())
            hour_of_day = (timestamp % 86400) / 86400
            daily_pattern = np.sin(2 * np.pi * hour_of_day)
            if mem_used_percent < 80:
                swap_effect = 0
            elif mem_used_percent < 92:
                swap_effect = (mem_used_percent - 80) * 0.3
            else:
                swap_effect = (mem_used_percent - 92) * 3.0 + 3.6

            if mem_used_percent < 80:
                swap_effect = 0
            elif mem_used_percent < 92:
                swap_effect = (mem_used_percent - 80) * 0.3
            else:
                swap_effect = (mem_used_percent - 92) * 3.0 + 3.6

            mem_swap_used_percent += swap_effect

            swap = mem_swap_used_percent
            if swap > 2:
                cpu_iowait_percent += (swap - 2) * 1.5
            if swap > 1:
                disk_io_time_ms += (swap - 1) * 8

            iowait = cpu_iowait_percent
            if iowait > 5:
                cpu_load_avg_1m += (iowait - 5) * 0.15
            data = {
                'tick': self.tick,
                'timestamp': timestamp,
                'server_id': 'srv-01',
                'cpu_usage_percent': max(0, min(100, cpu_usage_percent)),
                'cpu_iowait_percent': max(0, min(100, cpu_iowait_percent)),
                'mem_used_percent': max(0, min(100, mem_used_percent)),
                'mem_swap_used_percent': max(0, min(100, mem_swap_used_percent)),
                'cpu_load_avg_1m': max(0, cpu_load_avg_1m),
                'disk_io_time_ms': max(0, disk_io_time_ms)
            }
            self.kafka_producer.produce('raw-metrics',key='srv-01', value=json.dumps(data).encode('utf-8'))
            self.kafka_producer.flush()
            time.sleep(1)
if __name__ == "__main__":
    generator = DataGenerator()
    generator.generate_data()
    