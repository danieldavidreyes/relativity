import numpy as np
import pandas as pd
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic


class DataGenerator:
    def __init__(self, kafka_bootstrap_servers=9092):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.create_topic()
        
        self.kafka_producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_servers)
    def create_topic(self):
        admin_client = KafkaAdminClient(bootstrap_servers=self.kafka_bootstrap_servers)
        topic_name = 'CPU/MEM_DATA'
        topic_list = [NewTopic(name=topic_name, num_partitions=1, replication_factor=1)]
        
        try:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print(f"Topic '{topic_name}' created successfully.")
        except Exception as e:
            print(f"Error creating topic: {e}")

    def generate_data(self):
        while True:
            cpu_usage_percent = np.random.normal(loc=25,scale=8)
            cpu_iowait_percent = np.random.normal(loc=2,scale=1.5)
            mem_used_percent = np.random.normal(loc=65,scale=3)
            mem_swap_used_percent = np.random.normal(loc=0.5,scale=0.3)
            cpu_load_avg_1m = np.random.normal(loc=1.8,scale=0.6)
            disk_io_time_ms = np.random.normal(loc=15,scale=8)
            
            timestamp = int(pd.Timestamp.now().timestamp())
            hour_of_day = (timestamp % 86400) / 86400
            daily_pattern = np.sin(2 * np.pi * hour_of_day) * 15

            if mem_used_percent > 90:
                mem_swap_used_percent = (mem_used_percent - 90) * 3
                disk_io_time_ms = disk_io_time_ms * (1 + (mem_used_percent * 0.5))
                cpu_iowait_percent = cpu_iowait_percent + (mem_swap_used_percent * 2)

            cpu_usage_percent += daily_pattern * 15
            cpu_load_avg_1m += daily_pattern * 0.8
            mem_used_percent += daily_pattern * 3
            disk_io_time_ms += daily_pattern * 3

            data = {
                'timestamp': timestamp,
                'cpu_usage_percent': max(0, min(100, cpu_usage_percent)),
                'cpu_iowait_percent': max(0, min(100, cpu_iowait_percent)),
                'mem_used_percent': max(0, min(100, mem_used_percent)),
                'mem_swap_used_percent': max(0, min(100, mem_swap_used_percent)),
                'cpu_load_avg_1m': max(0, cpu_load_avg_1m),
                'disk_io_time_ms': max(0, disk_io_time_ms)
            }

            self.kafka_producer.send('CPU/MEM_DATA', value=str(data).encode('utf-8'))
if __name__ == "__main__":
    generator = DataGenerator()
    generator.generate_data()
    