import uuid
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import time
import random
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

# Kafka configuration
conf = {
    'bootstrap.servers': 'kafka:9092'
}
# List of predefined campaign IDs
campaign_ids = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140]
def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def generate_view_log():
    view_id = str(uuid.uuid4())
    start_time = datetime.now()
    end_time = start_time + timedelta(seconds=random.randint(1, 600))
    banner_id = random.randint(1, 10)
    campaign_id = random.choice(campaign_ids)  # Randomly select from predefined campaign_ids

    return {
        'view_id': view_id,
        'start_timestamp': start_time.isoformat(),
        'end_timestamp': end_time.isoformat(),
        'banner_id': banner_id,
        'campaign_id': campaign_id
    }

def stream_data():
    import json
    from confluent_kafka import Producer
    import logging

    producer = Producer(conf)
    start_time = time.time()
    end_time = start_time + 60  # Run for 10 minutes

    while time.time() < end_time:
        try:
            view_log = generate_view_log()
            producer.produce('view_log', key=view_log['view_id'], value=json.dumps(view_log), callback=delivery_report)
            producer.flush()
            time.sleep(random.uniform(0.1, 2))  # Sleep between sends
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            sys.exit(1)

with DAG('kafka_producer',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='kafka_producer',
        python_callable=stream_data
    )
