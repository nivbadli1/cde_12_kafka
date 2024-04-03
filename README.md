# Real-time User Clickstream Data Processing with Kafka

This repository contains exercises for practicing real-time user clickstream data processing using Kafka. The exercises simulate user click events, aggregate them in real-time, and demonstrate publishing and consuming messages with Kafka.

## Exercise 1: Simulating User Clickstream Data

### Instructions:

1. **Setup Kafka Environment**:
   - Ensure you have Kafka installed and running locally or on a server.
   - Create a Kafka topic named 'user_clickstream' using the following command:
     ```
     kafka-topics.sh --create --topic user_clickstream --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
     ```

2. **Write Simulated Click Events Script**:
   - Create a new Python file named `simulate_click_events.py`.
   - Within the script:
     - Import necessary libraries: `kafka`, `random`, and `time`.
     - Create a Kafka producer named `producer` to publish simulated click events to the 'user_clickstream' topic.
     - Define a function named `generate_click_event()` to generate random user click events.
     - Create an infinite loop within a function named `produce_click_events()` to continuously generate and publish click events.
   - Save the script and execute it using:
     ```
     python simulate_click_events.py
     ```

3. **Simulate Click Events**:
   - This script generates random user click events every second and publishes them to the 'user_clickstream' Kafka topic.

### Answers:

Here's a sample Python script `simulate_click_events.py` to generate simulated user click events and publish them to the Kafka topic 'user_clickstream':

```python
from kafka import KafkaProducer
import random
import time

bootstrap_servers = 'localhost:9092'
clickstream_topic = 'user_clickstream'

def generate_click_event():
    user_id = random.randint(1, 100)
    url = "/page" + str(random.randint(1, 10))
    timestamp = time.time()
    return f"User {user_id} clicked on {url} at {timestamp}"

def produce_click_events():
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    while True:
        click_event = generate_click_event()
        producer.send(clickstream_topic, value=click_event.encode('utf-8'))
        time.sleep(1)

if __name__ == "__main__":
    produce_click_events()
```


## Exercise 2: Aggregating User Click Events and Writing Results to Kafka

### Instructions:

1. **Setup Kafka Environment**:
   - Ensure Kafka is still running.
   - Create a Kafka topic named 'user_click_aggregation' using the following command:
     ```
     kafka-topics.sh --create --topic user_click_aggregation --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
     ```

2. **Write Aggregation Script**:
   - Create a new Python file named `aggregate_click_events.py`.
   - Within the script:
     - Import necessary libraries: `kafka`, `defaultdict`.
     - Create a Kafka consumer named `consumer` to consume click events from the 'user_clickstream' topic.
     - Define a function named `calculate_clicks_per_user()` to aggregate click events per user.
     - Define a function named `publish_aggregated_results()` to publish the aggregated results to the 'user_click_aggregation' Kafka topic.
   - Save the script.

3. **Write Consumer Script**:
   - Create a new Python file named `consume_aggregated_results.py`.
   - Within the script:
     - Import the `kafka` library.
     - Create a Kafka consumer named `consumer` to consume aggregated results from the 'user_click_aggregation' topic.
     - Define a function named `consume_aggregated_results()` to continuously consume and print aggregated results.
   - Save the script and execute it using:
     ```
     python consume_aggregated_results.py
     ```

4. **Consume Aggregated Results**:
   - This script continuously listens for messages from the 'user_click_aggregation' Kafka topic and prints the aggregated results to the console.

### Answers:

Here's a sample Python script `aggregate_click_events.py` to aggregate user click events and publish aggregated results to the Kafka topic 'user_click_aggregation':

```python
from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict

bootstrap_servers = 'localhost:9092'
clickstream_topic = 'user_clickstream'
click_aggregation_topic = 'user_click_aggregation'

def calculate_clicks_per_user():
    consumer = KafkaConsumer(clickstream_topic, bootstrap_servers=bootstrap_servers,
                             value_deserializer=lambda x: x.decode('utf-8'))

    click_count_per_user = defaultdict(int)
    for message in consumer:
        click_event = message.value
        user_id = click_event.split()[1]
        click_count_per_user[user_id] += 1

        publish_aggregated_results(user_id, click_count_per_user[user_id])

def publish_aggregated_results(user_id, click_count):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    result_message = f"User {user_id} has clicked {click_count} times."
    producer.send(click_aggregation_topic, value=result_message.encode('utf-8'))

if __name__ == "__main__":
    calculate_clicks_per_user()
```