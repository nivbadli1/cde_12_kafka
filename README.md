# Kafka Docker Compose Installation Guide

This repository contains a Docker Compose file and instructions for setting up a Kafka cluster along with Kafdrop for Kafka visualization. It also includes an exercise to help you familiarize yourself with Kafka and Docker.

## Instructions

Follow these steps to set up the Kafka cluster using Docker Compose and verify that all containers are running correctly:

1. Clone this repository to your local machine.

3. Navigate to the root directory of the cloned repository in your terminal.

4. Run the following command to start the Kafka cluster:
    ```bash
    docker-compose up
    ```

5. Once all containers are up and running, verify their status by running:
    ```bash
    docker ps
    ```

    ```bash
    CONTAINER ID   IMAGE                             COMMAND                  CREATED         STATUS              PORTS                                              NAMES
    2e3e2f570611   obsidiandynamics/kafdrop:3.30.0   "/kafdrop.sh"            2 minutes ago   Up About a minute   0.0.0.0:9003->9000/tcp                             cde_12_kafka-kafdrop-1
    f36a4b46d23a   confluentinc/cp-kafka:6.2.0       "/etc/confluent/dock…"   2 minutes ago   Up 2 minutes        0.0.0.0:9092->9092/tcp, 0.0.0.0:29092->29092/tcp   kafka-learn
    e56de1bfc415   confluentinc/cp-zookeeper:6.2.0   "/etc/confluent/dock…"   2 minutes ago   Up 2 minutes        2888/tcp, 0.0.0.0:2181->2181/tcp, 3888/tcp         zookeeper-learn
    ```

6. Open your web browser and navigate to [http://localhost:9003](http://localhost:9003) to access Kafdrop, a Kafka UI for monitoring topics and messages.

## Exercise: Real-time User Clickstream Data Processing with Confluent Kafka

In this exercise, you will simulate user click events and process them in real-time using Confluent Kafka in Python.

### Instructions:

1. **Setup Kafka Environment**:
   - Ensure you have Kafka installed and running locally or on a server.
   - Install the `confluent-kafka` Python library using pip:
     ```
     pip install confluent-kafka
     ```
   - Create a Kafka topic named 'user_clickstream' using the following command:
     ```
     kafka-topics --create --topic user_clickstream --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
     ```

2. **Write Clickstream Producer Script**:
   - Create a new Python file named `clickstream_producer.py`.
   - Within the script:
     - Import necessary libraries: `confluent_kafka`.
     - Define a function named `generate_click_event()` to generate random user click events.
     - Create a Kafka producer object.
     - Use the producer to publish click events to the 'user_clickstream' topic.
   - Save the script.

3. **Write Clickstream Consumer Script**:
   - Create a new Python file named `clickstream_consumer.py`.
   - Within the script:
     - Import necessary libraries: `confluent_kafka`.
     - Create a Kafka consumer object.
     - Subscribe to the 'user_clickstream' topic.
     - Continuously poll for new messages and print them.
   - Save the script.

4. **Run Producer and Consumer**:
   - Open two terminal windows.
   - In the first window, run the producer script:
     ```
     python clickstream_producer.py
     ```
   - In the second window, run the consumer script:
     ```
     python clickstream_consumer.py
     ```

5. **Simulate Click Events**:
   - The producer script generates random user click events and publishes them to the 'user_clickstream' Kafka topic.
   - The consumer script consumes and prints these click events in real-time.

### Answers:

#### Clickstream Producer Script (`clickstream_producer.py`):

```python
from confluent_kafka import Producer
import random
import time

bootstrap_servers = 'localhost:9092'
clickstream_topic = 'user_clickstream'

def generate_click_event():
    user_id = random.randint(1, 100)
    url = "/page" + str(random.randint(1, 10))
    timestamp = time.time()
    return f"User {user_id} clicked on {url} at {timestamp}"

def delivery_callback(err, msg):
    if err:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

producer_conf = {
    'bootstrap.servers': bootstrap_servers,
}

producer = Producer(producer_conf)

while True:
    click_event = generate_click_event()
    producer.produce(clickstream_topic, value=click_event.encode('utf-8'), callback=delivery_callback)
    producer.poll(0)
    time.sleep(1)
```