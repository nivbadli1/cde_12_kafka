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