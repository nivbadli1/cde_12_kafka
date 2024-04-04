#%%
from confluent_kafka import Producer
import random
import time

bootstrap_servers = 'localhost:29092'
clickstream_topic = 'user_clickstream'

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message: {} delivered to {} [{}]'.format(msg, msg.topic(), msg.partition()))


def generate_click_event():
    """
    Generate a simulated click event.

    Returns:
        str: A string representing the click event.
    """
    # Generate a random user ID, URL, and timestamp for the click event
    user_id = random.randint(1, 100)
    url = "/page" + str(random.randint(1, 10))
    timestamp = time.time()
    return f"User {user_id} clicked on {url} at {timestamp}"  

def produce_click_events():
    """
    Produce simulated click events to Kafka.
    """
    # Kafka producer configuration
    conf = {'bootstrap.servers': bootstrap_servers}
    # Create Kafka Producer instance
    producer = Producer(**conf)
    try:
        while True:
            # Generate a simulated click event
            click_event = generate_click_event()
            # Publish the click event to Kafka
            producer.produce(clickstream_topic, value=click_event.encode('utf-8'),callback=delivery_report)
            # Poll for events, set timeout to 0 for non-blocking behavior
            producer.flush()
            # Wait for 1 second before producing the next click event
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        # Close the Kafka producer
        producer.close()

#%%
produce_click_events()

# %%
