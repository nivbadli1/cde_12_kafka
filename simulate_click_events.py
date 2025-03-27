#%%
from kafka import KafkaProducer
import random
import time

bootstrap_servers = 'course-kafka:9092'
clickstream_topic = 'user_clickstream'

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message: {} delivered to {} [{}]'.format(msg.value, msg.topic, msg.partition))

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
    # Create KafkaProducer instance
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: x.encode('utf-8')
    )
    
    try:
        while True:
            # Generate a simulated click event
            click_event = generate_click_event()
            
            # Publish the click event to Kafka
            future = producer.send(clickstream_topic, value=click_event)
            # Get event results
            record_metadata = future.get(timeout=10)
            # Report
            print(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
            
            # Ensure all messages are sent
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
