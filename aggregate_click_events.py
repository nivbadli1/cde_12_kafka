#%%
from confluent_kafka import Consumer, Producer
from collections import defaultdict

bootstrap_servers = 'localhost:29092'
clickstream_topic = 'user_clickstream'
click_aggregation_topic = 'user_click_aggregation'

def calculate_clicks_per_user():
    """
    Calculate the number of clicks per user and publish the results.
    """
    # Kafka consumer configuration
    conf = {'bootstrap.servers': bootstrap_servers,  
            'group.id': 'click_aggregator',  
            'auto.offset.reset': 'earliest'}  
    # Create Kafka Consumer instance
    consumer = Consumer(conf)  
    # Subscribe to the clickstream topic
    consumer.subscribe([clickstream_topic])  

    # Dictionary to store click counts per user
    click_count_per_user = defaultdict(int)  
    try:
        while True:
            # Poll for messages with a timeout of 1 second
            message = consumer.poll(1.0)  
            # If no message is received, continue polling
            if message is None:  
                continue
            # If there's an error with the message, print the error
            elif message.error():  
                print("Consumer error: {}".format(message.error()))
                continue
            # Print the received message details
            print(f"Received message: {message.value().decode('utf-8')} "
                  f"on partition {message.partition()} "
                  f"with offset {message.offset()} "
                  f"from topic {message.topic()}")   
            
            # Decode the message value
            click_event = message.value().decode('utf-8')  
            # Extract user ID from the click event
            user_id = click_event.split()[1]  
            # Increment the click count for the user
            click_count_per_user[user_id] += 1  

            # Publish aggregated results
            publish_aggregated_results(user_id, click_count_per_user[user_id])  
    finally:
        # Close the consumer connection
        consumer.close()  


def publish_aggregated_results(user_id, click_count):
    """
    Publish aggregated click count per user to Kafka.

    Args:
        user_id (str): The ID of the user.
        click_count (int): The number of clicks for the user.
    """
    # Kafka producer configuration
    conf = {'bootstrap.servers': bootstrap_servers}  
    # Create Kafka Producer instance
    producer = Producer(**conf)  
    # Format the result message
    result_message = f"User {user_id} has clicked {click_count} times."  
    # Publish the result message to Kafka
    producer.produce(click_aggregation_topic, value=result_message.encode('utf-8'))  
    # Poll for events, set timeout to 0 for non-blocking behavior
    producer.flush()


def consume_aggrigeted_results():
    consumer_agg = Consumer({'bootstrap.servers': bootstrap_servers,  
            'group.id': 'consume_aggregator',  
            'auto.offset.reset': 'earliest'})
    
    # Subscribe to the clickstream topic
    consumer_agg.subscribe([click_aggregation_topic])

    try:
        while True:
            # Poll for messages with a timeout of 1 second
            message = consumer_agg.poll(1.0)  
            # If no message is received, continue polling
            if message is None:  
                continue
            # If there's an error with the message, print the error
            elif message.error():  
                print("Consumer error: {}".format(message.error()))
                continue
            # Print the received message details
            print(f"Received message: {message.value().decode('utf-8')} "
                  f"on partition {message.partition()} "
                  f"with offset {message.offset()} "
                  f"from topic {message.topic()}")   
             
    finally:
        # Close the consumer connection
        consumer_agg.close()
      
#%%
calculate_clicks_per_user()

#%%
consume_aggrigeted_results()
