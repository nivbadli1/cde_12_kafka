#%%
from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict

bootstrap_servers = 'course-kafka:9092'
clickstream_topic = 'user_clickstream'
click_aggregation_topic = 'user_click_aggregation'

def calculate_clicks_per_user():
    """
    Calculate the number of clicks per user and publish the results.
    """
    # Create Kafka Consumer instance
    consumer = KafkaConsumer(
        clickstream_topic,
        bootstrap_servers=bootstrap_servers,
        group_id='click_aggregator',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: x.decode('utf-8')
    )
    
    # Dictionary to store click counts per user
    click_count_per_user = defaultdict(int)
    
    try:
        # Process incoming messages
        for message in consumer:
            # Print the received message details
            print(f"Received message: {message.value} "
                  f"on partition {message.partition} "
                  f"with offset {message.offset} "
                  f"from topic {message.topic}")
            
            # Get the click event from message value
            click_event = message.value
            
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
    # Create Kafka Producer instance
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: x.encode('utf-8')
    )
    
    # Format the result message
    result_message = f"User {user_id} has clicked {click_count} times."
    
    # Publish the result message to Kafka
    producer.send(click_aggregation_topic, value=result_message)
    
    # Ensure all messages are sent
    producer.flush()

def consume_aggregated_results():
    """
    Consume and display aggregated click results.
    """
    # Create Kafka Consumer instance for aggregated results
    consumer_agg = KafkaConsumer(
        click_aggregation_topic,
        bootstrap_servers=bootstrap_servers,
        group_id='consume_aggregator',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: x.decode('utf-8')
    )
    
    try:
        # Process incoming aggregated messages
        for message in consumer_agg:
            # Print the received message details
            print(f"Received message: {message.value} "
                  f"on partition {message.partition} "
                  f"with offset {message.offset} "
                  f"from topic {message.topic}")
    finally:
        # Close the consumer connection
        consumer_agg.close()
      
#%%
calculate_clicks_per_user()
#%%
consume_aggregated_results()
