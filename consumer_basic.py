#%%
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Create KafkaConsumer instance
consumer_1 = KafkaConsumer(
    'kafka_python_test',
    bootstrap_servers='course-kafka:9092',
    group_id='mygroup1',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: x.decode('utf-8')
)

# Alternative configuration example (commented out)
"""
consumer = KafkaConsumer(
    bootstrap_servers='course-kafka:9092',
    group_id='my_consumer_group',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,
    session_timeout_ms=6000,
    key_deserializer=lambda x: x.decode('utf-8') if x else None,
    value_deserializer=lambda x: x.decode('utf-8') if x else None
)
"""

try:
    print("Polling for messages from topic: kafka_python_test")
    
    # In kafka-python, consumption is done by iterating over the consumer
    for message in consumer_1:
        try:
            # Print the received message
            print(f'Received message: {message.value}')
            print(f'Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}')
        except Exception as e:
            print(f'Error processing message: {e}')

except KeyboardInterrupt:
    pass
finally:
    # Close the consumer
    consumer_1.close()
