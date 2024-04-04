#%%
from confluent_kafka import Consumer, KafkaException, KafkaError

# Create Consumer instance
consumer_1 = Consumer({
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'mygroup1',
    'auto.offset.reset': 'earliest'
})



"""
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 5000,
    'session.timeout.ms': 6000,
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': StringDeserializer('utf_8'),
}

"""

# Subscribe to topic
consumer_1.subscribe(['kafka_python_test'])

try:
    while True:
        print("Polling for messages from topic: kafka_python_test")
        msg = consumer_1.poll(1.0)  # Wait for a message up to 1 second
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print('%% %s [%d] reached end at offset %d\n' %
                      (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            # Proper message
            print('Received message: {}'.format(msg.value().decode('utf-8')))
except KeyboardInterrupt:
    pass
finally:
    # Close down consumer to commit final offsets.
    consumer_1.close()

