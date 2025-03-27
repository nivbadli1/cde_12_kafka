#%%
from kafka import KafkaProducer

# Producer configuration options
producer_config = {
    'bootstrap_servers': 'course-kafka:9092',
    'client_id': 'my-producer',
    'acks': 'all',
    'retries': 5,
    'batch_size': 16384,
    'linger_ms': 5,
    'compression_type': 'gzip',
    'max_request_size': 1048576
}

# Producer with full configuration
producer = KafkaProducer(
    bootstrap_servers='course-kafka:9092',
    client_id='my-producer',
    acks='all',
    retries=5,
    batch_size=16384,
    linger_ms=5,
    compression_type='gzip',
    max_request_size=1048576,
    value_serializer=lambda v: str(v).encode('utf-8')
)

# Simpler producer with basic configuration
producer_1 = KafkaProducer(
    bootstrap_servers='course-kafka:9092',
    value_serializer=lambda v: str(v).encode('utf-8')
)

# Send messages
for i in range(20):
    future = producer_1.send('kafka_python_test', f'hello, world from cde_12 msg number {i}')
    # Get metadata for the sent message
    record_metadata = future.get(timeout=10)
    print(f'Message delivered to {record_metadata.topic} [{record_metadata.partition}] at offset {record_metadata.offset}')

# Wait for any outstanding messages to be delivered
producer_1.flush()
