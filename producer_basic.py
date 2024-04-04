#%%
from confluent_kafka import Producer


producer_config = {
    'bootstrap.servers': 'localhost:29092',
    'client.id': 'my-producer',
    'acks': 'all',
    'retries': 5,
    'batch.size': 16384,
    'linger.ms': 5,
    'compression.type': 'gzip',
    'max.request.size': 1048576
}
p  = Producer({'bootstrap.servers': 'localhost:29092'})
def delivery_report(err, msg):
    print(msg)
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Create Producer instance
producer_1 = Producer({'bootstrap.servers': 'localhost:29092'})

for i in range(20):
    producer_1.produce('kafka_python_test', f'hello, world from cde_12 msg number {i}', callback=delivery_report)


# Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered.
producer_1.flush()

