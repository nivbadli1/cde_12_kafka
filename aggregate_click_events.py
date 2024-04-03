from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict

bootstrap_servers = 'localhost:9092'
clickstream_topic = 'user_clickstream'
click_aggregation_topic = 'user_click_aggregation'

def calculate_clicks_per_user():
    consumer = KafkaConsumer(clickstream_topic, bootstrap_servers=bootstrap_servers,
                             value_deserializer=lambda x: x.decode('utf-8'))

    click_count_per_user = defaultdict(int)
    for message in consumer:
        click_event = message.value
        user_id = click_event.split()[1]
        click_count_per_user[user_id] += 1

        publish_aggregated_results(user_id, click_count_per_user[user_id])

def publish_aggregated_results(user_id, click_count):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    result_message = f"User {user_id} has clicked {click_count} times."
    producer.send(click_aggregation_topic, value=result_message.encode('utf-8'))

if __name__ == "__main__":
    calculate_clicks_per_user()