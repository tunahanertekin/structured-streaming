from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "demo",
    bootstrap_servers='localhost:9092'
)

for msg in consumer:
    print(msg.value.decode("utf-8"))