from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092'
)

producer.send("demo", b"tunahan")
producer.flush()
producer.close()