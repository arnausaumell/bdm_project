import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "comments")
