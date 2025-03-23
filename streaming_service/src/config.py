import os

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(
    ","
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "comments")
NUM_PARTITIONS = 1
REPLICATION_FACTOR = 1

# Webhook service configuration
WEBHOOK_SERVICE_URL = "http://localhost:8000"
COMMENTS_FILE = "fake_comments.json"
