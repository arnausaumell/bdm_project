from kafka import KafkaProducer
import json
from loguru import logger
from .config import KAFKA_BOOTSTRAP_SERVERS


class KafkaHandler:
    def __init__(self):
        self.producer = None

    def initialize(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            api_version=(0, 11, 5),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=5000,
            connections_max_idle_ms=5000,
            max_block_ms=120000,
            allow_auto_create_topics=True,
        )
        logger.info("Kafka producer initialized successfully")

    def close(self):
        try:
            if self.producer:
                self.producer.close()
                logger.info("Kafka producer closed")
        except Exception as e:
            logger.error(f"Error closing Kafka producer: {str(e)}")

    def send_message(self, topic: str, message: dict):
        try:
            if not self.producer:
                raise ValueError("Kafka producer is not initialized")

            future = self.producer.send(topic, value=message)
            future.get(timeout=20)
            self.producer.flush()
            logger.info(f"Sent to Kafka: {message.get('film_title')}")
        except Exception as e:
            logger.error(f"Error sending to Kafka: {str(e)}")
            logger.error(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
            raise
