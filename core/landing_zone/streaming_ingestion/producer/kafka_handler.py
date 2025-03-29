from kafka import KafkaProducer
import json
from loguru import logger
from config import KAFKA_BOOTSTRAP_SERVERS


class KafkaHandler:
    def __init__(self):
        self.producer = None

    def initialize(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        logger.info("✅ Kafka producer initialized")

    def send_message(self, topic, message):
        logger.debug(f"📦 Enviando mensaje a Kafka: {message}")
        self.producer.send(topic, value=message)
        self.producer.flush()
        logger.info(f"📤 Mensaje enviado a Kafka ({topic})")
