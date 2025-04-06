from kafka import KafkaConsumer
import json
import time
from loguru import logger
from core.landing_zone.deltalake_manager import DeltaLakeManager
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

TOPIC = "comments"
BOOTSTRAP_SERVERS = ["kafka:9092"]
OUTPUT_FILE = "data/consolidated_comments.json"
GROUP_ID = "cold_path_group"


def main():
    for i in range(10):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            )
            logger.success("Connected to Kafka")
            break
        except Exception as e:
            logger.warning(f"Kafka not ready ({i + 1}/10): {e}")
            time.sleep(5)
    else:
        logger.error("Unable to connect to Kafka")
        return

    while True:
        delta_lake_manager = DeltaLakeManager()
        msg_pack = consumer.poll(timeout_ms=1000)
        for tp, messages in msg_pack.items():
            for message in messages:
                logger.info(f"Received comment: {message.value['film_title']}")
            comments = [msg.value for msg in messages]
            delta_lake_manager.upsert_to_table(
                data=pd.DataFrame(comments),
                table_path="blog_comments",
                merge_key="comment_id",
            )


if __name__ == "__main__":
    main()
