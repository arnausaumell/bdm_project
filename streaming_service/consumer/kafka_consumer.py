from kafka import KafkaConsumer
import json
import os
import time
from loguru import logger

TOPIC = "comments"
BOOTSTRAP_SERVERS = ["kafka:9092"]
OUTPUT_FILE = "data/consolidated_comments.json"
GROUP_ID = "cold_path_group"

def prepare_output_file():
    if not os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "w") as f:
            json.dump([], f)
        logger.info(f"📝 Archivo creado en {OUTPUT_FILE}")

def main():
    # Esperar a que Kafka esté listo
    for i in range(10):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode("utf-8"))
            )
            logger.success("✅ Conectado a Kafka")
            break
        except Exception as e:
            logger.warning(f"Kafka no está listo ({i+1}/10): {e}")
            time.sleep(5)
    else:
        logger.error("❌ No se pudo conectar a Kafka")
        return

    prepare_output_file()
    logger.info("🎧 Escuchando mensajes de Kafka...")

    while True:
        msg_pack = consumer.poll(timeout_ms=1000)
        for tp, messages in msg_pack.items():
            for message in messages:
                comment = message.value
                logger.info(f"📥 Comentario recibido: {comment['film_title']}")

                try:
                    with open(OUTPUT_FILE, "r") as f:
                        comments = json.load(f)
                except:
                    comments = []

                comments.append(comment)

                with open(OUTPUT_FILE, "w") as f:
                    json.dump(comments, f, indent=2)

if __name__ == "__main__":
    main()
