from fastapi import FastAPI, Request
from loguru import logger
from kafka_handler import KafkaHandler
from config import KAFKA_TOPIC

app = FastAPI(title="Webhook Receiver")

kafka_handler = KafkaHandler()
kafka_handler.initialize()


@app.post("/webhook")
async def webhook(request: Request):
    data = await request.json()
    logger.info(f"📩 Webhook recibido: {data}")

    try:
        kafka_handler.send_message(KAFKA_TOPIC, data)
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"❌ Error al enviar a Kafka: {e}")
        return {"status": "error", "message": str(e)}
