from fastapi import FastAPI, Request
import uvicorn
from contextlib import asynccontextmanager
from loguru import logger

from src.kafka_handler import KafkaHandler
from src.storage import StorageHandler
from src.webhook import subscribe_to_webhook
from src.config import KAFKA_TOPIC

kafka_handler = KafkaHandler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan event handler for startup and shutdown events"""
    # Initialize Kafka producer
    kafka_handler.initialize()

    # Subscribe to webhook service
    await subscribe_to_webhook()

    yield  # Server is running and ready to handle requests

    # Shutdown: Clean up Kafka producer
    kafka_handler.close()


app = FastAPI(
    title="Comment Consumer Service",
    description="Service that receives webhook notifications and produces to Kafka",
    lifespan=lifespan,
)


@app.post("/webhook")
async def receive_webhook(request: Request):
    """Endpoint to receive webhook notifications"""
    comment_data = await request.json()
    logger.info(f"Received webhook: {comment_data}")

    try:
        # Produce to Kafka
        kafka_handler.send_message(KAFKA_TOPIC, comment_data)

        # Save to JSON file
        StorageHandler.save_comment(comment_data)

        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
