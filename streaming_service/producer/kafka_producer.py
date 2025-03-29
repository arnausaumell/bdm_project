from fastapi import FastAPI
from loguru import logger
from kafka_handler import KafkaHandler
from config import KAFKA_TOPIC
import asyncio
import websockets
import json
import uvicorn
from contextlib import asynccontextmanager


class AppState:
    def __init__(self):
        self.kafka_handler = KafkaHandler()
        self.websocket_task = None


async def process_websocket_messages():
    uri = "ws://localhost:8000/ws"
    while True:
        try:
            async with websockets.connect(uri) as websocket:
                logger.info("✅ Connected to WebSocket server")

                while True:
                    message = await websocket.recv()
                    data = json.loads(message)
                    logger.info(f"📩 Message received: {data}")

                    try:
                        app.state.state.kafka_handler.send_message(KAFKA_TOPIC, data)
                        logger.info(f"✈️ Message sent to Kafka: {data['film_title']}")
                    except Exception as e:
                        logger.error(f"❌ Error sending to Kafka: {e}")

        except Exception as e:
            logger.error(f"❌ WebSocket connection error: {e}")
            logger.info("🔄 Attempting to reconnect in 5 seconds...")
            await asyncio.sleep(5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    state = AppState()
    app.state.state = state

    # Initialize Kafka
    state.kafka_handler.initialize()
    logger.info("✅ Kafka handler initialized")

    # Start WebSocket client
    state.websocket_task = asyncio.create_task(process_websocket_messages())
    logger.info("🚀 WebSocket client started")

    yield

    # Shutdown
    if state.websocket_task:
        state.websocket_task.cancel()
        try:
            await state.websocket_task
        except asyncio.CancelledError:
            logger.info("💤 WebSocket client shutdown complete")


app = FastAPI(title="WebSocket to Kafka Bridge", lifespan=lifespan)


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
