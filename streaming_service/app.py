from fastapi import FastAPI, Request
import uvicorn
from contextlib import asynccontextmanager
from loguru import logger
import time

from src.kafka_handler import KafkaHandler
from src.storage import StorageHandler
from src.webhook import subscribe_to_webhook
from src.config import KAFKA_TOPIC
from src.comment_simulator import run_simulator

from fastapi.responses import JSONResponse
import json
import os

kafka_handler = KafkaHandler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    for attempt in range(10):
        try:
            kafka_handler.initialize()
            break
        except Exception as e:
            logger.warning(f"Kafka not ready (attempt {attempt + 1}/10): {e}")
            time.sleep(5)
    else:
        logger.error("❌ Could not connect to Kafka after multiple attempts.")
        exit(1)

    await subscribe_to_webhook()
    run_simulator()
    logger.info("🚀 Simulador de comentarios iniciado")
    yield
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





@app.get("/comments")
def get_all_comments():
    """Devuelve todos los comentarios del cold path (data/consolidated_comments.json)"""
    consolidated_path = "data/consolidated_comments.json"
    try:
        with open(consolidated_path, "r") as f:
            comments = json.load(f)
        return JSONResponse(content=comments)
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": "No comments found"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)

