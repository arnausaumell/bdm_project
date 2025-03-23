import httpx
from loguru import logger
from .config import WEBHOOK_SERVICE_URL


async def subscribe_to_webhook():
    """Subscribe to the webhook service"""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{WEBHOOK_SERVICE_URL}/subscribe",
                json={
                    "url": "http://localhost:8001/webhook",
                    "description": "Kafka consumer service for fake comments",
                },
            )
            if response.status_code == 200:
                logger.info("Successfully subscribed to webhook service")
            else:
                logger.error(f"Failed to subscribe: {response.text}")
        except Exception as e:
            logger.error(f"Error subscribing to webhook: {e}")
            raise
