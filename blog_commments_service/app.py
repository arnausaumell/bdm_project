from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
from datetime import datetime
from typing import Optional, List, Dict
import httpx
import asyncio
import random
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="Film Comments Webhook Provider",
    description="A service that sends notifications about fake comments to subscribers",
)

# Enable CORS if needed
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class WebhookSubscription(BaseModel):
    url: HttpUrl
    description: Optional[str] = None


# Store subscriptions in memory (in production, use a database)
webhook_subscribers: Dict[str, WebhookSubscription] = {}

# Sample data for simulation
SAMPLE_FILMS = [
    {"id": "film1", "title": "The Matrix"},
    {"id": "film2", "title": "Inception"},
    {"id": "film3", "title": "Interstellar"},
]

SAMPLE_COMMENTS = [
    "This movie is absolutely terrible! Don't waste your money!",
    "Best film ever! Everyone must watch it 1000 times!!!",
    "I can't believe how amazing this was! Life-changing!!!",
    "Worst movie in history. The director should quit.",
    "Just bought 10 copies of this movie! So good!",
]


async def notify_subscribers(notification: dict):
    async with httpx.AsyncClient() as client:
        tasks = []
        for subscriber_id, subscription in webhook_subscribers.items():
            task = client.post(str(subscription.url), json=notification, timeout=5.0)
            tasks.append(task)

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            success_count = sum(1 for r in results if not isinstance(r, Exception))
            print(f"Notification sent to {success_count}/{len(tasks)} subscribers")


async def generate_fake_comment():
    film = random.choice(SAMPLE_FILMS)
    return {
        "film_id": film["id"],
        "film_title": film["title"],
        "comment_id": f"sim_{datetime.now().timestamp()}",
        "comment_text": random.choice(SAMPLE_COMMENTS),
        "is_fake": True,
        "detected_at": datetime.now().isoformat(),
        "confidence_score": round(random.uniform(0.75, 0.99), 2),
    }


async def periodic_comment_simulator():
    while True:
        try:
            # Generate and send fake comment
            fake_comment = await generate_fake_comment()
            await notify_subscribers(fake_comment)
            print(
                f"Generated fake comment for {fake_comment['film_title']} at {fake_comment['detected_at']}"
            )

            # Wait for 5 minutes
            await asyncio.sleep(10)  # 300 seconds = 5 minutes
        except Exception as e:
            print(f"Error in comment simulator: {e}")
            await asyncio.sleep(10)  # Wait a bit before retrying if there's an error


@app.on_event("startup")
async def startup_event():
    # Start the periodic task when the application starts
    asyncio.create_task(periodic_comment_simulator())


# Endpoint to subscribe to webhooks
@app.post("/subscribe")
async def subscribe_webhook(subscription: WebhookSubscription):
    subscriber_id = str(len(webhook_subscribers) + 1)
    webhook_subscribers[subscriber_id] = subscription
    return {
        "status": "success",
        "subscriber_id": subscriber_id,
        "message": "Successfully subscribed to fake comment notifications",
    }


# Endpoint to unsubscribe
@app.delete("/unsubscribe/{subscriber_id}")
async def unsubscribe_webhook(subscriber_id: str):
    if subscriber_id not in webhook_subscribers:
        raise HTTPException(status_code=404, detail="Subscriber not found")

    del webhook_subscribers[subscriber_id]
    return {"status": "success", "message": "Successfully unsubscribed"}


# List all subscribers (for demonstration purposes)
@app.get("/subscribers")
async def list_subscribers():
    return webhook_subscribers


# Optional: endpoint to manually trigger a fake comment
@app.post("/simulate-fake-comment")
async def simulate_fake_comment():
    fake_comment = await generate_fake_comment()
    await notify_subscribers(fake_comment)
    return fake_comment


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
