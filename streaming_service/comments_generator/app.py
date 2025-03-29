from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket
import uvicorn
from datetime import datetime, timezone
import random
import uuid
from typing import Dict, List, Set
import asyncio


# Store state
class AppState:
    def __init__(self):
        self.comments_history: List[Dict] = []
        self.active_connections: Set[WebSocket] = set()
        self.generator_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    state = AppState()
    app.state.state = state

    # Start the comment generator
    state.generator_task = asyncio.create_task(generate_comments_periodically(state))
    print("Starting comment generator...")

    yield

    # Shutdown
    if state.generator_task:
        state.generator_task.cancel()
        try:
            await state.generator_task
        except asyncio.CancelledError:
            print("Comment generator shutdown complete")

    # Close all WebSocket connections
    for connection in state.active_connections:
        try:
            await connection.close()
        except:
            pass
    state.active_connections.clear()


app = FastAPI(lifespan=lifespan)

# Reuse the same movies and comments from the generator
MOVIES = [
    "The Matrix",
    "Inception",
    "Interstellar",
    "The Godfather",
    "Pulp Fiction",
    "The Dark Knight",
    "Fight Club",
    "Forrest Gump",
    "The Shawshank Redemption",
    "The Lord of the Rings",
    "Gladiator",
    "Avatar",
    "Titanic",
    "The Social Network",
    "Whiplash",
    "The Prestige",
    "Parasite",
    "Joker",
    "Mad Max: Fury Road",
    "La La Land",
    "Black Panther",
    "Avengers: Endgame",
    "Doctor Strange",
    "Oppenheimer",
    "Barbie",
    "Dune",
    "Everything Everywhere All at Once",
    "Spider-Man: No Way Home",
    "The Batman",
    "No Time to Die",
    "Coco",
    "Up",
    "WALL-E",
    "Inside Out",
    "Soul",
    "Ratatouille",
    "Shrek",
    "The Lion King",
    "Finding Nemo",
    "Toy Story",
    "Frozen",
    "Zootopia",
    "Moana",
    "Encanto",
    "Turning Red",
    "The Incredibles",
    "Brave",
    "Cars",
    "Luca",
    "Onward",
]
COMMENTS = [
    "Incredible plot and mind-blowing visuals!",
    "Absolutely terrible. Waste of time.",
    "The director did a great job!",
    "Can't believe how much I loved this!",
    "Worst movie ever made.",
    "10/10 would recommend to a friend.",
    "I fell asleep halfway through.",
    "Such a classic, never gets old.",
    "The characters were very flat.",
    "Oscar-worthy performance!",
    "Overrated and boring.",
    "Underrated gem!",
    "The soundtrack was amazing.",
    "The pacing was too slow.",
    "Too much CGI ruined it.",
    "Realistic and emotional.",
    "Felt like a rollercoaster.",
    "This movie changed my life.",
    "Predictable ending.",
    "Great twist at the end!",
    "Brilliant cinematography.",
    "Weak script but strong acting.",
    "Wouldn't watch it again.",
    "Surprisingly good!",
    "Left the theatre amazed.",
    "Cliché and uninspired.",
    "Beautiful visuals, bad story.",
    "Fun for the whole family.",
    "I laughed, I cried, I cheered.",
    "Plot holes everywhere!",
    "A masterpiece of modern cinema.",
    "Completely forgettable.",
    "The sequel was better.",
    "I expected more from this cast.",
    "Cultural impact is undeniable.",
    "Loved the character development.",
    "Worst ending I've ever seen.",
    "Dialogue felt unnatural.",
    "Soundtrack carried the movie.",
    "Visually stunning, emotionally empty.",
    "Will definitely watch again.",
    "Boring and predictable.",
    "Exceeded my expectations.",
    "Not for everyone.",
    "Impressive world-building.",
    "Left me speechless.",
    "Didn't live up to the hype.",
    "Surprised me in the best way.",
    "Felt like two movies mashed together.",
    "Perfect Sunday night movie.",
    "Painfully long and slow-paced.",
]


async def notify_subscribers(state: AppState, comment: dict):
    # Send the comment to all connected clients
    for connection in state.active_connections.copy():
        try:
            await connection.send_json(comment)
        except:
            state.active_connections.remove(connection)


async def generate_comments_periodically(state: AppState):
    while True:
        comment = {
            "comment_id": str(uuid.uuid4()),
            "film_title": random.choice(MOVIES),
            "comment_text": random.choice(COMMENTS),
            "is_fake": random.choice([True, False]),
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "confidence_score": round(random.uniform(0.5, 0.99), 2),
        }
        state.comments_history.append(comment)
        print(f'📝 Generated: {comment["film_title"]} - "{comment["comment_text"]}"')
        await notify_subscribers(state, comment)
        await asyncio.sleep(3)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    state = app.state.state
    await websocket.accept()
    state.active_connections.add(websocket)
    try:
        # Send existing comments on connection
        for comment in state.comments_history:
            await websocket.send_json(comment)
        # Keep connection alive and listen for any messages
        while True:
            await websocket.receive_text()
    except:
        state.active_connections.remove(websocket)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
