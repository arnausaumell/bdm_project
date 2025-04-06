from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket
import uvicorn
from datetime import datetime, timezone
import random
import uuid
from typing import Dict, List, Set
import asyncio
from loguru import logger
from dotenv import load_dotenv

from fake_comments import COMMENTS

load_dotenv()


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


async def notify_subscribers(state: AppState, comment: dict):
    # Send the comment to all connected clients
    for connection in state.active_connections.copy():
        try:
            await connection.send_json(comment)
        except:
            state.active_connections.remove(connection)


def get_db_tables() -> list[dict]:
    from core.landing_zone.deltalake_manager import DeltaLakeManager

    delta_lake_manager = DeltaLakeManager()
    db_films = delta_lake_manager.read_table("tmdb/movies_released")
    return db_films.to_dict(orient="records")


async def generate_comments_periodically(state: AppState):
    logger.info("Starting comment generation loop")
    while True:
        try:
            logger.info("Fetching movies from database...")
            movies = get_db_tables()
            if not movies:
                logger.warning("No movies found in the database, waiting 10 seconds...")
                await asyncio.sleep(10)
                continue

            logger.info(f"Found {len(movies)} movies in database")
            movie = random.choice(movies)
            comment = {
                "tmdb_id": movie["tmdb_id"],
                "comment_id": str(uuid.uuid4()),
                "film_title": movie["title"],
                "comment_text": random.choice(COMMENTS),
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            state.comments_history.append(comment)
            logger.info(
                f'Generated: {comment["film_title"]} - "{comment["comment_text"]}"'
            )
            await notify_subscribers(state, comment)
            await asyncio.sleep(10)
        except Exception as e:
            logger.error(f"Error in comment generation loop: {e}")
            await asyncio.sleep(10)  # Wait before retrying


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
