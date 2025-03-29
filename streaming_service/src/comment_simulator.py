import threading
import uuid
import random
import requests
import time
from datetime import datetime

MOVIES = [
    {"film_id": f"film{i+1}", "film_title": title} for i, title in enumerate([
        "The Matrix", "Inception", "Interstellar", "The Godfather", "Pulp Fiction",
        "The Dark Knight", "Fight Club", "Forrest Gump", "The Shawshank Redemption", "The Lord of the Rings",
        "Gladiator", "Avatar", "Titanic", "The Social Network", "Whiplash",
        "The Prestige", "Parasite", "Joker", "Mad Max: Fury Road", "La La Land",
        "Black Panther", "Avengers: Endgame", "Doctor Strange", "Oppenheimer", "Barbie",
        "Dune", "Everything Everywhere All at Once", "Spider-Man: No Way Home", "The Batman", "No Time to Die",
        "Coco", "Up", "WALL-E", "Inside Out", "Soul",
        "Ratatouille", "Shrek", "The Lion King", "Finding Nemo", "Toy Story",
        "Frozen", "Zootopia", "Moana", "Encanto", "Turning Red",
        "The Incredibles", "Brave", "Cars", "Luca", "Onward"
    ])
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
    "Great twist at the end!"
]

WEBHOOK_URL = "http://fastapi_kafka_app:8001/webhook"

def generate_comment():
    movie = random.choice(MOVIES)
    comment_text = random.choice(COMMENTS)
    return {
        "film_id": movie["film_id"],
        "film_title": movie["film_title"],
        "comment_id": f"sim_{str(uuid.uuid4())}",
        "comment_text": comment_text,
        "is_fake": random.choice([True, False]),
        "detected_at": datetime.utcnow().isoformat(),
        "confidence_score": round(random.uniform(0.5, 0.99), 2)
    }

def simulate_comments(interval=2):
    while True:
        comment = generate_comment()
        try:
            res = requests.post(WEBHOOK_URL, json=comment)
            print(f"📤 Sent: {comment['film_title']} - \"{comment['comment_text']}\" ({res.status_code})")
        except Exception as e:
            print(f"❌ Error sending comment: {e}")
        time.sleep(interval)

def run_simulator():
    # Lanza el simulador en un hilo separado
    threading.Thread(target=simulate_comments, daemon=True).start()