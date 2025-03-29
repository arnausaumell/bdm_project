import time
import uuid
import random
import json
from datetime import datetime
from kafka import KafkaProducer

MOVIES = [
    {"film_id": f"film{i}", "film_title": title} for i, title in enumerate([
        "The Matrix", "Inception", "Interstellar", "The Godfather", "Pulp Fiction",
        "Fight Club", "Forrest Gump", "The Dark Knight", "The Shawshank Redemption", "Gladiator",
        "Whiplash", "Joker", "Soul", "Inside Out", "The Batman", "La La Land", "Her", "Parasite",
        "Titanic", "Avatar", "Dune", "Oppenheimer", "Barbie", "No Country for Old Men", "Amélie",
        "Coco", "The Irishman", "The Revenant", "Tenet", "Arrival", "Knives Out", "1917",
        "Everything Everywhere All At Once", "The Social Network", "Mad Max: Fury Road", "Frozen"
    ], 1)
]

COMMENTS = [
    "Incredible plot and mind-blowing visuals!", "Absolutely terrible. Waste of time.",
    "The director did a great job!", "Can't believe how much I loved this!",
    "Worst movie ever made.", "10/10 would recommend to a friend.",
    "I fell asleep halfway through.", "Such a classic, never gets old.",
    "Too much CGI ruined it.", "The characters were very flat.",
    "Realistic and emotional.", "This was deeply disappointing.",
    "Best soundtrack ever!", "Overrated and boring."
]

producer = KafkaProducer(
    bootstrap_servers=["kafka:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

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

def stream_comments(interval=3):
    print("🎬 Starting comment stream directly to Kafka...")
    while True:
        comment = generate_comment()
        producer.send("comments", comment)
        print(f"📤 Sent to Kafka: {comment['film_title']} - \"{comment['comment_text']}\"")
        time.sleep(interval)

if __name__ == "__main__":
    stream_comments(interval=3)
