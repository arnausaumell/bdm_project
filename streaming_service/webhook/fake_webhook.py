import requests
import uuid
import random
import time
from datetime import datetime

MOVIES = [
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
    "Painfully long and slow-paced."
]
WEBHOOK_URL = "http://localhost:8001/webhook"

def generate_comment():
    return {
        "comment_id": str(uuid.uuid4()),
        "film_title": random.choice(MOVIES),
        "comment_text": random.choice(COMMENTS),
        "is_fake": random.choice([True, False]),
        "detected_at": datetime.utcnow().isoformat(),
        "confidence_score": round(random.uniform(0.5, 0.99), 2)
    }

def main():
    while True:
        comment = generate_comment()
        res = requests.post(WEBHOOK_URL, json=comment)
        print(f"📤 Sent: {comment['film_title']} - \"{comment['comment_text']}\" ({res.status_code})")
        time.sleep(2)

if __name__ == "__main__":
    main()
