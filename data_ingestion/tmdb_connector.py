"""
https://developer.themoviedb.org/reference/intro/getting-started
"""

import os
import requests
import dotenv
from tqdm import tqdm

dotenv.load_dotenv()


class TMDbConnector:
    def __init__(self):
        self._base_url = "https://api.themoviedb.org/3"
        self._api_key = os.getenv("TMDB_API_KEY")
        self._headers = {
            "Content-Type": "application/json",
        }

    def get_movies_by_date_range(self, start_date: str, end_date: str):
        genres = self.get_genre_list()
        params = {
            "api_key": self._api_key,
            "sort_by": "popularity.desc",
            "language": "en-US",
            "release_date.gte": start_date,
            "release_date.lte": end_date,
            "with_release_type": 4,
        }
        response = requests.get(f"{self._base_url}/discover/movie", params=params)
        total_pages = response.json().get("total_pages", 1)

        all_movies = []
        for page in tqdm(range(1, total_pages), desc="Fetching movies"):
            params["page"] = page
            response = requests.get(f"{self._base_url}/discover/movie", params=params)
            response.raise_for_status()
            movies = response.json().get("results", [])
            for movie in movies:
                movie["genres"] = [
                    genre["name"]
                    for genre in genres
                    if genre["id"] in movie["genre_ids"]
                ]
                movie["credits"] = self.get_movie_credits(movie["id"])
                # movie["providers"] = self.get_movie_providers(movie["id"])
                # movie["reviews"] = self.get_movie_reviews(movie["id"])
            all_movies.extend(movies)

        return all_movies

    def get_genre_list(self):
        response = requests.get(
            f"{self._base_url}/genre/movie/list", params={"api_key": self._api_key}
        )
        return response.json().get("genres", [])

    def get_movie_credits(self, movie_id: int):
        response = requests.get(
            f"{self._base_url}/movie/{movie_id}/credits",
            params={"api_key": self._api_key},
        )
        return response.json()

    def get_movie_reviews(self, movie_id: int):
        params = {"api_key": self._api_key, "language": "en-US", "page": 1}

        response = requests.get(
            f"{self._base_url}/movie/{movie_id}/reviews", params=params
        )
        response.raise_for_status()
        return response.json().get("results", [])

    def get_movie_providers(self, movie_id: int):
        response = requests.get(
            f"{self._base_url}/movie/{movie_id}/watch/providers",
            params={"api_key": self._api_key},
        )
        return response.json().get("results", {})
