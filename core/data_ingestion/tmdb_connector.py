"""
https://developer.themoviedb.org/reference/intro/getting-started
"""

import os
import requests
import dotenv
from tqdm import tqdm

dotenv.load_dotenv()


class TMDbConnector:
    def __init__(self, region="ES"):
        self._base_url = "https://api.themoviedb.org/3"
        self._region = region
        self._api_key = os.getenv("TMDB_API_KEY")
        self._headers = {
            "Content-Type": "application/json",
        }

    def get_movies_by_date_range(self, start_date: str, end_date: str):
        genres = self._get_genre_list()
        params = {
            "api_key": self._api_key,
            "sort_by": "popularity.desc",
            "region": self._region,
            "release_date.gte": start_date,
            "release_date.lte": end_date,
            # "with_release_type": 4,
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
                movie["tmdb_id"] = movie["id"]
                movie = self._enrich_movie(movie, genres)
                movie = self._clean_movie_details(movie)
                all_movies.append(movie)

        return all_movies

    def _enrich_movie(self, movie: dict, genres: list):
        movie["credits"] = self._get_movie_credits(movie["tmdb_id"])
        movie["reviews"] = self._get_movie_reviews(movie["tmdb_id"])
        movie["genres"] = [
            genre["name"] for genre in genres if genre["id"] in movie["genre_ids"]
        ]
        movie["languages"] = self._get_movie_languages(movie["tmdb_id"])
        movie["keywords"] = self._get_movie_keywords(movie["tmdb_id"])
        movie.update(self._get_movie_details(movie["tmdb_id"]))
        return movie

    def _get_genre_list(self):
        response = requests.get(
            f"{self._base_url}/genre/movie/list", params={"api_key": self._api_key}
        )
        return response.json().get("genres", [])

    def _get_movie_credits(self, movie_id: int):
        response = requests.get(
            f"{self._base_url}/movie/{movie_id}/credits",
            params={"api_key": self._api_key},
        )
        all_actors = response.json().get("cast", [])
        all_actors = [
            {
                "id": credit["id"],
                "role": "Acting",
                "name": credit["name"],
                "character": credit["character"],
            }
            for credit in all_actors
            if credit["order"] < 5
        ]
        crew = response.json().get("crew", [])
        director = next(
            (
                {
                    "id": crew["id"],
                    "role": "Director",
                    "name": crew["name"],
                    "character": None,
                }
                for crew in crew
                if crew["job"] == "Director" and crew["department"] == "Directing"
            ),
            None,
        )
        return all_actors + [director]

    def _get_movie_reviews(self, movie_id: int):
        params = {"api_key": self._api_key, "language": "en-US", "page": 1}

        response = requests.get(
            f"{self._base_url}/movie/{movie_id}/reviews", params=params
        )
        response.raise_for_status()
        return [
            {
                "author": review["author"],
                "content": review["content"],
                "created_at": review["created_at"],
                "url": review["url"],
            }
            for review in response.json().get("results", [])
        ]

    def get_movie_providers(self, movie_id: int):
        response = requests.get(
            f"{self._base_url}/movie/{movie_id}/watch/providers",
            params={"api_key": self._api_key},
        )
        return response.json().get("results", {}).get(self._region, {})

    def _get_movie_languages(self, movie_id: int):
        response = requests.get(
            f"{self._base_url}/movie/{movie_id}/translations",
            params={"api_key": self._api_key},
        )
        translations = response.json().get("translations", [])
        return [
            {
                "iso_639_1": translation["iso_639_1"],
                "iso_3166_1": translation["iso_3166_1"],
                "name": translation["name"],
                "overview": translation["data"]["overview"],
                "tagline": translation["data"]["tagline"],
            }
            for translation in translations
        ]

    def _get_movie_keywords(self, movie_id: int):
        response = requests.get(
            f"{self._base_url}/movie/{movie_id}/keywords",
            params={"api_key": self._api_key},
        )
        return response.json().get("keywords", [])

    def _get_movie_details(self, movie_id: int):
        response = requests.get(
            f"{self._base_url}/movie/{movie_id}",
            params={"api_key": self._api_key},
        )
        return response.json()

    def _clean_movie_details(self, movie_details: dict):
        selected_fields = [
            "tmdb_id",
            "title",
            "overview",
            "original_lanaguage",
            "original_title",
            "poster_path",
            "release_date",
            "credits",
            "reviews",
            "genres",
            "languages",
            "keywords",
            "budget",
            "origin_country",
            "runtime",
            "revenue",
            "tagline",
        ]
        return {
            field: movie_details[field]
            for field in selected_fields
            if field in movie_details
        }
