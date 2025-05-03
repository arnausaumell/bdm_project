import os
import requests
import dotenv

dotenv.load_dotenv()


class TraktConnector:
    def __init__(self):
        self._base_url = "https://api.trakt.tv"
        self._client_id = os.getenv("TRAKT_CLIENT_ID")
        self._client_secret = os.getenv("TRAKT_CLIENT_SECRET")
        self._headers = {
            "Content-Type": "application/json",
            "trakt-api-version": "2",
            "trakt-api-key": self._client_id,
        }

    def get_trending_movies(self):
        url = f"{self._base_url}/movies/trending"
        response = requests.get(url=url, headers=self._headers)
        return response.json()

    def get_movie_details(self, tmdb_movie_id: int):
        url = f"{self._base_url}/movies/{tmdb_movie_id}?extended=full"
        response = requests.get(url=url, headers=self._headers)
        all_details = response.json()
        return all_details

    def get_movie_details_tmdb_id(self, tmdb_movie_id: int):
        url = f"{self._base_url}/search/tmdb/{tmdb_movie_id}?id_type=movie"
        response = requests.get(url=url, headers=self._headers)
        ids_content_list = response.json()
        for content in ids_content_list:
            if content["type"] == "movie":
                trakt_movie_id = content["movie"]["ids"]["trakt"]
                all_movie_details = self.get_movie_details(trakt_movie_id)
                return {
                    "title": all_movie_details["title"],
                    "trailer": all_movie_details["trailer"],
                    "tmdb_id": all_movie_details["ids"]["tmdb"],
                    "trakt_id": all_movie_details["ids"]["trakt"],
                    "slug": all_movie_details["ids"]["slug"],
                    "imdb_id": all_movie_details["ids"]["imdb"],
                }
        return None
