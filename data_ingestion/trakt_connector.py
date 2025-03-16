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

    def get_movie_details(self, trakt_movie_id: int):
        url = f"{self._base_url}/movies/{trakt_movie_id}?extended=full"
        response = requests.get(url=url, headers=self._headers)
        return response.json()

    def get_movie_releases(self, trakt_movie_id: int):
        url = f"{self._base_url}/movies/{trakt_movie_id}/releases/countries"
        response = requests.get(url=url, headers=self._headers)
        return response.json()


if __name__ == "__main__":
    import json

    trakt_connector = TraktConnector()
    # print(json.dumps(trakt_connector.get_trending_movies(), indent=4))
    print(json.dumps(trakt_connector.get_movie_details(656016), indent=4))
