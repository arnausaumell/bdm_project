import os
import requests
import dotenv

dotenv.load_dotenv()


class OMDBConnector:
    def __init__(self):
        self._base_url = "http://www.omdbapi.com"
        self._api_key = os.getenv("OMDB_API_KEY")
        self._headers = {"Content-Type": "application/json"}

    def get_movie_by_title(self, title: str):
        url = f"{self._base_url}/?apikey={self._api_key}&t={title}"
        response = requests.get(url=url, headers=self._headers)
        return response.json()

    def get_movie_by_imdb_id(self, imdb_id: str):
        url = f"{self._base_url}/?apikey={self._api_key}&i={imdb_id}"
        response = requests.get(url=url, headers=self._headers)
        return response.json()


if __name__ == "__main__":
    import json

    omdb_connector = OMDBConnector()
    print(json.dumps(omdb_connector.get_movie_by_imdb_id("tt4772188"), indent=4))
