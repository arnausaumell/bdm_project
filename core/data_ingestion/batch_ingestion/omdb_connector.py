import os
import requests
import dotenv

dotenv.load_dotenv()


class OMDBConnector:
    def __init__(self):
        self._base_url = "http://www.omdbapi.com"
        self._api_key = os.getenv("OMDB_API_KEY")
        self._headers = {"Content-Type": "application/json"}

    def get_movie_by_imdb_id(self, imdb_id: str):
        url = f"{self._base_url}/?apikey={self._api_key}&i={imdb_id}"
        response = requests.get(url=url, headers=self._headers)
        all_details = response.json()
        return {
            "title": all_details.get("Title"),
            "poster": all_details.get("Poster"),
            "awards": all_details.get("Awards"),
            "ratings": all_details.get("Ratings"),
            "metascore": all_details.get("Metascore"),
            "imdb_rating": all_details.get("imdbRating"),
            "imdb_votes": all_details.get("imdbVotes"),
            "imdb_id": all_details.get("imdbID"),
        }


if __name__ == "__main__":
    import json

    omdb_connector = OMDBConnector()
    movie_ratings = omdb_connector.get_movie_by_imdb_id("tt28015403")
    with open("movie_ratings.json", "w") as f:
        json.dump(movie_ratings, f, indent=4)
