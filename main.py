import pandas as pd
import json
from datetime import datetime, timedelta
from data_ingestion.tmdb_connector import TMDbConnector
from data_ingestion.youtube_connector import YouTubeConnector
from data_ingestion.trakt_connector import TraktConnector
from data_ingestion.omdb_connector import OMDBConnector

trakt_connector = TraktConnector()
tmdb_connector = TMDbConnector()
youtube_connector = YouTubeConnector()
omdb_connector = OMDBConnector()


def save_to_json(data, filename):
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)


# TMBD
# Get movies
start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
end_date = datetime.now().strftime("%Y-%m-%d")
movies = tmdb_connector.get_movies_by_date_range(
    start_date=start_date,
    end_date=end_date,
)
save_to_json(movies, "data/movies_tmdb_released.json")


# # TRAKT
# # Get Trakt movie
# trakt_movies = trakt_connector.get_trending_movies()
# save_to_json(trakt_movies, "data/trakt_trending_movies.json")
# trakt_movie = trakt_connector.get_movie_details(
#     trakt_movies[0]["movie"]["ids"]["trakt"]
# )
# save_to_json(trakt_movie, "data/trakt_movie_details.json")

# # YOUTUBE
# # Get YouTube reviews
# yt_url = trakt_movie["trailer"]
# yt_video_id = yt_url.split("v=")[1]
# reviews = youtube_connector.get_video_statistics(video_id=yt_video_id)
# comments = youtube_connector.get_video_comments(video_id=yt_video_id)
# save_to_json(reviews, "data/youtube_reviews.json")
# save_to_json(comments, "data/youtube_comments.json")
