import os
import sys

sys.path.append(os.getcwd())

from datetime import datetime, timedelta
from prefect import flow, task
from loguru import logger

from core.data_ingestion.tmdb_connector import TMDbConnector
from core.data_ingestion.trakt_connector import TraktConnector
from core.data_ingestion.omdb_connector import OMDBConnector
from core.data_ingestion.youtube_connector import YouTubeConnector
from utils.deltalake_manager import DeltaLakeManager
import pandas as pd


N_DAYS_AGO = 4
REGION = "ES"


@task
def get_daily_releases(
    tmdb_connector: TMDbConnector,
    delta_lake_manager: DeltaLakeManager,
    n_days_ago: int = N_DAYS_AGO,
):
    """Get last N_DAYS_AGO releases from TMDB"""
    start_date = (datetime.now() - timedelta(days=n_days_ago)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    released_movies = tmdb_connector.get_movies_by_date_range(
        start_date=start_date,
        end_date=end_date,
    )
    delta_lake_manager.upsert_to_table(
        data=pd.DataFrame(released_movies),
        table_path="landing_zone/tmdb/movies_released",
        merge_key="tmdb_id",
    )
    return released_movies


@task
def get_trakt_details(
    released_movies: list[dict],
    trakt_connector: TraktConnector,
    delta_lake_manager: DeltaLakeManager,
):
    """Get trakt details for each movie"""
    trakt_movies = []
    for movie in released_movies:
        trakt_movie_details = trakt_connector.get_movie_details_tmdb_id(
            movie["tmdb_id"]
        )
        trakt_movies.append(trakt_movie_details)
    delta_lake_manager.upsert_to_table(
        data=pd.DataFrame(trakt_movies),
        table_path="landing_zone/trakt/movie_ids_and_trailer",
        merge_key="trakt_id",
    )
    return trakt_movies


@task
def update_movie_ratings(
    all_movies: list[dict],
    delta_lake_manager: DeltaLakeManager,
    omdb_connector: OMDBConnector,
):
    """Update movie ratings using OMDB"""
    movie_ratings = []
    for movie in all_movies:
        imdb_id = movie.get("imdb_id")
        if imdb_id:
            omdb_movie_details = omdb_connector.get_movie_by_imdb_id(imdb_id)
            movie_ratings.append(omdb_movie_details)
    delta_lake_manager.upsert_to_table(
        data=pd.DataFrame(movie_ratings),
        table_path="landing_zone/omdb/movie_ratings",
        merge_key="imdb_id",
    )


@task
def update_providers(
    all_movies: list[dict],
    tmdb_connector: TMDbConnector,
    delta_lake_manager: DeltaLakeManager,
):
    """Update movie providers using TMDB"""
    movie_providers = []
    for movie in all_movies:
        providers = tmdb_connector.get_movie_providers(movie["tmdb_id"])
        movie_providers.append(
            {
                "tmdb_id": movie["tmdb_id"],
                "providers": providers,
            }
        )
    delta_lake_manager.upsert_to_table(
        data=pd.DataFrame(movie_providers),
        table_path="landing_zone/tmdb/movies_providers",
        merge_key="tmdb_id",
    )


@task
def update_youtube_stats(
    all_movies: list[dict],
    youtube_connector: YouTubeConnector,
    delta_lake_manager: DeltaLakeManager,
):
    """Update YouTube stats for each movie"""
    movies_stats = []
    for movie in all_movies:
        yt_movie_trailer = {
            "tmdb_id": movie["tmdb_id"],
            "statistics": None,
            "comments": None,
        }
        trailer_url = movie.get("trailer")
        if trailer_url:
            video_id = trailer_url.split("v=")[1]
        else:
            video_id = youtube_connector.search_movie_trailer(movie["title"])
        if video_id:
            yt_movie_trailer["trailer_url"] = (
                f"https://www.youtube.com/watch?v={video_id}"
            )
            stats = youtube_connector.get_video_info(video_id)
            yt_movie_trailer.update(**stats)
        movies_stats.append(yt_movie_trailer)
    delta_lake_manager.upsert_to_table(
        data=pd.DataFrame(movies_stats),
        table_path="landing_zone/youtube/trailer_video_stats",
        merge_key="tmdb_id",
    )


@flow(name="Update Movie Data")
def update_movie_data(
    n_days_ago: int = N_DAYS_AGO,
):
    """Main flow to update movie data"""
    # Initialize connectors
    tmdb_connector = TMDbConnector(region=REGION)
    trakt_connector = TraktConnector()
    omdb_connector = OMDBConnector()
    youtube_connector = YouTubeConnector()

    delta_lake_manager = DeltaLakeManager(
        s3_bucket_name="bdm-movies-db",
    )

    # Get new releases and their Trakt details
    tmdb_released_movies = get_daily_releases(
        tmdb_connector=tmdb_connector,
        delta_lake_manager=delta_lake_manager,
        n_days_ago=n_days_ago,
    )
    trakt_movies = get_trakt_details(
        released_movies=tmdb_released_movies,
        trakt_connector=trakt_connector,
        delta_lake_manager=delta_lake_manager,
    )

    # Update ratings, providers, and YouTube stats
    all_movies_df = delta_lake_manager.read_table(
        table_path="landing_zone/trakt/movie_ids_and_trailer"
    )
    all_movies = all_movies_df.to_dict(orient="records")
    logger.info(f"Number of movies: {len(all_movies)}")
    update_movie_ratings(
        all_movies=all_movies,
        delta_lake_manager=delta_lake_manager,
        omdb_connector=omdb_connector,
    )
    update_providers(
        all_movies=all_movies,
        tmdb_connector=tmdb_connector,
        delta_lake_manager=delta_lake_manager,
    )
    update_youtube_stats(
        all_movies=all_movies,
        youtube_connector=youtube_connector,
        delta_lake_manager=delta_lake_manager,
    )

    # Show tables
    delta_lake_manager.show_table(table_path="landing_zone/tmdb/movies_released")
    delta_lake_manager.show_table(table_path="landing_zone/trakt/movie_ids_and_trailer")
    delta_lake_manager.show_table(table_path="landing_zone/omdb/movie_ratings")
    delta_lake_manager.show_table(table_path="landing_zone/tmdb/movies_providers")
    delta_lake_manager.show_table(table_path="landing_zone/youtube/trailer_video_stats")


if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/arnausaumell/bdm_project.git",
        entrypoint="core/landing_zone/load_tables.py:update_movie_data",
    ).deploy(
        name="update-movie-data",
        work_pool_name="bdm-movies-db-workpool",
        cron="* * * * *",  # Run daily at midnight
        description="Daily job to pull new movie releases and update movie data from various sources",
        image="prefecthq/prefect:3-latest",
    )
