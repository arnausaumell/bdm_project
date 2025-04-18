import os
import sys

sys.path.append(os.getcwd())

import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.docker import DockerImage

from core.data_ingestion.batch_ingestion.connectors.tmdb_connector import TMDbConnector
from core.data_ingestion.batch_ingestion.connectors.trakt_connector import (
    TraktConnector,
)
from core.data_ingestion.batch_ingestion.connectors.omdb_connector import OMDBConnector
from core.data_ingestion.batch_ingestion.connectors.youtube_connector import (
    YouTubeConnector,
)
from core.landing_zone.deltalake_manager import DeltaLakeManager
from dotenv import load_dotenv

load_dotenv()

N_DAYS_AGO = 1
REGION = "ES"


def get_daily_releases(
    tmdb_connector: TMDbConnector,
    delta_lake_manager: DeltaLakeManager,
    n_days_ago: int = N_DAYS_AGO,
):
    """Get last n_days_ago releases from TMDB"""
    start_date = (datetime.now() - timedelta(days=n_days_ago)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")
    released_movies = tmdb_connector.get_movies_by_date_range(
        start_date=start_date,
        end_date=end_date,
    )
    delta_lake_manager.upsert_to_table(
        data=pd.DataFrame(released_movies),
        table_path="tmdb/movies_released",
        merge_key="tmdb_id",
    )
    return released_movies


@task(cache_policy=NO_CACHE)
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
        table_path="trakt/movie_ids_and_trailer",
        merge_key="trakt_id",
    )
    return trakt_movies


@task(cache_policy=NO_CACHE)
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
        table_path="omdb/movie_ratings",
        merge_key="imdb_id",
    )


@task(cache_policy=NO_CACHE)
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
        table_path="tmdb/movies_providers",
        merge_key="tmdb_id",
    )


@task(cache_policy=NO_CACHE)
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
        table_path="youtube/trailer_video_stats",
        merge_key="tmdb_id",
    )


@task
def test_environment():
    logger.info("Testing environment variables")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_DEFAULT_REGION")

    logger.info(f"AWS Access Key exists: {bool(aws_access_key)}")
    logger.info(f"AWS Secret Key exists: {bool(aws_secret_key)}")
    logger.info(f"AWS Region: {aws_region}")

    return True


@flow(name="update-movie-data")
def update_movie_data(n_days_ago: int = N_DAYS_AGO):
    """Update movie data in Delta Lake tables"""
    logger.info("Starting update_movie_data flow")

    # Test environment first
    test_environment()

    # Initialize connectors
    logger.info("Initializing connectors")
    tmdb_connector = TMDbConnector(region=REGION)
    trakt_connector = TraktConnector()
    omdb_connector = OMDBConnector()
    youtube_connector = YouTubeConnector()

    logger.info("Initializing DeltaLakeManager")
    delta_lake_manager = DeltaLakeManager(
        s3_bucket_name="bdm-movies-db",
    )

    # Get new releases and their Trakt details
    logger.info("Getting daily releases")
    tmdb_released_movies = get_daily_releases(
        tmdb_connector=tmdb_connector,
        delta_lake_manager=delta_lake_manager,
        n_days_ago=n_days_ago,
    )

    logger.info(f"Found {len(tmdb_released_movies)} new releases")
    trakt_movies = get_trakt_details(
        released_movies=tmdb_released_movies,
        trakt_connector=trakt_connector,
        delta_lake_manager=delta_lake_manager,
    )

    # Update ratings, providers, and YouTube stats
    all_movies_df = delta_lake_manager.read_table(
        table_path="trakt/movie_ids_and_trailer"
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
    delta_lake_manager.show_table(table_path="tmdb/movies_released")
    delta_lake_manager.show_table(table_path="trakt/movie_ids_and_trailer")
    delta_lake_manager.show_table(table_path="omdb/movie_ratings")
    delta_lake_manager.show_table(table_path="tmdb/movies_providers")
    delta_lake_manager.show_table(table_path="youtube/trailer_video_stats")


if __name__ == "__main__":
    logger.info("Creating deployment")
    deployment = update_movie_data.deploy(
        name="batch-ingestion-deployment",
        work_pool_name="my-docker-pool",
        cron="0 0 * * *",
        image=DockerImage(
            name="arnausau11/batch-ingestion",
            tag="latest",
            dockerfile="core/data_ingestion/batch_ingestion/Dockerfile",
        ),
        push=True,
    )
    logger.info("Deployment created")
