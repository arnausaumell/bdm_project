import os
import sys

sys.path.append(os.getcwd())

import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
from prefect import flow, task
from prefect.cache_policies import NO_CACHE

from core.orchestration.utils import test_environment
from core.data_ingestion.batch_ingestion import (
    TMDbConnector,
    TraktConnector,
    OMDBConnector,
    YouTubeConnector,
)
from core.landing_and_trusted_zone.deltalake_manager import DeltaLakeManager
from dotenv import load_dotenv

load_dotenv()

N_DAYS_AGO = 7
REGION = "ES"


@task(cache_policy=NO_CACHE)
def get_daily_releases(
    tmdb_connector: TMDbConnector,
    delta_lake_manager: DeltaLakeManager,
    n_days_ago: int = N_DAYS_AGO,
):
    """Get last n_days_ago releases from TMDB"""
    try:
        start_date = (datetime.now() - timedelta(days=n_days_ago)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")
        released_movies = tmdb_connector.get_movies_by_date_range(
            start_date=start_date,
            end_date=end_date,
        )
        logger.info(f"Found {len(released_movies)} new releases")
        delta_lake_manager.upsert_to_table(
            data=pd.DataFrame(released_movies),
            table_path="tmdb/movies_released",
            merge_key="tmdb_id",
        )
        return released_movies
    except Exception as e:
        logger.error(f"Error in get_daily_releases: {e}")
        raise


@task(cache_policy=NO_CACHE)
def get_trakt_details(
    released_movies: list[dict],
    trakt_connector: TraktConnector,
    delta_lake_manager: DeltaLakeManager,
):
    """Get trakt details for each movie"""
    try:
        trakt_movies = []
        for movie in released_movies:
            try:
                trakt_movie_details = trakt_connector.get_movie_details_tmdb_id(
                    movie["tmdb_id"]
                )
                trakt_movies.append(trakt_movie_details)
            except Exception as e:
                logger.error(
                    f"Error getting Trakt details for movie {movie['tmdb_id']}: {e}"
                )

        if not trakt_movies:
            logger.warning("No Trakt movie details were retrieved successfully")
            return []

        delta_lake_manager.upsert_to_table(
            data=pd.DataFrame(trakt_movies),
            table_path="trakt/movie_ids_and_trailer",
            merge_key="trakt_id",
        )
        return trakt_movies
    except Exception as e:
        logger.error(f"Error in get_trakt_details: {e}")
        raise


@task(cache_policy=NO_CACHE)
def update_movie_ratings(
    all_movies: list[dict],
    delta_lake_manager: DeltaLakeManager,
    omdb_connector: OMDBConnector,
):
    """Update movie ratings using OMDB"""
    try:
        movie_ratings = []
        for movie in all_movies:
            imdb_id = movie.get("imdb_id")
            if imdb_id:
                try:
                    omdb_movie_details = omdb_connector.get_movie_by_imdb_id(imdb_id)
                    movie_ratings.append(omdb_movie_details)
                except Exception as e:
                    logger.error(f"Error getting OMDB details for movie {imdb_id}: {e}")

        if not movie_ratings:
            logger.warning("No OMDB movie ratings were retrieved successfully")
            return

        delta_lake_manager.upsert_to_table(
            data=pd.DataFrame(movie_ratings),
            table_path="omdb/movie_ratings",
            merge_key="imdb_id",
        )
    except Exception as e:
        logger.error(f"Error in update_movie_ratings: {e}")
        raise


@task(cache_policy=NO_CACHE)
def update_providers(
    all_movies: list[dict],
    tmdb_connector: TMDbConnector,
    delta_lake_manager: DeltaLakeManager,
):
    """Update movie providers using TMDB"""
    try:
        movie_providers = []
        for movie in all_movies:
            try:
                providers = tmdb_connector.get_movie_providers(movie["tmdb_id"])
                if providers:
                    movie_providers.append(
                        {
                            "tmdb_id": movie["tmdb_id"],
                            "providers": providers,
                        }
                    )
            except Exception as e:
                logger.error(
                    f"Error getting providers for movie {movie['tmdb_id']}: {e}"
                )

        if not movie_providers:
            logger.warning("No movie providers were retrieved successfully")
            return

        delta_lake_manager.upsert_to_table(
            data=pd.DataFrame(movie_providers),
            table_path="tmdb/movies_providers",
            merge_key="tmdb_id",
        )
    except Exception as e:
        logger.error(f"Error in update_providers: {e}")
        logger.debug(movie_providers)
        raise


@task(cache_policy=NO_CACHE)
def update_youtube_stats(
    all_movies: list[dict],
    youtube_connector: YouTubeConnector,
    delta_lake_manager: DeltaLakeManager,
):
    """Update YouTube stats for each movie"""
    try:
        movies_stats = []
        for movie in all_movies:
            try:
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
            except Exception as e:
                logger.error(
                    f"Error getting YouTube stats for movie {movie.get('tmdb_id', 'unknown')}: {e}"
                )

        if not movies_stats:
            logger.warning("No YouTube stats were retrieved successfully")
            return

        delta_lake_manager.upsert_to_table(
            data=pd.DataFrame(movies_stats),
            table_path="youtube/trailer_video_stats",
            merge_key="tmdb_id",
        )
    except Exception as e:
        logger.error(f"Error in update_youtube_stats: {e}")
        logger.debug(movies_stats)
        raise


@flow(name="from-external-to-landing")
def update_movie_data(n_days_ago: int = N_DAYS_AGO):
    """Update movie data in Delta Lake tables"""

    logger.info("Starting update_movie_data flow")

    try:
        # Test environment first
        test_environment()

        # Initialize connectors
        logger.info("Initializing connectors")
        tmdb_connector = TMDbConnector(region=REGION)
        trakt_connector = TraktConnector()
        omdb_connector = OMDBConnector()
        youtube_connector = YouTubeConnector()

        logger.info("Initializing DeltaLakeManager")
        landing_delta_lake_manager = DeltaLakeManager(
            s3_bucket_name="bdm-movies-db-landing",
        )

        # Get new releases and their Trakt details
        tmdb_released_movies = get_daily_releases(
            tmdb_connector=tmdb_connector,
            delta_lake_manager=landing_delta_lake_manager,
            n_days_ago=n_days_ago,
        )

        # Get trakt details
        _ = get_trakt_details(
            released_movies=tmdb_released_movies,
            trakt_connector=trakt_connector,
            delta_lake_manager=landing_delta_lake_manager,
        )

        # Update ratings, providers, and YouTube stats
        all_movies_df = landing_delta_lake_manager.read_table(
            table_path="trakt/movie_ids_and_trailer"
        )
        all_movies = all_movies_df.toPandas().to_dict(orient="records")

        # Update ratings
        update_movie_ratings(
            all_movies=all_movies,
            delta_lake_manager=landing_delta_lake_manager,
            omdb_connector=omdb_connector,
        )

        # Update providers
        update_providers(
            all_movies=all_movies,
            tmdb_connector=tmdb_connector,
            delta_lake_manager=landing_delta_lake_manager,
        )

        # Update YouTube stats
        update_youtube_stats(
            all_movies=all_movies,
            youtube_connector=youtube_connector,
            delta_lake_manager=landing_delta_lake_manager,
        )

        # Show tables
        landing_delta_lake_manager.show_table("tmdb/movies_released")
        landing_delta_lake_manager.show_table("trakt/movie_ids_and_trailer")
        landing_delta_lake_manager.show_table("omdb/movie_ratings")
        landing_delta_lake_manager.show_table("tmdb/movies_providers")
        landing_delta_lake_manager.show_table("youtube/trailer_video_stats")

    except Exception as e:
        logger.error(f"Error in update_movie_data flow: {e}")
        raise


if __name__ == "__main__":
    from prefect.docker import DockerImage

    logger.info("Creating deployment")
    deployment = update_movie_data.deploy(
        name="from-external-to-landing",
        work_pool_name="my-docker-pool",
        cron="0 0 * * *",
        image=DockerImage(
            name="arnausau11/orchestration",
            tag="latest",
            dockerfile="core/orchestration/Dockerfile",
        ),
        push=True,
    )
    logger.info("Deployment created")
