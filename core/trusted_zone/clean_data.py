import os
import sys

sys.path.append(os.getcwd())

from loguru import logger
from prefect import flow, task
from prefect.cache_policies import NO_CACHE
from prefect.docker import DockerImage
from pyspark.sql import SparkSession
from dotenv import load_dotenv

from core.landing_zone.deltalake_manager import DeltaLakeManager

load_dotenv()


@task(cache_policy=NO_CACHE)
def test_environment():
    logger.info("Testing environment variables")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_DEFAULT_REGION")

    logger.info(f"AWS Access Key exists: {bool(aws_access_key)}")
    logger.info(f"AWS Secret Key exists: {bool(aws_secret_key)}")
    logger.info(f"AWS Region: {aws_region}")

    return True


@task(cache_policy=NO_CACHE)
def initialize_spark():
    """Initialize Spark session with Delta Lake support"""
    logger.info("Initializing Spark session")
    spark = (
        SparkSession.builder.appName("MovieDataCleaning")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    return spark


@task(cache_policy=NO_CACHE)
def clean_tmdb_movies_released(spark, source_manager, target_manager):
    """Clean TMDB movies released data"""
    logger.info("Cleaning TMDB movies released data")

    # Read from landing zone
    df = source_manager.read_table_as_spark(spark, "tmdb/movies_released")

    # Cleaning operations
    cleaned_df = df.dropDuplicates(["tmdb_id"])
    cleaned_df = cleaned_df.na.fill("", ["overview", "poster_path"])

    # Write to trusted zone
    target_manager.write_spark_df_to_table(
        df=cleaned_df, table_path="tmdb/movies_released_clean", mode="overwrite"
    )

    logger.info(f"Cleaned {df.count()} TMDB movies to {cleaned_df.count()} records")


@task(cache_policy=NO_CACHE)
def clean_trakt_movie_data(spark, source_manager, target_manager):
    """Clean Trakt movie data"""
    logger.info("Cleaning Trakt movie data")

    # Read from landing zone
    df = source_manager.read_table_as_spark(spark, "trakt/movie_ids_and_trailer")

    # Cleaning operations
    cleaned_df = df.dropDuplicates(["trakt_id"])
    cleaned_df = cleaned_df.na.fill("", ["trailer"])

    # Write to trusted zone
    target_manager.write_spark_df_to_table(
        df=cleaned_df, table_path="trakt/movie_ids_and_trailer_clean", mode="overwrite"
    )

    logger.info(f"Cleaned {df.count()} Trakt movies to {cleaned_df.count()} records")


@task(cache_policy=NO_CACHE)
def clean_omdb_ratings(spark, source_manager, target_manager):
    """Clean OMDB movie ratings data"""
    logger.info("Cleaning OMDB movie ratings data")

    # Read from landing zone
    df = source_manager.read_table_as_spark(spark, "omdb/movie_ratings")

    # Cleaning operations
    cleaned_df = df.dropDuplicates(["imdb_id"])

    # Convert ratings to numeric values
    from pyspark.sql.functions import col, regexp_extract

    # Extract numeric ratings and convert to double
    for rating_col in ["imdb_rating", "metacritic", "rotten_tomatoes"]:
        if rating_col in df.columns:
            cleaned_df = cleaned_df.withColumn(
                f"{rating_col}_numeric", col(rating_col).cast("double")
            )

    # Write to trusted zone
    target_manager.write_spark_df_to_table(
        df=cleaned_df, table_path="omdb/movie_ratings_clean", mode="overwrite"
    )

    logger.info(f"Cleaned {df.count()} OMDB ratings to {cleaned_df.count()} records")


@task(cache_policy=NO_CACHE)
def clean_tmdb_providers(spark, source_manager, target_manager):
    """Clean TMDB movie providers data"""
    logger.info("Cleaning TMDB movie providers data")

    # Read from landing zone
    df = source_manager.read_table_as_spark(spark, "tmdb/movies_providers")

    # Cleaning operations
    cleaned_df = df.dropDuplicates(["tmdb_id"])

    # Write to trusted zone
    target_manager.write_spark_df_to_table(
        df=cleaned_df, table_path="tmdb/movies_providers_clean", mode="overwrite"
    )

    logger.info(f"Cleaned {df.count()} TMDB providers to {cleaned_df.count()} records")


@task(cache_policy=NO_CACHE)
def clean_youtube_stats(spark, source_manager, target_manager):
    """Clean YouTube trailer stats data"""
    logger.info("Cleaning YouTube trailer stats data")

    # Read from landing zone
    df = source_manager.read_table_as_spark(spark, "youtube/trailer_video_stats")

    # Cleaning operations
    cleaned_df = df.dropDuplicates(["tmdb_id"])

    # Convert numeric fields
    from pyspark.sql.functions import col

    numeric_cols = ["view_count", "like_count", "comment_count"]
    for num_col in numeric_cols:
        if num_col in df.columns:
            cleaned_df = cleaned_df.withColumn(num_col, col(num_col).cast("long"))

    # Write to trusted zone
    target_manager.write_spark_df_to_table(
        df=cleaned_df, table_path="youtube/trailer_video_stats_clean", mode="overwrite"
    )

    logger.info(f"Cleaned {df.count()} YouTube stats to {cleaned_df.count()} records")


@flow(name="clean-movie-data")
def clean_movie_data():
    """Clean movie data from landing zone and store in trusted zone"""
    logger.info("Starting clean_movie_data flow")

    # Test environment first
    test_environment()

    # Initialize Spark
    spark = initialize_spark()

    # Initialize DeltaLakeManagers for source and target
    logger.info("Initializing DeltaLakeManagers")
    source_manager = DeltaLakeManager(
        s3_bucket_name="bdm-movies-db",
    )

    target_manager = DeltaLakeManager(
        s3_bucket_name="bdm-movies-db-trusted",
    )

    # Clean each table
    clean_tmdb_movies_released(spark, source_manager, target_manager)
    clean_trakt_movie_data(spark, source_manager, target_manager)
    clean_omdb_ratings(spark, source_manager, target_manager)
    clean_tmdb_providers(spark, source_manager, target_manager)
    clean_youtube_stats(spark, source_manager, target_manager)

    # Show cleaned tables
    target_manager.show_table(table_path="tmdb/movies_released_clean")
    target_manager.show_table(table_path="trakt/movie_ids_and_trailer_clean")
    target_manager.show_table(table_path="omdb/movie_ratings_clean")
    target_manager.show_table(table_path="tmdb/movies_providers_clean")
    target_manager.show_table(table_path="youtube/trailer_video_stats_clean")

    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    logger.info("Creating deployment")
    deployment = clean_movie_data.deploy(
        name="trusted-zone-cleaning-deployment",
        work_pool_name="my-docker-pool",
        cron="0 2 * * *",  # Run at 2 AM daily (after batch ingestion)
        image=DockerImage(
            name="arnausau11/trusted-zone-cleaning",
            tag="latest",
            dockerfile="core/data_ingestion/trusted_zone/Dockerfile",
        ),
        push=True,
    )
    logger.info("Deployment created")
