import os
import sys
from typing import Tuple

sys.path.append(os.getcwd())

from loguru import logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    explode,
    col,
    to_timestamp,
    when,
    lit,
    concat_ws,
    from_json,
    format_number,
)
from pyspark.sql.types import (
    IntegerType,
    StringType,
    FloatType,
    StructType,
    StructField,
    ArrayType,
)
from dotenv import load_dotenv
from core.orchestration.utils import test_environment
from core.landing_and_trusted_zone.deltalake_manager import DeltaLakeManager
from prefect import flow, task
from prefect.cache_policies import NO_CACHE

load_dotenv()


@task(cache_policy=NO_CACHE)
def transform_tmdb_movies_released(
    source_manager: DeltaLakeManager,
    target_manager: DeltaLakeManager,
) -> None:
    """
    Clean TMDB movies released data and extract related tables.

    Args:
        source_manager: DeltaLakeManager for reading from landing zone
        target_manager: DeltaLakeManager for writing to trusted zone
    """
    try:
        logger.info("Cleaning TMDB movies released data")

        def _clean_movies_dataframe(movies_df: DataFrame) -> DataFrame:
            """
            Clean and transform the main movies dataframe.

            Args:
                movies_df: Raw movies dataframe from landing zone

            Returns:
                Cleaned movies dataframe
            """
            logger.info("Cleaning main movies dataframe")
            before_count = movies_df.count()

            # Cast ID and title columns and filter invalid records
            new_movies_df = movies_df.filter(
                col("tmdb_id").cast(IntegerType()).isNotNull()
            ).withColumn("tmdb_id", col("tmdb_id").cast(IntegerType()))

            new_movies_df = new_movies_df.withColumn(
                "title", col("title").cast(StringType())
            ).filter(col("title").isNotNull() & (col("title") != ""))

            # Format nullable string columns
            string_columns = ["overview", "original_title", "poster_path", "tagline"]
            for column in string_columns:
                new_movies_df = new_movies_df.withColumn(
                    column, col(column).cast(StringType())
                )

            # Format nullable numeric columns
            numeric_columns = ["budget", "revenue", "runtime"]
            for column in numeric_columns:
                new_movies_df = new_movies_df.withColumn(
                    column, col(column).cast(FloatType())
                )

            # Format dates
            new_movies_df = new_movies_df.withColumn(
                "release_date", col("release_date").cast(StringType())
            ).filter(col("release_date").rlike(r"^\d{4}-\d{2}-\d{2}$"))

            # Format origin_country
            new_movies_df = new_movies_df.withColumn(
                "origin_country", concat_ws("&", col("origin_country"))
            )

            after_count = new_movies_df.count()
            logger.info(
                f"Cleaned {before_count} records down to {after_count} valid TMDB movies"
            )

            return new_movies_df

        def _extract_cast_tables(movies_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
            """
            Extract cast and movies_cast tables from the movies dataframe.

            Args:
                movies_df: Cleaned movies dataframe

            Returns:
                Tuple containing (cast_df, movies_cast_df)
            """
            logger.info("Extracting cast and movies_cast tables")

            # Define the schema for the credits column
            credits_schema = ArrayType(
                StructType(
                    [
                        StructField("id", IntegerType(), True),
                        StructField("role", StringType(), True),
                        StructField("character", StringType(), True),
                        StructField("name", StringType(), True),
                    ]
                )
            )

            # Parse the credits column as JSON
            movies_df = movies_df.withColumn(
                "credits", from_json(col("credits"), credits_schema)
            )

            credits_expanded = movies_df.select(
                col("tmdb_id").alias("movie_tmdb_id"),
                explode(col("credits")).alias("credit"),
            ).filter(col("credit.id").cast(IntegerType()).isNotNull())

            movies_cast_df = credits_expanded.select(
                col("movie_tmdb_id").alias("tmdb_id"),
                col("credit.id").cast(IntegerType()).alias("cast_id"),
                col("credit.role").cast(StringType()).alias("role"),
                col("credit.character").cast(StringType()).alias("character"),
            )

            # Add a composite key column for merging
            movies_cast_df = movies_cast_df.withColumn(
                "movie_cast_id", concat_ws("_", col("tmdb_id"), col("cast_id"))
            )

            cast_df = (
                credits_expanded.select(
                    col("credit.id").cast(IntegerType()).alias("cast_id"),
                    col("credit.name").cast(StringType()).alias("name"),
                )
                .filter(col("cast_id").isNotNull() & col("name").isNotNull())
                .distinct()
            )

            logger.info(f"Extracted {cast_df.count()} cast records")

            # Ensure movie_cast_id is unique by deduplicating
            movies_cast_df = movies_cast_df.dropDuplicates(["movie_cast_id"])

            return cast_df, movies_cast_df

        def _extract_reviews_table(movies_df: DataFrame) -> DataFrame:
            """
            Extract reviews table from the movies dataframe.

            Args:
                movies_df: Cleaned movies dataframe

            Returns:
                Reviews dataframe
            """
            logger.info("Extracting reviews table")

            # Define the schema for the reviews column
            reviews_schema = ArrayType(
                StructType(
                    [
                        StructField("author", StringType(), True),
                        StructField("content", StringType(), True),
                        StructField("created_at", StringType(), True),
                        StructField("url", StringType(), True),
                    ]
                )
            )

            # Parse the reviews column as JSON
            movies_df = movies_df.withColumn(
                "reviews", from_json(col("reviews"), reviews_schema)
            )

            reviews_expanded = movies_df.select(
                col("tmdb_id").alias("movie_tmdb_id"),
                explode(col("reviews")).alias("review"),
            ).filter(
                col("review.author").cast(StringType()).isNotNull()
                & (col("review.author") != "")
                & col("review.content").cast(StringType()).isNotNull()
                & (col("review.content") != "")
                & col("review.created_at").cast(StringType()).isNotNull()
                & (col("review.created_at") != "")
            )

            reviews_df = (
                reviews_expanded.select(
                    col("movie_tmdb_id").alias("tmdb_id"),
                    col("review.author").cast(StringType()).alias("author"),
                    col("review.content").cast(StringType()).alias("content"),
                    # parse ISO8601 string into TimestampType
                    to_timestamp(
                        col("review.created_at"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                    ).alias("created_at"),
                    # validate URL, else null
                    when(
                        col("review.url")
                        .cast(StringType())
                        .rlike(r"^(https?|ftp)://[^\s/$.?#].[^\s]*$"),
                        col("review.url").cast(StringType()),
                    )
                    .otherwise(lit(None))
                    .alias("url"),
                ).filter(
                    col("created_at").isNotNull()
                )  # drop rows with malformed dates
            )

            logger.info(f"Extracted {reviews_df.count()} valid review records")

            # Ensure tmdb_id is unique by deduplicating
            reviews_df = reviews_df.dropDuplicates(["tmdb_id"])

            return reviews_df

        def _extract_genres_tables(movies_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
            """
            Extract genres and movies_genres tables from the movies dataframe.

            Args:
                movies_df: Cleaned movies dataframe

            Returns:
                Tuple containing (genres_df, movies_genres_df)
            """
            logger.info("Extracting genres and movies_genres tables")

            # Define the schema for the genres column
            genres_schema = ArrayType(
                StructType(
                    [
                        StructField("id", IntegerType(), True),
                        StructField("name", StringType(), True),
                    ]
                )
            )

            # Parse the genres column as JSON
            movies_df = movies_df.withColumn(
                "genres", from_json(col("genres"), genres_schema)
            )

            genres_expanded = movies_df.select(
                col("tmdb_id").alias("movie_tmdb_id"),
                explode(col("genres")).alias("genre"),
            ).filter(
                col("genre.id").cast(IntegerType()).isNotNull()
                & col("genre.name").cast(StringType()).isNotNull()
            )

            genres_df = genres_expanded.select(
                col("genre.id").cast(IntegerType()).alias("genre_id"),
                col("genre.name").cast(StringType()).alias("name"),
            ).distinct()

            movies_genres_df = genres_expanded.select(
                col("movie_tmdb_id").alias("tmdb_id"),
                col("genre.id").cast(IntegerType()).alias("genre_id"),
            )

            # Add a composite key column for merging
            movies_genres_df = movies_genres_df.withColumn(
                "movie_genre_id", concat_ws("_", col("tmdb_id"), col("genre_id"))
            )

            logger.info(f"Extracted {genres_df.count()} unique genres")
            logger.info(f"Extracted {movies_genres_df.count()} movie–genre rows")

            # Ensure genre_id is unique by deduplicating
            genres_df = genres_df.dropDuplicates(["genre_id"])
            movies_genres_df = movies_genres_df.dropDuplicates(["movie_genre_id"])

            return genres_df, movies_genres_df

        def _extract_languages_tables(
            movies_df: DataFrame,
        ) -> Tuple[DataFrame, DataFrame]:
            """
            Extract languages and movies_language tables from the movies dataframe.

            Args:
                movies_df: Cleaned movies dataframe

            Returns:
                Tuple containing (languages_df, movies_language_df)
            """
            logger.info("Extracting languages and movies_language tables")

            # Define the schema for the languages column
            languages_schema = ArrayType(
                StructType(
                    [
                        StructField("iso_639_1", StringType(), True),
                        StructField("iso_3166_1", StringType(), True),
                        StructField("name", StringType(), True),
                    ]
                )
            )

            # Parse the languages column as JSON
            movies_df = movies_df.withColumn(
                "languages", from_json(col("languages"), languages_schema)
            )

            langs_expanded = movies_df.select(
                col("tmdb_id").alias("movie_tmdb_id"),
                explode(col("languages")).alias("lang"),
            ).filter(
                col("lang.iso_639_1").cast(StringType()).isNotNull()
                & (col("lang.iso_639_1").cast(StringType()) != "")
                & col("lang.name").cast(StringType()).isNotNull()
                & (col("lang.name").cast(StringType()) != "")
            )

            languages_df = langs_expanded.select(
                col("lang.iso_639_1").cast(StringType()).alias("iso_639_1"),
                col("lang.iso_3166_1").cast(StringType()).alias("iso_3166_1"),
                col("lang.name").cast(StringType()).alias("name"),
            ).dropDuplicates(["iso_639_1"])

            movies_language_df = langs_expanded.select(
                col("movie_tmdb_id").alias("tmdb_id"),
                col("lang.iso_639_1").cast(StringType()).alias("iso_639_1"),
            )

            # Add a composite key column for merging
            movies_language_df = movies_language_df.withColumn(
                "movie_language_id", concat_ws("_", col("tmdb_id"), col("iso_639_1"))
            )

            logger.info(f"Extracted {languages_df.count()} languages")

            # Ensure iso_639_1 is unique by deduplicating
            languages_df = languages_df.dropDuplicates(["iso_639_1"])
            movies_language_df = movies_language_df.dropDuplicates(
                ["movie_language_id"]
            )

            return languages_df, movies_language_df

        def _extract_keywords_tables(
            movies_df: DataFrame,
        ) -> Tuple[DataFrame, DataFrame]:
            """
            Extract keywords and movies_keywords tables from the movies dataframe.

            Args:
                movies_df: Cleaned movies dataframe

            Returns:
                Tuple containing (keywords_df, movies_keywords_df)
            """
            logger.info("Extracting keywords and movies_keywords tables")

            # Define the schema for the keywords column
            keywords_schema = ArrayType(
                StructType(
                    [
                        StructField("id", IntegerType(), True),
                        StructField("name", StringType(), True),
                    ]
                )
            )

            # Parse the keywords column as JSON
            movies_df = movies_df.withColumn(
                "keywords", from_json(col("keywords"), keywords_schema)
            )

            keywords_expanded = movies_df.select(
                col("tmdb_id").alias("movie_tmdb_id"),
                explode(col("keywords")).alias("keyword"),
            ).filter(
                # id must cast to int
                col("keyword.id").cast(IntegerType()).isNotNull()
                &
                # name must be a non-null, capitalized string (each word starts with uppercase, rest lowercase)
                col("keyword.name").cast(StringType()).isNotNull()
            )

            keywords_df = keywords_expanded.select(
                col("keyword.id").cast(IntegerType()).alias("keyword_id"),
                col("keyword.name").cast(StringType()).alias("name"),
            ).distinct()

            movies_keywords_df = keywords_expanded.select(
                col("movie_tmdb_id").alias("tmdb_id"),
                col("keyword.id").cast(IntegerType()).alias("keyword_id"),
            )

            # Add a composite key column for merging
            movies_keywords_df = movies_keywords_df.withColumn(
                "movie_keyword_id", concat_ws("_", col("tmdb_id"), col("keyword_id"))
            )

            logger.info(f"Extracted {keywords_df.count()} unique keywords")
            logger.info(f"Extracted {movies_keywords_df.count()} movie–keyword rows")

            # Ensure keyword_id is unique by deduplicating
            keywords_df = keywords_df.dropDuplicates(["keyword_id"])
            movies_keywords_df = movies_keywords_df.dropDuplicates(["movie_keyword_id"])

            return keywords_df, movies_keywords_df

        # Read from landing zone
        movies_df = source_manager.read_table("tmdb/movies_released")

        # Clean and transform the main movies dataframe
        new_movies_df = _clean_movies_dataframe(movies_df)

        # Extract related tables
        cast_df, movies_cast_df = _extract_cast_tables(new_movies_df)
        new_movies_df = new_movies_df.drop("credits")

        reviews_df = _extract_reviews_table(new_movies_df)
        new_movies_df = new_movies_df.drop("reviews")

        genres_df, movies_genres_df = _extract_genres_tables(new_movies_df)
        new_movies_df = new_movies_df.drop("genres")

        languages_df, movies_language_df = _extract_languages_tables(new_movies_df)
        new_movies_df = new_movies_df.drop("languages")

        keywords_df, movies_keywords_df = _extract_keywords_tables(new_movies_df)
        new_movies_df = new_movies_df.drop("keywords")

        # Write all tables to trusted zone
        target_manager.upsert_to_table(new_movies_df, "movies", merge_key="tmdb_id")
        target_manager.upsert_to_table(cast_df, "cast", merge_key="cast_id")
        target_manager.upsert_to_table(
            movies_cast_df, "movies_cast", merge_key="movie_cast_id"
        )
        target_manager.upsert_to_table(reviews_df, "reviews", merge_key="tmdb_id")
        target_manager.upsert_to_table(genres_df, "genres", merge_key="genre_id")
        target_manager.upsert_to_table(
            movies_genres_df, "movies_genres", merge_key="movie_genre_id"
        )
        target_manager.upsert_to_table(languages_df, "languages", merge_key="iso_639_1")
        target_manager.upsert_to_table(
            movies_language_df, "movies_languages", merge_key="movie_language_id"
        )
        target_manager.upsert_to_table(keywords_df, "keywords", merge_key="keyword_id")
        target_manager.upsert_to_table(
            movies_keywords_df, "movies_keywords", merge_key="movie_keyword_id"
        )

        logger.info("Successfully wrote all TMDB movies tables to trusted zone")

    except Exception as e:
        logger.error(f"Error in transform_tmdb_movies_released: {e}")
        raise


@task(cache_policy=NO_CACHE)
def transform_tmdb_movies_providers(
    source_manager: DeltaLakeManager,
    target_manager: DeltaLakeManager,
) -> None:
    """
    Clean TMDB movie providers data and extract related tables.

    Args:
        source_manager: DeltaLakeManager for reading from landing zone
        target_manager: DeltaLakeManager for writing to trusted zone
    """
    try:
        logger.info("Cleaning TMDB movie providers data")

        # Read from landing zone
        providers_df = source_manager.read_table("tmdb/movies_providers")

        def _extract_providers_tables(
            providers_df: DataFrame,
        ) -> Tuple[DataFrame, DataFrame]:
            """
            Extract providers and movie_providers tables from the providers dataframe.

            Args:
                providers_df: Raw providers dataframe from landing zone

            Returns:
                Tuple containing (providers_df, movie_providers_df)
            """
            logger.info("Extracting providers and movie_providers tables")

            # Define the schema for the providers column
            providers_schema = ArrayType(
                StructType(
                    [
                        StructField("provider_id", IntegerType(), True),
                        StructField("provider_name", StringType(), True),
                        StructField("logo_path", StringType(), True),
                        StructField("display_priority", IntegerType(), True),
                        StructField("provider_type", StringType(), True),
                    ]
                )
            )

            # Parse the providers column as JSON
            providers_df = providers_df.withColumn(
                "providers", from_json(col("providers"), providers_schema)
            )

            # Explode the providers array
            providers_exploded = providers_df.select(
                col("tmdb_id").cast(IntegerType()).alias("tmdb_id"),
                explode(col("providers")).alias("provider"),
            ).filter(col("tmdb_id").isNotNull())

            # Extract unique providers
            providers_df = providers_exploded.select(
                col("provider.provider_id").cast(IntegerType()).alias("provider_id"),
                col("provider.provider_name").cast(StringType()).alias("provider_name"),
                col("provider.logo_path").cast(StringType()).alias("logo_path"),
                col("provider.display_priority")
                .cast(IntegerType())
                .alias("display_priority"),
            ).distinct()

            # Create movie_providers relationship table
            movie_providers_df = providers_exploded.select(
                col("tmdb_id").cast(IntegerType()).alias("tmdb_id"),
                col("provider.provider_id").cast(IntegerType()).alias("provider_id"),
                col("provider.provider_type").cast(StringType()).alias("provider_type"),
            )

            # Create a composite key column for merging
            movie_providers_df = movie_providers_df.withColumn(
                "movie_provider_id",
                concat_ws(
                    "_", col("tmdb_id"), col("provider_id"), col("provider_type")
                ),
            )

            logger.info(f"Extracted {providers_df.count()} unique providers")
            logger.info(
                f"Extracted {movie_providers_df.count()} movie-provider relationships"
            )

            return providers_df, movie_providers_df

        # Extract providers and movie_providers tables
        providers_df, movie_providers_df = _extract_providers_tables(providers_df)

        # Write tables to trusted zone
        target_manager.upsert_to_table(
            providers_df, "providers", merge_key="provider_id"
        )
        target_manager.upsert_to_table(
            movie_providers_df, "movies_providers", merge_key="movie_provider_id"
        )

        logger.info("Successfully wrote TMDB movie providers tables to trusted zone")

    except Exception as e:
        logger.error(f"Error in transform_tmdb_movies_providers: {e}")
        raise


@task(cache_policy=NO_CACHE)
def transform_trakt_movie_data(
    source_manager: DeltaLakeManager,
    target_manager: DeltaLakeManager,
) -> None:
    """
    Clean Trakt movie data with basic formatting checks and copy to trusted zone.

    Args:
        source_manager: DeltaLakeManager for reading from landing zone
        target_manager: DeltaLakeManager for writing to trusted zone
    """
    try:
        logger.info("Cleaning Trakt movie data")

        # Read from landing zone
        trakt_df = source_manager.read_table("trakt/movie_ids_and_trailer")

        # Log initial count
        before_count = trakt_df.count()
        logger.info(f"Read {before_count} records from Trakt movie details")

        # Perform basic formatting checks
        # Ensure tmdb_id is an integer and not null
        cleaned_df = trakt_df.filter(
            col("tmdb_id").cast(IntegerType()).isNotNull()
        ).withColumn("tmdb_id", col("tmdb_id").cast(IntegerType()))

        # Cast other columns to appropriate types but don't filter on them
        cleaned_df = cleaned_df.withColumn("title", col("title").cast(StringType()))
        cleaned_df = cleaned_df.withColumn("trailer", col("trailer").cast(StringType()))
        cleaned_df = cleaned_df.withColumn(
            "trakt_id", col("trakt_id").cast(IntegerType())
        )
        cleaned_df = cleaned_df.withColumn("slug", col("slug").cast(StringType()))
        cleaned_df = cleaned_df.withColumn("imdb_id", col("imdb_id").cast(StringType()))

        # Log final count
        after_count = cleaned_df.count()
        logger.info(
            f"Cleaned {before_count} records down to {after_count} valid Trakt movie details"
        )

        # Write to trusted zone
        target_manager.upsert_to_table(
            cleaned_df, "movie_ids_and_trailer", merge_key="tmdb_id"
        )

        logger.info("Successfully wrote Trakt movie details to trusted zone")

    except Exception as e:
        logger.error(f"Error in transform_trakt_movie_data: {e}")
        raise


@task(cache_policy=NO_CACHE)
def transform_omdb_ratings(
    source_manager: DeltaLakeManager,
    target_manager: DeltaLakeManager,
) -> None:
    """
    Clean OMDB ratings data and extract related tables.

    Args:
        source_manager: DeltaLakeManager for reading from landing zone
        target_manager: DeltaLakeManager for writing to trusted zone
    """
    try:
        logger.info("Cleaning OMDB ratings data")

        # Read from landing zone
        omdb_df = source_manager.read_table("omdb/movie_ratings")

        # Log initial count
        before_count = omdb_df.count()
        logger.info(f"Read {before_count} records from OMDB movie ratings")

        def _extract_imdb_data(omdb_df: DataFrame) -> DataFrame:
            """
            Extract and clean IMDb data from OMDB dataframe.

            Args:
                omdb_df: Raw OMDB dataframe from landing zone

            Returns:
                Cleaned IMDb dataframe
            """
            logger.info("Extracting IMDb data")

            # Select relevant columns and cast to appropriate types
            imdb_df = omdb_df.select(
                col("imdb_id").cast(StringType()).alias("imdb_id"),
                format_number(col("imdb_rating").cast(FloatType()), 2).alias(
                    "imdb_rating"
                ),
                col("imdb_votes").cast(IntegerType()).alias("imdb_votes"),
                col("metascore").cast(IntegerType()).alias("metascore"),
                col("awards").cast(StringType()).alias("awards"),
                col("poster").cast(StringType()).alias("poster"),
            ).filter(col("imdb_id").isNotNull())

            logger.info(f"Extracted {imdb_df.count()} IMDb data records")
            return imdb_df

        def _extract_external_ratings(omdb_df: DataFrame) -> DataFrame:
            """
            Extract and clean external ratings from OMDB dataframe.

            Args:
                omdb_df: Raw OMDB dataframe from landing zone

            Returns:
                Cleaned external ratings dataframe
            """
            logger.info("Extracting external ratings")

            # Define the schema for the ratings column
            ratings_schema = ArrayType(
                StructType(
                    [
                        StructField("Source", StringType(), True),
                        StructField("Value", StringType(), True),
                    ]
                )
            )

            # Parse the ratings column as JSON
            omdb_df = omdb_df.withColumn(
                "ratings", from_json(col("ratings"), ratings_schema)
            )

            # Explode the ratings array
            ratings_df = omdb_df.select(
                col("imdb_id").cast(StringType()).alias("imdb_id"),
                explode(col("ratings")).alias("rating"),
            ).filter(col("imdb_id").isNotNull())

            # Extract source and value from the rating struct
            ratings_df = ratings_df.select(
                col("imdb_id"),
                col("rating.Source").cast(StringType()).alias("source"),
                col("rating.Value").cast(StringType()).alias("value"),
            ).filter(
                col("source").isNotNull()
                & (col("source") != "")
                & col("value").isNotNull()
                & (col("value") != "")
            )

            # Create a composite key to ensure uniqueness
            ratings_df = ratings_df.withColumn(
                "rating_id", concat_ws("_", col("imdb_id"), col("source"))
            )

            logger.info(f"Extracted {ratings_df.count()} external rating records")
            return ratings_df

        # Extract IMDb data and external ratings
        imdb_df = _extract_imdb_data(omdb_df)
        ratings_df = _extract_external_ratings(omdb_df)

        # Write tables to trusted zone
        target_manager.upsert_to_table(imdb_df, "imdb_data", merge_key="imdb_id")
        target_manager.upsert_to_table(
            ratings_df, "external_ratings", merge_key="rating_id"
        )

        logger.info("Successfully wrote OMDB ratings tables to trusted zone")

    except Exception as e:
        logger.error(f"Error in transform_omdb_ratings: {e}")
        raise


@task(cache_policy=NO_CACHE)
def transform_youtube_stats(
    source_manager: DeltaLakeManager,
    target_manager: DeltaLakeManager,
) -> None:
    """
    Clean YouTube statistics and comments data and extract related tables.

    Args:
        source_manager: DeltaLakeManager for reading from landing zone
        target_manager: DeltaLakeManager for writing to trusted zone
    """
    try:
        logger.info("Cleaning YouTube statistics and comments data")

        # Read from landing zone
        youtube_df = source_manager.read_table("youtube/trailer_video_stats")

        # Log initial count
        before_count = youtube_df.count()
        logger.info(f"Read {before_count} records from YouTube movie statistics")

        def _extract_youtube_statistics(youtube_df: DataFrame) -> DataFrame:
            """
            Extract and clean YouTube statistics from the dataframe.

            Args:
                youtube_df: Raw YouTube dataframe from landing zone

            Returns:
                Cleaned YouTube statistics dataframe
            """
            logger.info("Extracting YouTube statistics")

            # Define the schema for the statistics column
            statistics_schema = StructType(
                [
                    StructField("viewCount", StringType(), True),
                    StructField("likeCount", StringType(), True),
                    StructField("commentCount", StringType(), True),
                ]
            )

            # Parse the statistics column as JSON
            youtube_df = youtube_df.withColumn(
                "statistics", from_json(col("statistics"), statistics_schema)
            )

            # Extract and cast the statistics fields
            stats_df = youtube_df.select(
                col("tmdb_id").cast(IntegerType()).alias("tmdb_id"),
                col("statistics.viewCount").cast(IntegerType()).alias("views"),
                col("statistics.likeCount").cast(IntegerType()).alias("likes"),
                col("statistics.commentCount").cast(IntegerType()).alias("comments"),
            ).filter(col("tmdb_id").isNotNull())
            stats_df = stats_df.dropDuplicates(["tmdb_id"])

            logger.info(f"Extracted {stats_df.count()} YouTube statistics records")
            return stats_df

        def _extract_youtube_comments(youtube_df: DataFrame) -> DataFrame:
            """
            Extract and clean YouTube comments from the dataframe.

            Args:
                youtube_df: Raw YouTube dataframe from landing zone

            Returns:
                Cleaned YouTube comments dataframe
            """
            logger.info("Extracting YouTube comments")

            # Define the schema for the comments column
            comments_schema = ArrayType(
                StructType(
                    [
                        StructField("author", StringType(), True),
                        StructField("text", StringType(), True),
                        StructField("likes", IntegerType(), True),
                        StructField("published_at", StringType(), True),
                    ]
                )
            )

            # Parse the comments column as JSON
            youtube_df = youtube_df.withColumn(
                "comments", from_json(col("comments"), comments_schema)
            )

            # Explode the comments array
            comments_df = youtube_df.select(
                col("tmdb_id").cast(IntegerType()).alias("tmdb_id"),
                explode(col("comments")).alias("comment"),
            ).filter(col("tmdb_id").isNotNull())

            # Extract comment details
            comments_df = comments_df.select(
                col("tmdb_id"),
                col("comment.author").cast(StringType()).alias("author"),
                col("comment.text").cast(StringType()).alias("text"),
                col("comment.likes").cast(IntegerType()).alias("likes"),
                to_timestamp(col("comment.published_at")).alias("published_at"),
            ).filter(
                col("author").isNotNull()
                & col("text").isNotNull()
                & col("published_at").isNotNull()
            )
            comments_df = comments_df.dropDuplicates(["tmdb_id"])

            logger.info(f"Extracted {comments_df.count()} YouTube comment records")
            return comments_df

        # Extract YouTube statistics and comments
        stats_df = _extract_youtube_statistics(youtube_df)
        comments_df = _extract_youtube_comments(youtube_df)

        # Write tables to trusted zone
        target_manager.upsert_to_table(
            stats_df, "youtube_statistics", merge_key="tmdb_id"
        )
        target_manager.upsert_to_table(
            comments_df, "youtube_comments", merge_key="tmdb_id"
        )

        logger.info(
            "Successfully wrote YouTube statistics and comments tables to trusted zone"
        )

    except Exception as e:
        logger.error(f"Error in transform_youtube_stats: {e}")
        raise


@flow(name="from-landing-to-trusted")
def transform_movie_data() -> None:
    """
    Main function to clean movie data from landing zone and store in trusted zone.
    """
    try:
        logger.info("Starting clean_movie_data flow")

        # Test environment first
        test_environment()

        # Initialize DeltaLakeManagers for source and target
        logger.info("Initializing DeltaLakeManagers")
        source_manager = DeltaLakeManager(
            s3_bucket_name="bdm-movies-db-landing",
        )

        target_manager = DeltaLakeManager(
            s3_bucket_name="bdm-movies-db-trusted",
        )

        # Clean each table
        transform_tmdb_movies_released(source_manager, target_manager)
        transform_tmdb_movies_providers(source_manager, target_manager)
        transform_trakt_movie_data(source_manager, target_manager)
        transform_omdb_ratings(source_manager, target_manager)
        transform_youtube_stats(source_manager, target_manager)

        logger.info("Movie data cleaning completed successfully")

    except Exception as e:
        logger.error(f"Error in transform_movie_data: {e}")
        raise


if __name__ == "__main__":
    from prefect.docker import DockerImage

    logger.info("Creating deployment")
    deployment = transform_movie_data.deploy(
        name="from-landing-to-trusted",
        work_pool_name="my-docker-pool",
        cron="0 1 * * *",
        image=DockerImage(
            name="arnausau11/orchestration",
            tag="latest",
            dockerfile="core/orchestration/Dockerfile",
        ),
        push=True,
    )
    logger.info("Deployment created")
