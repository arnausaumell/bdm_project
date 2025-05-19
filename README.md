## BDM Movies Database

### Landing Zone

Instructions to run the Landing zone ingestion:

```bash
run_orchestration.sh # Start the Prefect server, worker and set up deployments
docker-compose up -d # Start the streaming ingestion and the comments generator
```

File structure:

- core
  - data_ingestion
    - batch_ingestion: set of external connectors to ingest data from external sources
    - streaming_ingestion: 
        - comments_generator: synthetic service to generate comments for movies
        - consumer: Kafka consumer to ingest comments from the comments_generator
        - producer: Kafka producer to send comments to the comments_generator
  - landing_and_trusted_zone: S3 and DeltaLake managers
  - exploitation_zone: Supabase and Pinecone
  - orchestration: Prefect flows
    - from_external_to_landing
    - from_landing_to_trusted
    - from_trusted_to_exploitation

Data structure:

- landing_zone:
  - tmdb:
    - movies_released
      - tmdb_id
      - title
      - overview
      - original_title
      - poster_path
      - release_date
      - credits
        - tmdb_id
        - id
        - role
        - name
        - character
      - reviews
        - tmdb_id
        - author
        - content
        - created_at
      - genres
        - id
        - name
      - languages
        - iso_639_1
        - iso_3166_1
        - name
        - overview
        - tagline
      - keywords
        - id
        - name
      - budget
      - origin_country
      - runtime
      - revenue
      - tagline

    - movies_providers
      - link
      - buy
        - logo_path
        - provider_id
        - provider_name
        - display_priority
      - rent
        - logo_path
        - provider_id
        - provider_name
        - display_priority

  - trakt:
    - movie_ids_and_trailer
      - title
      - trailer
      - tmdb_id
      - trakt_id
      - slug
      - imdb_id

  - omdb:
    - movie_ratings
      - title
      - poster
      - awards
      - ratings
        - Source
        - Value
      - metascore
      - imdb_rating
      - imdb_votes
      - imdb_id

  - youtube:
    - trailer_video_stats
      - statistics
        - viewCount
        - likeCount
        - favoriteCount
        - commentCount
      - comments
        - author
        - text
        - likes
        - published_at
      