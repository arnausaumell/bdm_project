# bdm_project

## End goal

- Get trending movies right now in my country
- Search for movies by title, genre...
- Get movie details:
    - Genres
    - Providers (where to see it and how (rent, buy, flatrate))
    - Youtube reviews
    - Youtube comments
    - Twitter comments
    - Journalists reviews (from OMDB)
    - ...

## Frequency

- Batch:
    - Every day get ratings from OMDB
    - Every day get providers from TMDB
    - Every day get trending movies from Trakt
    - [For movies released during the last 30 days]
        - Get Youtube stats and comments
        - Get Twitter comments
        - Get Journalists reviews
- Hot path:
    - Every day get all movies released today from TMDB

## Data Model

- TMDB Movies - All released movies
  - tmdb_id: int
  - title: str
  - original_language: str
  - release_date: str
  - genres: list[str]
  - providers: json
- OMDB Movies - All movies
  - imdb_id: str
  - rating: json
  - imdb_rating: float
  - imdb_votes: int
  ...
- Trakt Movies - Trending Movies
  - trakt_id: int
  - ids: json
  - title: str
  - original_language: str
  - rating: float
  - votes: int
  - comment_count: int
  - genres: list[str]
  - trailer: str
  - homepage: str
  - status: str
  - certification: str
- Youtube Stats
  - video_id: str
  - view_count: int
  - like_count: int
  - comment_count: int
- Youtube Comments
  - video_id: str
  - author: str
  - text: str
  - published_at: str
  - likes: int
- Twitter Comments
  - ???
...
