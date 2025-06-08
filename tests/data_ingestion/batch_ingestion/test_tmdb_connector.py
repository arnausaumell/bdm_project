import pytest
import requests_mock
from unittest.mock import patch, mock_open
import json
import os

from core.data_ingestion.batch_ingestion.tmdb_connector import TMDbConnector


class TestTMDbConnector:
    @pytest.fixture
    def tmdb_connector(self):
        """Fixture to create TMDbConnector instance"""
        with patch.dict(os.environ, {"TMDB_API_KEY": "test_api_key"}):
            return TMDbConnector(region="ES")

    @pytest.fixture
    def sample_genres_response(self):
        """Fixture with sample genres response"""
        return {
            "genres": [
                {"id": 28, "name": "Action"},
                {"id": 12, "name": "Adventure"},
                {"id": 16, "name": "Animation"},
                {"id": 35, "name": "Comedy"},
                {"id": 80, "name": "Crime"},
            ]
        }

    @pytest.fixture
    def sample_movie_details(self):
        """Fixture with sample movie details response"""
        return {
            "id": 603,
            "title": "The Matrix",
            "overview": "A computer hacker learns...",
            "original_language": "en",
            "original_title": "The Matrix",
            "poster_path": "/f89U3ADr1oiB1s9GkdPOEpXUk5H.jpg",
            "release_date": "1999-03-30",
            "budget": 63000000,
            "revenue": 463517383,
            "runtime": 136,
            "tagline": "Welcome to the Real World.",
            "origin_country": ["US"],
        }

    @pytest.fixture
    def sample_credits_response(self):
        """Fixture with sample credits response"""
        return {
            "cast": [
                {"id": 6384, "name": "Keanu Reeves", "character": "Neo", "order": 0},
                {
                    "id": 2975,
                    "name": "Laurence Fishburne",
                    "character": "Morpheus",
                    "order": 1,
                },
            ],
            "crew": [
                {
                    "id": 905,
                    "name": "Lana Wachowski",
                    "job": "Director",
                    "department": "Directing",
                }
            ],
        }

    @pytest.fixture
    def sample_discover_response(self):
        """Fixture with sample discover movies response"""
        return {
            "page": 1,
            "total_pages": 1,
            "total_results": 1,
            "results": [
                {
                    "id": 603,
                    "title": "The Matrix",
                    "overview": "A computer hacker learns...",
                    "genre_ids": [28, 878],
                    "release_date": "1999-03-30",
                    "poster_path": "/f89U3ADr1oiB1s9GkdPOEpXUk5H.jpg",
                }
            ],
        }

    def test_init(self):
        """Test TMDbConnector basic initialization"""
        with patch.dict(os.environ, {"TMDB_API_KEY": "test_api_key"}):
            connector = TMDbConnector()
            assert connector._region == "ES"
            assert connector._base_url == "https://api.themoviedb.org/3"
            assert connector._api_key == "test_api_key"

    def test_init_default_region(self):
        """Test TMDbConnector initialization with default region"""
        with patch.dict(os.environ, {"TMDB_API_KEY": "test_api_key"}):
            connector = TMDbConnector()
            assert connector._region == "ES"
            assert connector._base_url == "https://api.themoviedb.org/3"
            assert connector._api_key == "test_api_key"

    def test_init_custom_region(self):
        """Test TMDbConnector initialization with custom region"""
        with patch.dict(os.environ, {"TMDB_API_KEY": "test_api_key"}):
            connector = TMDbConnector(region="US")
            assert connector._region == "US"

    def test_init_without_api_key(self):
        """Test TMDbConnector initialization without API key"""
        with patch.dict(os.environ, {}, clear=True):
            connector = TMDbConnector()
            assert connector._api_key is None

    def test_get_genre_list_success(
        self, requests_mock, tmdb_connector, sample_genres_response
    ):
        """Test successful genre list retrieval"""
        expected_url = "https://api.themoviedb.org/3/genre/movie/list"

        requests_mock.get(expected_url, json=sample_genres_response)

        result = tmdb_connector._get_genre_list()

        assert result == sample_genres_response["genres"]
        assert len(result) == 5
        assert result[0]["name"] == "Action"

        # Verify request parameters
        request = requests_mock.request_history[0]
        assert "api_key" in request.qs
        assert request.qs["api_key"] == ["test_api_key"]

    def test_get_genre_list_empty_response(self, requests_mock, tmdb_connector):
        """Test genre list retrieval with empty response"""
        expected_url = "https://api.themoviedb.org/3/genre/movie/list"

        requests_mock.get(expected_url, json={})

        result = tmdb_connector._get_genre_list()

        assert result == []

    def test_get_movie_details_success(
        self, requests_mock, tmdb_connector, sample_movie_details
    ):
        """Test successful movie details retrieval"""
        movie_id = 603
        expected_url = f"https://api.themoviedb.org/3/movie/{movie_id}"

        requests_mock.get(expected_url, json=sample_movie_details)

        result = tmdb_connector._get_movie_details(movie_id)

        assert result == sample_movie_details
        assert result["title"] == "The Matrix"
        assert result["id"] == 603

    def test_get_movie_credits_success(
        self, requests_mock, tmdb_connector, sample_credits_response
    ):
        """Test successful movie credits retrieval"""
        movie_id = 603
        expected_url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits"

        requests_mock.get(expected_url, json=sample_credits_response)

        result = tmdb_connector._get_movie_credits(movie_id)

        # Should return top 5 cast members + director
        assert len(result) == 3  # 2 cast + 1 director

        # Check cast members
        cast_members = [credit for credit in result if credit["role"] == "Acting"]
        assert len(cast_members) == 2
        assert cast_members[0]["name"] == "Keanu Reeves"
        assert cast_members[0]["character"] == "Neo"

        # Check director
        directors = [credit for credit in result if credit["role"] == "Director"]
        assert len(directors) == 1
        assert directors[0]["name"] == "Lana Wachowski"

    def test_get_movie_credits_no_director(self, requests_mock, tmdb_connector):
        """Test movie credits retrieval with no director"""
        movie_id = 603
        expected_url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits"

        # Response with no director
        credits_response = {
            "cast": [
                {"id": 6384, "name": "Keanu Reeves", "character": "Neo", "order": 0}
            ],
            "crew": [
                {
                    "id": 905,
                    "name": "Someone",
                    "job": "Producer",
                    "department": "Production",
                }
            ],
        }

        requests_mock.get(expected_url, json=credits_response)

        result = tmdb_connector._get_movie_credits(movie_id)

        # Should return cast + None for director
        assert len(result) == 2  # 1 cast + 1 None
        assert result[1] is None

    def test_get_movies_by_date_range_success(
        self,
        requests_mock,
        tmdb_connector,
        sample_genres_response,
        sample_discover_response,
    ):
        """Test successful movies retrieval by date range"""
        # Mock genre list
        genre_url = "https://api.themoviedb.org/3/genre/movie/list"
        requests_mock.get(genre_url, json=sample_genres_response)

        # Mock discover movies
        discover_url = "https://api.themoviedb.org/3/discover/movie"
        requests_mock.get(discover_url, json=sample_discover_response)

        # Mock individual movie details (will be called by _enrich_movie)
        movie_detail_url = "https://api.themoviedb.org/3/movie/603"
        requests_mock.get(movie_detail_url, json={"id": 603, "title": "The Matrix"})

        # Mock other endpoints that _enrich_movie calls
        credits_url = "https://api.themoviedb.org/3/movie/603/credits"
        requests_mock.get(credits_url, json={"cast": [], "crew": []})

        reviews_url = "https://api.themoviedb.org/3/movie/603/reviews"
        requests_mock.get(reviews_url, json={"results": []})

        translations_url = "https://api.themoviedb.org/3/movie/603/translations"
        requests_mock.get(translations_url, json={"translations": []})

        keywords_url = "https://api.themoviedb.org/3/movie/603/keywords"
        requests_mock.get(keywords_url, json={"keywords": []})

        with patch.object(
            tmdb_connector,
            "_enrich_movie",
            return_value={"tmdb_id": 603, "title": "The Matrix"},
        ):
            with patch.object(
                tmdb_connector,
                "_clean_movie_details",
                return_value={"tmdb_id": 603, "title": "The Matrix"},
            ):
                result = tmdb_connector.get_movies_by_date_range(
                    "2023-01-01", "2023-12-31"
                )

    def test_get_movie_reviews_success(self, requests_mock, tmdb_connector):
        """Test successful movie reviews retrieval"""
        movie_id = 603
        expected_url = f"https://api.themoviedb.org/3/movie/{movie_id}/reviews"

        reviews_response = {
            "results": [
                {
                    "author": "John Doe",
                    "content": "Great movie!",
                    "created_at": "2023-01-01T00:00:00Z",
                    "url": "https://example.com/review1",
                },
                {
                    "author": "Jane Smith",
                    "content": "Amazing film!",
                    "created_at": "2023-01-02T00:00:00Z",
                    "url": "https://example.com/review2",
                },
            ]
        }

        requests_mock.get(expected_url, json=reviews_response)

        result = tmdb_connector._get_movie_reviews(movie_id)

        assert len(result) == 2
        assert result[0]["author"] == "John Doe"
        assert result[1]["author"] == "Jane Smith"

    def test_get_movie_providers_success(self, requests_mock, tmdb_connector):
        """Test successful movie providers retrieval"""
        movie_id = 603
        expected_url = f"https://api.themoviedb.org/3/movie/{movie_id}/watch/providers"

        providers_response = {
            "results": {
                "ES": {
                    "link": "https://example.com",
                    "flatrate": [
                        {
                            "logo_path": "/logo.jpg",
                            "provider_id": 8,
                            "provider_name": "Netflix",
                            "display_priority": 1,
                        }
                    ],
                    "rent": [
                        {
                            "logo_path": "/logo2.jpg",
                            "provider_id": 2,
                            "provider_name": "Apple TV",
                            "display_priority": 2,
                        }
                    ],
                }
            }
        }

        requests_mock.get(expected_url, json=providers_response)

        result = tmdb_connector.get_movie_providers(movie_id)

        assert len(result) == 2
        assert result[0]["provider_name"] == "Netflix"
        assert result[0]["provider_type"] == "flatrate"
        assert result[1]["provider_name"] == "Apple TV"
        assert result[1]["provider_type"] == "rent"

    def test_clean_movie_details(self, tmdb_connector):
        """Test movie details cleaning functionality"""
        input_details = {
            "tmdb_id": 603,
            "title": "The Matrix",
            "overview": "A computer hacker learns...",
            "unnecessary_field": "This should be removed",
            "budget": 63000000,
            "revenue": 463517383,
            "genres": ["Action", "Sci-Fi"],
            "another_unnecessary_field": "Also removed",
        }

        result = tmdb_connector._clean_movie_details(input_details)

        # Should only contain selected fields
        assert "tmdb_id" in result
        assert "title" in result
        assert "overview" in result
        assert "budget" in result
        assert "revenue" in result
        assert "genres" in result
        assert "unnecessary_field" not in result
        assert "another_unnecessary_field" not in result

    def test_headers_format(self, tmdb_connector):
        """Test that headers are properly formatted"""
        assert isinstance(tmdb_connector._headers, dict)
        assert "Content-Type" in tmdb_connector._headers
        assert tmdb_connector._headers["Content-Type"] == "application/json"

    def test_url_construction(self, tmdb_connector):
        """Test URL construction for different endpoints"""
        base_url = tmdb_connector._base_url

        # Test movie details URL
        movie_id = 123
        details_url = f"{base_url}/movie/{movie_id}"
        assert details_url == "https://api.themoviedb.org/3/movie/123"

        # Test credits URL
        credits_url = f"{base_url}/movie/{movie_id}/credits"
        assert credits_url == "https://api.themoviedb.org/3/movie/123/credits"

        # Test discover URL
        discover_url = f"{base_url}/discover/movie"
        assert discover_url == "https://api.themoviedb.org/3/discover/movie"
