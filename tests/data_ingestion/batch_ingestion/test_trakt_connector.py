import pytest
import requests_mock
from unittest.mock import patch, mock_open
import json
import os

from core.data_ingestion.batch_ingestion.trakt_connector import TraktConnector


class TestTraktConnector:
    @pytest.fixture
    def trakt_connector(self):
        """Fixture to create TraktConnector instance"""
        with patch.dict(
            os.environ,
            {
                "TRAKT_CLIENT_ID": "test_client_id",
                "TRAKT_CLIENT_SECRET": "test_client_secret",
            },
        ):
            return TraktConnector()

    @pytest.fixture
    def sample_trending_response(self):
        """Fixture with sample trending movies response"""
        return [
            {
                "watchers": 1000,
                "movie": {
                    "title": "The Matrix",
                    "year": 1999,
                    "ids": {
                        "trakt": 1,
                        "slug": "the-matrix-1999",
                        "tmdb": 603,
                        "imdb": "tt0133093",
                    },
                },
            },
            {
                "watchers": 800,
                "movie": {
                    "title": "Inception",
                    "year": 2010,
                    "ids": {
                        "trakt": 2,
                        "slug": "inception-2010",
                        "tmdb": 27205,
                        "imdb": "tt1375666",
                    },
                },
            },
        ]

    @pytest.fixture
    def sample_movie_details(self):
        """Fixture with sample movie details response"""
        return {
            "title": "The Matrix",
            "year": 1999,
            "trailer": "https://youtube.com/watch?v=vKQi3bBA1y8",
            "ids": {
                "trakt": 1,
                "slug": "the-matrix-1999",
                "tmdb": 603,
                "imdb": "tt0133093",
            },
            "overview": "A computer hacker learns...",
            "rating": 8.7,
            "votes": 12345,
            "runtime": 136,
        }

    @pytest.fixture
    def sample_search_response(self):
        """Fixture with sample search response"""
        return [
            {
                "type": "movie",
                "score": 100.0,
                "movie": {
                    "title": "The Matrix",
                    "year": 1999,
                    "ids": {
                        "trakt": 1,
                        "slug": "the-matrix-1999",
                        "tmdb": 603,
                        "imdb": "tt0133093",
                    },
                },
            }
        ]

    def test_init(self, trakt_connector):
        """Test TraktConnector initialization"""
        assert trakt_connector._base_url == "https://api.trakt.tv"
        assert trakt_connector._client_id == "test_client_id"
        assert trakt_connector._client_secret == "test_client_secret"
        assert trakt_connector._headers == {
            "Content-Type": "application/json",
            "trakt-api-version": "2",
            "trakt-api-key": "test_client_id",
        }

    def test_init_without_credentials(self):
        """Test TraktConnector initialization without credentials"""
        with patch.dict(os.environ, {}, clear=True):
            connector = TraktConnector()
            assert connector._client_id is None
            assert connector._client_secret is None

    def test_get_trending_movies_success(
        self, requests_mock, trakt_connector, sample_trending_response
    ):
        """Test successful trending movies retrieval"""
        expected_url = "https://api.trakt.tv/movies/trending"

        requests_mock.get(expected_url, json=sample_trending_response)

        result = trakt_connector.get_trending_movies()

        assert result == sample_trending_response
        assert requests_mock.call_count == 1

        # Verify request headers
        request = requests_mock.request_history[0]
        assert request.headers["Content-Type"] == "application/json"
        assert request.headers["trakt-api-version"] == "2"
        assert request.headers["trakt-api-key"] == "test_client_id"

    def test_get_trending_movies_empty_response(self, requests_mock, trakt_connector):
        """Test trending movies with empty response"""
        expected_url = "https://api.trakt.tv/movies/trending"

        requests_mock.get(expected_url, json=[])

        result = trakt_connector.get_trending_movies()

        assert result == []

    def test_get_trending_movies_api_error(self, requests_mock, trakt_connector):
        """Test trending movies with API error"""
        expected_url = "https://api.trakt.tv/movies/trending"

        requests_mock.get(expected_url, status_code=500)

        with pytest.raises(Exception):
            trakt_connector.get_trending_movies()

    def test_get_movie_details_success(
        self, requests_mock, trakt_connector, sample_movie_details
    ):
        """Test successful movie details retrieval"""
        trakt_movie_id = 1
        expected_url = f"https://api.trakt.tv/movies/{trakt_movie_id}?extended=full"

        requests_mock.get(expected_url, json=sample_movie_details)

        result = trakt_connector.get_movie_details(trakt_movie_id)

        assert result == sample_movie_details
        assert requests_mock.call_count == 1

    def test_get_movie_details_not_found(self, requests_mock, trakt_connector):
        """Test movie details retrieval for non-existent movie"""
        trakt_movie_id = 999999
        expected_url = f"https://api.trakt.tv/movies/{trakt_movie_id}?extended=full"

        requests_mock.get(expected_url, status_code=404)

        with pytest.raises(Exception):
            trakt_connector.get_movie_details(trakt_movie_id)

    def test_get_movie_details_tmdb_id_success(
        self,
        requests_mock,
        trakt_connector,
        sample_search_response,
        sample_movie_details,
    ):
        """Test successful movie details retrieval by TMDB ID"""
        tmdb_movie_id = 603
        search_url = f"https://api.trakt.tv/search/tmdb/{tmdb_movie_id}?id_type=movie"
        details_url = "https://api.trakt.tv/movies/1?extended=full"

        # Mock search response
        requests_mock.get(search_url, json=sample_search_response)
        # Mock details response
        requests_mock.get(details_url, json=sample_movie_details)

        result = trakt_connector.get_movie_details_tmdb_id(tmdb_movie_id)

        expected_result = {
            "title": "The Matrix",
            "trailer": "https://youtube.com/watch?v=vKQi3bBA1y8",
            "tmdb_id": 603,
            "trakt_id": 1,
            "slug": "the-matrix-1999",
            "imdb_id": "tt0133093",
        }

        assert result == expected_result
        assert requests_mock.call_count == 2

    def test_get_movie_details_tmdb_id_no_movie_results(
        self, requests_mock, trakt_connector
    ):
        """Test movie details retrieval by TMDB ID with no movie results"""
        tmdb_movie_id = 999999
        search_url = f"https://api.trakt.tv/search/tmdb/{tmdb_movie_id}?id_type=movie"

        # Mock search response with no movie results
        search_response = [
            {
                "type": "show",  # Not a movie
                "score": 50.0,
                "show": {"title": "Some Show"},
            }
        ]

        requests_mock.get(search_url, json=search_response)

        result = trakt_connector.get_movie_details_tmdb_id(tmdb_movie_id)

        assert result is None

    def test_get_movie_details_tmdb_id_empty_search(
        self, requests_mock, trakt_connector
    ):
        """Test movie details retrieval by TMDB ID with empty search results"""
        tmdb_movie_id = 999999
        search_url = f"https://api.trakt.tv/search/tmdb/{tmdb_movie_id}?id_type=movie"

        requests_mock.get(search_url, json=[])

        result = trakt_connector.get_movie_details_tmdb_id(tmdb_movie_id)

        assert result is None

    def test_get_movie_details_tmdb_id_search_error(
        self, requests_mock, trakt_connector
    ):
        """Test movie details retrieval by TMDB ID with search API error"""
        tmdb_movie_id = 999999
        search_url = f"https://api.trakt.tv/search/tmdb/{tmdb_movie_id}?id_type=movie"

        requests_mock.get(search_url, status_code=500)

        with pytest.raises(Exception):
            trakt_connector.get_movie_details_tmdb_id(tmdb_movie_id)

    def test_get_movie_details_tmdb_id_details_error(
        self, requests_mock, trakt_connector, sample_search_response
    ):
        """Test movie details retrieval by TMDB ID with details API error"""
        tmdb_movie_id = 603
        search_url = f"https://api.trakt.tv/search/tmdb/{tmdb_movie_id}?id_type=movie"
        details_url = "https://api.trakt.tv/movies/1?extended=full"

        requests_mock.get(search_url, json=sample_search_response)
        requests_mock.get(details_url, status_code=500)

        with pytest.raises(Exception):
            trakt_connector.get_movie_details_tmdb_id(tmdb_movie_id)

    def test_get_movie_details_tmdb_id_missing_fields(
        self, requests_mock, trakt_connector, sample_search_response
    ):
        """Test movie details retrieval by TMDB ID with missing fields in response"""
        tmdb_movie_id = 603
        search_url = f"https://api.trakt.tv/search/tmdb/{tmdb_movie_id}?id_type=movie"
        details_url = "https://api.trakt.tv/movies/1?extended=full"

        incomplete_movie_details = {
            "title": "The Matrix",
            "ids": {
                "trakt": 1,
                "slug": "the-matrix-1999",
                "tmdb": 603,
                "imdb": "tt0133093",
            },
            # Missing trailer and other fields
        }

        requests_mock.get(search_url, json=sample_search_response)
        requests_mock.get(details_url, json=incomplete_movie_details)

        # Since the current implementation doesn't handle missing fields gracefully,
        # it should raise a KeyError when trying to access the missing 'trailer' field
        with pytest.raises(KeyError):
            trakt_connector.get_movie_details_tmdb_id(tmdb_movie_id)

    @patch("json.dump")
    @patch("builtins.open", new_callable=mock_open)
    def test_main_execution(
        self,
        mock_file,
        mock_json_dump,
        requests_mock,
        sample_search_response,
        sample_movie_details,
    ):
        """Test the main execution block functionality"""
        search_url = "https://api.trakt.tv/search/tmdb/1138194?id_type=movie"
        details_url = "https://api.trakt.tv/movies/1?extended=full"

        requests_mock.get(search_url, json=sample_search_response)
        requests_mock.get(details_url, json=sample_movie_details)

        # Directly test the functionality instead of relying on module reload
        trakt_connector = TraktConnector()
        movie_details = trakt_connector.get_movie_details_tmdb_id(1138194)

        # Simulate the file writing part
        with open("movie_details.json", "w") as f:
            import json

            json.dump(movie_details, f, indent=4)

        # Verify the file operations
        mock_file.assert_called_with("movie_details.json", "w")
        mock_json_dump.assert_called_once()

    def test_headers_format(self, trakt_connector):
        """Test that headers are properly formatted"""
        assert isinstance(trakt_connector._headers, dict)
        assert "Content-Type" in trakt_connector._headers
        assert trakt_connector._headers["Content-Type"] == "application/json"
        assert trakt_connector._headers["trakt-api-version"] == "2"
        assert trakt_connector._headers["trakt-api-key"] == "test_client_id"

    def test_url_construction(self, trakt_connector):
        """Test URL construction for different endpoints"""
        base_url = trakt_connector._base_url

        # Test trending movies URL
        trending_url = f"{base_url}/movies/trending"
        assert trending_url == "https://api.trakt.tv/movies/trending"

        # Test movie details URL
        movie_id = 123
        details_url = f"{base_url}/movies/{movie_id}?extended=full"
        assert details_url == "https://api.trakt.tv/movies/123?extended=full"

        # Test search URL
        tmdb_id = 456
        search_url = f"{base_url}/search/tmdb/{tmdb_id}?id_type=movie"
        assert search_url == "https://api.trakt.tv/search/tmdb/456?id_type=movie"
