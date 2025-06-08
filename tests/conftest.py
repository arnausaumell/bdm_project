import pytest
import os
from unittest.mock import patch


@pytest.fixture
def mock_env_vars():
    """Fixture to provide mock environment variables for testing"""
    return {
        "OMDB_API_KEY": "test_omdb_key",
        "YOUTUBE_API_KEY": "test_youtube_key",
        "TMDB_API_KEY": "test_tmdb_key",
        "TRAKT_CLIENT_ID": "test_trakt_client_id",
        "TRAKT_CLIENT_SECRET": "test_trakt_client_secret",
    }


@pytest.fixture
def clean_env():
    """Fixture to provide a clean environment without API keys"""
    with patch.dict(os.environ, {}, clear=True):
        yield


@pytest.fixture
def sample_movie_data():
    """Fixture with common movie data used across tests"""
    return {
        "tmdb_id": 603,
        "imdb_id": "tt0133093",
        "trakt_id": 1,
        "title": "The Matrix",
        "year": 1999,
        "overview": "A computer hacker learns from mysterious rebels about the true nature of his reality and his role in the war against its controllers.",
        "poster_path": "/f89U3ADr1oiB1s9GkdPOEpXUk5H.jpg",
        "release_date": "1999-03-30",
        "runtime": 136,
        "budget": 63000000,
        "revenue": 463517383,
    }


@pytest.fixture
def sample_api_responses():
    """Fixture with common API response structures"""
    return {
        "success_response": {"status": "success", "data": {}},
        "error_response": {"status": "error", "message": "API Error"},
        "empty_response": {},
        "not_found_response": {"status": "error", "message": "Not Found"},
    }
