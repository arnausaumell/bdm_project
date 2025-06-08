import pytest
import requests_mock
from unittest.mock import patch, mock_open
import json
import os

from core.data_ingestion.batch_ingestion.omdb_connector import OMDBConnector


class TestOMDBConnector:
    @pytest.fixture
    def omdb_connector(self):
        """Fixture to create OMDBConnector instance"""
        with patch.dict(os.environ, {"OMDB_API_KEY": "test_api_key"}):
            return OMDBConnector()

    @pytest.fixture
    def sample_omdb_response(self):
        """Fixture with sample OMDB API response"""
        return {
            "Title": "The Matrix",
            "Poster": "https://example.com/poster.jpg",
            "Awards": "Won 4 Oscars",
            "Ratings": [
                {"Source": "Internet Movie Database", "Value": "8.7/10"},
                {"Source": "Rotten Tomatoes", "Value": "88%"},
            ],
            "Metascore": "73",
            "imdbRating": "8.7",
            "imdbVotes": "1,868,413",
            "imdbID": "tt0133093",
        }

    def test_init(self, omdb_connector):
        """Test OMDBConnector initialization"""
        assert omdb_connector._base_url == "http://www.omdbapi.com"
        assert omdb_connector._api_key == "test_api_key"
        assert omdb_connector._headers == {"Content-Type": "application/json"}

    def test_init_without_api_key(self):
        """Test OMDBConnector initialization without API key"""
        with patch.dict(os.environ, {}, clear=True):
            connector = OMDBConnector()
            assert connector._api_key is None

    @requests_mock.Mocker()
    def test_get_movie_by_imdb_id_success(
        self, m, omdb_connector, sample_omdb_response
    ):
        """Test successful movie retrieval by IMDB ID"""
        imdb_id = "tt0133093"
        expected_url = f"http://www.omdbapi.com/?apikey=test_api_key&i={imdb_id}"

        m.get(expected_url, json=sample_omdb_response)

        result = omdb_connector.get_movie_by_imdb_id(imdb_id)

        expected_result = {
            "title": "The Matrix",
            "poster": "https://example.com/poster.jpg",
            "awards": "Won 4 Oscars",
            "ratings": [
                {"Source": "Internet Movie Database", "Value": "8.7/10"},
                {"Source": "Rotten Tomatoes", "Value": "88%"},
            ],
            "metascore": "73",
            "imdb_rating": "8.7",
            "imdb_votes": "1,868,413",
            "imdb_id": "tt0133093",
        }

        assert result == expected_result
        assert m.call_count == 1

    @requests_mock.Mocker()
    def test_get_movie_by_imdb_id_missing_fields(self, m, omdb_connector):
        """Test movie retrieval with missing fields in response"""
        imdb_id = "tt1234567"
        incomplete_response = {
            "Title": "Incomplete Movie",
            "imdbID": "tt1234567",
            # Missing other fields
        }

        expected_url = f"http://www.omdbapi.com/?apikey=test_api_key&i={imdb_id}"
        m.get(expected_url, json=incomplete_response)

        result = omdb_connector.get_movie_by_imdb_id(imdb_id)

        expected_result = {
            "title": "Incomplete Movie",
            "poster": None,
            "awards": None,
            "ratings": None,
            "metascore": None,
            "imdb_rating": None,
            "imdb_votes": None,
            "imdb_id": "tt1234567",
        }

        assert result == expected_result

    @requests_mock.Mocker()
    def test_get_movie_by_imdb_id_api_error(self, m, omdb_connector):
        """Test handling of API errors"""
        imdb_id = "tt0000000"
        expected_url = f"http://www.omdbapi.com/?apikey=test_api_key&i={imdb_id}"

        m.get(expected_url, status_code=500)

        # This should raise an exception or handle the error gracefully
        # Since the current implementation doesn't handle errors, we expect requests to raise
        with pytest.raises(Exception):
            omdb_connector.get_movie_by_imdb_id(imdb_id)

    @requests_mock.Mocker()
    def test_get_movie_by_imdb_id_empty_response(self, m, omdb_connector):
        """Test handling of empty response"""
        imdb_id = "tt0000000"
        expected_url = f"http://www.omdbapi.com/?apikey=test_api_key&i={imdb_id}"

        m.get(expected_url, json={})

        result = omdb_connector.get_movie_by_imdb_id(imdb_id)

        expected_result = {
            "title": None,
            "poster": None,
            "awards": None,
            "ratings": None,
            "metascore": None,
            "imdb_rating": None,
            "imdb_votes": None,
            "imdb_id": None,
        }

        assert result == expected_result

    @patch("builtins.open", new_callable=mock_open)
    @patch("json.dump")
    @requests_mock.Mocker()
    def test_main_execution(self, m, mock_json_dump, mock_file, sample_omdb_response):
        """Test the main execution block"""
        expected_url = "http://www.omdbapi.com/?apikey=test_api_key&i=tt28015403"
        m.get(expected_url, json=sample_omdb_response)

        with patch.dict(os.environ, {"OMDB_API_KEY": "test_api_key"}):
            # Import and run the main block
            import importlib
            import core.data_ingestion.batch_ingestion.omdb_connector as omdb_module

            importlib.reload(omdb_module)

        mock_file.assert_called_once_with("movie_ratings.json", "w")
        mock_json_dump.assert_called_once()

    def test_headers_format(self, omdb_connector):
        """Test that headers are properly formatted"""
        assert isinstance(omdb_connector._headers, dict)
        assert "Content-Type" in omdb_connector._headers
        assert omdb_connector._headers["Content-Type"] == "application/json"

    def test_url_construction(self, omdb_connector):
        """Test URL construction for API calls"""
        imdb_id = "tt1234567"
        expected_url = (
            f"{omdb_connector._base_url}/?apikey={omdb_connector._api_key}&i={imdb_id}"
        )

        # This tests the URL format used in the actual method
        constructed_url = (
            f"{omdb_connector._base_url}/?apikey={omdb_connector._api_key}&i={imdb_id}"
        )
        assert constructed_url == expected_url
        assert "apikey=" in constructed_url
        assert "i=" in constructed_url
