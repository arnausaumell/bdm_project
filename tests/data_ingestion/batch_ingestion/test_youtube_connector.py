import pytest
from unittest.mock import patch, MagicMock, mock_open
import json
import os
from googleapiclient.errors import HttpError

from core.data_ingestion.batch_ingestion.youtube_connector import YouTubeConnector


class TestYouTubeConnector:
    @pytest.fixture
    def youtube_connector(self):
        """Fixture to create YouTubeConnector instance"""
        with patch.dict(os.environ, {"YOUTUBE_API_KEY": "test_api_key"}):
            with patch(
                "core.data_ingestion.batch_ingestion.youtube_connector.build"
            ) as mock_build:
                mock_youtube = MagicMock()
                mock_build.return_value = mock_youtube
                connector = YouTubeConnector()
                connector._youtube = mock_youtube
                return connector

    @pytest.fixture
    def sample_search_response(self):
        """Fixture with sample YouTube search response"""
        return {"items": [{"id": {"videoId": "dQw4w9WgXcQ"}}]}

    @pytest.fixture
    def sample_video_stats(self):
        """Fixture with sample video statistics"""
        return {
            "items": [
                {
                    "statistics": {
                        "viewCount": "1000000",
                        "likeCount": "50000",
                        "commentCount": "1000",
                    }
                }
            ]
        }

    @pytest.fixture
    def sample_comments_response(self):
        """Fixture with sample comments response"""
        return {
            "items": [
                {
                    "snippet": {
                        "topLevelComment": {
                            "snippet": {
                                "authorDisplayName": "John Doe",
                                "textDisplay": "Great movie!",
                                "likeCount": 10,
                                "publishedAt": "2023-01-01T00:00:00Z",
                            }
                        }
                    }
                },
                {
                    "snippet": {
                        "topLevelComment": {
                            "snippet": {
                                "authorDisplayName": "Jane Smith",
                                "textDisplay": "Amazing trailer!",
                                "likeCount": 5,
                                "publishedAt": "2023-01-02T00:00:00Z",
                            }
                        }
                    }
                },
            ]
        }

    def test_init(self, youtube_connector):
        """Test YouTubeConnector initialization"""
        assert youtube_connector._api_key == "test_api_key"
        assert youtube_connector._youtube is not None

    def test_init_without_api_key(self):
        """Test YouTubeConnector initialization without API key"""
        with patch.dict(os.environ, {}, clear=True):
            with patch(
                "core.data_ingestion.batch_ingestion.youtube_connector.build"
            ) as mock_build:
                connector = YouTubeConnector()
                assert connector._api_key is None
                mock_build.assert_called_once_with("youtube", "v3", developerKey=None)

    def test_search_movie_trailer_success(
        self, youtube_connector, sample_search_response
    ):
        """Test successful movie trailer search"""
        movie_title = "The Matrix"

        # Mock the search method chain
        mock_search = MagicMock()
        mock_list = MagicMock()
        mock_execute = MagicMock()

        youtube_connector._youtube.search.return_value = mock_search
        mock_search.list.return_value = mock_list
        mock_list.execute.return_value = sample_search_response

        result = youtube_connector.search_movie_trailer(movie_title)

        assert result == "dQw4w9WgXcQ"
        mock_search.list.assert_called_once_with(
            q=f"{movie_title} official trailer",
            part="id",
            maxResults=1,
            type="video",
            videoDefinition="high",
        )

    def test_search_movie_trailer_no_results(self, youtube_connector):
        """Test movie trailer search with no results"""
        movie_title = "NonExistentMovie"

        # Mock empty response
        mock_search = MagicMock()
        mock_list = MagicMock()
        mock_execute = MagicMock()

        youtube_connector._youtube.search.return_value = mock_search
        mock_search.list.return_value = mock_list
        mock_list.execute.return_value = {"items": []}

        result = youtube_connector.search_movie_trailer(movie_title)

        assert result is None

    def test_search_movie_trailer_http_error(self, youtube_connector):
        """Test movie trailer search with HTTP error"""
        movie_title = "The Matrix"

        # Mock HTTP error
        mock_search = MagicMock()
        mock_list = MagicMock()

        youtube_connector._youtube.search.return_value = mock_search
        mock_search.list.return_value = mock_list
        mock_list.execute.side_effect = HttpError(
            resp=MagicMock(status=403), content=b"Forbidden"
        )

        result = youtube_connector.search_movie_trailer(movie_title)

        assert result is None

    def test_get_video_info(
        self, youtube_connector, sample_video_stats, sample_comments_response
    ):
        """Test getting video information"""
        video_id = "dQw4w9WgXcQ"

        # Mock statistics
        mock_videos = MagicMock()
        mock_videos_list = MagicMock()
        mock_videos_execute = MagicMock()

        youtube_connector._youtube.videos.return_value = mock_videos
        mock_videos.list.return_value = mock_videos_list
        mock_videos_list.execute.return_value = sample_video_stats

        # Mock comments
        mock_comment_threads = MagicMock()
        mock_comment_list = MagicMock()
        mock_comment_execute = MagicMock()
        mock_list_next = MagicMock()

        youtube_connector._youtube.commentThreads.return_value = mock_comment_threads
        mock_comment_threads.list.return_value = mock_comment_list
        mock_comment_list.execute.return_value = sample_comments_response
        youtube_connector._youtube.commentThreads.list_next.return_value = None

        result = youtube_connector.get_video_info(video_id)

        expected_result = {
            "statistics": {
                "viewCount": "1000000",
                "likeCount": "50000",
                "commentCount": "1000",
            },
            "comments": [
                {
                    "author": "John Doe",
                    "text": "Great movie!",
                    "likes": 10,
                    "published_at": "2023-01-01T00:00:00Z",
                },
                {
                    "author": "Jane Smith",
                    "text": "Amazing trailer!",
                    "likes": 5,
                    "published_at": "2023-01-02T00:00:00Z",
                },
            ],
        }

        assert result == expected_result

    def test_get_video_statistics_success(self, youtube_connector, sample_video_stats):
        """Test successful video statistics retrieval"""
        video_id = "dQw4w9WgXcQ"

        mock_videos = MagicMock()
        mock_list = MagicMock()
        mock_execute = MagicMock()

        youtube_connector._youtube.videos.return_value = mock_videos
        mock_videos.list.return_value = mock_list
        mock_list.execute.return_value = sample_video_stats

        result = youtube_connector._get_video_statistics(video_id)

        expected_result = {
            "viewCount": "1000000",
            "likeCount": "50000",
            "commentCount": "1000",
        }

        assert result == expected_result

    def test_get_video_statistics_no_items(self, youtube_connector):
        """Test video statistics retrieval with no items"""
        video_id = "dQw4w9WgXcQ"

        mock_videos = MagicMock()
        mock_list = MagicMock()
        mock_execute = MagicMock()

        youtube_connector._youtube.videos.return_value = mock_videos
        mock_videos.list.return_value = mock_list
        mock_list.execute.return_value = {"items": []}

        result = youtube_connector._get_video_statistics(video_id)

        expected_result = {"viewCount": 0, "likeCount": 0, "commentCount": 0}

        assert result == expected_result

    def test_get_video_statistics_http_error(self, youtube_connector):
        """Test video statistics retrieval with HTTP error"""
        video_id = "dQw4w9WgXcQ"

        mock_videos = MagicMock()
        mock_list = MagicMock()

        youtube_connector._youtube.videos.return_value = mock_videos
        mock_videos.list.return_value = mock_list
        mock_list.execute.side_effect = HttpError(
            resp=MagicMock(status=403), content=b"Forbidden"
        )

        result = youtube_connector._get_video_statistics(video_id)

        expected_result = {"viewCount": 0, "likeCount": 0, "commentCount": 0}

        assert result == expected_result

    def test_get_video_comments_success(
        self, youtube_connector, sample_comments_response
    ):
        """Test successful video comments retrieval"""
        video_id = "dQw4w9WgXcQ"
        max_results = 5

        mock_comment_threads = MagicMock()
        mock_list = MagicMock()
        mock_execute = MagicMock()

        youtube_connector._youtube.commentThreads.return_value = mock_comment_threads
        mock_comment_threads.list.return_value = mock_list
        mock_list.execute.return_value = sample_comments_response
        youtube_connector._youtube.commentThreads.list_next.return_value = None

        result = youtube_connector._get_video_comments(video_id, max_results)

        expected_result = [
            {
                "author": "John Doe",
                "text": "Great movie!",
                "likes": 10,
                "published_at": "2023-01-01T00:00:00Z",
            },
            {
                "author": "Jane Smith",
                "text": "Amazing trailer!",
                "likes": 5,
                "published_at": "2023-01-02T00:00:00Z",
            },
        ]

        assert result == expected_result

    def test_get_video_comments_http_error(self, youtube_connector):
        """Test video comments retrieval with HTTP error"""
        video_id = "dQw4w9WgXcQ"

        mock_comment_threads = MagicMock()
        mock_list = MagicMock()

        youtube_connector._youtube.commentThreads.return_value = mock_comment_threads
        mock_comment_threads.list.return_value = mock_list
        mock_list.execute.side_effect = HttpError(
            resp=MagicMock(status=403), content=b"Forbidden"
        )

        result = youtube_connector._get_video_comments(video_id, 5)

        assert result == []

    def test_get_video_comments_pagination(self, youtube_connector):
        """Test video comments retrieval with pagination"""
        video_id = "dQw4w9WgXcQ"

        # First page response
        first_page = {
            "items": [
                {
                    "snippet": {
                        "topLevelComment": {
                            "snippet": {
                                "authorDisplayName": "User1",
                                "textDisplay": "Comment1",
                                "likeCount": 1,
                                "publishedAt": "2023-01-01T00:00:00Z",
                            }
                        }
                    }
                }
            ]
        }

        # Second page response
        second_page = {
            "items": [
                {
                    "snippet": {
                        "topLevelComment": {
                            "snippet": {
                                "authorDisplayName": "User2",
                                "textDisplay": "Comment2",
                                "likeCount": 2,
                                "publishedAt": "2023-01-02T00:00:00Z",
                            }
                        }
                    }
                }
            ]
        }

        mock_comment_threads = MagicMock()
        mock_first_list = MagicMock()
        mock_second_list = MagicMock()

        youtube_connector._youtube.commentThreads.return_value = mock_comment_threads
        mock_comment_threads.list.return_value = mock_first_list
        mock_first_list.execute.return_value = first_page

        # Mock list_next to return second request, then None
        youtube_connector._youtube.commentThreads.list_next.side_effect = [
            mock_second_list,
            None,
        ]
        mock_second_list.execute.return_value = second_page

        result = youtube_connector._get_video_comments(video_id, 10)

        assert len(result) == 2
        assert result[0]["author"] == "User1"
        assert result[1]["author"] == "User2"

    @patch("builtins.open", new_callable=mock_open)
    @patch("json.dump")
    def test_main_execution(self, mock_json_dump, mock_file):
        """Test the main execution block"""
        with patch.dict(os.environ, {"YOUTUBE_API_KEY": "test_api_key"}):
            with patch(
                "core.data_ingestion.batch_ingestion.youtube_connector.build"
            ) as mock_build:
                mock_youtube = MagicMock()
                mock_build.return_value = mock_youtube

                # Mock the get_video_info method
                mock_stats = {"viewCount": "1000", "likeCount": "100"}
                mock_comments = [{"author": "Test", "text": "Test comment"}]

                # Import and run the main block
                import importlib
                import core.data_ingestion.batch_ingestion.youtube_connector as yt_module

                importlib.reload(yt_module)

        # Note: This test would need to be more sophisticated to properly test the main block
        # since it creates its own instance of YouTubeConnector
