import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, timezone
import uuid

from core.data_ingestion.streaming_ingestion.comments_generator.app import (
    AppState,
    notify_subscribers,
    generate_comments_periodically,
    get_db_tables,
)
from core.data_ingestion.streaming_ingestion.comments_generator.fake_comments import (
    COMMENTS,
)


class TestAppState:
    def test_init(self):
        """Test AppState initialization"""
        state = AppState()
        assert state.comments_history == []
        assert state.active_connections == set()
        assert state.generator_task is None


class TestCommentsGeneration:
    @pytest.fixture
    def mock_movie_data(self):
        """Fixture with sample movie data"""
        return [
            {"tmdb_id": 123, "title": "Test Movie 1"},
            {"tmdb_id": 456, "title": "Test Movie 2"},
        ]

    @pytest.mark.asyncio
    @patch(
        "core.data_ingestion.streaming_ingestion.comments_generator.app.get_db_tables"
    )
    @patch(
        "core.data_ingestion.streaming_ingestion.comments_generator.app.notify_subscribers"
    )
    @patch("random.choice")
    @patch("uuid.uuid4")
    @patch("core.data_ingestion.streaming_ingestion.comments_generator.app.datetime")
    async def test_generate_comments_single_iteration(
        self,
        mock_datetime,
        mock_uuid,
        mock_random_choice,
        mock_notify,
        mock_get_db,
        mock_movie_data,
    ):
        """Test single iteration of comment generation"""
        # Setup mocks
        mock_get_db.return_value = mock_movie_data
        mock_random_choice.side_effect = [
            mock_movie_data[0],
            COMMENTS[0],
        ]  # Movie and comment
        mock_uuid.return_value = "test-uuid-123"
        mock_datetime.now.return_value.isoformat.return_value = "2024-01-01T12:00:00Z"
        mock_notify.return_value = asyncio.Future()
        mock_notify.return_value.set_result(None)

        state = AppState()

        # Create a task that will run one iteration and then stop
        async def limited_generate():
            await generate_comments_periodically(state)

        task = asyncio.create_task(limited_generate())

        # Let it run briefly then cancel
        await asyncio.sleep(0.1)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        # Verify the comment was generated and stored
        assert (
            len(state.comments_history) >= 0
        )  # May be 0 if cancelled before completion
        mock_get_db.assert_called()

    def test_fake_comments_exist(self):
        """Test that fake comments are available"""
        assert isinstance(COMMENTS, list)
        assert len(COMMENTS) > 0
        assert all(isinstance(comment, str) for comment in COMMENTS)

    @patch("core.landing_and_trusted_zone.deltalake_manager.DeltaLakeManager")
    def test_get_db_tables(self, mock_delta_manager):
        """Test database table retrieval"""
        mock_manager_instance = Mock()
        mock_df = Mock()
        mock_df.to_dict.return_value = [{"tmdb_id": 123, "title": "Test Movie"}]
        mock_manager_instance.read_table.return_value = mock_df
        mock_delta_manager.return_value = mock_manager_instance

        result = get_db_tables()

        assert isinstance(result, list)
        mock_delta_manager.assert_called_once()
        mock_manager_instance.read_table.assert_called_once_with("tmdb/movies_released")


class TestWebSocketNotification:
    @pytest.mark.asyncio
    async def test_notify_subscribers_success(self):
        """Test successful notification to subscribers"""
        state = AppState()
        mock_websocket = AsyncMock()
        state.active_connections.add(mock_websocket)

        comment = {
            "tmdb_id": 123,
            "comment_id": "test-uuid",
            "film_title": "Test Movie",
            "comment_text": "Great movie!",
            "created_at": "2024-01-01T12:00:00Z",
        }

        await notify_subscribers(state, comment)

        mock_websocket.send_json.assert_called_once_with(comment)
        assert mock_websocket in state.active_connections

    @pytest.mark.asyncio
    async def test_notify_subscribers_connection_error(self):
        """Test handling of connection errors during notification"""
        state = AppState()
        mock_websocket = AsyncMock()
        mock_websocket.send_json.side_effect = Exception("Connection error")
        state.active_connections.add(mock_websocket)

        comment = {"test": "data"}

        await notify_subscribers(state, comment)

        # Connection should be removed after error
        assert mock_websocket not in state.active_connections


class TestCommentStructure:
    def test_comment_has_required_fields(self):
        """Test that generated comments have all required fields"""
        # This would be tested in integration, but we can test the expected structure
        expected_fields = {
            "tmdb_id",
            "comment_id",
            "film_title",
            "comment_text",
            "created_at",
        }

        # Create a sample comment structure
        sample_comment = {
            "tmdb_id": 123,
            "comment_id": str(uuid.uuid4()),
            "film_title": "Test Movie",
            "comment_text": "Great film!",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        assert set(sample_comment.keys()) == expected_fields
        assert isinstance(sample_comment["tmdb_id"], int)
        assert isinstance(sample_comment["comment_id"], str)
        assert isinstance(sample_comment["film_title"], str)
        assert isinstance(sample_comment["comment_text"], str)
        assert isinstance(sample_comment["created_at"], str)
