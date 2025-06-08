import pytest
import random

from core.data_ingestion.streaming_ingestion.comments_generator.fake_comments import (
    COMMENTS,
)


class TestFakeComments:
    def test_comments_is_list(self):
        """Test that COMMENTS is a list"""
        assert isinstance(COMMENTS, list)

    def test_comments_not_empty(self):
        """Test that COMMENTS contains data"""
        assert len(COMMENTS) > 0

    def test_all_comments_are_strings(self):
        """Test that all comments are strings"""
        assert all(isinstance(comment, str) for comment in COMMENTS)

    def test_no_empty_comments(self):
        """Test that no comments are empty strings"""
        assert all(len(comment.strip()) > 0 for comment in COMMENTS)

    def test_comments_variety(self):
        """Test that there's a reasonable variety of comments"""
        assert len(COMMENTS) >= 10  # Should have at least 10 different comments

    def test_random_comment_selection(self):
        """Test that random comment selection works"""
        # Set seed for reproducible test
        random.seed(42)
        comment1 = random.choice(COMMENTS)

        # Reset seed and get same comment
        random.seed(42)
        comment2 = random.choice(COMMENTS)

        assert comment1 == comment2
        assert comment1 in COMMENTS

    def test_comments_content_quality(self):
        """Test basic quality checks on comment content"""
        for comment in COMMENTS:
            # Should not be excessively long
            assert len(comment) <= 200
            # Should not contain only punctuation
            assert any(c.isalnum() for c in comment)

    def test_positive_and_negative_comments_exist(self):
        """Test that both positive and negative comments exist"""
        # This is a basic check - in reality you might want more sophisticated sentiment analysis
        positive_indicators = [
            "great",
            "amazing",
            "love",
            "brilliant",
            "perfect",
            "recommend",
            "masterpiece",
        ]
        negative_indicators = ["terrible", "worst", "bad", "boring", "awful", "waste"]

        has_positive = any(
            any(indicator in comment.lower() for indicator in positive_indicators)
            for comment in COMMENTS
        )
        has_negative = any(
            any(indicator in comment.lower() for indicator in negative_indicators)
            for comment in COMMENTS
        )

        assert has_positive, "Should have some positive comments"
        assert has_negative, "Should have some negative comments"
