import json
import os
from loguru import logger
from .config import COMMENTS_FILE


class StorageHandler:
    @staticmethod
    def save_comment(comment_data: dict):
        """Save comment to JSON file, maintaining an array of comments"""
        try:
            # Read existing comments
            if os.path.exists(COMMENTS_FILE):
                with open(COMMENTS_FILE, "r") as f:
                    comments = json.load(f)
            else:
                comments = []

            # Add new comment
            comments.append(comment_data)

            # Write back to file
            with open(COMMENTS_FILE, "w") as f:
                json.dump(comments, f, indent=2)

            logger.info(f"Saved comment for film: {comment_data['film_title']}")
        except Exception as e:
            logger.error(f"Error saving to JSON: {e}")
            raise
