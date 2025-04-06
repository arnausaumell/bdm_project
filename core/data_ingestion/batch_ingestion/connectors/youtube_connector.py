import os
from typing import Optional, Dict, List
import dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

dotenv.load_dotenv()


class YouTubeConnector:
    def __init__(self):
        self._api_key = os.getenv("YOUTUBE_API_KEY")
        self._youtube = build("youtube", "v3", developerKey=self._api_key)

    def search_movie_trailer(self, movie_title: str) -> Optional[str]:
        """
        Search for a movie trailer and return its video ID
        """
        try:
            search_response = (
                self._youtube.search()
                .list(
                    q=f"{movie_title} official trailer",
                    part="id",
                    maxResults=1,
                    type="video",
                    videoDefinition="high",
                )
                .execute()
            )

            if search_response.get("items"):
                return search_response["items"][0]["id"]["videoId"]
            return None
        except HttpError as e:
            print(f"An error occurred: {e}")
            return None

    def get_video_info(self, video_id: str) -> dict:
        """
        Get video info including statistics and comments
        """
        return {
            "statistics": self._get_video_statistics(video_id),
            "comments": self._get_video_comments(video_id, max_results=5),
        }

    def _get_video_statistics(self, video_id: str) -> Optional[dict]:
        """
        Get video statistics including like count and view count
        """
        try:
            stats_response = (
                self._youtube.videos().list(part="statistics", id=video_id).execute()
            )

            if stats_response.get("items"):
                return stats_response["items"][0]["statistics"]
            return None
        except HttpError as e:
            print(f"An error occurred: {e}")
            return None

    def _get_video_comments(self, video_id: str, max_results: int = 100) -> List[Dict]:
        """
        Get comments for a video
        """
        try:
            comments = []
            request = self._youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=min(max_results, 100),
                order="relevance",
            )

            while request and len(comments) < max_results:
                response = request.execute()

                for item in response["items"]:
                    comment = item["snippet"]["topLevelComment"]["snippet"]
                    comments.append(
                        {
                            "author": comment["authorDisplayName"],
                            "text": comment["textDisplay"],
                            "likes": comment["likeCount"],
                            "published_at": comment["publishedAt"],
                        }
                    )

                # Get the next page of comments if available
                request = self._youtube.commentThreads().list_next(request, response)

            return comments
        except HttpError as e:
            print(f"An error occurred: {e}")
            return []


if __name__ == "__main__":
    import json

    youtube_connector = YouTubeConnector()

    # Example usage
    movie_title = "Oppenheimer"

    # Find the trailer
    video_id = youtube_connector.search_movie_trailer(movie_title)
    if video_id:
        print(f"Found trailer: https://youtube.com/watch?v={video_id}")

        # Get statistics
        stats = youtube_connector.get_video_info(video_id)
        print("\nVideo Statistics:")
        print(json.dumps(stats, indent=2))
