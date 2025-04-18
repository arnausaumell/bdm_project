import boto3
from botocore.exceptions import ClientError
import logging
from typing import Optional, BinaryIO, Union
from dotenv import load_dotenv
import os

load_dotenv()


class S3Manager:
    def __init__(self, bucket_name: str):
        """
        Initialize S3 manager with a bucket name

        Args:
            bucket_name (str): Name of the S3 bucket
        """
        self.s3_client = boto3.client("s3")
        self.bucket_name = bucket_name
        self.logger = logging.getLogger(__name__)

    def upload_file(self, file_path: str, s3_key: str) -> None:
        """
        Upload a file to S3

        Args:
            file_path: Local path to the file
            s3_key: S3 key where the file will be stored
        """
        if os.path.isdir(file_path):
            self.logger.error(
                f"Cannot upload directory '{file_path}'. Please specify a file path."
            )
            return

        try:
            self.s3_client.upload_file(file_path, self.bucket_name, s3_key)
            self.logger.info(
                f"File uploaded successfully to s3://{self.bucket_name}/{s3_key}"
            )
        except Exception as e:
            self.logger.error(f"Error uploading file to S3: {str(e)}")

    def download_file(self, s3_key: str, local_path: str) -> bool:
        """
        Download a file from S3 bucket

        Args:
            s3_key (str): Path of the file in S3
            local_path (str): Local destination path

        Returns:
            bool: True if download successful, False otherwise
        """
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            return True
        except ClientError as e:
            self.logger.error(f"Failed to download file: {e}")
            return False

    def delete_file(self, s3_key: str) -> bool:
        """
        Delete a file from S3 bucket

        Args:
            s3_key (str): Path of the file in S3

        Returns:
            bool: True if deletion successful, False otherwise
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            self.logger.error(f"Failed to delete file: {e}")
            return False

    def file_exists(self, s3_key: str) -> bool:
        """
        Check if a file exists in S3 bucket

        Args:
            s3_key (str): Path of the file in S3

        Returns:
            bool: True if file exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            self.logger.error(f"Error checking if file exists: {e}")
            return False

    def get_file_url(self, s3_key: str, expiration: int = 3600) -> Optional[str]:
        """
        Generate a presigned URL for a file

        Args:
            s3_key (str): Path of the file in S3
            expiration (int): URL expiration time in seconds (default: 1 hour)

        Returns:
            Optional[str]: Presigned URL if successful, None otherwise
        """
        try:
            url = self.s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket_name, "Key": s3_key},
                ExpiresIn=expiration,
            )
            return url
        except ClientError as e:
            self.logger.error(f"Failed to generate presigned URL: {e}")
            return None

    def folder_exists(self, folder_path: str) -> bool:
        """
        Check if a folder exists in S3 bucket.
        Note: In S3, folders are virtual and exist only as prefixes to objects.

        Args:
            folder_path (str): Path of the folder in S3 (with or without trailing slash)

        Returns:
            bool: True if folder exists (has at least one object with this prefix), False otherwise
        """
        # Ensure the folder path ends with a slash
        folder_path = folder_path.rstrip("/") + "/"

        try:
            # List objects with the specified prefix, limiting to 1 result
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=folder_path, MaxKeys=1
            )
            # If there are any contents, the folder exists
            return "Contents" in response
        except ClientError as e:
            self.logger.error(f"Error checking if folder exists: {e}")
            return False


if __name__ == "__main__":
    BUCKET_NAME = "bdm-movies-db"
    s3 = S3Manager(BUCKET_NAME)
    print(s3.folder_exists("stream/blog_comments"))
