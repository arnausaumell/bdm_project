import os
import sys

sys.path.append(os.getcwd())

from loguru import logger
from dotenv import load_dotenv

load_dotenv()


def test_environment():
    logger.info("Testing environment variables")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_DEFAULT_REGION")

    logger.info(f"AWS Access Key exists: {bool(aws_access_key)}")
    logger.info(f"AWS Secret Key exists: {bool(aws_secret_key)}")
    logger.info(f"AWS Region: {aws_region}")

    return True
