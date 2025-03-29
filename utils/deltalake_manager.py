import os, sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from delta import DeltaTable
import pandas as pd
from loguru import logger
from utils.s3_manager import S3Manager

pd.DataFrame.iteritems = pd.DataFrame.items


class DeltaLakeManager:
    def __init__(self, s3_bucket_name: str = "bdm-movies-db"):
        self.spark = self._initialize_spark(s3_bucket_name)
        self.s3_manager = S3Manager(bucket_name=s3_bucket_name)

    def _initialize_spark(self, app_name: str) -> SparkSession:
        """Initialize Spark session with Delta Lake configurations"""
        spark = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config(
                "spark.jars.packages",
                "io.delta:delta-core_2.12:2.0.0,"
                "org.apache.hadoop:hadoop-aws:3.2.2,"
                "com.amazonaws:aws-java-sdk-bundle:1.11.1026",
            )
            # Add S3 configurations
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        return spark

    def create_table(
        self, data: pd.DataFrame, table_path: str, mode: str = "overwrite"
    ) -> None:
        """
        Create a Delta Lake table from a pandas DataFrame in S3

        Args:
            data: Pandas DataFrame containing the data
            table_path: S3 path where the Delta Lake table will be stored (e.g., 's3a://bucket-name/path/to/table')
            mode: Write mode ('overwrite', 'append', 'ignore', 'error')
        """
        s3_path = f"s3a://{self.s3_manager.bucket_name}/{table_path}"
        spark_df = self.spark.createDataFrame(data)
        spark_df.write.format("delta").mode(mode).save(s3_path)
        logger.info(f"Table created at {s3_path}")

    def upsert_to_table(
        self, data: pd.DataFrame, table_path: str, merge_key: str
    ) -> None:
        """
        Perform upsert (update/insert) operation on Delta Lake table in S3

        Args:
            data: Pandas DataFrame containing the new/updated data
            table_path: S3 path to the existing Delta Lake table
            merge_key: Column name to use as merge key
        """
        s3_path = f"s3a://{self.s3_manager.bucket_name}/{table_path}"

        if not self.s3_manager.folder_exists(table_path):
            logger.info(f"Table does not exist at {s3_path}. Creating new table.")
            self.create_table(data, table_path)
        else:
            new_data = self.spark.createDataFrame(data)
            delta_table = DeltaTable.forPath(self.spark, s3_path)

            delta_table.alias("target").merge(
                new_data.alias("source"), f"target.{merge_key} = source.{merge_key}"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            logger.info(f"Upserted data to {s3_path}")

    def read_table(self, table_path: str) -> pd.DataFrame:
        """
        Read a Delta Lake table from S3 into a pandas DataFrame

        Args:
            table_path: S3 path to the Delta Lake table
        """
        s3_path = f"s3a://{self.s3_manager.bucket_name}/{table_path}"
        df = self.spark.read.format("delta").load(s3_path).toPandas()
        logger.info(f"Read table from {s3_path}")
        return pd.DataFrame(df.to_dict(orient="records"))

    def show_table(self, table_path: str) -> None:
        """
        Show the contents of a Delta Lake table from S3

        Args:
            table_path: S3 path to the Delta Lake table
        """
        s3_path = f"s3a://{self.s3_manager.bucket_name}/{table_path}"
        table = self.spark.read.format("delta").load(s3_path)
        logger.info(f"Showing table from {table_path}. Records: {table.count()}")
        table.show()

    def delete_table(self, table_path: str) -> None:
        """
        Delete a Delta Lake table by removing its directory

        Args:
            table_path: Path to the Delta Lake table
        """
        if self.s3_manager.file_exists(table_path):
            self.s3_manager.delete_file(table_path)
            logger.info(f"Table deleted at {table_path}")
        else:
            logger.info(f"Table does not exist at {table_path}")


# Example usage:
if __name__ == "__main__":
    # Initialize DeltaLakeManager
    delta_manager = DeltaLakeManager(s3_bucket_name="bdm-movies-db")

    TABLE_PATH = "test/delta-lake-table"

    # Delete table
    delta_manager.delete_table(TABLE_PATH)

    # Create table
    df = pd.DataFrame(
        [
            {"id": 1, "name": "John", "age": 28},
            {"id": 2, "name": "Jane", "age": 34},
            {"id": 3, "name": "Jim", "age": 29},
        ]
    )
    delta_manager.create_table(df, TABLE_PATH)

    # Upsert new data
    new_df = pd.DataFrame(
        [
            {"id": 4, "name": "Alice", "age": 31},
            {"id": 5, "name": "Bob", "age": 26},
        ]
    )
    delta_manager.upsert_to_table(new_df, TABLE_PATH, "id")

    # Show table
    delta_manager.show_table(TABLE_PATH)

    print(delta_manager.read_table(TABLE_PATH))
    print(type(delta_manager.read_table(TABLE_PATH)))
