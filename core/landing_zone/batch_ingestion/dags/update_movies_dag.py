from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "update_movies_data",
    default_args=default_args,
    description="Daily update of movies data from various sources",
    schedule_interval="0 0 * * *",  # Run at midnight every day
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["movies", "batch"],
)

run_batch_ingestion = DockerOperator(
    task_id="run_batch_ingestion",
    image="batch_ingestion:latest",
    api_version="auto",
    auto_remove=True,
    docker_url="unix://var/run/docker.sock",
    network_mode="kafka_net",
    mounts=[Mount(source="/path/to/your/data", target="/app/data", type="bind")],
    environment={
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY"),
        "TMDB_API_KEY": "{{ var.value.TMDB_API_KEY }}",
        "TRAKT_CLIENT_ID": "{{ var.value.TRAKT_CLIENT_ID }}",
        "TRAKT_CLIENT_SECRET": "{{ var.value.TRAKT_CLIENT_SECRET }}",
        "OMDB_API_KEY": "{{ var.value.OMDB_API_KEY }}",
        "YOUTUBE_API_KEY": "{{ var.value.YOUTUBE_API_KEY }}",
    },
    dag=dag,
)

run_batch_ingestion
