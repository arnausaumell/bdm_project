from prefect import flow


@flow
def my_flow(name):
    print(f"hello {name}")


if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/arnausaumell/bdm_project.git",
        entrypoint="test.py:my_flow",
    ).deploy(
        name="dep-test2",
        work_pool_name="bdm-movies-db-workpool",
        cron="* * * * *",  # Run daily at midnight
        description="Daily job to pull new movie releases and update movie data from various sources",
        image="prefecthq/prefect:3-latest",
    )
