from pathlib import Path
from prefect import flow


@flow(log_prints=True)
def my_flow(name: str = "world"):
    print(f"Hello {name}! I'm a flow from a Python script!")


if __name__ == "__main__":
    my_flow.from_source(
        source=str(Path(__file__).parent),
        entrypoint="test.py:my_flow",
    ).deploy(
        name="my-deployment",
        parameters=dict(name="Marvin"),
        work_pool_name="bdm-movies-db-workpool",
    )
