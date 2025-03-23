from prefect import flow
from prefect.client.orchestration import get_client
import asyncio
from typing import Callable, Optional


class PrefectManager:
    def __init__(self, work_pool_name: str):
        self.work_pool_name = work_pool_name

    async def create_deployment(
        self,
        flow_func: Callable,
        deployment_name: str,
        cron: str,
        description: Optional[str] = None,
        func_args: Optional[dict] = None,
    ):
        # Ensure connection to Prefect Cloud
        async with get_client() as client:
            await client.hello()

        # Decorate the function as a flow if it isn't already
        if not hasattr(flow_func, "to_deployment"):
            flow_func = flow(flow_func)

        # Create deployment
        deployment = await flow_func.to_deployment(
            name=deployment_name,
            cron=cron,
            description=description,
            parameters=func_args,
        )  # type: ignore

        await deployment.apply(
            work_pool_name=self.work_pool_name,
        )  # type: ignore

        return deployment


# Example usage
@flow
def example_flow():
    print("Hello, Prefect!")


async def main():
    manager = PrefectManager(work_pool_name="bdm-movies-workpool")
    await manager.create_deployment(
        flow_func=example_flow,
        deployment_name="example-deployment",
        cron="*/5 * * * *",
        description="An example deployment",
    )


if __name__ == "__main__":
    asyncio.run(main())
