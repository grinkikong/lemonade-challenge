from prefect.client.orchestration import get_client
from prefect.deployments import run_deployment
from prefect import task, get_run_logger
import asyncio

POLLING_SLEEP_TIME_SECONDS = 10

@task
async def trigger_and_wait_prefect_job(prefect_flow_name, prefect_job_name, timeout=120):
    logger = get_run_logger()
    logger.info(f'Triggering Prefect job: {prefect_job_name}')

    # Trigger Prefect deployment
    flow_run = await run_deployment(name=f"{prefect_flow_name}/{prefect_job_name}", timeout=timeout)
    flow_run_id = flow_run.id
    logger.info(f"Triggered Prefect job {prefect_job_name} with flow_run_id: {flow_run_id}")

    # Polling for job completion
    async with get_client() as client:
        while True:
            flow_run = await client.read_flow_run(flow_run_id)
            flow_state = flow_run.state

            logger.info(f"Run status: {flow_state.name}")

            if flow_state.is_final():
                if flow_state.type == "COMPLETED":
                    logger.info("Job completed successfully!")
                elif flow_state.type == "FAILED":
                    logger.error("Job failed!")
                break

            await asyncio.sleep(POLLING_SLEEP_TIME_SECONDS)
