import json
import logging

from utils.azure.auth import obtain_sp_credential
from utils.azure.storage import instantiate_blob_client
from utils.epinow2.constants import azure_storage
from utils.epinow2.functions import (
    extract_user_args,
    generate_task_configs,
    generate_timestamp,
    generate_uuid,
    validate_args,
)

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    """
    A script to generate `epinow2` configuration objects. This
    will be invoked either by 1) a scheduled GH action using
    default values, or 2) a user-triggered workflow with supplied
    state, pathogen, report_date, and reference_date parameters.
    This script is the entrypoint to the workflow that generates
    configuration objects, validates them against a schema, and
    writes them to Blob Storage.
    """

    # Pull run parameters from environment
    user_args = extract_user_args()

    # Validate and sanitize args
    sanitized_args = validate_args(**user_args)
    # Generate job-specific parameters
    as_of_date = generate_timestamp()
    job_id = generate_uuid()
    # Generate task-specific configs
    task_configs, job_name = generate_task_configs(
        **sanitized_args, as_of_date=as_of_date, job_id=job_id
    )

    # Push task configs to Azure Blob Storage
    try:
        sp_credential = obtain_sp_credential()
        storage_client = instantiate_blob_client(
            sp_credential=sp_credential,
            account_url=azure_storage["azure_storage_account_url"],
        )
        container_client = storage_client.get_container_client(
            container=azure_storage["azure_container_name"]
        )
        for task in task_configs:
            blob_name = f"{job_name}/{task['task_id']}.json"
            container_client.upload_blob(
                name=blob_name, data=json.dumps(task), overwrite=True
            )
    except (LookupError, ValueError) as e:
        logger.error(f"Error pushing to Azure: {e}")
        raise e

    logger.info(
        f"Successfully generated configs for job: {job_id}. Tasks stored: {len(task_configs)}"
    )
