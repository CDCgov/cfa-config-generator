import json
import logging

from src.cfa_config_generator.utils.azure.auth import obtain_sp_credential
from src.cfa_config_generator.utils.azure.storage import instantiate_blob_service_client
from src.cfa_config_generator.utils.epinow2.constants import azure_storage
from src.cfa_config_generator.utils.epinow2.functions import (
    extract_user_args,
    generate_task_configs,
    generate_timestamp,
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

    # Generate job-specific parameters
    as_of_date = generate_timestamp()

    # Pull run parameters from environment
    user_args = extract_user_args(as_of_date=as_of_date)

    # Validate and sanitize args
    sanitized_args = validate_args(**user_args)

    # Generate task-specific configs
    task_configs, job_id = generate_task_configs(**sanitized_args)

    # Push task configs to Azure Blob Storage
    try:
        sp_credential = obtain_sp_credential()
        storage_client = instantiate_blob_service_client(
            sp_credential=sp_credential,
            account_url=azure_storage["azure_storage_account_url"],
        )
        container_client = storage_client.get_container_client(
            container=azure_storage["azure_container_name"]
        )
        for task in task_configs:
            blob_name = f"{job_id}/{task['task_id']}.json"
            container_client.upload_blob(
                name=blob_name, data=json.dumps(task, indent=2), overwrite=True
            )
    except (LookupError, ValueError) as e:
        logger.error(f"Error pushing to Azure: {e}")
        raise e

    logger.info(
        f"Successfully generated configs for job; tasks stored in {job_id} directory."
    )
