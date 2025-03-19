import json
import logging
import os

import polars as pl
from azure.storage.blob import ContainerClient, ContentSettings

from cfa_config_generator.utils.azure.auth import obtain_sp_credential
from cfa_config_generator.utils.azure.storage import (
    instantiate_blob_service_client,
    prep_blob_path,
    read_blob_csv,
)
from cfa_config_generator.utils.epinow2.constants import azure_storage
from cfa_config_generator.utils.epinow2.functions import (
    extract_user_args,
    generate_task_configs,
    generate_tasks_excl_from_data_excl,
    generate_timestamp,
    validate_args,
)

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    """
    A script to generate `epinow2` configuration objects. This
    will be invoked either by a user-triggered workflow with
    a supplied data_exclusions_path parameter.
    The .csv file located at the data_exclusions_path location
    should have columns of `state`, `disease`, `report_date`, `reference_date`.
    If the file is Azure Blob Storage, the path should be of the form
    `az://<container_name>/<blob_name>`.
    This script is the entrypoint to the workflow that generates
    configuration objects, validates them against a schema, and
    writes them to Blob Storage.
    """

    # Generate job-specific parameters
    as_of_date = generate_timestamp()

    # Pull run parameters from environment
    user_args = extract_user_args(as_of_date=as_of_date)

    # Pull the data_exclusions file path from the env vars. It it not in the user_args,
    # because in the general case, we don't need to know about the data_exclusions file,
    # so that is only handled in this script.
    excl_path: str | None = os.getenv("data_exclusions_path")
    if excl_path is None:
        raise ValueError("data_exclusions_path not found in environment variables.")

    # The desired schema for the data_exclusions file
    schema = pl.Schema(
        [
            ("state", pl.Utf8),
            ("disease", pl.Utf8),
            ("report_date", pl.Date),
            ("reference_date", pl.Date),
        ]
    )

    # Download (if necessary) and read the data_exclusions file
    try:
        sp_credential = obtain_sp_credential()
        storage_client = instantiate_blob_service_client(
            sp_credential=sp_credential,
            account_url=azure_storage["azure_storage_account_url"],
        )
    except Exception as e:
        logger.error(f"Error obtaining storage client: {e}")
        raise e

    if excl_path.startswith("az://"):
        # Extract container name and blob name from the path
        container_name, path_in_blob = prep_blob_path(excl_path)
        ctr_client: ContainerClient = storage_client.get_container_client(
            container_name
        )
        exclusions: pl.DataFrame = read_blob_csv(
            container_client=ctr_client, blob_name=path_in_blob, schema=schema
        )
    else:
        exclusions: pl.DataFrame = pl.read_csv(excl_path, schema=schema)

    # Validate the file path exists and has the proper columns
    task_excl_str: str = generate_tasks_excl_from_data_excl(excl_df=exclusions)

    # Update task_Exclusions argument
    user_args["task_exclusions"] = task_excl_str

    # Add the path to the data exclusions file to the user_args
    excl_field = (
        {"path": path_in_blob, "blob_storage_container": container_name}
        if excl_path.startswith("az://")
        else {"path": excl_path, "blob_storage_container": None}
    )
    user_args["exclusions"] = excl_field

    # Validate and sanitize args
    sanitized_args = validate_args(**user_args)

    # Generate task-specific configs
    task_configs, job_id = generate_task_configs(**sanitized_args)

    # Push task configs to Azure Blob Storage
    try:
        container_client = storage_client.get_container_client(
            container=azure_storage["azure_container_name"]
        )
        for task in task_configs:
            blob_name = f"{job_id}/{task['task_id']}.json"
            container_client.upload_blob(
                name=blob_name,
                data=json.dumps(task, indent=2),
                overwrite=True,
                content_settings=ContentSettings(content_type="application/json"),
            )
    except (LookupError, ValueError) as e:
        logger.error(f"Error pushing to Azure: {e}")
        raise e

    logger.info(
        f"Successfully generated configs for job; tasks stored in {job_id} directory."
    )
