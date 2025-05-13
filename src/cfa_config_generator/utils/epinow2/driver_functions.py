import logging
from datetime import date

import polars as pl
from azure.identity import AzureCliCredential
from azure.storage.blob import BlobServiceClient, ContainerClient

from cfa_config_generator.utils.azure.auth import obtain_sp_credential
from cfa_config_generator.utils.azure.storage import (
    instantiate_blob_service_client,
    prep_blob_path,
    read_blob_csv,
    upload_configs,
)
from cfa_config_generator.utils.epinow2.constants import azure_storage
from cfa_config_generator.utils.epinow2.functions import (
    generate_task_configs,
    generate_tasks_excl_from_data_excl,
    validate_args,
)

logger = logging.getLogger(__name__)


def generate_config(
    state: str,
    disease: str,
    report_date: date,
    reference_dates: list[date],
    data_path: str,
    data_container: str,
    production_date: date,
    job_id: str,
    as_of_date: str,
    output_container: str,
    task_exclusions: str | None = None,
    exclusions: dict | None = None,
):
    """
    A function to generate `epinow2` configuration objects based on provided arguments.
    This function validates the arguments, generates configuration objects,
    and writes them to Blob Storage.

    Parameters:
        state: geography to run model
        disease: disease to run
        report_date: date of model run
        reference_dates: list of reference (event) dates
        data_path: path to input data
        data_container: container for input data
        production_date: production date of model run
        job_id: unique identifier for job
        as_of_date: iso format timestamp of model run
        output_container: Azure container to store output
        task_exclusions: state:disease pair to exclude from model run
        exclusions: path to exclusions csv
    """
    # Validate and sanitize args directly
    sanitized_args = validate_args(
        state=state,
        disease=disease,
        report_date=report_date,
        reference_dates=reference_dates,
        data_path=data_path,
        data_container=data_container,
        production_date=production_date,
        job_id=job_id,
        as_of_date=as_of_date,
        output_container=output_container,
        task_exclusions=task_exclusions,
        exclusions=exclusions,
    )

    # Generate task-specific configs
    task_configs, generated_job_id = generate_task_configs(**sanitized_args)

    # Push task configs to Azure Blob Storage
    try:
        sp_credential: AzureCliCredential = obtain_sp_credential()
        storage_client: BlobServiceClient = instantiate_blob_service_client(
            sp_credential=sp_credential,
            account_url=azure_storage["azure_storage_account_url"],
        )
        container_client = storage_client.get_container_client(
            container=azure_storage["azure_container_name"]
        )
        failed_uploads: list[str] = upload_configs(
            task_configs=task_configs,
            job_id=generated_job_id,
            container_client=container_client,
        )
    except (LookupError, ValueError, Exception) as e:
        logger.error(f"Error pushing to Azure: {e}")
        raise e

    # Check for failed uploads
    if any(failed_uploads):
        logger.error(
            f"Failed to upload the following tasks: {', '.join(failed_uploads)}"
        )
        raise ValueError(
            f"Failed to upload the following tasks: {', '.join(failed_uploads)}"
        )

    logger.info(
        f"Successfully generated configs for job; tasks stored in {generated_job_id} directory."
    )


def generate_rerun_config(
    state: str,
    disease: str,
    report_date: date,
    reference_dates: list[date],
    data_path: str,
    data_container: str,
    production_date: date,
    job_id: str,
    as_of_date: str,
    output_container: str,
    data_exclusions_path: str | None = None,
):
    """
    A function to generate `epinow2` configuration objects for re-running tasks
    based on a data exclusions file.

    This function reads a data exclusion file, determines which tasks need to be re-run,
    generates configuration objects for those tasks, validates them, and writes them
    to Blob Storage.

    Parameters:
        state: geography to run model
        disease: disease to run
        report_date: date of model run
        reference_dates: list of reference (event) dates
        data_path: path to input data
        data_container: container for input data
        production_date: production date of model run
        job_id: unique identifier for job
        as_of_date: iso format timestamp of model run
        output_container: Azure container to store output
        data_exclusions_path: Path to the data exclusion CSV file. If in Blob, use form `az://<container-name>/<path>`.
                              Defaults to `az://nssp-etl/outliers-v2/<report_date>.csv` if None or empty.
    """
    # Handle default data_exclusions_path
    excl_path = data_exclusions_path
    if (excl_path is None) or (excl_path == ""):
        # Use the provided date object directly
        rd: date = report_date
        excl_path = azure_storage["outliers_blob_path"].format(rd.isoformat())
        logger.info(f"data_exclusions_path not provided, defaulting to: {excl_path}")

    # The desired schema for the data_exclusions file
    schema = pl.Schema(
        [
            ("state", pl.String),
            ("disease", pl.String),
            ("report_date", pl.Date),
            ("reference_date", pl.Date),
        ]
    )

    try:
        sp_credential = obtain_sp_credential()
        storage_client = instantiate_blob_service_client(
            sp_credential=sp_credential,
            account_url=azure_storage["azure_storage_account_url"],
        )
    except Exception as e:
        logger.error(f"Error obtaining storage client: {e}")
        raise e

    # Download (if necessary) and read the data_exclusions file
    path_in_blob = None
    container_name = None
    if excl_path.startswith("az://"):
        try:
            # Extract container name and blob name from the path
            container_name, path_in_blob = prep_blob_path(excl_path)
            ctr_client: ContainerClient = storage_client.get_container_client(
                container_name
            )
            exclusions_df: pl.DataFrame = read_blob_csv(
                container_client=ctr_client,
                blob_name=path_in_blob,
                schema_overrides=schema,
            ).select(["state", "disease", "report_date", "reference_date"])
        except Exception as e:
            logger.error(
                f"Error reading data exclusions from Azure Blob Storage ({excl_path}): {e}"
            )
            raise e
    else:
        try:
            exclusions_df: pl.DataFrame = pl.read_csv(
                excl_path, schema_overrides=schema
            ).select(["state", "disease", "report_date", "reference_date"])
        except Exception as e:
            logger.error(
                f"Error reading data exclusions file from local path ({excl_path}): {e}"
            )
            raise e

    # Generate the task exclusion string based on the *inverse* of the exclusions file
    # (i.e., we want to *exclude* tasks that are *not* in the file)
    task_excl_str: str = generate_tasks_excl_from_data_excl(excl_df=exclusions_df)

    # Add the path to the data exclusions file to the user_args
    excl_field = (
        {"path": path_in_blob, "blob_storage_container": container_name}
        if excl_path.startswith("az://") and path_in_blob and container_name
        else {"path": excl_path, "blob_storage_container": None}
    )

    # Validate and sanitize args directly
    sanitized_args = validate_args(
        state=state,
        disease=disease,
        report_date=report_date,
        reference_dates=reference_dates,
        data_path=data_path,
        data_container=data_container,
        production_date=production_date,
        job_id=job_id,
        as_of_date=as_of_date,
        output_container=output_container,
        task_exclusions=task_excl_str,
        exclusions=excl_field,
    )

    # Generate task-specific configs
    task_configs, generated_job_id = generate_task_configs(**sanitized_args)

    # Push task configs to Azure Blob Storage
    try:
        container_client = storage_client.get_container_client(
            container=azure_storage["azure_container_name"]
        )
        failed_uploads: list[str] = upload_configs(
            task_configs=task_configs,
            job_id=generated_job_id,
            container_client=container_client,
        )
    except (LookupError, ValueError, Exception) as e:
        logger.error(f"Error pushing to Azure: {e}")
        raise e

    # Check for failed uploads
    if any(failed_uploads):
        logger.error(
            f"Failed to upload the following tasks: {', '.join(failed_uploads)}"
        )
        raise ValueError(
            f"Failed to upload the following tasks: {', '.join(failed_uploads)}"
        )

    logger.info(
        f"Successfully generated configs for job; tasks stored in {generated_job_id} directory."
    )


def generate_backfill_config():
    pass
