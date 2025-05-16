import json
import logging
from datetime import date

import polars as pl
from azure.identity import AzureCliCredential, DefaultAzureCredential
from azure.storage.blob import (
    BlobClient,
    BlobServiceClient,
    ContainerClient,
    ContentSettings,
)
from tqdm import tqdm

from cfa_config_generator.utils.azure.auth import obtain_sp_credential
from cfa_config_generator.utils.azure.storage import (
    instantiate_blob_service_client,
    prep_blob_path,
    read_blob_csv,
    upload_configs,
)
from cfa_config_generator.utils.epinow2.constants import azure_storage
from cfa_config_generator.utils.epinow2.functions import (
    generate_ref_date_tuples,
    generate_task_configs,
    generate_tasks_excl_from_data_excl,
    validate_args,
)

logger = logging.getLogger(__name__)


def generate_config(
    state: str,
    disease: str,
    report_date: date,
    reference_dates: tuple[date, date],
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

    Parameters
    ----------
    state: str
        Geography to run model
    disease: str
        Disease to run
    report_date: date
        Date of snapshot to use for model run
    reference_dates: tuple[date, date]
        Length two tuple of the minimum and maximum reference (event) dates
    data_path: str
        Path to input data
    data_container: str
        Blob storage container for input data
    production_date: date
        Production date of model run
    job_id: str
        Unique identifier for job
    as_of_date: str
        ISO format timestamp specifying the timestamp as of which to fetch
        the parameters for. This should usually be the same as the report date.
    output_container: str
        Blob storage container to store output
    task_exclusions: str | None
        Comma separated state:disease pair to exclude from model run
    exclusions: dict | None
        Dictionary with keys 'path' and 'blob_storage_container' for the exclusions file.
        If provided, this will be used to generate the task exclusions string.

    Returns
    -------
    None
        The function writes the generated configuration objects to Blob Storage.

    Raises
    ------
    ValueError
        If the report_dates and reference_dates are not the same length.
    LookupError
        If there is an error obtaining the storage client.
    ValueError
        If there is an error pushing the configuration objects to Azure Blob Storage.
    Exception
        If there is an error during the process.
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
        upload_fail_message = f"Failed to upload the following tasks: {', '.join(failed_uploads)}"
        logger.error(upload_fail_message)
        raise ValueError(upload_fail_message)

    logger.info(
        f"Successfully generated configs for job; tasks stored in {generated_job_id} directory."
    )


def generate_rerun_config(
    state: str,
    disease: str,
    report_date: date,
    reference_dates: tuple[date, date],
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

    Parameters
    ----------
    state: str
        Geography to run model
    disease: str
        Disease to run
    report_date: date
        Date of snapshot to use for model run
    reference_dates: tuple[date, date]
        Length two tuple of the minimum and maximum reference (event) dates
    data_path: str
        Path to input data
    data_container: str
        Blob storage container for input data
    production_date: date
        Production date of model run
    job_id: str
        Unique identifier for job
    as_of_date: str
        ISO format timestamp specifying the timestamp as of which to fetch
        the parameters for. This should usually be the same as the report date.
    output_container: str
        Blob storage container to store output
    data_exclusions_path: str | None
        Path to the data exclusion CSV file. If in Blob, use form
        `az://<container-name>/<path>`. Defaults to
        `az://nssp-etl/outliers-v2/<report_date>.csv` if None or empty.
    Returns
    -------
    None
        The function writes the generated configuration objects to Blob Storage.

    Raises
    ------
    ValueError
        If the report_dates and reference_dates are not the same length.
    LookupError
        If there is an error obtaining the storage client.
    ValueError
        If there is an error pushing the configuration objects to Azure Blob Storage.
    Exception
        If there is an error during the process.
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


def generate_backfill_config(
    state: str,
    disease: str,
    report_dates: list[date],
    reference_date_time_span: str,
    data_paths: list[str],
    data_container: str,
    backfill_name: str,
    as_of_dates: list[str],
    output_container: str,
    task_exclusions: str | None = None,
):
    """
    This function can be seen a like a loop over generate_config for a number of report
    dates.
    - It provides the same level of configuration as generate_config
    - It allows the caller to set the data source location. For this data source, it
        will search for any existing exclusions files and add them to the matching
        report date config files, if any.
    - It will use a unique job ID for each report date to allow the post processor (as
        it currently exists) to run separately for each report date.
    - It will take a `name` argument for the backfill run. E.g. the NAME could be
        "nssp_api_v2" or "epidist_delays". Each job id will be `<name>_<report_date>`.
    - For a given report date, it will check in blob if that report date has a
        corresponding exclusions file. If it does, it will add the exclusions file to
        the config.
    - It will write an additional metadata file to the config storage container called
        `backfill/<name>_jobids.json`, and containing a list of all the job ids in the
        backfill run. This is to allow us to easily find all the job ids for a backfill
        run later.

    Parameters
    ----------
    state: str
        Geography(ies) to run model
    disease: str
        Disease(s) to run
    report_dates: list[date]
        List of snapshots to use for model run
    reference_date_time_span: str
        A string representing the time span for the earliest reference date relative to
        the report date. This should be formatted following the conventions of polars
        `.dt.offset_by()`. Usually, this will be a string like "8w" or "1d" (for 8 weeks
        or 1 day).
    data_paths: list[str]
        A list of paths to the input data for each report date. This is required even if
        all runs will use the same data path.
    data_container: str
        Blob storage container for input data
    backfill_name: str
        Name of the backfill run. This will be used to generate the job IDs for each
        report date in the format `<backfill_name>_<report_date>`.
    as_of_dates: list[str]
        A list of ISO format timestamps specifying the time as of which to fetch
        the parameters for. Note that what the times are will depend on your use case.
        If you want to see the model working on the parameters known at the time of a
        given report date, it should be the same as the report date. If you want to
        see the model working on the parameters known at the current time, this should
        be the current time. This should be a list of the same length as the report dates.
    output_container: str
        Blob storage container to store output.
    task_exclusions: str | None
        Comma separated state:disease pair to exclude from model run.
    """
    # === Set up =======================================================================
    if not len(report_dates) == len(data_paths) == len(as_of_dates):
        lens = [len(report_dates), len(data_paths), len(as_of_dates)]
        raise ValueError(
            f"report_dates, data_paths, and as_of_dates must all be the same length. "
            f"Got lengths {lens} respectively.."
        )

    # Create the list of reference dates based on the reference date time span and the
    # report dates. Each report date will have a tuple of reference dates.
    reference_dates: list[tuple[date, date]] = generate_ref_date_tuples(
        report_dates=report_dates, delta=reference_date_time_span
    )

    # For accessing blob
    bsc: BlobServiceClient = BlobServiceClient(
        account_url=azure_storage["azure_storage_account_url"],
        credential=DefaultAzureCredential(),
    )
    config_ctr_client: ContainerClient = bsc.get_container_client(
        container=azure_storage["azure_container_name"]
    )
    data_ctr_client: ContainerClient = bsc.get_container_client(
        container=data_container
    )

    # === Managing data exclusions =====================================================
    # For keeping track of which report dates have exclusions files
    exclusions_dict: dict[date, dict] = {}

    # For each report date, check if there is a corresponding exclusions file
    logger.info("Checking for exclusions files...")
    for rep_date in report_dates:
        # Check if the exclusions file exists in the data container
        blob_name = f"outliers-v2/{rep_date.isoformat()}.csv"
        blob_client: BlobClient = data_ctr_client.get_blob_client(blob_name)
        # Check if the blob exists
        if blob_client.exists():
            logger.info(f"Exclusions file found for {rep_date.isoformat()}")
            # Create the dictionary for the exclusions file
            exclusions_dict[rep_date] = {
                "path": blob_name,
                "blob_storage_container": data_container,
            }
        else:
            logger.info(f"No exclusions file found for {rep_date.isoformat()}")

    # Create the list of job ids
    job_ids: list[str] = [
        f"{backfill_name}_{rep_date.isoformat()}" for rep_date in report_dates
    ]

    # For each report date, generate the configs.
    # This can take a bit, so use a progress bar.
    for rep_date, job_id, ref_dates, data_path, as_of_date in tqdm(
        zip(
            report_dates,
            job_ids,
            reference_dates,
            data_paths,
            as_of_dates,
            strict=True,
        ),
        total=len(report_dates),
        desc="Generating configs",
    ):
        # Generate the config for this report date
        generate_config(
            state=state,
            disease=disease,
            report_date=rep_date,
            reference_dates=ref_dates,
            data_path=data_path,
            data_container=data_container,
            production_date=rep_date,
            job_id=job_id,
            as_of_date=as_of_date,
            output_container=output_container,
            task_exclusions=task_exclusions,
            exclusions=exclusions_dict.get(rep_date, None),
        )
        logger.info(
            f"Successfully generated config for {job_id} with report date {rep_date.isoformat()}"
        )

    # Write the metadata file to the config storage container
    metadata_path = f"backfill/{backfill_name}_jobids.json"
    config_ctr_client.upload_blob(
        name=metadata_path,
        data=json.dumps(job_ids, indent=2),
        overwrite=True,
        content_settings=ContentSettings(content_type="application/json"),
    )
    logger.info(
        f"Successfully wrote metadata file to {metadata_path} in {azure_storage['azure_container_name']}."
    )
