import json
import re

import polars as pl
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient

from cfa_config_generator.utils.epinow2.constants import azure_storage


def instantiate_blob_service_client(
    sp_credential: DefaultAzureCredential | None = None, account_url: str = ""
) -> BlobServiceClient:
    """Function to instantiate blob service client to interact
    with Azure Storage.
    Args:
        sp_credential (DefaultAzureCredential): Service principal credential object
        for use in authenticating with Storage API.
        account_url (str): URL of the storage account.
    Returns:
        BlobServiceClient: Azure Blob Storage client
    Raises:
        ValueError: If sp_credential is invalid or BlobServiceClient
        fails to instantiate.
    """

    if not sp_credential:
        raise ValueError("Service principal credential not provided.")

    blob_service_client = BlobServiceClient(account_url, credential=sp_credential)

    return blob_service_client


def get_date_from_job_id(file_names: list | None = None) -> dict:
    """Function to extract dates from a list of job IDs.
    Args:
        job_list (list): List of blobs from Azure Storage.
    Returns:
        list: List of unique job IDs.
    """

    pattern_yyyy_mm_dd = r"\b\d{4}-\d{2}-\d{2}"
    pattern_yyyyMMdd = r"\b(\d{8})"

    # Extract dates
    extracted_dates = []
    for file_name in file_names:
        match_mm_dd = re.search(pattern_yyyy_mm_dd, file_name)
        match_yyyyMMdd = re.search(pattern_yyyyMMdd, file_name)
        if match_mm_dd:
            extracted_dates.append(match_mm_dd.group(0))
        elif match_yyyyMMdd:
            str_match = match_yyyyMMdd.group(0)
            year = str_match[:4]
            month = str_match[4:6]
            day = str_match[6:]
            extracted_dates.append(f"{year}-{month}-{day}")
        else:
            extracted_dates.append("")

    dict_output = dict(zip(file_names, extracted_dates))

    return dict(sorted(dict_output.items(), key=lambda item: item[1], reverse=True))


def get_unique_jobs_from_blobs(blob_list: list | None = None) -> list:
    """Function to extract unique job IDs from a list of blobs.
    Args:
        blob_list (list): List of blobs from Azure Storage.
    Returns:
        list: List of unique job IDs.
    """
    unique_jobs = set()
    blob_list = blob_list or []
    for blob in blob_list:
        job_id = blob.name.split("/")[0]
        unique_jobs.add(job_id)
    return sorted(unique_jobs)


def get_tasks_for_job_id(
    blob_list: list | None = None,
    job_id: str = "",
    state: str | None = None,
    disease: str | None = None,
) -> list:
    """Function to extract tasks for a specific job ID from a list of blobs.
    Args:
        blob_list (list): List of blobs from Azure Storage.
        job_id (str): Job ID to filter tasks by.
    Returns:
        list: List of tasks for the specified job ID.
    """
    tasks_for_job = []
    blob_list = blob_list or []
    for blob in blob_list:
        try:
            blob_job_id, blob_task_id = blob.name.split("/")
            if blob_job_id == job_id:
                tasks_for_job.append(blob_task_id)
        except ValueError:
            raise ValueError(
                "Blob name does not match expected format. Check that blobs are formatted as <job_id>/<task_id>.json."
            )
    if state:
        tasks_for_job = [task for task in tasks_for_job if state in task]

    if disease:
        tasks_for_job = [task for task in tasks_for_job if disease in task]

    return sorted(tasks_for_job)


def download_blob(
    blob_path: str = "", sp_credential: DefaultAzureCredential | None = None
) -> dict:
    """Function to download a blob from Azure Storage and return its contents in JSON format.
    Args:
        blob_path (str): Path to the blob to download.
        sp_credential (DefaultAzureCredential): Service principal credential object
        for use in authenticating with Storage API.
    Returns:
        dict: JSON object representing the contents of the blob.
    """
    blob_service_client = instantiate_blob_service_client(
        sp_credential=sp_credential,
        account_url=azure_storage["azure_storage_account_url"],
    )
    blob_client = blob_service_client.get_blob_client(
        container=azure_storage["azure_container_name"], blob=blob_path
    )
    downloader = blob_client.download_blob(max_concurrency=1, encoding="utf-8")
    blob_text = downloader.readall()
    return json.loads(blob_text)


def read_blob_csv(
    container_client: ContainerClient, blob_name: str, **kwargs
) -> pl.DataFrame:
    """
    Read a CSV file from blob storage into memory as a polars DataFrame.

    Parameters
    ----------
    container_client : ContainerClient
        An instantiated client for the blob container, used to retrieve the blob.
    blob_name : str
        The name of the blob containing the CSV file.
    **kwargs
        Additional keyword arguments to pass to polars' read_csv() function. See list at
        https://docs.pola.rs/api/python/stable/reference/api/polars.read_csv.html
    """
    return pl.read_csv(
        container_client.get_blob_client(blob_name).download_blob().readall(),
        **kwargs,
    )


def prep_blob_path(blob_path: str) -> tuple[str, str]:
    """
    Takes in a path in blob, of the form `az://<container_name>/<blob_name>`,
    and returns the container name and blob name as a tuple.

    Parameters
    ----------
    blob_path : str
        The path to the blob in the format `az://<container_name>/<blob_name>`.

    Returns
    -------
    tuple[str, str]
        A tuple containing the container name and blob name.
        The container name is the first part of the path, and the blob name is the
        second part of the path.
        For example, if the input is `az://my_container/my_blob.csv`, the output
        is `("my_container", "my_blob.csv")`.

    Raises
    ------
    ValueError
        If the input path is not in the expected format.
        If the input path does not contain a container name and blob name.
        If the input path is empty.
    """
    if not blob_path:
        raise ValueError("Blob path is empty.")

    if not blob_path.startswith("az://"):
        raise ValueError(f"Blob path {blob_path} does not start with 'az://'.")

    # Remove the 'az://' prefix
    blob_path = blob_path.replace("az://", "", 1)
    if not blob_path:
        raise ValueError("Blob path is empty after removing 'az://' prefix.")

    # Split the path into container and blob
    parts: list[str] = blob_path.split("/", 1)
    if len(parts) != 2:
        raise ValueError(
            f"Blob path {blob_path} does not contain a container name and blob name."
        )

    container, blob = parts

    # Check that the parts are not empty or just a slash
    if not blob or blob == "/":
        raise ValueError(f"Blob name {blob} is empty or just a slash.")

    if not container or container == "/":
        raise ValueError(f"Container name {container} is empty or just a slash.")

    return container, blob
