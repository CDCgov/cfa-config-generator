from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


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


def get_tasks_for_job_id(blob_list: list | None = None, job_id: str = "") -> list:
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
        blob_job_id, blob_task_id = blob.name.split("/")
        if blob_job_id == job_id:
            tasks_for_job.append(blob_task_id)
    return sorted(tasks_for_job)
