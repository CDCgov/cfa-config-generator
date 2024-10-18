from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


def instantiate_blob_client(
    sp_credential: DefaultAzureCredential | None = None, account_url: str | None = None
) -> BlobServiceClient:
    """Function to instantiate blob storage client to interact
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

    # Instantiate BlobStorageClient
    blob_service_client = BlobServiceClient(account_url, credential=sp_credential)

    return blob_service_client
