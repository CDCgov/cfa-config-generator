from azure.identity import DefaultAzureCredential


def obtain_sp_credential() -> DefaultAzureCredential:
    """Obtains service principal credentials from Azure.
    Returns:
        Instance of DefaultAzureCredential.
    Raises:
        LookupError if credential not found.
    """

    # The DefaultAzureCredential reads from the environment directly
    # see the docs for credential ordering
    # https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python
    credential = DefaultAzureCredential()

    return credential
