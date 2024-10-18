from azure.identity import DefaultAzureCredential


def obtain_sp_credential() -> DefaultAzureCredential:
    """Obtains service principal credentials from Azure.
    Returns:
        Instance of DefaultAzureCredential.
    Raises:
        LookupError if credential not found.
    """

    # The DefaultAzureCredential reads from the environment directly
    # if running locally. Check that SP credentials
    # are in environment if running locally.
    # Deployed versions use Managed Identity.
    sp_credential = DefaultAzureCredential()

    return sp_credential
