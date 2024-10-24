from azure.identity import AzureCliCredential


def obtain_sp_credential() -> AzureCliCredential:
    """Obtains service principal credentials from Azure.
    Returns:
        Instance of AzureCliCredential.
    Raises:
        LookupError if credential not found.
    """

    # The AzureCliCredential reads from the environment directly
    # if running locally. Check that SP credentials
    # are in environment if running locally.
    # Deployed versions use Managed Identity.
    sp_credential = AzureCliCredential()

    return sp_credential
