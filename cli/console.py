import typer
from azure.core.exceptions import ClientAuthenticationError
from azure.identity import CredentialUnavailableError
from rich import print
from rich.console import Console

from utils.azure.auth import obtain_sp_credential
from utils.azure.storage import get_unique_jobs_from_blobs, instantiate_blob_client
from utils.epinow2.constants import azure_storage

app = typer.Typer()
console = Console()


@app.command("check-login")
def check_login():
    with console.status("Checking Azure login...\n", spinner="aesthetic"):
        sp_credential = obtain_sp_credential()
        try:
            sp_credential.get_token(azure_storage["scope_url"])
        except (CredentialUnavailableError, ClientAuthenticationError) as e:
            console.print(
                "[italic red] You are not logged into Azure. Run `az login` to log in."
            )
            raise e

        console.print("[italic green] You are logged into Azure. :sparkles:")


@app.command("list-jobs")
def list_jobs():
    container_name = azure_storage["azure_container_name"]
    with console.status(
        f"Fetching jobs in {container_name} container...\n", spinner="aesthetic"
    ):
        sp_credential = obtain_sp_credential()
        try:
            blob_client = instantiate_blob_client(
                sp_credential=sp_credential,
                account_url=azure_storage["azure_storage_account_url"],
            )
            container_client = blob_client.get_container_client(container_name)
            blob_list = container_client.list_blobs()
            unique_jobs = get_unique_jobs_from_blobs(blob_list)
            console.print(unique_jobs)
        except ValueError as e:
            console.print(
                "[italic red] Error instantiating blob client. Check your credentials."
            )
            raise e


@app.command("list-tasks")
def list_tasks(job_id: str):
    print(f"Tasks for {job_id}: ")


@app.command("inspect-task")
def inspect_task(job_id: str, task_id: str):
    print(f"Inspecting task {task_id} for job {job_id}: ")


def main():
    app()
