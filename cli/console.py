import json

import typer
from azure.core.exceptions import ClientAuthenticationError, ResourceNotFoundError
from azure.identity import CredentialUnavailableError
from rich.console import Console
from typing_extensions import Annotated

from utils.azure.auth import obtain_sp_credential
from utils.azure.storage import (
    get_tasks_for_job_id,
    get_unique_jobs_from_blobs,
    instantiate_blob_client,
)
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
                "[italic red] :triangular_flag: You are not logged into Azure. Run `az login` to log in."
            )
            raise e

        console.print("[italic green] You are logged into Azure. :sparkles:")


@app.command("list-jobs")
def list_jobs():
    container_name = azure_storage["azure_container_name"]
    with console.status(
        f"Fetching jobs in {container_name} container...\n",
        spinner="aesthetic",
    ):
        sp_credential = obtain_sp_credential()
        try:
            blob_client = instantiate_blob_client(
                sp_credential=sp_credential,
                account_url=azure_storage["azure_storage_account_url"],
            )
            container_client = blob_client.get_container_client(container_name)
            blob_list = container_client.list_blobs()
            unique_jobs = get_unique_jobs_from_blobs(blob_list=blob_list)
            console.print(unique_jobs)
        except (ValueError, ResourceNotFoundError, ClientAuthenticationError) as e:
            console.print(
                "[italic red] :triangular_flag: Error instantiating blob client or finding specified resource."
            )
            raise e


@app.command("list-tasks")
def list_tasks(
    job_id: Annotated[
        str, typer.Option("--job-id", "-j", help="Job ID to list tasks for")
    ],
):
    container_name = azure_storage["azure_container_name"]
    with console.status(
        f"Fetching tasks in {container_name} container for job_id {job_id}...\n",
        spinner="aesthetic",
    ):
        sp_credential = obtain_sp_credential()
        try:
            blob_client = instantiate_blob_client(
                sp_credential=sp_credential,
                account_url=azure_storage["azure_storage_account_url"],
            )
            container_client = blob_client.get_container_client(container_name)
            blob_list = container_client.list_blobs()
            tasks_for_job = get_tasks_for_job_id(blob_list=blob_list, job_id=job_id)
            console.print(tasks_for_job)
        except (ResourceNotFoundError, ValueError, ClientAuthenticationError) as e:
            console.print(
                "[italic red] :triangular_flag: Error instantiating blob client or finding specified resource."
            )
            raise e


@app.command("inspect-task")
def inspect_task(
    job_id: Annotated[
        str, typer.Option("--job-id", "-j", help="Job ID to list tasks for")
    ],
    task_filename: Annotated[
        str, typer.Option("--task-filename", "-t", help="Task filename to inspect")
    ],
):
    with console.status(
        f"Inspecting task in {task_filename} container for job_id {job_id}...\n",
        spinner="aesthetic",
    ):
        sp_credential = obtain_sp_credential()
        full_blob_path = f"{job_id}/{task_filename}"
        try:
            blob_client = instantiate_blob_client(
                sp_credential=sp_credential,
                account_url=azure_storage["azure_storage_account_url"],
            )
            blob_client = blob_client.get_blob_client(
                container=azure_storage["azure_container_name"], blob=full_blob_path
            )
            downloader = blob_client.download_blob(max_concurrency=1, encoding="utf-8")
            blob_text = downloader.readall()
            console.print(json.loads(blob_text))
        except (ResourceNotFoundError, ValueError, ClientAuthenticationError) as e:
            console.print(
                "[italic red] :triangular_flag: Error instantiating blob client or finding specified resource."
            )
            raise e


def main():
    app()
