import json

import typer
from azure.core.exceptions import ClientAuthenticationError, ResourceNotFoundError
from azure.identity import CredentialUnavailableError
from rich.console import Console
from typing_extensions import Annotated

from utils.azure.auth import obtain_sp_credential
from utils.azure.storage import (
    download_blob,
    get_tasks_for_job_id,
    get_unique_jobs_from_blobs,
    instantiate_blob_service_client,
)
from utils.cli.functions import update_config
from utils.epinow2.constants import azure_storage, modifiable_params
from utils.epinow2.functions import generate_timestamp, update_task_id

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

        console.print("[italic green] :sparkles: You are logged into Azure.")


@app.command("list-jobs")
def list_jobs(
    num_jobs: Annotated[
        int,
        typer.Option(
            "--num-jobs", "-n", help="Number of jobs to display (default 10)."
        ),
    ] = 10,
):
    container_name = azure_storage["azure_container_name"]
    with console.status(
        f"Fetching jobs in {container_name} container...\n",
        spinner="aesthetic",
    ):
        sp_credential = obtain_sp_credential()
        try:
            blob_service_client = instantiate_blob_service_client(
                sp_credential=sp_credential,
                account_url=azure_storage["azure_storage_account_url"],
            )
            container_client = blob_service_client.get_container_client(container_name)
            blob_list = container_client.list_blobs()
            unique_jobs = get_unique_jobs_from_blobs(blob_list=blob_list)
            console.print(unique_jobs[0:num_jobs])
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
    num_tasks: Annotated[
        int,
        typer.Option(
            "--num-tasks", "-n", help="Number of tasks to display (default 10)."
        ),
    ] = 10,
    state: Annotated[
        str | None,
        typer.Option("--state", "-s", help="State to filter tasks by (default all)."),
    ] = None,
    disease: Annotated[
        str | None,
        typer.Option(
            "--disease",
            "-d",
            help="Disease to filter tasks by (default COVID-19, Influenza).",
        ),
    ] = None,
):
    container_name = azure_storage["azure_container_name"]
    with console.status(
        f"Fetching tasks in {container_name} container for job_id {job_id}...\n",
        spinner="aesthetic",
    ):
        sp_credential = obtain_sp_credential()
        try:
            blob_service_client = instantiate_blob_service_client(
                sp_credential=sp_credential,
                account_url=azure_storage["azure_storage_account_url"],
            )
            container_client = blob_service_client.get_container_client(container_name)
            blob_list = container_client.list_blobs()
            tasks_for_job = get_tasks_for_job_id(
                blob_list=blob_list, job_id=job_id, state=state, disease=disease
            )
            console.print(tasks_for_job[0:num_tasks])
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
            blob_data = download_blob(
                blob_path=full_blob_path, sp_credential=sp_credential
            )
            console.print(blob_data)

        except (ResourceNotFoundError, ValueError, ClientAuthenticationError) as e:
            console.print(
                "[italic red] :triangular_flag: Error instantiating blob client or finding specified resource."
            )
            raise e


@app.command("modify-task")
def modify_task(
    job_id: Annotated[
        str, typer.Option("--job-id", "-j", help="Job ID to list tasks for")
    ],
    task_filename: Annotated[
        str, typer.Option("--task-filename", "-t", help="Task filename to inspect")
    ],
):
    sp_credential = obtain_sp_credential()
    full_blob_path = f"{job_id}/{task_filename}"
    try:
        task_config = download_blob(
            blob_path=full_blob_path, sp_credential=sp_credential
        )
        console.print("[bold] :pencil: Modifying task with contents:\n")
        console.print(task_config)
        updated_config = update_config(task_config, modifiable_params, console)
        # Add back metadata
        full_config = dict(task_config, **updated_config)

        # Only write to Azure if updates were made
        if full_config != task_config:
            console.print(
                "[bold] :white_heavy_check_mark: Updated task with contents:\n"
            )
            console.print(updated_config)
            with console.status(
                ":cloud: Task configuration updated successfully, pushing to Azure...\n",
                spinner="aesthetic",
            ):
                # Update timestamps and push to Azure
                timestamp = generate_timestamp()
                full_config["as_of_date"] = timestamp
                updated_task_id = update_task_id(full_config["task_id"], timestamp)
                task_path = f"{job_id}/{updated_task_id}.json"
                storage_client = instantiate_blob_service_client(
                    sp_credential=sp_credential,
                    account_url=azure_storage["azure_storage_account_url"],
                )
                container_client = storage_client.get_container_client(
                    container=azure_storage["azure_container_name"]
                )
                container_client.upload_blob(
                    name=task_path, data=json.dumps(full_config), overwrite=True
                )
                console.print(
                    f"[italic green] :sparkles: Task successfully pushed to Azure at {task_path}."
                )
        else:
            console.print("No changes made to task configuration. Exiting.\n")

    except (ResourceNotFoundError, ValueError, ClientAuthenticationError) as e:
        console.print(
            "[italic red] :triangular_flag: Error instantiating blob client or finding specified resource."
        )
        raise e


def main():
    app()
