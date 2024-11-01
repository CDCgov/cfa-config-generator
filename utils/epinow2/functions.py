import os
from datetime import date, datetime, timedelta, timezone
from uuid import UUID, uuid1

from utils.epinow2.constants import (
    all_diseases,
    all_states,
    nssp_states_omit,
    shared_params,
)


def extract_user_args() -> dict:
    """Extracts user-provided arguments from environment variables or uses default if none provided."""
    state = os.environ.get("state") or "all"
    disease = os.environ.get("disease") or "all"
    report_date = os.environ.get("report_date") or date.today()
    production_date = os.environ.get("production_date") or date.today()
    min_reference_date, max_reference_date = get_reference_date_range(report_date)
    reference_dates = os.environ.get("reference_dates") or [
        min_reference_date,
        max_reference_date,
    ]

    data_source = os.environ.get("data_source") or "nssp"
    data_path = os.environ.get("data_path") or "gold/"
    data_container = os.environ.get("data_container") or None
    return {
        "state": state,
        "disease": disease,
        "report_date": report_date,
        "reference_dates": reference_dates,
        "data_source": data_source,
        "data_path": data_path,
        "data_container": data_container,
        "production_date": production_date,
    }


def generate_timestamp() -> int:
    """Generates a timestamp of the current time using UTC timezone."""
    return int(datetime.timestamp(datetime.now(timezone.utc)))


def get_reference_date_range(report_date: date) -> tuple[date, date]:
    """Returns a tuple of the minimum and maximum reference dates
    based on the report date, in the case that no reference_dates
    are provided.
    Parameters:
        report_date: date of model run
    """
    # Convert user-provided report_date to date object
    if isinstance(report_date, str):
        report_date = date.fromisoformat(report_date)

    max_reference_date = report_date - timedelta(days=1)
    min_reference_date = report_date - timedelta(weeks=8)

    return min_reference_date, max_reference_date


def generate_uuid() -> UUID:
    """Generates a UUID1 object to associate
    with job IDs. UUID1 can be sorted by
    timestamp by default, which is desirable
    when managing multiple configurations."""
    return uuid1()


def generate_job_name(job_id: UUID | None = None, as_of_date: int | None = None) -> str:
    """Generate a human-readable slug based on job UUID and as_of_date.
    Parameters:
        job_id: UUID for job
        as_of_date: timestamp of model run
    """
    job_name = f"Rt-estimation-{job_id.hex}-{datetime.fromtimestamp(as_of_date).isoformat()}".replace(
        ":", "-"
    )
    return job_name


def validate_args(
    state: str | None = None,
    disease: str | None = None,
    report_date: date | None = None,
    reference_dates: list[date] | None = None,
    data_source: str | None = None,
    data_path: str | None = None,
    data_container: str | None = None,
    production_date: date | None = None,
) -> dict:
    """Checks that user-supplied arguments are valid and returns them
    in a standardized format for downstream use.
    Parameters:
        state: geography to run model
        disease: disease to run
        report_date: date of model run
        reference_dates: array of reference (event) dates
        data_source: source of input data
        data_container: container for input data
        data_path: path to input data
        production_date: production date of model run
    Returns:
        A dictionary of sanitized arguments.
    """
    args_dict = {}
    if state == "all":
        if data_source == "nssp":
            args_dict["state"] = list(set(all_states) - set(nssp_states_omit))
        elif data_source == "nhsn":
            args_dict["state"] = all_states
        else:
            raise ValueError(
                f"Data source {data_source} not recognized. Valid options are 'nssp' or 'nhsn'."
            )
    elif state not in all_states:
        raise ValueError(f"State {state} not recognized.")
    else:
        args_dict["state"] = [state]

    if disease == "all":
        args_dict["disease"] = all_diseases
    elif disease not in all_diseases:
        raise ValueError(
            f"Disease {disease} not recognized. Valid options are 'COVID-19' or 'Influenza'."
        )
    else:
        args_dict["disease"] = [disease]

    # Standardize reference_dates
    if isinstance(reference_dates, str):
        try:
            min_ref, max_ref = reference_dates.split(",")
            reference_dates = [
                date.fromisoformat(min_ref),
                date.fromisoformat(max_ref),
            ]
        except ValueError:
            raise ValueError(
                "Invalid reference_dates. Ensure they are in the format 'YYYY-MM-DD,YYYY-MM-DD'."
            )

    # Standardize report_date and production_date
    if isinstance(report_date, str):
        report_date = date.fromisoformat(report_date)

    if isinstance(production_date, str):
        production_date = date.fromisoformat(production_date)

    # Check valid reference_date
    if not all(ref <= report_date for ref in reference_dates):
        raise ValueError(
            "Invalid reference_date. Ensure all reference_dates are before the report date."
        )
    args_dict["reference_dates"] = reference_dates
    args_dict["report_date"] = report_date
    args_dict["data_path"] = data_path
    args_dict["data_container"] = data_container
    args_dict["production_date"] = production_date
    return args_dict


def generate_task_id(
    job_id: UUID | None = None,
    state: str | None = None,
    disease: str | None = None,
) -> str:
    """Generates a task_id which consists of the hex code of the job_id
    and information on the state and pathogen. Also timestamps the task
    in case they are updated at different times.
    Parameters:
        job_id: UUID of job
        state: state being run
        disease: disease being run
    """
    timestamp = generate_timestamp()
    return f"{job_id.hex}_{state}_{disease}_{timestamp}"


def update_task_id(
    task_id: str | None = None,
    timestamp: int | None = None,
) -> str:
    """Updates a task_id with new timestamp.
    Parameters:
        task_id: task_id to update
        timestamp: updated timestamp
    """
    try:
        # Task id format: <job_id>_<state>_<disease>_<timestamp>
        # eg bf57b6e297ad11efb20c00155d63cada_WY_Influenza_1730395796
        job_id, state, disease, _ = task_id.split("_")
        return f"{job_id}_{state}_{disease}_{timestamp}"
    except ValueError:
        raise ValueError(
            "Task ID does not match expected format. Check that task IDs are formatted as <job_id>_<state>_<disease>_<timestamp>."
        )


def generate_task_configs(
    state: list | None = None,
    disease: list | None = None,
    report_date: date | None = None,
    reference_dates: list[date] | None = None,
    data_container: str | None = None,
    data_path: str | None = None,
    as_of_date: int | None = None,
    job_id: UUID | None = None,
    production_date: date | None = None,
) -> tuple[list[dict], str]:
    """
    Generates a list of configuration objects based on
    supplied parameters.
    Parameters:
        state: geography to run model
        disease: pathogen to run
        report_date: date of model run
        reference_dates: array of reference (event) dates
        as_of_date: timestamp of model run
        job_id: UUID for job
        data_container: container for input data
        data_path: path to input data
        production_date: production date of model run
    Returns:
        A list of configuration objects.
    """
    configs = []
    # Create tasks for each state-pathogen combination
    job_name = generate_job_name(job_id=job_id, as_of_date=as_of_date)
    for s in state:
        for d in disease:
            task_config = {
                "job_id": job_name,
                "task_id": generate_task_id(job_id=job_id, state=s, disease=d),
                "as_of_date": as_of_date,
                "disease": d,
                "geo_value": [s],
                "geo_type": "state" if s != "US" else "country",
                "parameters": shared_params["parameters"],
                "data": {
                    "path": data_path,
                    "blob_storage_container": data_container,
                    "report_date": [report_date.isoformat()],
                    "reference_date": [x.isoformat() for x in reference_dates],
                    "production_date": [production_date.isoformat()],
                },
                "seed": shared_params["seed"],
                "horizon": shared_params["horizon"],
                "priors": shared_params["priors"],
                "sampler_opts": shared_params["sampler_opts"],
            }
            configs.append(task_config)
    return configs, job_name
