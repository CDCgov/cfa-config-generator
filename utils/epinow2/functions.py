from datetime import date
from uuid import UUID, uuid1

from utils.epinow2.constants import (
    nssp_states_omit,
    pathogens,
    shared_params,
    states,
)


def generate_job_id() -> UUID:
    """Generates a UUID1 object to associate
    with job IDs. UUID1 can be sorted by
    timestamp by default, which is desirable
    when managing multiple configurations."""
    return uuid1()


def validate_args(
    state: str | None = None,
    pathogen: str | None = None,
    report_date: date | None = None,
    reference_dates: list[date] | None = None,
    data_source: str | None = None,
) -> dict:
    """Checks that user-supplied arguments are valid and returns them
    in a standardized format for downstream use.
    Parameters:
        state: geography to run model
        pathogen: pathogen to run
        report_date: date of model run
        reference_dates: array of reference (event) dates
        data_source: source of input data
    Returns:
        A dictionary of sanitized arguments.
    """
    args_dict = {}
    if state == "all":
        if data_source == "nssp":
            args_dict["state"] = list(set(states) - set(nssp_states_omit))
        elif data_source == "nhsn":
            args_dict["state"] = [states]
        else:
            raise ValueError(f"Data source {data_source} not recognized.")
    elif state not in states:
        raise ValueError(f"State {state} not recognized.")
    else:
        args_dict["state"] = [state]

    if pathogen == "all":
        args_dict["pathogen"] = pathogens
    elif pathogen not in pathogens:
        raise ValueError(f"Pathogen {pathogen} not recognized.")
    else:
        args_dict["pathogen"] = [pathogen]

    # Standardize reference_dates
    reference_dates = [
        date.fromisoformat(x) if isinstance(x, str) else x
        for x in reference_dates
    ]

    # Check valid reference_date
    if not all(ref <= report_date for ref in reference_dates):
        raise ValueError(
            "Invalid reference_date. Ensure all reference_dates are before the report date."
        )
    args_dict["reference_dates"] = reference_dates
    args_dict["report_date"] = report_date
    return args_dict


def generate_task_id(
    job_id: UUID | None = None,
    state: str | None = None,
    pathogen: str | None = None,
) -> str:
    """Generates a task_id which consists of the hex code of the job_id
    and information on the state and pathogen.
    Parameters:
        job_id: UUID of job
        state: state being run
        pathogen: pathogen being run
    """
    return f"{job_id.hex}_{state}_{pathogen}"


def generate_task_configs(
    state: list | None = None,
    pathogen: list | None = None,
    report_date: date | None = None,
    reference_dates: list[date] | None = None,
    as_of_date: int | None = None,
    job_id: UUID | None = None,
) -> list[dict]:
    """
    Generates a list of configuration objects based on
    supplied parameters.
    Parameters:
        state: geography to run model
        pathogen: pathogen to run
        report_date: date of model run
        reference_dates: array of reference (event) dates
        as_of_date: timestamp of model run
        job_id: UUID for job
    Returns:
        A list of configuration objects.
    """
    configs = []
    # Create tasks for each state-pathogen combination
    for s in state:
        for p in pathogen:
            task_config = {
                "job_id": str(job_id),
                "task_id": generate_task_id(
                    job_id=job_id, state=s, pathogen=p
                ),
                "as_of_date": as_of_date,
                "disease": p,
                "geo_value": [s],
                "geo_type": "state",
                "parameters": shared_params["parameters"],
                "data": {
                    "path": "gold/",
                    "blob_storage_container": None,
                    "report_date": [report_date],
                    "reference_date": reference_dates,
                },
                "seed": shared_params["seed"],
                "horizon": shared_params["horizon"],
                "priors": shared_params["priors"],
                "sampler_opts": shared_params["sampler_opts"],
            }
            configs.append(task_config)
    return configs
