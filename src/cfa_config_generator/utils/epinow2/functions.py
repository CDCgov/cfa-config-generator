import os
import polars as pl
from datetime import date, datetime, timedelta, timezone
from uuid import UUID, uuid1

from cfa_config_generator.utils.epinow2.constants import (
    all_diseases,
    all_states,
    nssp_states_omit,
    shared_params,
)


def extract_user_args(as_of_date: str) -> dict:
    """Extracts user-provided arguments from environment variables or uses default if none provided.
    Parameters:
        as_of_date: iso format timestamp of model run
    """
    task_exclusions = os.getenv("task_exclusions") or None
    exclusions = os.getenv("exclusions") or None
    state = os.getenv("state") or "all"
    disease = os.getenv("disease") or "all"
    report_date = os.getenv("report_date") or date.today()
    production_date = os.getenv("production_date") or date.today()
    min_reference_date, max_reference_date = get_reference_date_range(report_date)
    reference_dates = os.getenv("reference_dates") or [
        min_reference_date,
        max_reference_date,
    ]

    data_source = os.getenv("data_source") or "nssp"
    data_path = os.getenv("data_path") or f"gold/{report_date}.parquet"
    data_container = os.getenv("data_container") or "nssp-etl"
    job_id = os.getenv("job_id") or generate_default_job_id(as_of_date=as_of_date)
    return {
        "task_exclusions": task_exclusions,
        "exclusions": exclusions,
        "state": state,
        "disease": disease,
        "report_date": report_date,
        "reference_dates": reference_dates,
        "data_source": data_source,
        "data_path": data_path,
        "data_container": data_container,
        "production_date": production_date,
        "job_id": job_id,
        "as_of_date": as_of_date,
    }


def generate_timestamp() -> str:
    """Generates an iso format string of the current time using UTC timezone."""
    return datetime.now(timezone.utc).isoformat()


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


def generate_default_job_id(as_of_date: str | None = None) -> str:
    """Generate a human-readable slug based on job UUID and as_of_date,
    if no job_id is supplied by user.
    Parameters:
        as_of_date: iso format timestamp of model run
    """
    job_uuid = generate_uuid()
    job_id = f"Rt-estimation-{as_of_date}-{job_uuid.hex}".replace(":", "-")
    return job_id

def validate_data_exclusions_path(
    task_exclusions: str | None = None,
    exclusions: str | None = None,
    state: str | None = None,
    disease: str | None = None,
    report_date: date | None = None,
    reference_dates: list[date] | None = None,
    data_source: str | None = None,
    data_path: str | None = None,
    data_container: str | None = None,
    production_date: date | None = None,
    job_id: str | None = None,
    as_of_date: str | None = None,
) -> str:
    """ 
    Confirms that file exists at the path listed, within the given data container,
    and with the required variables state, disease, reference_date, report_date
    """
    class CustomError(Exception):
        pass

    excl_df = pl.read_csv(exclusions)

    df_cols = ["state", "disease", "report_date", "reference_date"]
    excl_cols = set(excl_df.columns)
    missing_cols = list(set(df_cols).difference(excl_cols))
    if len(missing_cols) > 0:
        raise CustomError(f"data exclusions file missing: {missing_cols}")

    args_dict = {}
    args_dict["state"] = excl_df.get_column("state").to_list()
    args_dict["disease"] = excl_df.get_column("disease").to_list()
    # args_dict["report_date"] = excl_df.get_column("report_date").to_list()
    # args_dict["reference_date"] = excl_df.get_column("reference_date").to_list()
    args_dict["exclusions"] = exclusions

    return args_dict

def generate_tasks_excl_from_data_excl(
    task_exclusions: str | None = None,
    exclusions: str | None = None,
    state: str | None = None,
    disease: str | None = None,
    report_date: date | None = None,
    reference_dates: list[date] | None = None,
    data_source: str | None = None,
    data_path: str | None = None,
    data_container: str | None = None,
    production_date: date | None = None,
    job_id: str | None = None,
    as_of_date: str | None = None,
) -> str:
    """ 
    Confirms that file exists at the path listed, within the given data container,
    and with the required variables state, disease, reference_date, report_date.
    Next, creates an output string in the task exclusion form in the state:disease 
    pair form with all of the states and disease to not have tasks created for
    """
    class CustomError(Exception):
        pass

    excl_df = pl.read_csv(exclusions)

    df_cols = ["state", "disease", "report_date", "reference_date"]
    excl_cols = set(excl_df.columns)
    missing_cols = list(set(df_cols).difference(excl_cols))
    if len(missing_cols) > 0:
        raise CustomError(f"data exclusions file missing: {missing_cols}")

    all_set = []
    for state in all_states:
        for disease in all_diseases:
            dict_inst = [state, disease]
            all_set.append(dict_inst)

    incl_states = excl_df.get_column("state").to_list()
    incl_disease = excl_df.get_column("disease").to_list()
    incl_set = [list(x) for x in zip(incl_states, incl_disease)]

    excl_set = [x for x in all_set if x not in incl_set]

    element_delimiter=":"
    sublist_delimiter=","
    int_list = []
    for i in excl_set:
        int_list.append(element_delimiter.join(map(str, i)))
    out_str = sublist_delimiter.join(int_list)

    return out_str

def validate_args(
    task_exclusions: str | None = None,
    exclusions: str | None = None,
    state: str | None = None,
    disease: str | None = None,
    report_date: date | None = None,
    reference_dates: list[date] | None = None,
    data_source: str | None = None,
    data_path: str | None = None,
    data_container: str | None = None,
    production_date: date | None = None,
    job_id: str | None = None,
    as_of_date: str | None = None,
) -> dict:
    """Checks that user-supplied arguments are valid and returns them
    in a standardized format for downstream use.
    Parameters:
        task_exclusions: state:disease pair to exclude from model run
        exclusions: path to exclusions csv
        state: geography to run model
        disease: disease to run
        report_date: date of model run
        reference_dates: array of reference (event) dates
        data_source: source of input data
        data_container: container for input data
        data_path: path to input data
        production_date: production date of model run
        job_id: unique identifier for job
        as_of_date: iso format timestamp of model run
    Returns:
        A dictionary of sanitized arguments.
    """
    args_dict = {}
    # Split up input string of task_exclusions and validate
    if task_exclusions is not None:
        try:
            task_pairs = task_exclusions.split(",")
            state_excl = [item.split(":")[0] for item in task_pairs]
            disease_excl = [item.split(":")[1] for item in task_pairs]
            for ind_state in state_excl:
                if ind_state not in all_states:
                    raise ValueError(f"State {ind_state} not recognized.")
            for ind_disease in disease_excl:
                if ind_disease not in all_diseases:
                    raise ValueError(
                        f"Disease {ind_disease} not recognized. Valid options are 'COVID-19' or 'Influenza'"
                    )
            args_dict["task_exclusions"] = {
                "geo_value": state_excl,
                "disease": disease_excl,
            }
        except IndexError:
            raise (
                "Task exclusions should be in the form 'state:disease,state:disease'"
            )        
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
    args_dict["job_id"] = job_id
    args_dict["as_of_date"] = as_of_date
    args_dict["exclusions"] = exclusions
    return args_dict


def generate_task_id(
    state: str | None = None,
    disease: str | None = None,
) -> str:
    """Generates a task_id which consists of the hex code of the job_id
    and information on the state and pathogen. Also timestamps the task
    in case they are updated at different times.
    Parameters:
        state: state being run
        disease: disease being run
    """
    timestamp = generate_timestamp()
    return f"{state}_{disease}_{timestamp}"


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
        # Task id format: <state>_<disease>_<timestamp>
        # eg WY_Influenza_1730395796
        state, disease, _ = task_id.split("_")
        return f"{state}_{disease}_{timestamp}"
    except ValueError:
        raise ValueError(
            "Task ID does not match expected format. Check that task IDs are formatted as <job_id>_<state>_<disease>_<timestamp>."
        )


def generate_task_configs(
    task_exclusions: list | None = None,
    exclusions: str | None = None,
    state: list | None = None,
    disease: list | None = None,
    report_date: date | None = None,
    reference_dates: list[date] | None = None,
    data_container: str | None = None,
    data_path: str | None = None,
    as_of_date: str | None = None,
    production_date: date | None = None,
    job_id: str | None = None,
) -> tuple[list[dict], str]:
    """
    Generates a list of configuration objects based on
    supplied parameters.
    Parameters:
        task_exclusions: state:disease to exclude
        exclusions: a path to exclusions csv
        state: geography to run model
        disease: pathogen to run
        report_date: date of model run
        reference_dates: array of reference (event) dates
        as_of_date: iso format datetime string of model run
        data_container: container for input data
        data_path: path to input data
        production_date: production date of model run
        job_id: unique identifier for job
    Returns:
        A list of configuration objects.
    """
    configs = []
    # Create tasks for each state-pathogen combination
    for s in state:
        for d in disease:
            task_config = {
                "job_id": job_id,
                "task_id": generate_task_id(state=s, disease=d),
                "exclusions": exclusions,
                "min_reference_date": min(reference_dates).isoformat(),
                "max_reference_date": max(reference_dates).isoformat(),
                "disease": d,
                "geo_value": s,
                "geo_type": "state" if s != "US" else "country",
                "report_date": report_date.isoformat(),
                "production_date": production_date.isoformat(),
                "parameters": {
                    "as_of_date": as_of_date,
                    "generation_interval": {
                        "path": "prod.parquet",
                        "blob_storage_container": "prod-param-estimates",
                    },
                    "delay_interval": {
                        "path": "prod.parquet",
                        "blob_storage_container": "prod-param-estimates",
                    },
                    "right_truncation": {
                        "path": "prod.parquet",
                        "blob_storage_container": "prod-param-estimates",
                    },
                },
                "data": {
                    "path": data_path,
                    "blob_storage_container": data_container,
                },
                **shared_params,
            }
            configs.append(task_config)

    if task_exclusions is not None:
        configs = exclude_data(configs, task_exclusions)

    return configs, job_id


def exclude_data(config_data, filters):
    """
    Excludes a list of dictionaries based on multiple key-value pairs.

    Args:
        data: A list of dictionaries.
        filters: A dictionary where keys are the keys to filter on,
                 and values are the values to match. This dictionary
                 should hold a "geo_value" and "disease"; ex.
                 {"geo_value": "NY", "disease": "COVID-19"}

    Returns:
        A new list containing the dictionaries that do not match all filter criteria.
    """
    excl_list = []
    for i in zip(filters["geo_value"], filters["disease"]):
        excl_list.append(i)

    filter_set = set(excl_list)

    filtered_data = [
        entry
        for entry in config_data
        if (entry.get("geo_value"), entry.get("disease")) not in filter_set
    ]

    return filtered_data
