import itertools
import os
from collections.abc import Iterable
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid1

import polars as pl

from cfa_config_generator.utils.epinow2.constants import (
    all_diseases,
    nssp_valid_states,
    shared_params,
)


def extract_user_args(as_of_date: str) -> dict[str, Any]:
    """Extracts user-provided arguments from environment variables or uses default if none provided.
    Parameters:
        as_of_date: iso format timestamp of model run
    """
    task_exclusions = os.getenv("task_exclusions") or None
    exclusions = os.getenv("exclusions") or None
    state = os.getenv("state") or "all"
    disease = os.getenv("disease") or "all"

    # Handle report_date
    report_date_str = os.getenv("report_date")
    try:
        report_date = (
            date.fromisoformat(report_date_str) if report_date_str else date.today()
        )
    except ValueError:
        raise ValueError(
            f"Invalid report_date format: {report_date_str}. Use YYYY-MM-DD."
        )

    # Handle production_date
    production_date_str = os.getenv("production_date")
    try:
        production_date = (
            date.fromisoformat(production_date_str)
            if production_date_str
            else date.today()
        )
    except ValueError:
        raise ValueError(
            f"Invalid production_date format: {production_date_str}. Use YYYY-MM-DD."
        )

    min_reference_date, max_reference_date = get_reference_date_range(report_date)
    reference_dates_str = os.getenv("reference_dates")
    if reference_dates_str:
        try:
            min_ref_str, max_ref_str = reference_dates_str.split(",")
            reference_dates = [
                date.fromisoformat(min_ref_str.strip()),
                date.fromisoformat(max_ref_str.strip()),
            ]
        except ValueError:
            raise ValueError(
                f"Invalid reference_dates format: {reference_dates_str}. Use 'YYYY-MM-DD, YYYY-MM-DD'."
            )
    else:
        reference_dates = [min_reference_date, max_reference_date]

    data_path = f"gold/{report_date.isoformat()}.parquet"
    data_container = os.getenv("data_container") or "nssp-etl"
    output_container = os.getenv("output_container") or "nssp-rt-testing"
    job_id = os.getenv("job_id") or generate_default_job_id(as_of_date=as_of_date)
    return {
        "task_exclusions": task_exclusions,
        "exclusions": exclusions,
        "state": state,
        "disease": disease,
        "report_date": report_date,
        "reference_dates": reference_dates,
        "data_path": data_path,
        "data_container": data_container,
        "production_date": production_date,
        "job_id": job_id,
        "as_of_date": as_of_date,
        "output_container": output_container,
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


def generate_tasks_excl_from_data_excl(excl_df: pl.DataFrame) -> str:
    """
    Confirms that file exists at the path listed, within the given data container,
    and with the required variables state, disease, reference_date, report_date.
    Next, creates an output string in the task exclusion form in the state:disease
    pair form with all of the states and disease to not have tasks created for.

    Parameters
    -----------
        excl_df: a dataframe of the exclusions

    Returns
    -----------
        A string of the form state:disease,state:disease
        for all of the state:disease pairs that should
        be excluded from the task generation.

    Raises
    -----------
        ValueError: If the exclusions file is missing required columns.

    Notes
    -----------
    Assumes that `excl_df` has schema
    [
        (state, pl.String),
        (disease, pl.String),
        (report_date, pl.Date),
        (reference_date, pl.Date)
    ]
    """
    want_cols: set[str] = {"state", "disease", "report_date", "reference_date"}
    got_cols: set[str] = set(excl_df.columns)
    missing_cols: set[str] = want_cols.difference(got_cols)
    if any(missing_cols):
        raise ValueError(f"data exclusions file missing: {missing_cols}")

    all_set: set[tuple[str, str]] = set(
        itertools.product(nssp_valid_states, all_diseases)
    )

    incl_states = excl_df.get_column("state").to_list()
    incl_disease = excl_df.get_column("disease").to_list()
    incl_set: set[tuple[str, str]] = set(zip(incl_states, incl_disease))

    excl_set: set[tuple[str, str]] = all_set.difference(incl_set)

    # Create a list of strings in the form
    # ["state:disease", "state:disease", "state:disease"]
    intermediate_list: list[str] = [
        state + ":" + disease for state, disease in excl_set
    ]
    return ",".join(intermediate_list)


def validate_args(
    state: str,
    disease: str,
    report_date: date,
    reference_dates: tuple[date, date],
    data_path: str,
    data_container: str,
    production_date: date,
    job_id: str,
    as_of_date: str,
    output_container: str,
    task_exclusions: str | None = None,
    exclusions: dict | None = None,
) -> dict:
    """Checks that user-supplied arguments are valid and returns them
    in a standardized format for downstream use.
    Parameters:
        state: geography to run model
        disease: disease to run
        report_date: date of model run
        reference_dates: Length two tuple of the minimum and maximum reference (event) dates
        data_container: container for input data
        data_path: path to input data
        production_date: production date of model run
        job_id: unique identifier for job
        as_of_date: iso format timestamp specifying the timestamp as of which to fetch
            the parameters for. This should usually be the same as the report date.
        output_container: Azure container to store output
        task_exclusions: comma separated state:disease pairs to exclude
        exclusions: A dictionary with `path` and `blob_storage_container` keys
        to specify the path to the exclusions file and its container.
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
                if ind_state not in nssp_valid_states:
                    raise ValueError(f"State {ind_state} not recognized.")
            for ind_disease in disease_excl:
                if ind_disease not in all_diseases:
                    raise ValueError(
                        f"Disease {ind_disease} not recognized. Valid options are 'COVID-19', 'Influenza', or 'RSV'."
                    )
            args_dict["task_exclusions"] = {
                "geo_value": state_excl,
                "disease": disease_excl,
            }
        except IndexError:
            raise IndexError(
                "Task exclusions should be in the form 'state:disease,state:disease'"
            )

    args_dict["state"] = parse_options(state, nssp_valid_states)

    args_dict["disease"] = parse_options(disease, all_diseases)

    # Check valid reference_date range relative to report_date
    if not all(isinstance(ref, date) for ref in reference_dates):
        raise ValueError("All elements in reference_dates must be date objects.")
    if len(reference_dates) != 2:
        raise ValueError("reference_dates must contain exactly two date objects.")
    if not all(ref <= report_date for ref in reference_dates):
        raise ValueError(
            "Invalid reference_date. Ensure all reference_dates are on or before the report date."
        )

    args_dict["reference_dates"] = reference_dates
    args_dict["report_date"] = report_date
    args_dict["data_path"] = data_path
    args_dict["data_container"] = data_container
    args_dict["production_date"] = production_date
    args_dict["job_id"] = job_id
    args_dict["as_of_date"] = as_of_date
    args_dict["exclusions"] = exclusions
    args_dict["output_container"] = output_container
    return args_dict


def generate_task_id(
    state: str,
    disease: str,
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
    task_id: str,
    timestamp: str,
) -> str:
    """Updates a task_id with new timestamp.
    Parameters:
        task_id: task_id to update
        timestamp: updated timestamp string
    """
    try:
        # Task id format: <state>_<disease>_<timestamp>
        # eg WY_Influenza_1730395796
        state, disease, _ = task_id.split("_")
        # Reconstruct with the new timestamp
        return f"{state}_{disease}_{timestamp}"
    except ValueError as e:
        raise ValueError(
            f"Task ID '{task_id}' does not match expected format. Check that task IDs are formatted as <state>_<disease>_<timestamp>. Error: {e}"
        )


def generate_task_configs(
    state: list[str],
    disease: list[str],
    report_date: date,
    reference_dates: list[date],
    data_container: str,
    data_path: str,
    as_of_date: str,
    production_date: date,
    job_id: str,
    output_container: str,
    task_exclusions: dict[str, list[str]] | None = None,
    exclusions: str | None = None,
) -> tuple[list[dict], str]:
    """
    Generates a list of configuration objects based on
    supplied parameters.
    Parameters:
        state: list of geographies to run model
        disease: list of pathogens to run
        report_date: date of model run
        reference_dates: array of reference (event) dates
        data_container: container for input data
        data_path: path to input data
        as_of_date: iso format datetime string of model run
        production_date: production date of model run
        job_id: unique identifier for job
        output_container: Azure container for output
        task_exclusions: dictionary of state:disease pairs to exclude
        exclusions: a path to exclusions csv
    Returns:
        A list of configuration objects and the job_id.
    """
    configs = []
    # Create tasks for each state-pathogen combination
    for s in state:
        for d in disease:
            task_config = {
                **shared_params,
                "job_id": job_id,
                "task_id": generate_task_id(state=s, disease=d),
                "exclusions": exclusions or {"path": None},
                "min_reference_date": min(reference_dates).isoformat(),
                "max_reference_date": max(reference_dates).isoformat(),
                "disease": d,
                "geo_value": s,
                "geo_type": "state" if s != "US" else "country",
                "report_date": report_date.isoformat(),
                "production_date": production_date.isoformat(),
                "output_container": output_container,
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
            }
            configs.append(task_config)

    if task_exclusions is not None:
        configs = exclude_task(configs, task_exclusions)

    return configs, job_id


def exclude_task(config_data: list[dict], filters: dict[str, list[str]]) -> list[dict]:
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

    filter_set: set[tuple[str, str]] = set(
        zip(filters["geo_value"], filters["disease"])
    )

    filtered_data = [
        entry
        for entry in config_data
        if (entry.get("geo_value"), entry.get("disease")) not in filter_set
    ]

    return filtered_data


def parse_options(
    raw_input: str | list[str], valid_options: Iterable[str]
) -> list[str]:
    """
    Parses the raw_input and returns a list of options. This is primarily
    intended to handle parsing of states and diseases

    Parameters:
    ----------
        raw_input: str
            A string representing a single option, a comma-separated list of options,
            or the string "all" to include all available options..

    Returns:
    -------
        list[str]
            list of options to use.

    Raises:
    -------
        ValueError: If the option is not recognized or if the format is invalid.
    """
    match raw_input:
        case "all" | "*":
            return list(valid_options)
        case str(o) if "," not in o:
            # This is a single option as a string
            o = o.strip()
            if o not in valid_options:
                raise ValueError(f"Option {o} not recognized.")
            return [o]
        case str(o) if "," in o:
            # This is a list of options as a string
            option_list: list[str] = [os.strip() for os in o.split(",")]

            # Perform set diff to catch any invalid options and raise an error
            invalid_options: set[str] = set(option_list).difference(valid_options)
            if any(invalid_options):
                raise ValueError(
                    (
                        f"Options {invalid_options} not recognized."
                        " Valid options are {valid_options}."
                    )
                )

            return option_list
        case list(o):
            # This is a list of options
            if not all(isinstance(os, str) for os in o):
                raise ValueError(
                    f"All elements in the list must be strings. Got {type(o)} instead."
                )

            parsed: list[str] = [os.strip() for os in o]
            invalid_options: set[str] = set(parsed).difference(valid_options)
            if any(invalid_options):
                raise ValueError(
                    (
                        f"Options {invalid_options} not recognized."
                        " Valid options are {valid_options}."
                    )
                )
            return o
        case _:
            raise ValueError(
                (
                    f"Options {raw_input} not recognized. Valid options are 'all', "
                    " a singleton, or a comma-separated list of options as a string, "
                    f"or a list of strings. Got {type(raw_input)} instead."
                )
            )


def generate_ref_date_tuples(
    report_dates: list[date], delta: str = "-8w"
) -> list[tuple[date, date]]:
    """
    Generates a list of tuples of reference dates based on the report dates and a time
    delta.

    Parameters
    ----------
    report_dates: list[date]
        A list of report dates to generate reference dates from.
    delta: str
        A string in the format used by polars `dt.offset_by()` to specify the time
        delta. See the polars documentation for more details.
        https://docs.pola.rs/api/python/stable/reference/series/api/polars.Series.dt.offset_by.html
        This will go backwards in time from the report date regardless of if the
        string is positive or negative. For example, if the delta is "1w", it will
        subtract 1 week from the report date. If the delta is "-1w", it will subtract
        1 week from the report date as well. This is because the reference date is
        always before the report date.

    Returns
    -------
    list[tuple[date, date]]
        A list of tuples, where each tuple contains a report date and its
        corresponding min and max reference dates.
    """
    # Make sure the delta is always negative
    if not delta.startswith("-"):
        delta = "-" + delta

    df: pl.DataFrame = pl.DataFrame(dict(report_date=report_dates)).select(
        max_ref_date=pl.col.report_date.dt.offset_by("-1d"),
        min_ref_date=pl.col.report_date.dt.offset_by(delta),
    )

    return list(zip(df["max_ref_date"], df["min_ref_date"]))
