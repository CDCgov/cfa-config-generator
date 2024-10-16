from uuid import uuid1
from datetime import datetime, timedelta
from utils.epinow2.constants import states, pathogens, nssp_states_omit


def generate_job_id():
    """Generates a UUID1 object to associate
    with job IDs. UUID1 can be sorted by
    timestamp by default, which is desirable
    when managing multiple configurations."""
    return uuid1()


def validate_args(
    state=None, pathogen=None, report_date=None, reference_date=None, data_source=None
):
    """Checks that user-supplied arguments are valid and returns them
    in a standardized format for downstream use.
    Parameters:
        state: geography to run model
        pathogen: pathogen to run
        report_date: date of model run
        reference_date: array of reference (event) dates
        data_source: source of input data
    """
    args_dict = {}
    if state == "all":
        if data_source == "nssp":
            args_dict["state"] = list(set(states) - set(nssp_states_omit))
            args_dict["data_source"] = data_source
        elif data_source == "nhsn":
            args_dict["state"] = [states]
            args_dict["data_source"] = data_source
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

    # Convert dates to datetime
    reference_datetime = [datetime.strptime(x, "%Y-%m-%d") for x in reference_date]
    report_datetime = datetime.strptime(report_date, "%Y-%m-%d")

    # Check valid reference_date
    if not all(ref <= report_datetime for ref in reference_datetime):
        raise ValueError(
            "Invalid reference_date. Ensure all reference_dates are before the report date."
        )
    args_dict["reference_date"] = reference_date
    args_dict["report_date"] = report_date
    return args_dict


def generate_task_configs(
    state=None,
    pathogen=None,
    report_date=None,
    reference_date=None,
    as_of_date=None,
    job_id=None,
    data_source=None,
):
    """
    Generates a list of configuration objects based on
    supplied parameters.
    Parameters:
        state: geography to run model
        pathogen: pathogen to run
        report_date: date of model run
        reference_date: array of reference (event) dates
        as_of_date: timestamp of model run
        job_id: UUID for job
        data_source: source of input data
    Returns:
        A list of configuration objects.
    """
    return []
