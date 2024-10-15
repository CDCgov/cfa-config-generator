from uuid import uuid1


def generate_job_id():
    """Generates a UUID1 object to associate
    with job IDs. UUID1 can be sorted by
    timestamp by default, which is desirable
    when managing multiple configurations."""
    return uuid1()


def generate_task_configs(
    state=None,
    report_date=None,
    reference_date=None,
    as_of_date=None,
    job_id=None,
    data_source=None,
):
    """
    Generates a list of configuration objects based on
    supplied.
    Parameters:
        state: geography to run model
        report_date: date of model run
        reference_date: array of reference (event) dates
        as_of_date: timestamp of model run
        job_id: UUID for job
        data_source: source of input data
    Returns:
        A list of configuration objects.
    """
    return []
