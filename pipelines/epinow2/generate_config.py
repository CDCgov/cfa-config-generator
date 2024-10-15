import os
from datetime import datetime

from utils.epinow2.functions import generate_job_id, generate_task_configs

if __name__ == "__main__":
    """
    A script to generate `epinow2` configuration objects. This
    will be invoked either by 1) a scheduled GH action using
    default values, or 2) a user-triggered workflow with supplied
    state, pathogen, report_date, and reference_date parameters.
    This script is the entrypoint to the workflow that generates
    configuration objects, validates them against a schema, and
    writes them to Blob Storage.
    """

    # Pull run parameters from environment
    state = os.environ.get("state", "all")
    pathogen = os.environ.get("pathogen", "all")
    report_date = os.environ.get(
        "report_date", datetime.today().strftime("%Y-%m-%d")
    )
    reference_date = os.environ.get("reference_date", [report_date])
    data_source = os.environ.get("data_source", "nssp")

    # Generate job-specific parameters
    as_of_date = int(datetime.timestamp(datetime.now()))
    job_id = generate_job_id()

    # Generate task-specific configs
    task_configs = generate_task_configs(
        state=state,
        report_date=report_date,
        reference_date=reference_date,
        as_of_date=as_of_date,
        job_id=job_id,
        data_source=data_source,
    )
