import os
from datetime import date

from utils.epinow2.functions import (
    generate_task_configs,
    generate_timestamp,
    generate_uuid,
    get_reference_date_range,
    validate_args,
)

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
    disease = os.environ.get("disease", "all")
    report_date = os.environ.get("report_date", date.today())

    min_reference_date, max_reference_date = get_reference_date_range(report_date)
    reference_dates = os.environ.get(
        "reference_date", [min_reference_date, max_reference_date]
    )
    data_source = os.environ.get("data_source", "nssp")
    data_path = os.environ.get("data_path", "gold/")
    data_container = os.environ.get("data_container", None)

    # Validate and sanitize args
    sanitized_args = validate_args(
        state=state,
        disease=disease,
        report_date=report_date,
        reference_dates=reference_dates,
        data_source=data_source,
        data_path=data_path,
        data_container=data_container,
    )
    # Generate job-specific parameters
    as_of_date = generate_timestamp()
    job_id = generate_uuid()
    # Generate task-specific configs
    task_configs = generate_task_configs(
        **sanitized_args, as_of_date=as_of_date, job_id=job_id
    )
