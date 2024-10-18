from utils.epinow2.functions import (
    generate_task_configs,
    generate_timestamp,
    generate_uuid,
    extract_user_args,
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
    user_args = extract_user_args()

    # Validate and sanitize args
    sanitized_args = validate_args(**user_args)
    # Generate job-specific parameters
    as_of_date = generate_timestamp()
    job_id = generate_uuid()
    # Generate task-specific configs
    task_configs = generate_task_configs(
        **sanitized_args, as_of_date=as_of_date, job_id=job_id
    )
