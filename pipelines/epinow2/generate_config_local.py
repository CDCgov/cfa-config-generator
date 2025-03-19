import json
import logging
import os

from cfa_config_generator.utils.epinow2.functions import (
    extract_user_args,
    generate_task_configs,
    generate_timestamp,
    validate_args,
)

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    """
    A script to generate `epinow2` configuration objects. This
    will be invoked either by 1) a scheduled GH action using
    default values, or 2) a user-triggered workflow with supplied
    state, pathogen, report_date, and reference_date parameters.
    This script is the entrypoint to the workflow that generates
    configuration objects, validates them against a schema, and
    writes them to a folder under the 'target/' directory.
    """

    # Generate job-specific parameters
    as_of_date = generate_timestamp()

    # Pull run parameters from environment
    user_args = extract_user_args(as_of_date=as_of_date)

    # Validate and sanitize args
    sanitized_args = validate_args(**user_args)

    # Generate task-specific configs
    task_configs, job_id = generate_task_configs(**sanitized_args)

    # Path to the 'output' directory
    output_dir = f"target/{job_id}"

    # Check if the directory exists
    if os.path.exists(output_dir):
        # Delete all files in the directory
        for filename in os.listdir(output_dir):
            file_path = os.path.join(output_dir, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
    else:
        # Create the directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

    # Loop through the list and write each item to a new file
    for task in task_configs:
        file_name = f"{task['task_id']}.json"
        file_path = os.path.join(output_dir, file_name)

        # Write the item to the file
        with open(file_path, "w") as file:
            file.write(json.dumps(task, indent=2))

    logger.info(
        f"Successfully generated configs for job; tasks stored in {job_id} directory."
    )
