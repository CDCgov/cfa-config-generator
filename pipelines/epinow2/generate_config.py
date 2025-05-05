import logging
import os

from cfa_config_generator.utils.epinow2.functions import generate_timestamp

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # Generate job-specific parameters
    as_of_date: str = generate_timestamp()

    task_exclusions = os.getenv("task_exclusions")
    exclusions = os.getenv("exclusions")
    state = os.getenv("state")
    disease = os.getenv("disease")

    # Handle report_date
    report_date_str = os.getenv("report_date")

    # Handle production_date
    production_date_str = os.getenv("production_date")

    reference_dates_str = os.getenv("reference_dates")

    data_container = os.getenv("data_container")
    output_container = os.getenv("output_container")
    job_id = os.getenv("job_id")

    # Print out all of the above variables
    print("Environment Variables:")
    print(f"{task_exclusions=}")
    print(f"{exclusions=}")
    print(f"{state=}")
    print(f"{disease=}")
    print(f"{report_date_str=}")
    print(f"{production_date_str=}")
    print(f"{reference_dates_str=}")
    print(f"{data_container=}")
    print(f"{output_container=}")
    print(f"{job_id=}")
