import logging
from typing import Any

from cfa_config_generator.utils.epinow2.driver_functions import generate_config
from cfa_config_generator.utils.epinow2.functions import (
    extract_user_args,
    generate_timestamp,
)

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # Generate job-specific parameters
    as_of_date: str = generate_timestamp()

    # Pull run parameters from environment
    user_args: dict[str, Any] = extract_user_args(as_of_date=as_of_date)

    generate_config(
        state=user_args["state"],
        disease=user_args["disease"],
        report_date=user_args["report_date"],
        reference_dates=user_args["reference_dates"],
        data_path=user_args["data_path"],
        data_container=user_args["data_container"],
        production_date=user_args["production_date"],
        job_id=user_args["job_id"],
        as_of_date=user_args["as_of_date"],
        output_container=user_args["output_container"],
        task_exclusions=user_args.get("task_exclusions"),
        exclusions=user_args.get("exclusions"),
        facility_active_proportion=user_args["facility_active_proportion"],
    )
