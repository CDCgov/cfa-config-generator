import logging
import os
from typing import Any

from cfa_config_generator.utils.epinow2.driver_functions import generate_rerun_config
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

    # Get the data_exclusions_path separately from the environment
    data_exclusions_path: str | None = os.getenv("data_exclusions_path")

    # generate_rerun_config() now accepts arguments directly
    generate_rerun_config(data_exclusions_path=data_exclusions_path, **user_args)
