import logging
import os
from pprint import pprint
from typing import Any

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

    # Print out all the user args, and the data_exclusions_path so we can see the
    # defaults
    pprint(user_args)
    pprint(data_exclusions_path)
