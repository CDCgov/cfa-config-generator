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

    # generate_config() now accepts arguments directly
    generate_config(**user_args)
