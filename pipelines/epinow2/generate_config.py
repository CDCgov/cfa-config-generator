import logging
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

    # Print out all the user_args, so we can see what the defaults are
    pprint(user_args)
