import logging

from cfa_config_generator.utils.epinow2.driver_functions import generate_config

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # generate_config() get all of its arguments from the environment
    # so we don't need to pass any arguments here.
    generate_config()
