"""Common functions for fmu.ensemble"""

import os
import logging

logger = logging.getLogger(__name__)

ENV_NAME = "FMU_CONCURRENCY"


def use_concurrent():
    """Determine whether we should use concurrency or not

    This is based on both an environment variable
    and presence of concurrent.futures, and on Python version

    Returns:
        bool: True if concurrency mode should be used
    """
    if ENV_NAME not in os.environ:
        return True
    env_var = os.environ[ENV_NAME]
    if (
        str(env_var) == "0"
        or str(env_var).lower() == "false"
        or str(env_var).lower() == "no"
    ):
        return False
    return True


def set_concurrent(concurrent):
    """Set the concurrency mode used by fmu.ensemble.

    This is done through modifying the enviroment variable
    for the current Python process

    If concurrency is asked for by but not possible, a warning
    will be printed and the code will continue in sequential mode.

    Args:
        concurrent (bool): Set to True if concurrent mode is requested,
            False if not.
    """
    if isinstance(concurrent, bool):
        os.environ[ENV_NAME] = str(concurrent)
    else:
        raise TypeError
    # Check for success:
    if concurrent and not use_concurrent():
        logger.warning("Unable to activate concurrent code")
