"""Common functions for fmu.ensemble"""

import os
import sys

import six


def use_concurrent():
    """Determine whether we should use concurrency or not

    This is based on both an environment variable
    and presence of concurrent.futures, and on Python version
    (Py2 deliberately not attempted to support)

    Returns:
        bool: True if concurrency mode should be used
    """
    if six.PY2:
        # Py2-support not attempted
        return False
    env_name = "FMU_CONCURRENCY"
    if "concurrent.futures" in sys.modules:
        if env_name not in os.environ:
            return True
        env_var = os.environ[env_name]
        if (
            str(env_var) == "0"
            or str(env_var).lower() == "false"
            or str(env_var).lower() == "no"
        ):
            return False
        return True
    # If concurrent.futures is not available to import, we end here.
    return False
