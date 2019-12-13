"""Common functions for fmu.ensemble"""

import os
import sys


def use_concurrent():
    """Determine whether we should use concurrency or not

    This is based on both an environment variable
    and presence of concurrent.futures.

    Returns:
        bool: True if concurrency mode should be used
    """
    env_name = "FMU_CONCURRENCY"
    if "concurrent.futures" in sys.modules:
        if env_name not in os.environ:
            return True
        else:
            env_var = os.environ[env_name]
            if str(env_var) == "0" or str(env_var).lower() == "false":
                return False
            else:
                return True
    else:
        # If concurrent.futures is not available, we end here.
        return False
