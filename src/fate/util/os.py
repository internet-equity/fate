import errno
import os
import sys

from .compat.path import is_relative_to


#
# pid_exists: adapted from psutil
#

def pid_exists(pid):
    """Check whether pid exists in the current process table."""
    if pid <= 0:
        raise ValueError(f'invalid PID {pid}')

    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            # ESRCH == No such process
            return False

        if err.errno == errno.EPERM:
            # EPERM clearly means there's a process to deny access to
            return True

        # According to "man 2 kill" possible error values are
        # (EINVAL, EPERM, ESRCH) therefore we should never get
        # here. If we do let's be explicit in considering this
        # an error.
        raise

    return True


#
# system_path: see also! fate-pyz:src/__main__.py
#

def system_path(path):
    """Whether the given Path `path` appears to be a non-user path.

    Returns bool – or None if called on an unsupported platform
    (_i.e._ implicitly False).

    """
    if sys.platform == 'linux':
        return not is_relative_to(path, '/home') and not is_relative_to(path, '/root')

    if sys.platform == 'darwin':
        return not is_relative_to(path, '/Users')
