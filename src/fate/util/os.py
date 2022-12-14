import errno
import os


# adapted from psutil

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
