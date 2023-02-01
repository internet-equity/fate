import multiprocessing
import os


COMPAT_INTERVAL = 0.02


def cpu_count():
    try:
        # glibc-only
        sched_getaffinity = os.sched_getaffinity
    except AttributeError:
        # Note: this *may* be inaccurate in some shared environs
        return multiprocessing.cpu_count()
    else:
        return len(sched_getaffinity(0))


def get_interval(default=COMPAT_INTERVAL):
    try:
        sched_rr_get_interval = os.sched_rr_get_interval
    except AttributeError:
        # unsupported (e.g. darwin)
        return default
    else:
        return sched_rr_get_interval(0)
