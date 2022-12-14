import multiprocessing
import os


def cpu_count():
    try:
        # glibc-only
        sched_getaffinity = os.sched_getaffinity
    except AttributeError:
        # Note: this *may* be inaccurate in some shared environs
        return multiprocessing.cpu_count()
    else:
        return len(sched_getaffinity(0))
