import os
import time

from fate import sched


class SchedulerFixture:

    def __init__(self, conf, logger):
        self.scheduler = sched.TieredTenancyScheduler(conf, logger)

        # ensure that -- regardless of OS -- scheduler polls consistently for patching
        self.scheduler.poll_frequency = 0.01

    def set_last_check(self, last_check=None, *, offset=None):
        if (
                (last_check is not None and offset is not None)
                or (last_check is None and offset is None)
        ):
            raise TypeError

        if last_check is None:
            last_check = time.time() - offset

        self.scheduler.timing.path_check.touch()
        os.utime(self.scheduler.timing.path_check, (last_check, last_check))
