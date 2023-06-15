import os
import time

from fate import sched


class SchedulerFixture:

    def __init__(self, conf, logger):
        self.scheduler = sched.TieredTenancyScheduler(conf, logger)

    def set_last_check(self, offset):
        self.scheduler.timing.path_check.touch()

        last_check = time.time() - offset
        os.utime(self.scheduler.timing.path_check, (last_check, last_check))
