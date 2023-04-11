import os
import time
from dataclasses import dataclass
from pathlib import Path

from descriptors import cachedproperty

from fate.conf import ConfGroup
from fate.util.log import StructLogger


@dataclass
class SchedulerTiming:
    """TaskScheduler timing.

    Features stable (caching) properties for the scheduler's last check,
    current check and when it should next check.

    All such properties return timestamps.

    """
    conf: ConfGroup
    logger: StructLogger
    path_check: Path        # path to empty file with which time of last
                            # check is stored               # noqa: E116

    @cachedproperty
    def time_check(self):
        return time.time()

    @cachedproperty
    def last_check(self):
        return self._check_state_(update=True)

    @property
    def next_check(self):
        return self._next_check_tasks_ or self._next_check_max_

    def _check_state_(self, update=False):
        try:
            stat_result = os.stat(self.path_check)
        except FileNotFoundError:
            last_check = None
        else:
            last_check = stat_result.st_mtime

        if update:
            if not self.path_check.exists():
                self.path_check.touch()

            os.utime(self.path_check, (self.time_check, self.time_check))

        return last_check

    @cachedproperty
    def _next_check_tasks_(self):
        next_check = None

        for task in self.conf.task.values():
            next_check = task.schedule_next_(
                self.time_check,             # t0
                next_check,                  # t1
                next_check,                  # default
                max_years_between_matches=1  # quit if it's that far out
            )

        return next_check

    _next_max_ = 60 * 60 * 24 * 365  # 1 year in seconds

    @property
    def _next_check_max_(self):
        return self.time_check + self._next_max_
