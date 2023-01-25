import abc
import hashlib
import os
import time
import typing

from descriptors import cachedproperty, classproperty

from fate.conf import ConfBracketError
from fate.util.iteration import storeresult

from .scheduled_task import ScheduledTask
from .util.reset import resets, Resets


class TaskScheduler(Resets):
    """Abstract base class of task schedulers.

    Method `exec_tasks` must be implemented by concrete subclasses -- to
    execute tasks -- as a synchronous generator of completed tasks.
    (Task execution may be and likely should be asynchronous / pooled.)

    Invocation of a scheduler instance -- via `__call__` -- returns an
    iterator of completed tasks (derived from `exec_tasks`). Upon
    exhaustion, this iterator's attribute `info` is set to an instance
    of SchedInfo.

    Method `collect_tasks` generates tasks to be executed as a stream of
    ScheduledTask objects.

    Invocation of `collect_tasks` records the current timestamp on the
    file system and under scheduler property `time_check`. The timestamp
    of the previous "check" (retrieved from the file system) is stored
    under scheduler property `last_check`.

    The Boolean argument `reset` of methods `collect_tasks`, `__call__`,
    (and `reset`), serve to clear the above scheduler properties, such
    that a new "check" -- and a new round of task-collection -- may take
    place. Otherwise, `collect_tasks` is idempotent.

    """
    class SchedInfo(typing.NamedTuple):

        count: int
        next: float

    def __init__(self, conf, logger):
        super().__init__()
        self.conf = conf
        self.logger = logger.set(sched=self.module_short)

    @abc.abstractmethod
    def exec_tasks(self, reset=False):
        yield from ()
        return 0

    @storeresult('info')
    def __call__(self, reset=False):
        count = yield from self.exec_tasks(reset=reset)
        return self.SchedInfo(count, self.next_check)

    @classproperty
    def module_short(cls):
        (*_root, name) = cls.__module__.rsplit('.', 1)
        return name.replace('_', '-')

    @cachedproperty
    def path_check(self):
        signature = os.pathsep.join(sorted(str(conf.__path__) for conf in self.conf))
        file_hash = hashlib.md5(signature.encode()).hexdigest()
        return self.conf._prefix_.state / 'check' / file_hash

    def _check_state_(self, update=False):
        try:
            stat_result = os.stat(self.path_check)
        except FileNotFoundError:
            last_check = None
        else:
            last_check = stat_result.st_mtime

        if update:
            if not self.path_check.exists():
                self.path_check.mkdir(parents=True)
                for conf in self.conf:
                    link_path = self.path_check / conf.__path__.name
                    link_path.symlink_to(conf.__path__)

            os.utime(self.path_check, (self.time_check, self.time_check))

        return last_check

    @resets
    @cachedproperty
    def last_check(self):
        return self._check_state_(update=True)

    @resets
    @cachedproperty
    def time_check(self):
        return time.time()

    @resets
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

    next_max = 60 * 60 * 24 * 365  # 1 year in seconds

    @property
    def _next_check_max_(self):
        return self.time_check + self.next_max

    @property
    def next_check(self):
        return self._next_check_tasks_ or self._next_check_max_

    def _iter_tasks_due_(self):
        # bring last_check & time_check local to ensure consistent generation
        # across resets (as unlikely/discouraged as this situation might be).
        if (last_check := self.last_check) is None:
            return

        time_check = self.time_check

        for task in self.conf.task.values():
            if task.scheduled_(last_check, time_check):
                try:
                    may_schedule = task.if_
                except ConfBracketError as exc:
                    self.logger.warning(task=task.__name__, key=exc.path, msg=str(exc))
                    may_schedule = exc.evaluation

                if not may_schedule:
                    self.logger.info(task=task.__name__,
                                     msg='skipped: suppressed by if/unless condition')
                    continue

                yield ScheduledTask.schedule(task)

    def collect_tasks(self, reset=False):
        """Generate ScheduledTasks to be executed.

        If `reset` then the scheduler's current "check" is cleared, such
        that subsequently-scheduled tasks may be generated. Otherwise,
        the set of scheduled tasks is idempotent.

        """
        if reset:
            self.reset()

        yield from self._iter_tasks_due_()
