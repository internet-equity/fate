import collections
import itertools
import operator
import time

from fate.util.compat.os import get_interval

from .base import TaskProcessPool, TaskScheduler


get_tenancy = operator.attrgetter('scheduling_.tenancy')


class TieredTenancyScheduler(TaskScheduler):
    """Task scheduler grouping tasks' execution by configured "tenancy".

    The execution concurrency of tasks whose schedules coincide is
    limited. By default, tasks abide by a level of shared tenancy equal
    to a default value, which is a function of the CPU cores available
    to the execution environment. The level of shared tenancy which a
    task permits may be configured. (For example, a task requiring the
    exclusive use of all available CPUs, or a task requiring exclusive
    use of the network, might ensure that no other tasks execute
    concurrently, by configuring a scheduling tenancy of `1`.)

    The tiered tenancy scheduler queries the task schedule to collect a
    batch of scheduled tasks. This batch's lowest-tenancy tasks are
    executed first. As permitted by lower-tenancy tasks' configuration,
    tasks permitting higher tenancy *may* be executed concurrently.
    (For example: given a single task with a configured tenancy of `2`,
    one additional task *may* execute concurrently, so long as the batch
    contains additional tasks with configured tenancies of `2` or
    greater.) As a batch's lowest-tenancy tasks complete, the number of
    tasks which may execute concurrently is increased.

    During the course of task execution, should additional tasks become
    due, then subsequent batches of tasks are collected. Tasks collected
    from earlier batches -- also known as "cohorts" -- are prioritized
    over those of later batches. However, later-cohort tasks *may* be
    executed prior to earlier-cohort tasks, to ensure as many concurrent
    tasks are running as is permitted by the tenancy configurations of
    executing and to-be-executed tasks.

    """
    # Note: optimum time to wait before polling unclear.
    # For now, let's just wait a "slice":
    poll_frequency = get_interval()

    def exec_tasks(self, reset=False):
        count_completed = 0

        tasks_0 = self.collect_tasks(reset=reset)
        queue = SchedulingQueue([tasks_0])

        self.logger.debug(cohort=0, size=queue[0].size, tenancies=queue[0].infomap,
                          msg='enqueued cohort')

        while task_0 := queue.get_task():

            pool = TaskProcessPool([task_0], size=1)

            self.logger.debug(active=pool.count, msg='launched pool')

            while pool.active:
                min_tenancy = min(get_tenancy(task) for task in pool.iter_tasks())

                if min_tenancy > pool.size:
                    pool.expand(queue.tenancy_tasks(min_tenancy), size=min_tenancy)
                    self.logger.debug(tenancy=pool.size, active=pool.count, msg='expanded pool')

                if time.time() >= self.timing.next_check:
                    tasks_1 = self.collect_tasks(reset=True)
                    queue.append(tasks_1)

                    self.logger.debug(cohort=(len(queue) - 1), size=queue[-1].size,
                                      tenancies=queue[-1].infomap, msg='enqueued cohort')

                    count_fill = pool.fill(queue.tenancy_tasks(pool.size))

                    if count_fill:
                        self.logger.debug(active=pool.count, msg='filled pool')

                time.sleep(self.poll_frequency)

                count_ready = yield from pool.iter_ready(refill=queue.tenancy_tasks(pool.size))

                if count_ready:
                    count_completed += count_ready

                    self.logger.debug(completed=count_ready,
                                      total=count_completed,
                                      active=pool.count)

        return count_completed


class TenancyGroup:
    """Iterator of tasks sharing a given tenancy."""

    def __init__(self, tenancy, tasks):
        self.tenancy = int(tenancy)
        self.tasks = tuple(tasks)
        self.index = 0

    def __repr__(self):
        return (f"<{self.__class__.__name__}: "
                f"{{index={self.index!r}, done={self.done!r}}} "
                f"(tenancy={self.tenancy!r}, tasks={list(self.tasks)!r})>")

    def __iter__(self):
        return self

    def __next__(self):
        try:
            task = self.get()
        except IndexError:
            raise StopIteration

        self.index += 1
        return task

    def get(self):
        return self.tasks[self.index]

    @property
    def size(self):
        return len(self.tasks)

    @property
    def done(self):
        return self.index >= self.size

    @property
    def remaining(self):
        return self.size - self.index

    @property
    def info(self):
        return (self.tenancy, self.size)

    @property
    def status(self):
        return (self.tenancy, self.size, self.remaining)


def enumerate_backwards(seq):
    indices = range(len(seq) - 1, -1, -1)
    return zip(indices, reversed(seq))


class SchedulingCohort:
    """Collection of tasks resulting from the same scheduling "check".

    Tasks are sorted and grouped by their configured tenancy as
    TenancyGroups.

    Groups may be iterated via method `tenancy_groups`.

    Tasks may be iterated via method `tenancy_tasks`.

    Note: `tenancy_tasks` generates tasks by iteration over
    the cohort's TenancyGroups, consuming these iterators.
    `tenancy_groups` generates TenancyGroups from the cohort, and does
    not alter these iterators on its own. (However, given `prune=True`,
    either method will remove exhausted groups from the cohort.)

    To generate only those tasks configured with at least a minimum
    tenancy, specify this value to `tenancy_tasks`, _e.g._:

        for task in cohort.tenancy_tasks(tenancy=5):
            ...

    """
    group_class = TenancyGroup

    sort_key = get_tenancy

    def __init__(self, tasks=(), prune=True):
        # reverse in order to permit direct pruning of list
        tasks_sorted = sorted(tasks, key=self.sort_key, reverse=True)
        self._groups = [self.group_class(tenancy, group)
                        for (tenancy, group) in itertools.groupby(tasks_sorted, self.sort_key)]

        self.prune = prune

        self._done = False

    def __repr__(self):
        return f"<{self.__class__.__name__}: {list(reversed(self._groups))!r}>"

    def __len__(self):
        return len(self._groups)

    @property
    def done(self):
        # once done, cache this, rather than continue to check
        if not self._done:
            self._done = all(group.done for group in self._groups)

        return self._done

    @property
    def size(self):
        return sum(group.size for group in self._groups)

    @property
    def info(self):
        return [group.info for group in reversed(self._groups)]

    @property
    def infomap(self):
        return dict(self.info)

    def tenancy_groups(self):
        for (index, group) in enumerate_backwards(self._groups):
            if not group.done:
                yield group

            if self.prune and group.done:
                del self._groups[index]

    def tenancy_tasks(self, tenancy=1):
        for group in self.tenancy_groups():
            if group.tenancy >= tenancy:
                yield from group


class SchedulingQueue:
    """Collections of tasks resulting from various scheduling "checks".

    Batches of tasks -- or "cohorts" -- may be added to the queue
    dynamically via methods "append" and "extend".

    Supports the same group-querying and task-consumption operations as
    SchedulingCohort, but which may span multiple collected cohorts.

    """
    cohort_class = SchedulingCohort

    def __init__(self, cohorts=(), prune=True):
        # note: will be reversed in order to permit pruning of deque
        self._cohorts = collections.deque()

        self.prune = prune

        self.extend(cohorts)

    def __repr__(self):
        return f"<{self.__class__.__name__}: {list(reversed(self._cohorts))!r}>"

    def __len__(self):
        return len(self._cohorts)

    def __getitem__(self, index):
        if isinstance(index, slice):
            raise NotImplementedError(f'{self.__class__.__name__} does not support slices')

        if not isinstance(index, int):
            raise TypeError(f'{self.__class__.__name__} indices must be integers, '
                            f'not {index.__class__.__name__}')

        try:
            return self._cohorts[-1 - index]
        except IndexError:
            pass

        raise IndexError(f'{self.__class__.__name__} index out of range')

    @property
    def size(self):
        return sum(cohort.size for cohort in self._cohorts)

    def append(self, cohort):
        self._cohorts.appendleft(self.cohort_class(cohort, prune=self.prune))

    def extend(self, cohorts):
        self._cohorts.extendleft(self.cohort_class(cohort, prune=self.prune) for cohort in cohorts)

    def cohorts(self):
        for (index, cohort) in enumerate_backwards(self._cohorts):
            if not cohort.done:
                yield cohort

            if self.prune and cohort.done:
                del self._cohorts[index]

    def tenancy_groups(self):
        for cohort in self.cohorts():
            yield from cohort.tenancy_groups()

    def tenancy_tasks(self, tenancy=1):
        for cohort in self.cohorts():
            yield from cohort.tenancy_tasks(tenancy)

    def get_task(self, tenancy=1, default=None):
        return next(self.tenancy_tasks(tenancy), default)
