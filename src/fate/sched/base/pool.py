import itertools


class TaskProcessPool:
    """A set-size, iterable collection of tasks launched for execution
    in subprocesses.

    A TaskProcessPool may be given any size, thereby setting the maximum
    number of "slots" for concurrently-executing subprocesses. And, an
    existing pool may be expanded via method `expand`; however, pools
    may not be shrunk.

    Slots may be occupied by launched (invoked) tasks in the form of
    ScheduledTasks. Upon construction (`__init__`) and expansion
    (`expand`), an iterable of unlaunched ScheduledTasks may be
    supplied, from which available slots will be filled, with the tasks,
    launched.

    Note that -- (unlike with construction of a list in the form
    `list(iterable)`) -- only as much of the given iterable is consumed
    as necessary to fill the pool's available slots.

    Method `iter_ready` checks the pool's slotted tasks for completion,
    generating a stream of those tasks which have completed. Similar to
    the above, an iterable may be supplied of ScheduledTasks to be
    executed; these given tasks will "refill" slots emptied by completed
    tasks.

    Insofar as there are no tasks to execute, slots are occupied by
    `None`.

    """
    _fill_empty_ = itertools.repeat(None)

    def __init__(self, tasks=(), *, size=0):
        self.slots = []
        self.size = 0

        self.expand(tasks, size=size)

    def expand(self, tasks=(), *, size):
        if size < self.size:
            raise ValueError(f"cannot decrease size: {size} < {self.size}")

        if size == self.size:
            return

        tasks_fill = itertools.chain(tasks, self._fill_empty_)
        tasks_add = itertools.islice(tasks_fill, size - self.size)
        slots_add = (None if task is None else task()
                     for task in tasks_add)

        self.slots.extend(slots_add)

        self.size = int(size)

    def fill(self, tasks):
        count = 0

        refill = iter(tasks)

        for (index, slot) in enumerate(self.slots):
            if slot is None:
                try:
                    task = next(refill)
                except StopIteration:
                    break

                self.slots[index] = task()
                count += 1

        return count

    def __iter__(self):
        return iter(self.slots)

    def enumerate_tasks(self):
        # ensure index reflects actual index in slots (rather than in
        # this method's resulting iterator)
        for (index, slot) in enumerate(self):
            if slot is not None:
                yield (index, slot)

    def iter_tasks(self):
        for (_index, task) in self.enumerate_tasks():
            yield task

    def iter_ready(self, refill=None, *, clear=None):
        if not clear and clear is not None and refill is not None:
            raise TypeError(f"ambiguous argumentation: refill specified as "
                            f"{refill!r} yet clear false-y as {clear!r}")

        reservoir = None if refill is None else iter(refill)

        count = 0

        for (index, task) in self.enumerate_tasks():
            if task.ready():
                count += 1

                if reservoir:
                    task_fill = next(reservoir, None)
                    self.slots[index] = None if task_fill is None else task_fill()
                elif clear:
                    self.slots[index] = None

                yield task

        return count

    @property
    def active(self):
        try:
            next(self.iter_tasks())
        except StopIteration:
            return False
        else:
            return True

    @property
    def count(self):
        return sum(1 for _task in self.iter_tasks())
