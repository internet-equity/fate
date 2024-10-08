"""Scheduling of tasks for execution."""
from . import ext, invoked_task as invocation


class ScheduledTask(ext.BoundTask):
    """Task extended for scheduling to be invoked."""

    @ext.BoundTask._constructor_
    def schedule(cls, task, state):
        """Construct a ScheduledTask extending the specified Task."""
        return cls(task, state)

    def __call__(self) -> invocation.InvokedTask:
        """Execute the task's program in a background process.

        Returns an InvokedTask – either a successfully-spawned
        SpawnedTask or a FailedInvocationTask.

        """
        try:
            return invocation.SpawnedTask.spawn(self, self._state_)
        except invocation.TaskInvocationError as exc:
            return invocation.FailedInvocationTask.fail(self, exc)
