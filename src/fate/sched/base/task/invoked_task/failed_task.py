from .base import InvokedTask
from .event import TaskEvents, TaskInvocationFailureEvent


class TaskInvocationError(LookupError):
    """Exception raised for a task whose command could not be invoked."""


class FailedInvocationTask(InvokedTask):
    """Task whose command could not be invoked."""

    @InvokedTask._constructor_
    def fail(cls, task, err):
        return cls(task, err)

    def __init__(self, data, /, err):
        super().__init__(data)
        self.error = err
        self._events_ = TaskEvents(
            [TaskInvocationFailureEvent(self)],
            closed=True,
        )

    def ready_(self) -> bool:
        return True

    def events_(self) -> TaskEvents:
        return self._events_
