from __future__ import annotations

import abc
import itertools
import sys
import typing
from dataclasses import dataclass, field, InitVar

from fate.common.log import LogRecord
from fate.common.output import TaskOutput, TaskResult
from fate.conf import OutputEncodeError, ResultEncodeError

if typing.TYPE_CHECKING:
    import datetime

    from fate.util import stream

    from .base import InvokedTask
    from .failed_task import TaskInvocationError


@dataclass
class TaskEvent(abc.ABC):

    task: InvokedTask
    read: bool = field(default=False, init=False)


class TaskInvocationFailureEvent(TaskEvent):

    @property
    def error(self) -> TaskInvocationError:
        return self.task.error


@dataclass
class TaskLogEvent(TaskEvent):

    message: bytes

    def record(self) -> LogRecord:
        return LogRecord.parse(self.message.decode(), self.task.format_['log'], self.task.__name__)


@dataclass
class TaskReadyEvent(TaskEvent):

    returncode: int

    @property
    def duration(self) -> datetime.timedelta:
        return self.task.duration_()

    @property
    def ended(self) -> float:
        return self.task.ended_()

    @property
    def expires(self) -> typing.Optional[float]:
        return self.task.expires_()

    @property
    def stdout(self) -> stream.ProgressiveOutput:
        return self.task.stdout_

    @property
    def stderr(self) -> stream.BufferedOutput:
        return self.task.stderr_

    @property
    def stopped(self) -> typing.Optional[float]:
        return self.task.stopped_

    def results(self) -> typing.List[TaskResult]:
        try:
            outputs = [*TaskOutput.parse(bytes(self.task.stdout_),
                                         self.task.__lib__,
                                         self.task.format_['result'])]
        except OutputEncodeError as exc:
            if result := TaskResult.compose(self.task, exc.output):
                raise ResultEncodeError(result, exc.format, *exc.errors)

            return []
        else:
            return [result
                    for output in outputs
                    if (result := TaskResult.compose(self.task, output))]


@dataclass
class TaskEvents:

    iterable: InitVar[typing.Iterable[TaskEvent]] = ()

    closed: bool = (field(default=False, kw_only=True) if sys.version_info >= (3, 10)
                    else field(default=False))

    _events: typing.List[TaskEvent] = field(default_factory=list, init=False)

    def __post_init__(self, iterable) -> None:
        self._events.extend(iterable)

    def __iter__(self) -> typing.Iterator[TaskEvent]:
        yield from self._events

    def _iter_unread_(self) -> typing.Iterator[TaskEvent]:
        for event in self:
            if not event.read:
                yield event

    def read(self, count=None) -> typing.Iterator[TaskEvent]:
        for event in itertools.islice(self._iter_unread_(), count):
            event.read = True
            yield event

    def write(self, *events: TaskEvent) -> None:
        if self.closed:
            raise ValueError("TaskEvents is closed")

        self._events.extend(events)

    def close(self) -> None:
        self.closed = True
