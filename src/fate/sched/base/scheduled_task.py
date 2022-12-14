import plumbum
from descriptors import classonlymethod

from fate.conf.error import LogsDecodingError
from fate.conf.types import TaskConfDict, TaskChainMap
from fate.util.datastructure import at_depth, adopt


class ImmediateFailure:
    """Dummy Future for tasks failing to initialize."""

    returncode = None
    stdout = None
    stderr = None

    def __init__(self, exc):
        self.exception = exc

    @staticmethod
    def poll():
        return True

    ready = poll


class ScheduledTask(TaskConfDict):
    """Task class extended for processing by the operating system."""

    @classonlymethod
    def schedule(cls, task):
        """Construct a ScheduledTask extending the specified Task."""
        self = cls(task)
        task.__parent__.__adopt__(task.__name__, self)
        return self

    def __adopt_parent__(self, name, mapping):
        if mapping.__parent__ is None:
            mapping.__parent__ = self
            return

        # we've likely taken over for existing configuration via schedule().
        # rather than insist that child is in our tree, merely check
        # that its tree looks the same.
        assert isinstance(mapping.__parent__, TaskConfDict)
        assert mapping.__parent__.__path__ == self.__path__

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__future__ = None
        self.returncode = None
        self.stdout = None
        self.stderr = None

    def __call__(self):
        """Execute the task's program in a background process.

        A ScheduledTask may only be executed once -- subsequent
        invocations are idempotent / no-op.

        Returns the execution-initiated ScheduledTask (self).

        """
        if self.__future__ is None:
            try:
                if isinstance(exec_ := self.exec_, str):
                    cmd = plumbum.local[exec_]
                else:
                    (root, *arguments) = exec_
                    cmd = plumbum.local[root][arguments]
            except plumbum.CommandNotFound as exc:
                future = ImmediateFailure(exc)
            else:
                bound = cmd << self.param_
                future = bound.run_bg()

            self.__future__ = future

        return self

    def exception(self):
        """The in-process exception, raised by Task program
        initialization, if any.

        """
        return self.__future__.exception if isinstance(self.__future__, ImmediateFailure) else None

    def poll(self):
        """Return whether the Task program's process has exited.

        Sets the ScheduledTask's `returncode`, `stdout` and `stderr`
        when the process has exited.

        """
        if self.__future__ is None:
            return False

        if ready := self.__future__.poll():
            self.returncode = self.__future__.returncode
            self.stdout = self.__future__.stdout
            self.stderr = self.__future__.stderr

        return ready

    ready = poll

    def logs(self):
        """Parse LogRecords from `stderr`.

        Raises LogsDecodingError to indicate decoding errors when the
        encoding of a task's stderr log output is configured explicitly.
        Note, in this case, the parsed logs *may still* be retrieved
        from the exception.

        """
        if self.stderr is None:
            return None

        stream = self._iter_logs_(self.stderr)
        logs = tuple(stream)

        if stream.status.errors:
            raise LogsDecodingError(*stream.status, logs)

        return logs

    @property
    @adopt('path')
    def path_(self):
        default = super().path_
        return ScheduledTaskChainMap(*default.maps)


class ScheduledTaskChainMap(TaskChainMap):

    @at_depth('*.path')
    def result_(self, *args, **kwargs):
        return self._result_(self.__parent__.stdout, *args, **kwargs)
