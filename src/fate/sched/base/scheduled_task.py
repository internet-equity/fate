"""Execution of scheduled tasks."""
import os
import typing

import plumbum
from descriptors import cachedproperty, classonlymethod

from fate.conf.error import LogsDecodingError
from fate.conf.types import TaskConfDict, TaskChainMap
from fate.util.datastructure import at_depth, adopt


class ImmediateFailure:
    """Dummy Future for tasks failing to initialize."""

    returncode = None
    stdout = None
    stderr = None

    __slots__ = ('exception',)

    def __init__(self, exc):
        self.exception = exc

    @staticmethod
    def poll():
        return True

    ready = poll


class PipeRW(typing.NamedTuple):
    """A readable output and writable input pair of file descriptors.

    Though seemingly the same as a single pipe -- with a readable end
    and a writable end -- this structure is intended to collect *one*
    end of a *pair* of pipes.

    """
    output: int
    input: int


class Pipe(typing.NamedTuple):
    """An OS pipe consisting of a readable output end and a writable
    input end.

    """
    output: int
    input: int

    @classonlymethod
    def open(cls):
        """Create and construct a Pipe."""
        return cls._make(os.pipe())


class ScheduledTask(TaskConfDict):
    """Task extended for processing by the operating system."""

    #
    # state communication pipes
    #
    # we'll ensure that our state pipes are available (copied) to descriptors
    # 3 & 4 in the child process (for simplicity)
    #
    _state_child_ = PipeRW(input=3, output=4)

    #
    # in the parent process, each task's pipes will be provisioned once
    # (and file descriptors cached)
    #
    _statein_ = cachedproperty.static(Pipe.open)

    _stateout_ = cachedproperty.static(Pipe.open)

    @classonlymethod
    def schedule(cls, task, state):
        """Construct a ScheduledTask extending the specified Task."""
        self = cls(task, state)

        # force-link the new instance to its parent
        task.__parent__.__adopt__(task.__name__, self)

        return self

    def __init__(self, data, /, state):
        super().__init__(data)

        self.state = state

        self._future_ = None
        self.returncode = None
        self.stdout = None
        self.stderr = None

    def __adopt_parent__(self, name, mapping):
        if mapping.__parent__ is None:
            mapping.__parent__ = self
            return

        # we've likely taken over for existing configuration via schedule().
        # rather than insist that child is in our tree, merely check
        # that its tree looks the same.
        assert isinstance(mapping.__parent__, TaskConfDict)
        assert mapping.__parent__.__path__ == self.__path__

    @staticmethod
    def _dup_fd_(src, dst):
        """Duplicate (copy) file descriptor `src` to `dst`.

        `dst` *may not* be one of the standard file descriptors (0-2).
        `dst` is not otherwise checked.

        The duplicate descriptor is set inheritable.

        It is presumed that this method is used in the context of a
        process fork, *e.g.* as the `preexec_fn` of `subprocess.Popen`
        -- and with `close_fds=True`. (As such, any file descriptor may
        be available for use as `dst`.)

        """
        if src == dst:
            return

        if dst < 3:
            raise ValueError(f"will not overwrite standard file descriptor: {dst}")

        os.dup2(src, dst, inheritable=True)

    def _set_fds_(self):
        """Duplicate inherited state file descriptors to conventional
        values in the task subprocess.

        """
        for (parent, child) in zip(self._state_parent_, reversed(self._state_child_)):
            self._dup_fd_(parent, child)

    @property
    def _state_parent_(self):
        """The parent process's originals of its child's pair of
        readable and writable state file descriptors.

        """
        return PipeRW(self._statein_.output, self._stateout_.input)

    @property
    def _pass_fds_(self):
        """The child process's readable and writable state file
        descriptors -- *both* the originals and their desired
        conventional values.

        These descriptors must be inherited by the child process -- and
        not closed -- for inter-process communication of task state.

        """
        return self._state_parent_ + self._state_child_

    def __call__(self):
        """Execute the task's program in a background process.

        A ScheduledTask may only be executed once -- subsequent
        invocations are idempotent / no-op.

        Returns the execution-initiated ScheduledTask (self).

        """
        if self._future_ is None:
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

                state = self.state.read()

                with open(self._statein_.input, 'w') as file:
                    file.write(state)

                future = bound.run_bg(retcode=None,
                                      pass_fds=self._pass_fds_,
                                      preexec_fn=self._set_fds_)

                # close child's descriptors in parent process
                for parent_desc in self._state_parent_:
                    os.close(parent_desc)

            self._future_ = future

        return self

    def exception(self):
        """The in-process exception, raised by Task program
        initialization, if any.

        """
        return self._future_.exception if isinstance(self._future_, ImmediateFailure) else None

    def poll(self):
        """Return whether the Task program's process has exited.

        Sets the ScheduledTask's `returncode`, `stdout` and `stderr`
        when the process has exited.

        """
        if self._future_ is None:
            return False

        if ready := self._future_.poll():
            self.returncode = self._future_.returncode
            self.stdout = self._future_.stdout
            self.stderr = self._future_.stderr

            # Note: with retry this will also permit 42
            if self.returncode == 0:
                with open(self._stateout_.output) as file:
                    data = file.read()

                self.state.write(data)

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
