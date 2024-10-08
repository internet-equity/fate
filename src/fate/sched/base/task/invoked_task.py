"""Execution of scheduled tasks."""
import abc
import os
import shutil
import subprocess
import typing

from descriptors import cachedproperty, classonlymethod

from fate.conf.error import LogsDecodingError
from fate.conf.types import TaskChainMap
from fate.util.datastructure import at_depth, adopt

from .ext import BoundTask, TaskConfExt


class TaskInvocationError(LookupError):
    """Exception raised for a task whose command could not be invoked."""


class InvokedTask(TaskConfExt):
    """Abstract base to task classes extended for invocation of their
    commands by the operating system.

    """
    @abc.abstractmethod
    def ready_(self) -> bool:
        pass


class FailedInvocationTask(InvokedTask):
    """Task whose command could not be invoked."""

    @InvokedTask._constructor_
    def fail(cls, task, err):
        return cls(task, err)

    def __init__(self, data, /, err):
        super().__init__(data)
        self.error = err

    def ready_(self) -> bool:
        return True


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


class SpawnedTask(BoundTask, InvokedTask):
    """Task whose process has been spawned."""

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

    @InvokedTask._constructor_
    def spawn(cls, task, state):
        """Construct a SpawnedTask extending the specified Task."""
        spawned = cls(task, state)

        # _constructor_ would otherwise handle linking/adoption for us;
        # but, we need this in order to spawn, so we'll do it here:
        cls._link_(spawned, task)

        spawned._spawn_()

        return spawned

    def __init__(self, data, /, state):
        super().__init__(data, state)

        self._process_ = None

        self.returncode_ = None
        self.stdout_ = None
        self.stderr_ = None

    def _spawn_(self):
        self._process_ = self._popen()

    def _popen(self) -> subprocess.Popen:
        (program, *args) = self.exec_

        executable = shutil.which(program)

        if executable is None:
            raise TaskInvocationError(f'command not found on path: {program}')

        process = subprocess.Popen(
            [executable] + args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            pass_fds=self._pass_fds_,
            preexec_fn=self._set_fds_,
        )

        # write inputs
        process.stdin.write(self.param_.encode())

        try:
            process.stdin.close()
        except BrokenPipeError:
            pass

        with open(self._statein_.input, 'w') as file:
            file.write(self._state_.read())

        # close child's descriptors in parent process
        for parent_desc in self._state_parent_:
            os.close(parent_desc)

        return process

    def _poll_(self) -> int | None:
        """Check whether the task program has exited and return its exit
        code.

        Sets the SpawnedTask's `returncode`, `stdout` and `stderr`,
        and records the task's state output, when the process has indeed
        terminated.

        """
        returncode = self._process_.poll()

        if returncode is not None and self.returncode_ is None:
            self.returncode_ = returncode

            self.stdout_ = self._process_.stdout.read()
            self.stderr_ = self._process_.stderr.read()

            # Note: with retry this will also permit 42
            if returncode == 0:
                with open(self._stateout_.output) as file:
                    data = file.read()

                self._state_.write(data)

        return returncode

    def ready_(self) -> bool:
        """Return whether the task program's process has terminated.

        See _poll_().

        """
        return self._poll_() is not None

    def logs_(self):
        """Parse LogRecords from `stderr`.

        Raises LogsDecodingError to indicate decoding errors when the
        encoding of a task's stderr log output is configured explicitly.
        Note, in this case, the parsed logs *may still* be retrieved
        from the exception.

        """
        if self.stderr_ is None:
            return None

        stream = self._iter_logs_(self.stderr_)
        logs = tuple(stream)

        if stream.status.errors:
            raise LogsDecodingError(*stream.status, logs)

        return logs

    @property
    @adopt('path')
    def path_(self):
        default = super().path_
        return SpawnedTaskChainMap(*default.maps)


class SpawnedTaskChainMap(TaskChainMap):

    @at_depth('*.path')
    def result_(self, *args, **kwargs):
        return self._result_(self.__parent__.stdout_, *args, **kwargs)
