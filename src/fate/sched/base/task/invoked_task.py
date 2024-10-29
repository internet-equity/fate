"""Execution of scheduled tasks."""
from __future__ import annotations

import abc
import datetime
import io
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import typing
from dataclasses import dataclass, field

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


@dataclass(eq=False)
class BufferedOutput:
    """Buffer of data read from a given file object.

    The descriptor of the given file may have been set blocking or non-
    blocking. By default, it is assumed that this class is given a file
    whose descriptor has been configured for non-blocking reads. In this
    configuration, the `receive` method may be invoked regularly,
    without needlessly blocking execution. Any data that is received may
    be inspected at `data` or by casting the `BufferedOutput` object
    itself to `bytes` or `str`. (Note that this data may be incomplete.)

    Alternatively, a file with a blocking descriptor (the language
    default) may be given. In this case, `receive` will block until the
    file is completely read, (precisely the usual `file.read()`).

    """
    file: typing.BinaryIO
    data: bytes = field(default=b'', init=False)

    def __bytes__(self) -> bytes:
        return self.data

    def __str__(self) -> str:
        return self.data.decode()

    def __iadd__(self, chunk) -> BufferedOutput:
        self.data += chunk
        return self

    def receive(self) -> None:
        # data may be empty/None so we test it
        if read := self.file.read():
            self += read

    def close(self) -> None:
        self.file.close()


@dataclass(eq=False)
class StagedOutput(BufferedOutput):
    """High-performance buffer of data read from a given file object.

    StagedOutput operates like BufferedOutput, with the distinction that
    data is initially "staged", (in an internal list), for improved
    performance. Upon close, this staged data is gathered into user-
    readable data, (available by casting the object to str or bytes).

    The descriptor of the given file object is presumed to be non-
    blocking. This implementation is only necessary when performing very
    large numbers of repeated read operations.

    See: `ProgressiveOutput`.

    """
    _stage: list = field(default_factory=list, init=False)

    def __iadd__(self, chunk) -> StagedOutput:
        self._stage.append(chunk)
        return self

    def close(self) -> None:
        super().close()
        self.data += b''.join(self._stage)
        self._stage.clear()


class ProgressiveOutput(StagedOutput, threading.Thread):
    """Buffer of data which may be read from the given file object in a
    parallel thread.

    As a StagedOutput, the descriptor of the given file object is
    presumed to have been set non-blocking. A new daemon thread may be
    launched via the `start` method, which will (repeatedly) read the
    given file and store its output.

    Read data may be made available for inspection, and the read file
    closed, via the `stop` (alias `close`) method.

    """
    def __init__(self, file: typing.BinaryIO, thread_name: str | None):
        super().__init__(file)
        threading.Thread.__init__(self, name=thread_name, daemon=True)
        self._closed = threading.Event()

    def run(self):
        while not self._closed.is_set():
            time.sleep(1e-6)
            self.receive()

    def close(self):
        self._closed.set()
        self.join()
        super().close()

    stop = close


@dataclass(eq=False)
class BufferedInput:
    """Buffer of data which is written to a given file object in chunks.

    The descriptor of the given file may have been set blocking or non-
    blocking. By default, it is assumed that this class is given a file
    whose descriptor has been configured for non-blocking writes. In
    this configuration, the `send` method may be invoked regularly,
    without needlessly blocking execution.

    """
    data: bytes
    file: typing.BinaryIO
    buffersize: int = io.DEFAULT_BUFFER_SIZE
    position: int = field(default=0, init=False)

    @cachedproperty
    def datasize(self):
        return len(self.data)

    @property
    def finished(self) -> bool:
        return self.file.closed

    def send(self) -> None:
        if self.finished:
            return

        chunk = self.data[self.position:self.buffersize]
        self.file.write(chunk)

        self.position = min(self.position + self.buffersize, self.datasize)

        if self.position == self.datasize:
            try:
                self.file.close()
            except BrokenPipeError:
                pass


def progressive_output(file: typing.BinaryIO, name: str | None) -> ProgressiveOutput:
    """Launch a `ProgressiveOutput` reader in a parallel thread."""
    os.set_blocking(file.fileno(), False)
    reader = ProgressiveOutput(file, name)
    reader.start()
    return reader


def nonblocking_output(file: typing.BinaryIO) -> BufferedOutput:
    """Construct a `BufferedOutput` non-blocking reader of the given
    file.

    The descriptor of the given file is set non-blocking.

    """
    os.set_blocking(file.fileno(), False)
    return BufferedOutput(file)


def nonblocking_input(file: typing.BinaryIO, data: bytes) -> BufferedInput:
    """Construct a `BufferedInput` non-blocking writer of the `data` to
    `file`.

    The descriptor of the given file is set non-blocking.

    """
    os.set_blocking(file.fileno(), False)
    return BufferedInput(data, file)


class _TaskProcess(typing.NamedTuple):

    process: subprocess.Popen
    stdin: BufferedInput
    statein: BufferedInput
    stdout: ProgressiveOutput
    stderr: BufferedOutput
    stateout: BufferedOutput


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

    @cachedproperty
    def _stateinfile_(self) -> typing.BinaryIO:
        return open(self._statein_.input, 'wb')

    @cachedproperty
    def _stateoutfile_(self) -> typing.BinaryIO:
        return open(self._stateout_.output, 'rb')

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
        self._started_ = None
        self._ended_ = None

        self.terminated_ = None
        self.killed_ = None

        self.stdout_ = None
        self.stderr_ = None
        self.stdin_ = None
        self.statein_ = None
        self.stateout_ = None

    @property
    def stopped_(self) -> typing.Optional[float]:
        return self.killed_ or self.terminated_

    def _spawn_(self):
        if self._process_ is not None:
            raise ValueError("task already spawned")

        (
            self._process_,
            self.stdin_,
            self.statein_,
            self.stdout_,
            self.stderr_,
            self.stateout_,
        ) = self._popen()

        self._started_ = time.time()

    def _preexec_legacy(self) -> None:
        # Assign (duplicate) state file descriptors to expected values
        self._set_fds_()

        # Assign the child process its own new process group
        os.setpgrp()

    def _popen(self) -> _TaskProcess:
        (program, *args) = self.exec_

        executable = shutil.which(program)

        if executable is None:
            raise TaskInvocationError(f'command not found on path: {program}')

        # We prefer to have Popen handle child setup as much as possible
        # so we opt into new features as they become available.
        if sys.version_info < (3, 11):
            # Popen doesn't yet offer process_group so we'll do it ourselves
            kwargs = dict(
                preexec_fn=self._preexec_legacy,
            )
        else:
            # We can just use process_group
            kwargs = dict(
                # Assign the child process its own new process group
                process_group=0,

                preexec_fn=self._set_fds_,
            )

        process = subprocess.Popen(
            [executable] + args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            pass_fds=self._pass_fds_,
            **kwargs,
        )

        result = _TaskProcess(
            process=process,

            # stdout needn't be inspected until the task completes; and, synchronous, non-blocking
            # processing of the pipe is relatively inefficient (for large payloads). instead,
            # we'll launch a daemon thread to sit on it and read it as efficiently as possible.
            stdout=progressive_output(process.stdout,
                                      f'Reader ({self.__name__} {process.pid}): stdout'),

            # we don't expect any other IPC data to be huge; and, at least in the case of
            # stderr, we want to inspect it as it comes in.
            #
            # for simplicity: make pipe descriptors non-blocking & initialize buffer handlers
            #
            # (note: this works for pipes on Win32 but only as of Py312)
            stderr=nonblocking_output(process.stderr),
            stateout=nonblocking_output(self._stateoutfile_),

            stdin=nonblocking_input(process.stdin, self.param_.encode()),
            statein=nonblocking_input(self._stateinfile_, self._state_.read().encode()),
        )

        # write inputs (at least up to buffer size)
        result.stdin.send()
        result.statein.send()

        # close child's descriptors in parent process
        for parent_desc in self._state_parent_:
            os.close(parent_desc)

        return result

    def started_(self) -> typing.Optional[float]:
        return self._started_

    def ended_(self) -> typing.Optional[float]:
        if self._ended_ is None:
            self.poll_()

        return self._ended_

    def duration_(self) -> typing.Optional[datetime.timedelta]:
        return (ended := self.ended_()) and datetime.timedelta(seconds=ended - self.started_())

    def expires_(self) -> typing.Optional[float]:
        if (started := self.started_()) is None:
            return None

        if (timeout := self.timeout_) is None:
            return None

        return started + timeout.total_seconds()

    def expired_(self) -> bool:
        expires = self.expires_()
        return expires is not None and expires <= time.time()

    def _signal(self, signal) -> None:
        if self._process_ is None:
            raise ValueError("task not spawned")

        if os.getpgid(self._process_.pid) == self._process_.pid:
            # as expected: signal group
            os.killpg(self._process_.pid, signal)
        else:
            # unexpected: stick to process itself
            self._process_.send_signal(signal)

    def _terminate_(self) -> None:
        self._signal(signal.SIGTERM)
        self.terminated_ = time.time()

    def _kill_(self) -> None:
        self._signal(signal.SIGKILL)
        self.killed_ = time.time()

    def poll_(self) -> typing.Optional[int]:
        """Check whether the task program has exited and return its exit
        code if any.

        If the task has expired, i.e. run past a configured timeout,
        this method sends the process the TERM signal; if execution has
        continued past this, the KILL signal is sent.

        BufferedInput and BufferedOutput handlers are invoked to send/
        receive remaining data.

        Sets the SpawnedTask's `ended` time, and records the task's
        state output, when the process has terminated.

        """
        if self.expired_() and self._process_.poll() is None:
            if self.terminated_ is None:
                self._terminate_()
            else:
                self._kill_()

        returncode = self._process_.poll()

        self.stdin_.send()
        self.statein_.send()

        self.stderr_.receive()
        self.stateout_.receive()

        if returncode is not None and self._ended_ is None:
            self._ended_ = time.time()

            self.stdout_.close()

            # Note: with retry this will also permit 42
            if returncode == 0:
                self._state_.write(str(self.stateout_))

        return returncode

    def ready_(self) -> bool:
        """Return whether the task program's process has terminated.

        See poll_().

        """
        return self.poll_() is not None

    def logs_(self):
        """Parse LogRecords from `stderr_`.

        Raises LogsDecodingError to indicate decoding errors when the
        encoding of a task's stderr log output is configured explicitly.
        Note, in this case, the parsed logs *may still* be retrieved
        from the exception.

        """
        if self.stderr_ is None:
            return None

        stream = self._iter_logs_(bytes(self.stderr_))
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
        return self._result_(bytes(self.__parent__.stdout_), *args, **kwargs)
