"""Execution of scheduled tasks."""
from __future__ import annotations

import datetime
import os
import shutil
import signal
import subprocess
import sys
import time
import typing

from descriptors import cachedproperty, classonlymethod

from fate.common.log import LogReader
from fate.common.output import CompletingTask
from fate.util import stream

from .. import ext

from .base import InvokedTask
from .event import TaskEvents, TaskLogEvent, TaskReadyEvent
from .failed_task import TaskInvocationError


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


class _TaskProcess(typing.NamedTuple):

    process: subprocess.Popen
    stdin: stream.BufferedInput
    statein: stream.BufferedInput
    stdout: stream.ProgressiveOutput
    stderr: stream.BufferedOutput
    stateout: stream.BufferedOutput


class SpawnedTask(ext.BoundTask, InvokedTask, CompletingTask):
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

        self._events_ = None
        self._log_reader = None

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

        self._events_ = TaskEvents()
        self._log_reader = LogReader(self.stderr_)

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
            stdout=stream.progressive_output(process.stdout,
                                             f'Reader ({self.__name__} {process.pid}): stdout'),

            # we don't expect any other IPC data to be huge; and, at least in the case of
            # stderr, we want to inspect it as it comes in.
            #
            # for simplicity: make pipe descriptors non-blocking & initialize buffer handlers
            #
            # (note: this works for pipes on Win32 but only as of Py312)
            stderr=stream.nonblocking_output(process.stderr),
            stateout=stream.nonblocking_output(self._stateoutfile_),

            stdin=stream.nonblocking_input(process.stdin, self.param_.encode()),
            statein=stream.nonblocking_input(self._stateinfile_, self._state_.read().encode()),
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

    def events_(self) -> typing.Optional[TaskEvents]:
        if self._events_ is not None and not self._events_.closed:
            self.poll_()

        return self._events_

    def _record_events_(self, returncode: typing.Optional[int]) -> None:
        if self._events_ is None:
            raise ValueError("task not spawned")

        if self._events_.closed:
            return

        final = returncode is not None

        for record in self._log_reader.read(final):
            self._events_.write(TaskLogEvent(self, record))

        if final:
            self._events_.write(TaskReadyEvent(self, returncode))
            self._events_.close()

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

        self._record_events_(returncode)

        return returncode

    def ready_(self) -> bool:
        """Return whether the task program's process has terminated.

        See poll_().

        """
        return self.poll_() is not None
