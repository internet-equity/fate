from __future__ import annotations

import functools
import signal
import sys
import typing
import weakref


class timeout:

    disabling_events = frozenset(['pdb.Pdb'])

    _running = weakref.WeakSet()

    @classmethod
    def _audit_hook(cls, name: str, *args) -> None:
        if name in cls.disabling_events:
            for timeout in list(cls._running):
                timeout.disable()

    def __init__(self, seconds: float) -> None:
        self.seconds = seconds

    def __enter__(self) -> timeout:
        self.enable()
        return self

    def __exit__(self, *exc_info) -> None:
        self.disable()

    def __call__(self, func) -> typing.Callable:
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            with self:
                return func(*args, **kwargs)

        return wrapped

    def enable(self) -> None:
        signal.signal(signal.SIGALRM, self.raise_timeout)
        signal.alarm(self.seconds)

        self._running.add(self)

    def disable(self) -> None:
        signal.alarm(0)

        self._running.discard(self)

    def raise_timeout(self, signum, frame) -> None:
        raise self.Timeout(self.seconds, signum, frame)

    class Timeout(RuntimeError):

        def __init__(self, seconds, signum, frame):
            self.seconds = seconds
            self.signal = signal.Signals(signum)
            self.frame = frame

        def __str__(self):
            return f"timed out after {self.seconds}s by {self.signal.name} at {self.frame}"

        def __repr__(self):
            return f"{self.__class__.__name__}({self.seconds}, {self.signal.value}, {self.frame})"


sys.addaudithook(timeout._audit_hook)
