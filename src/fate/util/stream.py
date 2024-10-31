from __future__ import annotations

import io
import os
import threading
import time
import typing
from dataclasses import dataclass, field

from descriptors import cachedproperty


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
