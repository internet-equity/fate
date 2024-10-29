import time


class StopWatch:

    def __init__(self):
        self.started = None
        self.stopped = None

    def start(self):
        if self.started is not None:
            raise ValueError("already started")

        self.started = time.time()

    def stop(self):
        if self.started is None:
            raise ValueError("not started")

        if self.stopped is not None:
            raise ValueError("already stopped")

        self.stopped = time.time()

    @property
    def seconds(self):
        if self.started is None:
            raise ValueError("not started")

        if self.stopped is None:
            raise ValueError("not stopped")

        return self.stopped - self.started

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *exc_info):
        self.stop()
