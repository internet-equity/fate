import pytest

from .fixture import (
    LockingTaskFixture,
    SchedulerFixture,
)


@pytest.fixture
def schedpatch(confpatch):
    return SchedulerFixture(confpatch.conf, confpatch.logger)


@pytest.fixture
def locking_task(tmp_path):
    lock = LockingTaskFixture(tmp_path)

    lock.acquire()

    try:
        yield lock
    finally:
        if lock.locked:
            lock.release()
