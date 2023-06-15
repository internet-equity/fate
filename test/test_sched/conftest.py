import pytest

from .fixture import SchedulerFixture


@pytest.fixture
def schedpatch(confpatch):
    return SchedulerFixture(confpatch.conf, confpatch.logger)
