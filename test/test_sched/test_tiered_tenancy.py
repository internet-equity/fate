import gzip
import os
import re
import signal
import textwrap
import time
from collections import deque

import pytest

from fate import sched

from test.fixture import StopWatch


class TimeMock:

    def __init__(self, *times, sleep=time.sleep):
        self.sleep = sleep
        self._times_ = deque(times)
        self._past_ = []

    def time(self):
        time = self._times_.popleft()
        self._past_.append(time)
        return time


def test_due(confpatch, schedpatch):
    #
    # configure a single task which should run
    #
    confpatch.set_tasks(
        {
            'run-me': {
                'exec': ['echo', 'done'],
                'schedule': "H/5 * * * *",
            },
        }
    )

    #
    # set up scheduler with a long-previous check s.t. task should execute
    #
    schedpatch.set_last_check(offset=3600)

    #
    # execute scheduler with captured logs
    #
    with confpatch.caplog() as logs:
        completed_tasks = list(schedpatch.scheduler())

    #
    # task should run and this should be logged
    #
    assert len(completed_tasks) == 1

    (task,) = completed_tasks
    assert task.poll_() == 0
    assert str(task.stdout_) == 'done\n'
    assert str(task.stderr_) == ''

    assert logs.field_equals(completed=1, total=1, active=0)


def test_skips(confpatch, schedpatch, monkeypatch):
    #
    # configure a single task which should be skipped
    #
    monkeypatch.delenv('TESTY', raising=False)

    confpatch.set_tasks(
        {
            'skip-me': {
                'exec': ['echo', 'done'],
                'schedule': "H/5 * * * *",
                'if': 'env.TESTY | default("0") | int == 1',
            },
        }
    )

    #
    # set up scheduler with a long-previous check s.t. task should otherwise execute
    #
    schedpatch.set_last_check(offset=3600)

    #
    # execute scheduler with captured logs
    #
    with confpatch.caplog('INFO') as logs:
        completed_tasks = list(schedpatch.scheduler())

    #
    # task should NOT run and this should be logged
    #
    assert len(completed_tasks) == 0

    assert logs.field_equals(msg='skipped: suppressed by if/unless condition')


def test_binary_result(confpatch, schedpatch):
    #
    # configure a binary-producing task
    #
    confpatch.set_tasks(
        {
            'binary': {
                'exec': ['gzip', '-c'],
                'schedule': "H/5 * * * *",
                'param': 'very special characters\n\n(really)\n',
            },
        }
    )

    #
    # set up scheduler with a long-previous check s.t. task should execute
    #
    schedpatch.set_last_check(offset=3600)

    #
    # execute scheduler with captured logs
    #
    with confpatch.caplog() as logs:
        completed_tasks = list(schedpatch.scheduler())

    #
    # task should run and this should be logged
    #
    assert len(completed_tasks) == 1

    (task,) = completed_tasks

    assert task.poll_() == 0

    assert gzip.decompress(bytes(task.stdout_)) == confpatch.conf.task.binary.param.encode()

    assert bytes(task.stderr_) == b''

    assert logs.field_equals(completed=1, total=1, active=0)


def test_large_result(confpatch, schedpatch):
    #
    # configure a task producing significant output
    #
    HUNDRED_MB = 100 * 1024 ** 2

    confpatch.set_tasks(
        {
            'big': {
                'shell': f'head -c {HUNDRED_MB} </dev/zero',
                'schedule': "H/5 * * * *",
            },
        }
    )

    #
    # set up scheduler with a long-previous check s.t. task should execute
    #
    schedpatch.set_last_check(offset=3600)

    #
    # execute scheduler with captured logs
    #
    with (
        confpatch.caplog() as logs,
        StopWatch() as session
    ):
        completed_tasks = list(schedpatch.scheduler())

    #
    # task should run and this should be logged
    #
    assert len(completed_tasks) == 1

    (task,) = completed_tasks

    assert task.poll_() == 0

    assert str(task.stderr_) == ''

    assert logs.field_equals(completed=1, total=1, active=0)

    assert task.duration_().total_seconds() < 1

    assert session.seconds < 1

    assert len(bytes(task.stdout_)) == HUNDRED_MB


def test_timeout_noop(confpatch, schedpatch):
    #
    # configure a task with an easy-to-match timeout
    #
    confpatch.set_tasks(
        {
            'easy-timeout': {
                'exec': ['echo', 'done'],
                'schedule': "H/5 * * * *",
                'timeout': 60,  # seconds
            },
        }
    )

    #
    # set up scheduler with a long-previous check s.t. task should execute
    #
    schedpatch.set_last_check(offset=3600)

    #
    # execute scheduler with captured logs
    #
    with confpatch.caplog() as logs:
        completed_tasks = list(schedpatch.scheduler())

    #
    # task should run and this should be logged
    #
    assert len(completed_tasks) == 1

    (task,) = completed_tasks

    assert task.poll_() == 0

    assert bytes(task.stdout_) == b'done\n'

    assert bytes(task.stderr_) == b''

    assert logs.field_equals(completed=1, total=1, active=0)


def test_timeout_child(confpatch, schedpatch):
    #
    # configure a task with an impossible timeout created by a well-behaving child process
    #
    confpatch.set_tasks(
        {
            'impossible-timeout': {
                # bash *may* feature a "sleep" built-in so we can pause the child in-process
                # but, this is not reliable -- so, we'll use python
                'shell': {
                    'executable': 'python',
                    'script': textwrap.dedent('''\
                        import os, time

                        print('started:', os.getpid(), os.getpgid(0), flush=True)

                        time.sleep(10)

                        print('finished')
                    '''),
                },
                'schedule': "H/5 * * * *",
                'timeout': '1s',
            },
        }
    )

    #
    # set up scheduler with a long-previous check s.t. task should execute
    #
    schedpatch.set_last_check(offset=3600)

    #
    # execute scheduler with captured logs
    #
    with (
        confpatch.caplog() as logs,
        StopWatch() as session
    ):
        completed_tasks = list(schedpatch.scheduler())

    #
    # task should run and this should be logged
    #
    assert len(completed_tasks) == 1

    (task,) = completed_tasks

    assert task.poll_() == -signal.SIGTERM

    assert str(task.stderr_) == ''

    assert logs.field_equals(completed=1, total=1, active=0)

    assert task.ended_() >= task.expires_()

    assert 1 <= task.duration_().total_seconds() < 2

    assert 1 <= session.seconds < 2

    # we shouldn't see "finished"
    stdout_match = re.fullmatch(
        r'started: (?P<cpid>\d+) +(?P<cpgid>\d+)\n',
        str(task.stdout_)
    )

    assert stdout_match, str(task.stdout_)

    (cpid, cpgid) = (int(group) for group in (stdout_match['cpid'],
                                              stdout_match['cpgid']))

    assert cpid == cpgid == task._process_.pid

    # nothing should remain in process group
    with pytest.raises(ProcessLookupError):
        os.killpg(cpgid, 0)


def test_timeout_child_trap(confpatch, schedpatch):
    #
    # configure a task with an impossible timeout created by a misbehaving child process
    #
    confpatch.set_tasks(
        {
            'impossible-timeout': {
                # bash *may* feature a "sleep" built-in so we can pause the child in-process
                # but, this is not reliable -- so, we'll use python
                'shell': {
                    'executable': 'python',
                    'script': textwrap.dedent('''\
                        import os, signal, time

                        print('started:', os.getpid(), os.getpgid(0), flush=True)

                        signal.signal(signal.SIGTERM, signal.SIG_IGN)

                        time.sleep(10)

                        print('finished')
                    '''),
                },
                'schedule': "H/5 * * * *",
                'timeout': '1s',
            },
        }
    )

    #
    # set up scheduler with a long-previous check s.t. task should execute
    #
    schedpatch.set_last_check(offset=3600)

    #
    # execute scheduler with captured logs
    #
    with (
        confpatch.caplog() as logs,
        StopWatch() as session
    ):
        completed_tasks = list(schedpatch.scheduler())

    #
    # task should run and this should be logged
    #
    assert len(completed_tasks) == 1

    (task,) = completed_tasks

    assert task.poll_() == -signal.SIGKILL

    assert str(task.stderr_) == ''

    assert logs.field_equals(completed=1, total=1, active=0)

    assert task.ended_() >= task.expires_()

    assert 1 <= task.duration_().total_seconds() < 2

    assert 1 <= session.seconds < 2

    # we shouldn't see "finished"
    stdout_match = re.fullmatch(
        r'started: (?P<cpid>\d+) +(?P<cpgid>\d+)\n',
        str(task.stdout_)
    )

    assert stdout_match, str(task.stdout_)

    (cpid, cpgid) = (int(group) for group in (stdout_match['cpid'],
                                              stdout_match['cpgid']))

    assert cpid == cpgid == task._process_.pid

    # nothing should remain in process group
    with pytest.raises(ProcessLookupError):
        os.killpg(cpgid, 0)


def test_timeout_grandchild(confpatch, schedpatch):
    #
    # configure a task with an impossible timeout created by a well-behaving --
    # but file descriptor-inheriting -- grandchild process.
    #
    confpatch.set_tasks(
        {
            'impossible-timeout': {
                'shell': '''\
                    pgid="$(ps -o pgid= -p $$)"
                    echo "started: $$ $pgid"

                    sh -c '
                        pgid="$(ps -o pgid= -p $$)"
                        echo "grandchild: $$ $pgid"
                        sleep 10
                    '

                    echo finished
                ''',
                'schedule': "H/5 * * * *",
                'timeout': '1s',
            },
        }
    )

    #
    # set up scheduler with a long-previous check s.t. task should execute
    #
    schedpatch.set_last_check(offset=3600)

    #
    # execute scheduler with captured logs
    #
    with (
        confpatch.caplog() as logs,
        StopWatch() as session
    ):
        completed_tasks = list(schedpatch.scheduler())

    #
    # task should run and this should be logged
    #
    assert len(completed_tasks) == 1

    (task,) = completed_tasks

    assert task.poll_() == -signal.SIGTERM

    assert str(task.stderr_) == ''

    assert logs.field_equals(completed=1, total=1, active=0)

    assert task.ended_() >= task.expires_()

    assert 1 <= task.duration_().total_seconds() < 2

    assert 1 <= session.seconds < 2

    # we shouldn't see "finished"
    stdout_match = re.fullmatch(
        r'started: (?P<cpid>\d+) +(?P<cpgid>\d+)\n'
        r'grandchild: (?P<gpid>\d+) +(?P<gpgid>\d+)\n',
        str(task.stdout_)
    )

    assert stdout_match, str(task.stdout_)

    (cpid, cpgid, gpid, gpgid) = (int(group) for group in (stdout_match['cpid'],
                                                           stdout_match['cpgid'],
                                                           stdout_match['gpid'],
                                                           stdout_match['gpgid']))

    assert cpid == task._process_.pid

    assert gpid != task._process_.pid

    assert cpid == cpgid == gpgid

    # grandchild was stopped as well
    with pytest.raises(ProcessLookupError):
        os.kill(gpid, 0)

    # nothing should remain in process group
    with pytest.raises(ProcessLookupError):
        os.killpg(cpgid, 0)


def test_timeout_grandchild_trap(confpatch, schedpatch):
    #
    # configure a task with an impossible timeout created by a misbehaving --
    # and file descriptor-inheriting -- grandchild process.
    #
    confpatch.set_tasks(
        {
            'impossible-timeout': {
                # we want to configure how the sleeping subprocess operates,
                # so we'll use python again, but also force it into its own
                # subprocess via "shell" (like: sh -c 'python -c ...')
                'shell': {
                    'executable': 'bash',
                    'script': textwrap.dedent('''\
                        python <<<"
                        import os, signal, time

                        print('started:', os.getpid(), os.getpgid(0), flush=True)

                        signal.signal(signal.SIGTERM, signal.SIG_IGN)

                        time.sleep(10)

                        print('finished')
                        "
                    '''),
                },
                'schedule': "H/5 * * * *",
                'timeout': '1s',
            },
        }
    )

    #
    # set up scheduler with a long-previous check s.t. task should execute
    #
    schedpatch.set_last_check(offset=3600)

    #
    # execute scheduler with captured logs
    #
    with (
        confpatch.caplog() as logs,
        StopWatch() as session
    ):
        completed_tasks = list(schedpatch.scheduler())

    #
    # task should run and this should be logged
    #
    assert len(completed_tasks) == 1

    (task,) = completed_tasks

    assert task.poll_() == -signal.SIGTERM

    assert str(task.stderr_) == ''

    assert logs.field_equals(completed=1, total=1, active=0)

    assert task.ended_() >= task.expires_()

    assert 1 <= task.duration_().total_seconds() < 2

    assert 1 <= session.seconds < 2

    # we shouldn't see "finished"
    stdout_match = re.fullmatch(
        r'started: (?P<gpid>\d+) +(?P<gpgid>\d+)\n',
        str(task.stdout_)
    )

    assert stdout_match, str(task.stdout_)

    (gpid, gpgid) = (int(group) for group in (stdout_match['gpid'],
                                              stdout_match['gpgid']))

    assert gpid != task._process_.pid

    assert gpgid == task._process_.pid

    # grandchild was *not* stopped (but we continued nonetheless)
    os.kill(gpid, 0)  # this check-in won't raise an exception

    # grandchild remains in process group
    os.killpg(gpgid, 0)


def test_refill_primary_cohort(locking_task, confpatch, schedpatch, monkeypatch, tmp_path):
    #
    # configure a long-running task kicked off at minute 0 and another task at minute 1
    #
    # if they were both short tasks, this would not exercise a "recheck" / "refill" -- only
    # because the initial task is still running past the time that the subsequent task is
    # scheduled, the scheduler performs a recheck which picks up the subsequent task, and with
    # which it refills its queue.
    #
    # because there's no tenancy-related hold-up, the primary cohort (initial check) is
    # immediately enqueued, and the refill simply recreates this cohort.
    #
    # (really the initial task will just wait on release of a file lock.
    # as such, the initial task will run only as long as needed for the test.)
    #
    confpatch.set_tasks(
        {
            'runs-long': {
                **locking_task.conf,
                'schedule': '0 * * * *',
            },
            'runs-late': {
                'exec': ['echo', 'done'],
                'schedule': '1 * * * *',
            },
        }
    )

    #
    # set up & patch scheduler s.t. initial task will start and
    # recheck/refill will immediately trigger
    #
    schedpatch.set_last_check(-60)  # one minute before the epoch

    monkeypatch.setattr(
        'fate.sched.base.timing.time',
        TimeMock(
            0.001,   # first check time: cron minute is 0
            60.001,  # second check time (immediately following recheck)
        )
    )

    # scheduler loop must also check current time
    #
    # recheck 0: one minute into the epoch: cron minute is 1
    # recheck 1: (non-refill): nothing to do
    # recheck n: (non-refill): nothing to do (number depends on OS scheduler -- we patch)
    #
    # assuming 1ms pauses we'll advance the clock 2ms each time:
    check_times = (step / 1_000 for step in range(60_000, 65_000, 2))

    monkeypatch.setattr(
        'fate.sched.tiered_tenancy.time',
        TimeMock(
            *check_times,
        )
    )

    #
    # execute scheduler with captured logs
    #
    with confpatch.caplog() as logs:
        #
        # task "runs-long" is blocked and we've patched the scheduler loop's time s.t.
        # a minute will immediately appear to have passed -- therefore the first task
        # to complete should be "runs-late", enqueued by the re-check.
        #
        tasks = schedpatch.scheduler()

        task0 = next(tasks)

        assert task0.__name__ == 'runs-late'
        assert isinstance(task0, sched.SpawnedTask)
        assert task0.poll_() == 0
        assert bytes(task0.stdout_) == b'done\n'
        assert bytes(task0.stderr_) == b''

        #
        # the primary cohort will have enqueued twice -- for "runs-long" and then
        # for the refill's "runs-late".
        #
        assert logs.field_count(level='debug', cohort=0, size=1, msg="enqueued cohort") == 2

        assert logs.field_equals(level='debug', active=1, msg="launched pool")
        assert logs.field_equals(level='debug', active=1, msg="expanded pool")
        assert logs.field_equals(level='debug', active=2, msg="filled pool")

        # permit "runs-long" to (finally) complete
        locking_task.release()

        # exhaust the scheduler of completed tasks
        (task1,) = tasks

        assert task1.__name__ == 'runs-long'
        assert isinstance(task1, sched.SpawnedTask)
        assert task1.poll_() == 0
        assert str(task1.stdout_) == locking_task.result
        assert str(task1.stderr_) == ''

        assert logs.field_equals(level='debug', completed=1, total=1, active=1)
        assert logs.field_equals(level='debug', completed=1, total=2, active=0)

    assert tasks.info.count == 2
    assert tasks.info.next == 3600  # one hour past the epoch


def test_refill_secondary_cohort(locking_task, confpatch, schedpatch, monkeypatch, tmp_path):
    #
    # configure a long-running single-tenancy task kicked off at minute 0, along with another
    # task also at minute 0, and another task at minute 1.
    #
    # if not for the long-running single-tenancy task, creating a backlog in its minute-zero
    # cohort, this would not exercise the secondary cohort functionality. only because the long-
    # running task must run alone, the second task is not executed, and their initial cohort
    # remains in the queue. the long-running task (eventually) forces a "recheck" / "refill", and
    # the third task must be added to a second (lower-priority) cohort.
    #
    # (really the initial task will just wait on release of a file lock ... which we'll control
    # in test / with a patch, to fully control the task's timing.)
    #
    confpatch.set_tasks(
        {
            'runs-long': {
                **locking_task.conf,
                'schedule': '0 * * * *',
                'scheduling': {'tenancy': 1},
            },
            'on-deck': {
                'exec': ['echo', 'done'],
                'schedule': '0 * * * *',
            },
            'runs-late': {
                'exec': ['echo', 'done'],
                'schedule': '1 * * * *',
            },
        }
    )

    #
    # set up & patch scheduler s.t. initial task will start and
    # recheck/refill will immediately trigger
    #
    def patched_sleep(duration):
        """release task "runs-long" during first sleep"""
        if locking_task.locked:
            locking_task.release()

        return time.sleep(duration)

    schedpatch.set_last_check(-60)  # one minute before the epoch

    # scheduler "timing" caches "check time" -- unless reset for a "refill"
    monkeypatch.setattr(
        'fate.sched.base.timing.time',
        TimeMock(
            0.001,   # first check time: cron minute is 0
            60.001,  # second check time (immediately following recheck)
        )
    )

    # scheduler loop must also check current time
    #
    # recheck 0: one minute into the epoch: cron minute is 1
    # recheck 1: (non-refill): nothing to do
    # recheck n: (non-refill): nothing to do (number depends on OS scheduler -- we patch)
    check_times = (step / 1_000 for step in range(60_000, 65_000, 2))

    monkeypatch.setattr(
        'fate.sched.tiered_tenancy.time',
        TimeMock(
            *check_times,
            sleep=patched_sleep,
        )
    )

    #
    # execute scheduler with captured logs
    #
    with confpatch.caplog() as logs:
        tasks = schedpatch.scheduler()

        task0 = next(tasks)

        assert task0.__name__ == 'runs-long'
        assert isinstance(task0, sched.SpawnedTask)
        assert str(task0.stdout_) == locking_task.result
        assert str(task0.stderr_) == ''

        assert logs.field_equals(level='debug', cohort=0, size=2, msg="enqueued cohort")
        assert logs.field_equals(level='debug', active=1, msg="launched pool")
        assert logs.field_equals(level='debug', cohort=1, size=1, msg="enqueued cohort")

        #
        # Issue #28: RuntimeError: "deque mutated during iteration"
        #
        # (during subsequent enqueuing of task "runs-late" and clean-up of primary cohort)
        #
        task1 = next(tasks)

        assert task1.__name__ == 'on-deck'
        assert isinstance(task1, sched.SpawnedTask)
        assert bytes(task1.stdout_) == b'done\n'
        assert bytes(task1.stderr_) == b''

        assert logs.field_equals(level='debug', completed=1, total=1, active=1)
        assert logs.field_equals(level='debug', active=2, msg="expanded pool")

        (task2,) = tasks

        assert task2.__name__ == 'runs-late'
        assert isinstance(task2, sched.SpawnedTask)
        assert bytes(task2.stdout_) == b'done\n'
        assert bytes(task2.stderr_) == b''

        assert logs.field_equals(level='debug', completed=2, total=3, active=0)

    assert tasks.info.count == 3
    assert tasks.info.next == 3600  # one hour past the epoch
