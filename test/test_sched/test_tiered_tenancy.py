import gzip
import json
import os
import re
import signal
import textwrap
import time
from collections import deque

import pytest

from fate import sched
from fate.conf import LogRecordDecodeError

from test.fixture import StopWatch, timeout


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
        events = list(schedpatch.scheduler())

    #
    # task should run and this should be logged
    #
    assert len(events) == 1

    (event,) = events
    assert event.returncode == 0
    assert str(event.stdout) == 'done\n'
    assert str(event.stderr) == ''
    assert event.stopped is None

    (result,) = event.results()
    assert result.value == b'done\n'
    assert result.path.name.endswith(event.task.__name__)

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
        events = list(schedpatch.scheduler())

    #
    # task should NOT run and this should be logged
    #
    assert len(events) == 0

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
        events = list(schedpatch.scheduler())

    #
    # task should run and this should be logged
    #
    assert len(events) == 1

    (event,) = events

    assert event.returncode == 0

    assert gzip.decompress(bytes(event.stdout)) == confpatch.conf.task.binary.param.encode()

    assert bytes(event.stderr) == b''

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
    with confpatch.caplog() as logs, \
         StopWatch() as session:
        events = list(schedpatch.scheduler())

    #
    # task should run and this should be logged
    #
    assert len(events) == 1

    (event,) = events

    assert event.returncode == 0

    assert str(event.stderr) == ''

    assert logs.field_equals(completed=1, total=1, active=0)

    # timing is machine-dependent -- and should really be *very* quick -- but we can't ensure this
    # across test platforms. rather, the below duration is a compromise, and "fast" relative to
    # previous stdout-reading implementations.
    assert event.duration.total_seconds() < 5

    assert session.seconds < 5

    assert len(bytes(event.stdout)) == HUNDRED_MB


def test_invocation_failure(confpatch, schedpatch):
    #
    # configure a task whose executable cannot be found on PATH
    #
    confpatch.set_tasks(
        {
            'missing': {
                'exec': 'fohdfskjh',
                'schedule': '0 * * * *',
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
    events = list(schedpatch.scheduler())

    assert len(events) == 1

    (fail_event,) = events

    assert isinstance(fail_event, sched.TaskInvocationFailureEvent)

    assert str(fail_event.error) == 'command not found on path: fohdfskjh'


@timeout(2)
def test_log_event(locking_task, confpatch, schedpatch):
    #
    # configure a locking task which writes log records to stderr
    #
    msgs = (
        "I'm just getting set up here...",
        {'level': 'WARN', 'message': "NOW we're cookin'!"},
        "...See ya'",
    )
    confpatch.set_tasks(
        {
            'logs': {
                **locking_task.conf(logs=msgs),
                'schedule': '0 * * * *',
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
        events = schedpatch.scheduler()

        # we should get a log event right away
        log_event = next(events)

        assert isinstance(log_event, sched.TaskLogEvent)

        assert log_event.message == b'<2> %b' % msgs[0].encode()

        assert log_event.record() == ('INFO', msgs[0])

        # but the task should be hung
        assert not log_event.task.ready_()

        # now let's release it
        locking_task.release()

        tail_events = list(events)

    #
    # task should be complete and all should be logged
    #
    assert len(tail_events) == 3

    (log_event1, log_event2, ready_event) = tail_events

    assert isinstance(log_event1, sched.TaskLogEvent)

    record1 = {key: value for key, value in msgs[1].items() if key != 'level'}

    assert log_event1.record() == ('WARNING', record1)

    assert isinstance(log_event2, sched.TaskLogEvent)

    assert log_event2.record() == ('INFO', msgs[2])

    assert isinstance(ready_event, sched.TaskReadyEvent)

    assert ready_event.returncode == 0

    assert bytes(ready_event.stdout) == b'done\n'

    message1 = json.dumps(record1)

    assert str(ready_event.stderr) == (f'<2> {msgs[0]}\0'
                                       f'<3> {message1}\0'
                                       f'<2> {msgs[2]}\0')

    assert logs.field_equals(completed=0, total=0, active=1, events=1)
    assert logs.field_equals(completed=0, total=0, active=1, events=2)
    assert logs.field_equals(completed=1, total=1, active=0, events=1)


def test_bad_logs(confpatch, schedpatch):
    #
    # configure a task which writes bad log records to stderr
    #
    confpatch.set_tasks(
        {
            'logs': {
                # echo is annoyingly inconsistent across platforms
                # -- at least as far as the -n option (no trailing newline)
                # printf should be reliable
                'shell': r'''
                    printf '{bad json...\0' >&2
                    printf 'not unicode!\0' | gzip -c >&2
                    printf '{"technically": "fine"}\0' >&2
                    printf 'no terminator remainder' >&2

                    echo done
                ''',
                'schedule': '0 * * * *',
                'format': {'log': 'json'},
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
    events = list(schedpatch.scheduler())

    #
    # compressed data will contain null bytes s.t. it is interpreted as ~3 bad records
    # (but this might be inconsistent across platforms)
    #
    assert len(events) > 5

    (log_event0, *gzip_events, log_event_n0, log_event_n1, ready_event) = events

    assert isinstance(log_event0, sched.TaskLogEvent)

    assert log_event0.message == b'{bad json...'

    with pytest.raises(LogRecordDecodeError) as exc:
        log_event0.record()

    assert exc.value.format == 'json'
    assert isinstance(exc.value.error, json.JSONDecodeError)
    assert exc.value.record == ('INFO', log_event0.message.decode())

    assert gzip_events
    for event in gzip_events:
        assert isinstance(event, sched.TaskLogEvent)

        with pytest.raises((LogRecordDecodeError, UnicodeDecodeError)):
            event.record()

    assert isinstance(log_event_n0, sched.TaskLogEvent)
    assert log_event_n0.message == b'{"technically": "fine"}'
    assert log_event_n0.record() == ('INFO', {"technically": "fine"})

    assert isinstance(log_event_n1, sched.TaskLogEvent)
    assert log_event_n1.message == b'no terminator remainder'

    with pytest.raises(LogRecordDecodeError) as exc:
        log_event_n1.record()

    assert exc.value.format == 'json'
    assert isinstance(exc.value.error, json.JSONDecodeError)
    assert exc.value.record == ('INFO', log_event_n1.message.decode())

    assert isinstance(ready_event, sched.TaskReadyEvent)
    assert ready_event.returncode == 0
    assert str(ready_event.stdout) == 'done\n'


def test_results_ext(confpatch, schedpatch):
    #
    # configure a task's result file extension
    #
    confpatch.set_tasks(
        {
            'test-results': {
                'exec': ['echo', 'done'],
                'schedule': "H/5 * * * *",
                'path': {
                    'result': '{{ default }}.test',
                },
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
    events = list(schedpatch.scheduler())

    #
    # task should run and this should be logged
    #
    assert len(events) == 1

    (event,) = events
    assert event.returncode == 0
    assert str(event.stdout) == 'done\n'
    assert str(event.stderr) == ''
    assert event.stopped is None

    (result,) = event.results()
    assert result.value == b'done\n'

    assert result.path.is_absolute()
    assert result.path.parent.parent.name == 'fate'
    assert result.path.parent.name == 'result'
    assert result.path.name.endswith(f'-{event.task.__name__}.test')


def test_results_empty(confpatch, schedpatch):
    #
    # configure a task which writes no results
    #
    confpatch.set_tasks(
        {
            'no-result': {
                'shell': 'echo done >&2',
                'schedule': "H/5 * * * *",
            },
        }
    )

    #
    # set up scheduler with a long-previous check s.t. task should execute
    #
    schedpatch.set_last_check(offset=3600)

    #
    # execute scheduler
    #
    events = list(schedpatch.scheduler())

    #
    # task should run and this should be logged
    #
    assert len(events) == 2

    (log_event, ready_event) = events

    assert ready_event.stopped is None

    assert ready_event.returncode == 0

    assert bytes(ready_event.stdout) == b''

    assert bytes(ready_event.stderr) == b'done\n'

    assert ready_event.results() == []

    assert isinstance(log_event, sched.TaskLogEvent)
    assert log_event.record() == ('INFO', 'done\n')


def test_results_disabled(confpatch, schedpatch):
    #
    # configure a task with no path to which to write its results
    #
    confpatch.set_tasks(
        {
            'results-disabled': {
                'shell': '''\
                    echo running >&2
                    echo done
                ''',
                'schedule': "H/5 * * * *",
                'path': {
                    'result': None,
                },
            },
        }
    )

    #
    # set up scheduler with a long-previous check s.t. task should execute
    #
    schedpatch.set_last_check(offset=3600)

    #
    # execute scheduler
    #
    events = list(schedpatch.scheduler())

    #
    # task should run and this should be logged
    #
    assert len(events) == 2

    (log_event, ready_event) = events

    assert ready_event.stopped is None

    assert ready_event.returncode == 0

    assert bytes(ready_event.stdout) == b'done\n'

    assert bytes(ready_event.stderr) == b'running\n'

    assert ready_event.results() == []

    assert isinstance(log_event, sched.TaskLogEvent)
    assert log_event.record() == ('INFO', 'running\n')


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
        events = list(schedpatch.scheduler())

    #
    # task should run and this should be logged
    #
    assert len(events) == 1

    (event,) = events

    assert event.stopped is None

    assert event.returncode == 0

    assert bytes(event.stdout) == b'done\n'

    assert bytes(event.stderr) == b''

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
    with confpatch.caplog() as logs, \
         StopWatch() as session:
        events = list(schedpatch.scheduler())

    #
    # task should run and this should be logged
    #
    assert len(events) == 1

    (event,) = events

    assert event.stopped
    assert event.stopped == pytest.approx(event.ended)

    assert event.returncode == -signal.SIGTERM

    assert str(event.stderr) == ''

    assert logs.field_equals(completed=1, total=1, active=0)

    assert event.ended >= event.expires

    assert 1 <= event.duration.total_seconds() < 2

    assert 1 <= session.seconds < 2

    # we shouldn't see "finished"
    stdout_match = re.fullmatch(
        r'started: (?P<cpid>\d+) +(?P<cpgid>\d+)\n',
        str(event.stdout)
    )

    assert stdout_match, str(event.stdout)

    (cpid, cpgid) = (int(group) for group in (stdout_match['cpid'],
                                              stdout_match['cpgid']))

    assert cpid == cpgid == event.task._process_.pid

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
    with confpatch.caplog() as logs, \
         StopWatch() as session:
        events = list(schedpatch.scheduler())

    #
    # task should run and this should be logged
    #
    assert len(events) == 1

    (event,) = events

    assert event.returncode == -signal.SIGKILL

    assert str(event.stderr) == ''

    assert logs.field_equals(completed=1, total=1, active=0)

    assert event.ended >= event.expires

    assert 1 <= event.duration.total_seconds() < 2

    assert 1 <= session.seconds < 2

    # we shouldn't see "finished"
    stdout_match = re.fullmatch(
        r'started: (?P<cpid>\d+) +(?P<cpgid>\d+)\n',
        str(event.stdout)
    )

    assert stdout_match, str(event.stdout)

    (cpid, cpgid) = (int(group) for group in (stdout_match['cpid'],
                                              stdout_match['cpgid']))

    assert cpid == cpgid == event.task._process_.pid

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
    with confpatch.caplog() as logs, \
         StopWatch() as session:
        events = list(schedpatch.scheduler())

    #
    # task should run and this should be logged
    #
    assert len(events) == 1

    (event,) = events

    assert event.returncode == -signal.SIGTERM

    assert str(event.stderr) == ''

    assert logs.field_equals(completed=1, total=1, active=0)

    assert event.ended >= event.expires

    assert 1 <= event.duration.total_seconds() < 2

    assert 1 <= session.seconds < 2

    # we shouldn't see "finished"
    stdout_match = re.fullmatch(
        r'started: (?P<cpid>\d+) +(?P<cpgid>\d+)\n'
        r'grandchild: (?P<gpid>\d+) +(?P<gpgid>\d+)\n',
        str(event.stdout)
    )

    assert stdout_match, str(event.stdout)

    (cpid, cpgid, gpid, gpgid) = (int(group) for group in (stdout_match['cpid'],
                                                           stdout_match['cpgid'],
                                                           stdout_match['gpid'],
                                                           stdout_match['gpgid']))

    assert cpid == event.task._process_.pid

    assert gpid != event.task._process_.pid

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
    with confpatch.caplog() as logs, \
         StopWatch() as session:
        events = list(schedpatch.scheduler())

    #
    # task should run and this should be logged
    #
    assert len(events) == 1

    (event,) = events

    assert event.returncode == -signal.SIGTERM

    assert str(event.stderr) == ''

    assert logs.field_equals(completed=1, total=1, active=0)

    assert event.ended >= event.expires

    assert 1 <= event.duration.total_seconds() < 2

    assert 1 <= session.seconds < 2

    # we shouldn't see "finished"
    stdout_match = re.fullmatch(
        r'started: (?P<gpid>\d+) +(?P<gpgid>\d+)\n',
        str(event.stdout)
    )

    assert stdout_match, str(event.stdout)

    (gpid, gpgid) = (int(group) for group in (stdout_match['gpid'],
                                              stdout_match['gpgid']))

    assert gpid != event.task._process_.pid

    assert gpgid == event.task._process_.pid

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
                **locking_task.conf(),
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
        events = schedpatch.scheduler()

        event0 = next(events)

        assert event0.task.__name__ == 'runs-late'
        assert isinstance(event0, sched.TaskReadyEvent)
        assert event0.returncode == 0
        assert bytes(event0.stdout) == b'done\n'
        assert bytes(event0.stderr) == b''

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
        (event1,) = events

        assert event1.task.__name__ == 'runs-long'
        assert isinstance(event1, sched.TaskReadyEvent)
        assert event1.returncode == 0
        assert str(event1.stdout) == locking_task.result
        assert str(event1.stderr) == ''

        assert logs.field_equals(level='debug', completed=1, total=1, active=1)
        assert logs.field_equals(level='debug', completed=1, total=2, active=0)

    assert events.info.completed_count == 2
    assert events.info.next_time == 3600  # one hour past the epoch


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
                **locking_task.conf(),
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
        events = schedpatch.scheduler()

        event0 = next(events)

        assert event0.task.__name__ == 'runs-long'
        assert isinstance(event0, sched.TaskReadyEvent)
        assert str(event0.stdout) == locking_task.result
        assert str(event0.stderr) == ''

        assert logs.field_equals(level='debug', cohort=0, size=2, msg="enqueued cohort")
        assert logs.field_equals(level='debug', active=1, msg="launched pool")
        assert logs.field_equals(level='debug', cohort=1, size=1, msg="enqueued cohort")

        #
        # Issue #28: RuntimeError: "deque mutated during iteration"
        #
        # (during subsequent enqueuing of task "runs-late" and clean-up of primary cohort)
        #
        event1 = next(events)

        assert event1.task.__name__ == 'on-deck'
        assert isinstance(event1, sched.TaskReadyEvent)
        assert bytes(event1.stdout) == b'done\n'
        assert bytes(event1.stderr) == b''

        assert logs.field_equals(level='debug', completed=1, total=1, active=1)
        assert logs.field_equals(level='debug', active=2, msg="expanded pool")

        (event2,) = events

        assert event2.task.__name__ == 'runs-late'
        assert isinstance(event2, sched.TaskReadyEvent)
        assert bytes(event2.stdout) == b'done\n'
        assert bytes(event2.stderr) == b''

        assert logs.field_equals(level='debug', completed=2, total=3, active=0)

    assert events.info.completed_count == 3
    assert events.info.next_time == 3600  # one hour past the epoch
