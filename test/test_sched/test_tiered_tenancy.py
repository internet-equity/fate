def test_due(confpatch, schedpatch):
    #
    # configure a single task which should run
    #
    confpatch.set_tasks(
        {
            'run-me': {
                'exec': 'echo',
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
    assert task.returncode == 0

    assert logs.field_equals(completed=1, total=1, active=0)


def test_skips(confpatch, schedpatch, monkeypatch):
    #
    # configure a single task which should be skipped
    #
    monkeypatch.delenv('TESTY', raising=False)

    confpatch.set_tasks(
        {
            'skip-me': {
                'exec': 'echo',
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
