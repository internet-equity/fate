import fcntl
import pathlib

from descriptors import cachedproperty


class LockingTaskFixture:

    def __init__(self, basedir, path='opt/done.lock', result='done\n'):
        self.lock_path = basedir / path
        self.result = result
        self.locked = False

    def conf(self, logs=()):
        return {
            'exec': str(pathlib.Path(__file__).parent / 'locking_task.py'),
            'param': {
                'lock_path': str(self.lock_path),
                'result': self.result,
                'logs': list(logs),
            },
        }

    @cachedproperty
    def lock_fd(self):
        self.lock_path.parent.mkdir()
        return self.lock_path.open('w')

    def acquire(self):
        fcntl.lockf(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.locked = True

    def release(self):
        fcntl.lockf(self.lock_fd, fcntl.LOCK_UN)
        self.lock_fd.close()
        self.locked = False
