import fcntl
import textwrap

from descriptors import cachedproperty


class LockingTaskFixture:

    def __init__(self, basedir, path='opt/done.lock', result='done\n'):
        self.lock_path = basedir / path
        self.result = result
        self.locked = False

    @property
    def conf(self):
        return {
            'shell': {
                'executable': 'python',
                'script': textwrap.dedent(
                    '''\
                    import fcntl, json, sys

                    param = json.load(sys.stdin)

                    with open(param['lock_path'], 'w') as fd:
                        fcntl.lockf(fd, fcntl.LOCK_EX)
                        print(param['result'], end='')
                        fcntl.lockf(fd, fcntl.LOCK_UN)
                    '''
                ),
            },
            'param': {
                'lock_path': str(self.lock_path),
                'result': self.result,
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
