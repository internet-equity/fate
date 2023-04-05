#
# Note: drop this along with Python v3.9
#
# * importlib_resources needed for files() found only in Python v3.9
#
# * ...AND for a bug in its initial implementation in 3.9
#
# (See: #18)
#
import importlib
import itertools

try:
    import importlib_resources
except ImportError:
    importlib_resources = None


_target_ = importlib_resources or importlib.import_module('importlib.resources')


def __dir__():
    members = itertools.chain(globals(), dir(_target_))
    return sorted(members)


def __getattr__(name):
    return getattr(_target_, name)
