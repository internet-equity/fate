#
# TODO: drop this along with Python v3.8
#
# (importlib_resources needed for e.g. files() found only in Python v3.9)
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
