import functools

from .base import ConfGroup
from .error import ConfValueError, MultiConfError, NoConfError  # noqa: F401


@functools.wraps(ConfGroup, assigned=('__doc__', '__annotations__'), updated=())
def get(*args, **kwargs):
    return ConfGroup(*args, **kwargs)
