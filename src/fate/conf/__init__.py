import functools

from .base import ConfGroup
from .error import (  # noqa: F401
    ConfBracketError,
    ConfSyntaxError,
    ConfTypeError,
    ConfValueError,
    LogsDecodingError,
    MultiConfError,
    NoConfError,
    ResultEncodingError,
    StateEncodingError,
)


spec = ConfGroup._Spec


@functools.wraps(ConfGroup, assigned=('__doc__', '__annotations__'), updated=())
def get(*args, **kwargs):
    return ConfGroup(*args, **kwargs)
