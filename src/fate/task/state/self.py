"""Task state persistence via file descriptor input and output."""
import functools

from fate.util.format import Dumper, SLoader


def _ignore_bfd(func):
    """Decorator suppressing exceptions regarding bad file descriptors.

    Should the decorated function raise an `OSError` exception whose
    `errno` is set to `9` -- indicating a bad file descriptor -- the
    exception will be silently suppressed.

    In this case, `None` is returned in lieu of the decorated function's
    typical result (if any).

    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except OSError as exc:
            #
            # errno 9 -- bad file descriptor -- suggests we're just not in the scheduler
            #
            # (that should be OK)
            #
            if exc.errno != 9:
                raise

    return wrapper


@_ignore_bfd
def read(*, format='auto', fd=3):
    """Load (parameterized) state from file descriptor `fd` (defaulting
    to the Fate standard state input descriptor).

    Input is deserialized "auto-magically" according to `format`
    (defaulting to `auto`). The input serialization format may be
    specified as one of: `{}`.

    """
    with open(fd) as file:
        data = file.read()

    try:
        (state, _loader) = SLoader.autoload(data, format)
    except SLoader.NonAutoError:
        pass
    else:
        return state

    try:
        loader = SLoader[format]
    except KeyError:
        raise ValueError(f"unsupported format: {format!r}")

    return loader(data)

read.__doc__ = read.__doc__.format(SLoader.__names__)


@_ignore_bfd
def write(results, /, format='json', fd=4):
    """Write task state to file dsecriptor `fd` (defaulting to the Fate
    standard state output descriptor).

    State is presumed to be structured, *i.e.* in the form of a
    `dict`. These will be serialized according to `format` (defaulting
    to `json`). Supported formats include: `{}`.

    Serialization may be disabled by setting `format` to `None`.

    """
    try:
        dumper = str if format is None else Dumper[format]
    except KeyError:
        raise ValueError(f"unsupported format: {format!r}")

    serialized = dumper(results)

    with open(fd, 'w') as file:
        file.write(serialized)

write.__doc__ = write.__doc__.format(Dumper.__names__)
