"""Task result recording compatible with the Fate scheduler."""
import sys

from fate.util.format import Dumper


def write(results, /, format='json', file=sys.stdout):
    """Write task results to `file` (defaulting to standard output).

    Results are presumed to be structured, *i.e.* in the form of a
    `dict`. These will be serialized according to `format` (defaulting
    to `json`). Supported formats include: `{}`.

    Serialization may be disabled by setting `format` to `None`.

    The object provided to `file` must feature a `write` method.

    """
    try:
        dumper = str if format is None else Dumper[format]
    except KeyError:
        raise ValueError(f"unsupported format: {format!r}")

    file.write(dumper(results))

write.__doc__ = write.__doc__.format(Dumper.__names__)
