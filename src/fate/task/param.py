"""Task parameterized input handling."""
import sys

from fate.util.datastructure import AttributeDict
from fate.util.format import SLoader


def read(*, defaults=None, format='auto', file=sys.stdin):
    """Load (parameterized) input from `file` (defaulting to standard
    input).

    `defaults`, if provided, are returned in lieu of missing input. If
    `defaults` is a `dict`, then these are merged; otherwise, it's
    all-or-nothing.

    Input is deserialized "auto-magically" according to `format`
    (defaulting to `auto`). The input serialization format may be
    specified as one of: `{}`.

    Structured input is returned as an instance of `AttributeDict`.

    """
    if isinstance(defaults, dict):
        defaults = _deep_cast_dict(AttributeDict, defaults)

    if stdin := file.read():
        try:
            (params, _loader) = SLoader.autoload(stdin, format, dict_=AttributeDict)
        except SLoader.NonAutoError:
            try:
                loader = SLoader[format]
            except KeyError:
                raise ValueError(f"unsupported format: {format!r}")

            params = loader(stdin, dict_=AttributeDict)

        return params if defaults is None else AttributeDict(defaults, **params)

    return defaults

read.__doc__ = read.__doc__.format(SLoader.__names__)


def _deep_cast_dict(cast, target):
    """Cast all instances of `dict` in the given `target` to `cast`."""
    if isinstance(target, dict):
        return cast((key, _deep_cast_dict(cast, value)) for (key, value) in target.items())

    if isinstance(target, (list, tuple)):
        return type(target)(_deep_cast_dict(cast, item) for item in target)

    return target
