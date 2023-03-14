"""Task parameterized input handling."""
import functools
import os
import sys

from schema import Schema

from fate.util.datastructure import AttributeDict
from fate.util.format import SLoader
from fate.util.compat.types import NoneType


Infer = object()


def read(*, schema=None, empty=Infer, format='auto', file=sys.stdin):
    """Load (parameterized) input from `file` (defaulting to standard
    input).

    `schema`, if provided, is either a `Schema` or a construction
    argument for `Schema`, and is used to validate input (and to supply
    defaults).

    `empty` is a default value to treat as the deserialized input if
    `file` is empty. By default this value is inferred from the provided
    schema.

    Input is deserialized "auto-magically" according to `format`
    (defaulting to `auto`). The input serialization format may be
    specified as one of: `{}`.

    Structured input mappings are returned as instances of
    `AttributeDict`.

    """
    if not isinstance(schema, (NoneType, Schema)):
        schema = Schema(schema)

    if empty is Infer:
        if schema and isinstance(schema.schema, (dict, list)):
            empty = type(schema.schema)()
        else:
            empty = None

    if (not file.isatty() or os.getenv('FATE_READ_TTY_PARAM') == '1') and (stdin := file.read()):
        try:
            (params, _loader) = SLoader.autoload(stdin, format, dict_=AttributeDict)
        except SLoader.NonAutoError:
            try:
                loader = SLoader[format]
            except KeyError:
                raise ValueError(f"unsupported format: {format!r}")

            params = loader(stdin, dict_=AttributeDict)
    else:
        params = _to_attributedict(empty)

    if schema:
        return _to_attributedict(schema.validate(params))

    return params

read.__doc__ = read.__doc__.format(SLoader.__names__)


def _deep_cast_dict(cast, target):
    """Cast all instances of `dict` in the given `target` to `cast`."""
    if isinstance(target, dict):
        return cast((key, _deep_cast_dict(cast, value)) for (key, value) in target.items())

    if isinstance(target, (list, tuple)):
        return type(target)(_deep_cast_dict(cast, item) for item in target)

    return target


_to_attributedict = functools.partial(_deep_cast_dict, AttributeDict)
