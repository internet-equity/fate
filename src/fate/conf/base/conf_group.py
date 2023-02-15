import re
import typing

from descriptors import cachedproperty, classonlymethod

from fate.util.datastructure import NamedTupleEnum, StrEnum

from ..path import PrefixPaths

from ..types import (
    DefaultConf,
    DefaultConfDict,
    DefaultConfList,
    TaskConfDict,
)

from .conf import Conf


class ConfSpec(typing.NamedTuple):

    name: str
    filename: typing.Optional[str] = None
    types: typing.Optional[dict] = None
    conf: typing.Optional[type] = Conf


class BuiltinConfSpec(typing.NamedTuple):

    # where in package to find built-in configuration files (with
    # comments, etc.) for conf initialization (copying into host), and
    # even for fallback in case of non-initialization (missing
    # configuration)
    #
    # (may be overridden with import path to *any* package)
    #
    path: str = 'fate.conf.include'

    # whether to permit falling back to built-in configuration in case
    # of missing host configuration
    #
    fallback: bool = False

    @classonlymethod
    def _from_group(cls, **kwargs):
        proper = {}

        for (name, value) in kwargs.items():
            if match := re.fullmatch(r'builtin_(.+)', name):
                proper[match[1]] = value
            else:
                raise TypeError(f"unexpected keyword argument {name!r}")

        return cls(**proper)


class ConfGroup:
    """Namespaced collection of Conf objects."""

    class _Spec(ConfSpec, NamedTupleEnum):

        task = ConfSpec('task', types={'dict': TaskConfDict})
        default = ConfSpec('default', conf=DefaultConf, types={'dict': DefaultConfDict,
                                                               'list': DefaultConfList})

    class _Default(StrEnum):

        lib = 'fate'

    def __init__(self, *specs, lib=None, **builtin_spec):
        self._lib_ = lib or self._Default.lib  # disallow empty

        self._builtin_ = BuiltinConfSpec._from_group(**builtin_spec)

        data = dict(self._iter_conf_(specs))
        self.__dict__.update(data)
        self._names_ = tuple(data)

        self._link_conf_()

    @cachedproperty
    def _prefix_(self):
        return PrefixPaths.infer(self._lib_)

    def _iter_conf_(self, specs):
        for spec in (specs or self._Spec):
            if isinstance(spec, str):
                spec = (spec, None, None, Conf)

            (conf_name, file_name, types, conf_type) = spec

            yield (
                conf_name,
                conf_type(
                    conf_name,
                    self._lib_,
                    self._builtin_,
                    self._prefix_,
                    file_name,
                    types,
                )
            )

    def _link_conf_(self):
        for name0 in self._names_:
            conf0 = getattr(self, name0)

            for name1 in self._names_:
                if name1 == name0:
                    continue

                conf1 = getattr(self, name1)
                setattr(conf0.__other__, name1, conf1)

    def __iter__(self):
        for name in self._names_:
            yield getattr(self, name)

    def __repr__(self):
        return f'<{self.__class__.__name__} [%s]>' % ', '.join(self._names_)
