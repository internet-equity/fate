import typing

from descriptors import cachedproperty

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


class ConfGroup:
    """Namespaced collection of Conf objects."""

    class _Spec(ConfSpec, NamedTupleEnum):

        task = ConfSpec('task', types={'dict': TaskConfDict})
        default = ConfSpec('default', conf=DefaultConf, types={'dict': DefaultConfDict,
                                                               'list': DefaultConfList})

    class _Default(StrEnum):

        lib = 'fate'

    def __init__(self, *specs, lib=None):
        self.__lib__ = lib or self._Default.lib

        data = dict(self._iter_conf_(specs))
        self.__dict__.update(data)
        self.__names__ = tuple(data)

        self._link_conf_()

    @cachedproperty
    def __prefix__(self):
        return PrefixPaths._infer(self.__lib__)

    def _iter_conf_(self, specs):
        for spec in (specs or self._Spec):
            if isinstance(spec, str):
                spec = (spec, None, None, Conf)

            (conf_name, file_name, types, conf_type) = spec

            yield (
                conf_name,
                conf_type(
                    conf_name,
                    self.__lib__,
                    self.__prefix__,
                    file_name,
                    types,
                )
            )

    def _link_conf_(self):
        for name0 in self.__names__:
            conf0 = getattr(self, name0)

            for name1 in self.__names__:
                if name1 == name0:
                    continue

                conf1 = getattr(self, name1)
                setattr(conf0.__other__, name1, conf1)

    def __iter__(self):
        for name in self.__names__:
            yield getattr(self, name)

    def __repr__(self):
        return f'<{self.__class__.__name__} [%s]>' % ', '.join(self.__names__)
