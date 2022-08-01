"""In-memory access to supported configuration files."""
import typing
from types import SimpleNamespace

from descriptors import cachedproperty

from .datastructure import (
    AttributeDict,
    LazyLoadDict,
    NamedTupleEnum,
    NestingConf,
    SimpleEnum,
    StrEnum,
)
from .error import ConfSyntaxError, MultiConfError, NoConfError
from .format import Loader
from .path import SystemPrefix
from .types import TaskConf


class Conf(NestingConf, LazyLoadDict, AttributeDict):
    """Dictionary- and object-style access to a configuration file."""

    _Format = Loader

    class _ConfType(SimpleEnum):

        dict_ = AttributeDict

    def __init__(self, name, lib, filename=None, types=None, **others):
        super().__init__()
        self.__name__ = name
        self.__lib__ = lib
        self.__filename__ = filename or f"{name}s"
        self.__types__ = types
        self.__other__ = SimpleNamespace(**others)

    def __repr__(self):
        if self.__filename__ == f"{self.__name__}s":
            filename = ""
        else:
            filename = f", filename={self.__filename__!r}"

        default = super().__repr__()

        return (f"<{self.__class__.__name__}"
                f"({self.__name__!r}, {self.__lib__!r}{filename}) "
                f"-> {default}>")

    @cachedproperty
    def _indicator_(self):
        return SystemPrefix.conf / self.__lib__ / self.__filename__

    @cachedproperty
    def __path__(self):
        paths = (self._indicator_.with_suffix(format_.suffix)
                 for format_ in self._Format)

        extant = [path for path in paths if path.exists()]

        try:
            (path, *extra) = extant
        except ValueError:
            pass
        else:
            if extra:
                raise MultiConfError(*extant)

            return path

        raise NoConfError("%s{%s}" % (
            self._indicator_,
            ','.join(format_.suffix for format_ in self._Format),
        ))

    @property
    def _format_(self):
        return self.__path__.suffix[1:]

    @property
    def _loader_(self):
        return self._Format[self._format_]

    def __getdata__(self):
        dict_ = (self.__types__ and self.__types__.get('dict')) or self._ConfType[['dict_']]

        try:
            return self._loader_(self.__path__, dict_=dict_)
        except self._loader_.raises as exc:
            raise ConfSyntaxError(self._loader_.name, exc)

    @classmethod
    def fromkeys(cls, _iterable, _value=None):
        raise TypeError(f"fromkeys unsupported for type '{cls.__name__}'")


class ConfSpec(typing.NamedTuple):

    name: str
    filename: typing.Optional[str] = None
    types: typing.Optional[dict] = None


class ConfGroup:
    """Namespaced collection of Conf objects."""

    class _Spec(ConfSpec, NamedTupleEnum):

        task = ConfSpec('task', types={'dict': TaskConf})
        default = ConfSpec('default')

    class _Default(StrEnum):

        lib = 'fate'

    def __init__(self, *names, lib=None):
        data = dict(self._iter_conf_(names, lib))
        self.__dict__.update(data)
        self.__names__ = tuple(data)
        self._link_conf_()

    @classmethod
    def __new_conf__(cls, name, lib=None, filename=None, types=None):
        return Conf(name, lib or cls._Default.lib, filename, types)

    @classmethod
    def _iter_conf_(cls, names, lib):
        for name in (names or cls._Spec):
            (conf_name, file_name, types) = (name, None, None) if isinstance(name, str) else name
            yield (conf_name, cls.__new_conf__(conf_name, lib, file_name, types))

    def _link_conf_(self):
        for name0 in self.__names__:
            conf0 = getattr(self, name0)

            for name1 in self.__names__:
                if name1 == name0:
                    continue

                conf1 = getattr(self, name1)
                setattr(conf0.__other__, name1, conf1)

    def __repr__(self):
        return f'<{self.__class__.__name__} [%s]>' % ', '.join(self.__names__)
