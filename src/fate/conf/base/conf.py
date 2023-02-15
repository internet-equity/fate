"""In-memory access to supported configuration files."""
from types import SimpleNamespace

from descriptors import cachedproperty

from fate.util.compat import resources
from fate.util.datastructure import (
    AttributeDict,
    AttributeAccessMap,
    LazyLoadProxyMapping,
    loads,
    NestingConf,
    SimpleEnum,
)
from fate.util.format import Loader

from ..error import (
    ConfSyntaxError,
    MultiConfError,
    NoConfError,
)


class Conf(AttributeAccessMap, NestingConf, LazyLoadProxyMapping):
    """Dictionary- and object-style access to a configuration file."""

    _Format = Loader

    class _ConfType(SimpleEnum):

        dict_ = AttributeDict
        list_ = list

    def __init__(self, name, lib, builtin, paths, filename=None, types=None, **others):
        super().__init__()

        self.__name__ = name
        self.__lib__ = lib

        self._builtin_ = builtin
        self._prefix_ = paths

        self.__filename__ = filename or f"{name}s"

        self._types_ = types

        self.__other__ = SimpleNamespace(**others)

    def __repr__(self):
        if self.__filename__ == f"{self.__name__}s":
            filename = ""
        else:
            filename = f", filename={self.__filename__!r}"

        default = repr(dict(self))

        return (f"<{self.__class__.__name__}"
                f"({self.__name__!r}, {self.__lib__!r}{filename}) "
                f"-> {default}>")

    @cachedproperty
    @loads
    def _indicator_(self):
        return self._prefix_.conf / self.__filename__

    @cachedproperty
    @loads
    def _indicator_builtin_(self):
        return resources.files(self._builtin_.path) / self.__filename__

    def _iter_builtins_(self):
        for format_ in self._Format:
            builtin_path = self._indicator_builtin_.with_suffix(format_.suffix)

            if builtin_path.is_file():
                yield builtin_path

    def _iter_paths_(self, prefix=None):
        indicator = prefix / self.__filename__ if prefix else self._indicator_

        for format_ in self._Format:
            path = indicator.with_suffix(format_.suffix)

            if path.is_file():
                yield path

    def _get_path_(self, prefix=None):
        try:
            (path, *extra) = self._iter_paths_(prefix=prefix)
        except ValueError:
            return None

        if extra:
            raise MultiConfError(path, *extra)

        return path

    @cachedproperty
    @loads
    def __path__(self):
        if path := self._get_path_():
            return path

        if self._builtin_.fallback:
            # fall back to first built-in found
            try:
                return next(self._iter_builtins_())
            except StopIteration:
                pass

        raise NoConfError("%s{%s}" % (
            self._indicator_,
            ','.join(format_.suffix for format_ in self._Format),
        ))

    @property
    @loads
    def _format_(self):
        return self.__path__.suffix[1:]

    @property
    @loads
    def _loader_(self):
        return self._Format[self._format_]

    def __getdata__(self):
        types = {
            conf_type.name: (
                self._types_ and self._types_.get(conf_type.name.rstrip('_'))
            ) or conf_type.value
            for conf_type in self._ConfType
        }

        try:
            return self._loader_(self.__path__, **types)
        except self._loader_.raises as exc:
            raise ConfSyntaxError(self._loader_.name, exc)

    @classmethod
    def fromkeys(cls, _iterable, _value=None):
        raise TypeError(f"fromkeys unsupported for type '{cls.__name__}'")
