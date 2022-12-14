import abc
import collections

from fate.util.abstract import abstractmember


class ProxyCollection(abc.ABC):
    """Abstract mix-in of collection classes which mediate interaction
    with a proxied object (such as a list or a dict).

    Concrete subclasses must at least define class-level attribute
    `__collection_type__`, _e.g._:

        __collection_type__ = list

    """
    __collection_type__ = abstractmember()

    __slots__ = ('__collection__',)

    def __init__(self, *args, **kwargs):
        self.__collection__ = self.__collection_type__(*args, **kwargs)

    def __getitem__(self, key):
        return self.__collection__[key]

    def __len__(self):
        return len(self.__collection__)

    def __repr__(self):
        return f'{self.__class__.__name__}({self.__collection__!r})'


class ProxyMutableCollection(ProxyCollection):
    """Mutable extension to ProxyCollection."""

    __slots__ = ()

    def __setitem__(self, key, value):
        self.__collection__[key] = value

    def __delitem__(self, key):
        del self.__collection__[key]


class ProxyMapping(ProxyCollection, collections.abc.Mapping):
    """ProxyCollection providing read-only access to a dictionary."""

    __collection_type__ = dict

    __slots__ = ()

    def __iter__(self):
        yield from self.__collection__


class ProxyDict(ProxyMapping, ProxyMutableCollection, collections.abc.MutableMapping):
    """ProxyCollection providing a full-featured dictionary."""

    __slots__ = ()


class ProxySequence(ProxyCollection, collections.abc.Sequence):
    """ProxyCollection providing read-only access to a list."""

    __collection_type__ = list

    __slots__ = ()


class ProxyList(ProxySequence, ProxyMutableCollection, collections.abc.MutableSequence):
    """ProxyCollection providing a full-featured list."""

    __slots__ = ()

    def insert(self, index, value):
        self.__collection__.insert(index, value)
