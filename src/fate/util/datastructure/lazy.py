import abc
import functools

from .access import nomap
from .collection import ProxyMapping


class LazyLoadMap(abc.ABC):
    """Abstract mix-in for mapping classes to load data lazily.

    Only upon look-up is the mapping updated with the result of abstract
    method `__getdata__`.

    To customize how the mapping is updated, override `__setdata__`.

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._loaded_ = False

    @abc.abstractmethod
    def __getdata__(self):
        return {}

    def __setdata__(self, data):
        self.update(data)

    def _load_(self):
        data = self.__getdata__()
        self.__setdata__(data)
        self._loaded_ = True

    def _loadif_(self):
        if not self._loaded_:
            self._load_()

    def __getitem__(self, key):
        self._loadif_()
        return super().__getitem__(key)

    def __repr__(self):
        self._loadif_()
        return super().__repr__()

    def __iter__(self):
        self._loadif_()
        return super().__iter__()

    def __len__(self):
        self._loadif_()
        return super().__len__()


class LazyLoadProxyMapping(LazyLoadMap, ProxyMapping):
    """Abstract base class for a lazy-loading ProxyMapping.

    Only upon look-up is the mapping updated with the result of abstract
    method `__getdata__`.

    """
    def __setdata__(self, data):
        self.__collection__.update(data)


class LoadAttributeError(Exception):
    """Stand-in for `AttributeError`.

    Does *not* extend `AttributeError`, such that descriptors
    responsible for lazily loading map data may raise it without
    triggering `__getattr__`.

    """


loads = functools.partial(nomap, exc_class=LoadAttributeError)
