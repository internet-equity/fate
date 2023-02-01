import collections
import functools

from .collection import ProxyDict


class AttributeAccessMap:
    """Mix-in for mapping classes such that items may additionally be
    retrieved via attribute access.

    """
    __slots__ = ()

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            pass

        try:
            getter = super().__getattr__
        except AttributeError:
            pass
        else:
            return getter(name)

        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute {name!r}")


def nomap(func, exc_class):
    """Decorator base to wrap descriptor getter functions such that they
    *may not* raise `AttributeError`.

    As such, descriptors produced from wrapped functions will never fall
    back to the class's `__getattr__`.

    This is intended for use with subclasses of `AttributeAccessMap`, to
    decorate descriptor functions for which falling back to dictionary
    access -- `__getitem__` -- is problematic, (*e.g.* those involved in
    dictionary access itself).

    """
    @functools.wraps(func)
    def wrapper(self):
        try:
            return func(self)
        except AttributeError as exc:
            raise exc_class(exc)

    return wrapper


class AttributeDict(AttributeAccessMap, dict):
    """dict whose items may additionally be retrieved via attribute
    access.

    """
    __slots__ = ()


class AttributeProxyDict(AttributeAccessMap, ProxyDict):
    """mutable mapping whose items may additionally be retrieved via
    attribute access.

    """
    __slots__ = ()


#
# ...and, for example:
#
# class AttributeMapping(AttributeAccessMap, ProxyMapping):
#     """mapping whose items may additionally be retrieved via attribute access."""
#
#     __slots__ = ()
#


class AttributeChainMap(AttributeAccessMap, collections.ChainMap):
    """ChainMap whose items may additionally be retrieved via attribute
    access.

    """
