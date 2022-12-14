import collections

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
