from fate.util.datastructure import (
    AttributeChainMap,
    AttributeProxyDict,
    DecoratedNestedConf,
    ProxyList,
)


class ConfType(DecoratedNestedConf):
    """Generic base for configuration classes.

    This base does not provide the configuration's container class.

    """
    @property
    def __lib__(self):
        return self.__root__.__lib__

    @property
    def __prefix__(self):
        return self.__root__.__prefix__


"""Configuration container base classes.

These do *not* extend the classes provided by datastructure. Rather,
these are set as they are for clarity and symmetry.

"""
ConfChain = AttributeChainMap

ConfDict = AttributeProxyDict

ConfList = ProxyList


class ConfMapping(ConfType, ConfDict):
    """Default base for configuration mapping classes."""
