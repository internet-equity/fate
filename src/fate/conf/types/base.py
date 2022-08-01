from ..datastructure import (
    AttributeChain,
    AttributeDict,
    DecoratedNestedConf,
)


class ConfInterface(DecoratedNestedConf):

    @property
    def __default__(self):
        return self.__root__.__other__.default

    @property
    def __lib__(self):
        return self.__root__.__lib__


# no add'l features for now but for clarity and symmetry
ConfDict = AttributeDict

ConfChain = AttributeChain


class ConfType(ConfInterface, ConfDict):
    pass
