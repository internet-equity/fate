import abc
import collections
import enum
import functools
import itertools

from descriptors import cachedproperty, classonlymethod


class StrEnum(str, enum.Enum):

    def __str__(self):
        return str(self.value)


class SimpleEnumMeta(enum.EnumMeta):

    def __getitem__(cls, name):
        simple = isinstance(name, list)

        if simple:
            (ident,) = name
        else:
            ident = name

        member = super().__getitem__(ident)

        return member.value if simple else member


class SimpleEnum(enum.Enum, metaclass=SimpleEnumMeta):
    pass


class CallableEnum(enum.Enum):

    def __call__(self, *args, **kwargs):
        return self.value(*args, **kwargs)


class callable_member:

    def __init__(self, func):
        functools.update_wrapper(self, func)
        self.__func__ = func

    def __call__(self, *args, **kwargs):
        return self.__func__(*args, **kwargs)


CallableEnum.member = callable_member


class FileFormatEnum(enum.Enum):

    @property
    def suffix(self):
        return f'.{self.name}'


def _make(cls, iterable):
    candidates = (getattr(base, '_make', None) for base in cls.mro()
                  if not issubclass(base, enum.Enum))

    try:
        make = next(filter(None, candidates))
    except StopIteration:
        pass
    else:
        return make(iterable)

    raise TypeError("no suitable namedtuple base found")


class NamedTupleEnumMeta(enum.EnumMeta):

    def __new__(metacls, cls, bases, classdict, **kwds):
        if any(hasattr(base, '_make') for base in bases):
            classdict.setdefault('_make', classmethod(_make))

        return super().__new__(metacls, cls, bases, classdict, **kwds)


class NamedTupleEnum(enum.Enum, metaclass=NamedTupleEnumMeta):
    pass


class AttributeMap:
    """Mapping whose items may additionally be retrieved via attribute access."""

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


class AttributeDict(AttributeMap, dict):
    """dict whose items may additionally be retrieved via attribute access."""

    __slots__ = ()


class LazyLoadDict(dict):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._loaded_ = False

    def _load_(self):
        data = self.__getdata__()
        self.update(data)
        self._loaded_ = True

    def _loadif_(self):
        if not self._loaded_:
            self._load_()

    def __repr__(self):
        self._loadif_()
        return super().__repr__()

    def __contains__(self, key):
        self._loadif_()
        return super().__contains__(key)

    def __getitem__(self, key):
        self._loadif_()
        return super().__getitem__(key)

    def __iter__(self):
        self._loadif_()
        return super().__iter__()

    def __len__(self):
        self._loadif_()
        return super().__len__()

    def copy(self):
        self._loadif_()
        return super().copy()

    def get(self, key, default=None):
        self._loadif_()
        return super().get(key, default)

    def items(self):
        self._loadif_()
        return super().items()

    def keys(self):
        self._loadif_()
        return super().keys()

    def values(self):
        self._loadif_()
        return super().values()

    def __getdata__(self):
        raise NotImplementedError('__getdata__')


class NestingConf:

    _undefined_ = object()

    def __adopt__(self, name, mapping):
        depth0 = getattr(self, '__depth__', -1)

        if depth0 is None:
            # abort! we might be mid-loading.
            return

        depth1 = depth0 + 1

        if mapping.__depth__ is None:
            mapping.__depth__ = depth1
        else:
            assert mapping.__depth__ == depth1

        if mapping.__name__ is self._undefined_:
            mapping.__name__ = name
        else:
            assert mapping.__name__ == name

        if mapping.__parent__ is None:
            mapping.__parent__ = self
        else:
            assert mapping.__parent__ is self

    def __getitem__(self, key):
        value = super().__getitem__(key)

        if isinstance(value, NestedConf):
            self.__adopt__(key, value)

        return value


class NestedConf(NestingConf):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__depth__ = None
        self.__parent__ = None
        self.__name__ = self._undefined_

    @property
    def __root__(self):
        node = self
        while (parent := getattr(node, '__parent__', None)):
            node = parent
        return node


class at_depth:

    def __init__(self, level):
        self.__level__ = level
        self.__wrapped__ = None
        self.__dict__.update(dict.fromkeys(functools.WRAPPER_ASSIGNMENTS))

    def __present__(self, instance):
        if isinstance(self.__level__, int):
            return self.__level__ == instance.__depth__

        (node0, node1) = (None, instance)

        for name in reversed(self.__level__.split('.')):
            if name != '*' and name != getattr(node1, '__name__', None):
                return False

            (node0, node1) = (node1, getattr(node1, '__parent__', None))
        else:
            return getattr(node0, '__depth__', None) == 0

    def __call__(self, desc):
        if self.__wrapped__ is not None:
            raise TypeError

        functools.update_wrapper(self, desc)
        return self

    def __get__(self, instance, cls):
        if instance is None:
            return self

        if self.__wrapped__ is None:
            raise TypeError

        if not self.__present__(instance):
            raise AttributeError

        return self.__wrapped__.__get__(instance, cls)


class DecoratedNestedType(type):

    def __init__(cls, name, bases, namespace, **kwargs):
        super().__init__(name, bases, namespace, **kwargs)

        # determine cls's _atdepth_members_

        # cls will inherit at_depth members from bases
        atdepth_members = set(
            itertools.chain.from_iterable(
                getattr(base, '_atdepth_members_', ()) for base in bases
            )
        )
        # but cls will ignore *any* inherited names it overrides
        atdepth_members -= namespace.keys()

        # cls's effective at_depth members are those inherited plus
        # its own at_depth members
        atdepth_members.update(
            name for (name, obj) in namespace.items()
            if isinstance(obj, at_depth)
        )

        cls._atdepth_members_ = tuple(atdepth_members)


class DecoratedNestedABCMeta(DecoratedNestedType, abc.ABCMeta):
    pass


class DecoratedNestedConf(NestedConf, metaclass=DecoratedNestedType):

    @cachedproperty
    def _atdepth_hidden_(self):
        return frozenset(name for name in self._atdepth_members_
                         if not getattr(self.__class__, name).__present__(self))

    def __dir__(self):
        # let's try just excluding those defined here
        return [name for name in super().__dir__() if name not in self._atdepth_hidden_]


class NestableChainMap(collections.ChainMap, metaclass=DecoratedNestedABCMeta):
    """ChainMap with support for the at_depth descriptors of DecoratedNestedConf."""

    @classonlymethod
    def nest(cls, parent, name, *maps):
        """Construct a new ChainMap nested under `parent`."""
        instance = cls(*maps)
        parent.__adopt__(name, instance)
        return instance


class DecoratedNestedChain(DecoratedNestedConf, NestableChainMap):
    pass


class AttributeChain(AttributeMap, NestableChainMap):
    pass
