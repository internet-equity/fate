import enum
import functools


class MixedEnumMeta(enum.EnumMeta):

    # note: override only necessary prior to Python 3.12
    def __contains__(cls, obj):
        if not isinstance(obj, enum.Enum):
            for member in cls:
                if obj == member.value:
                    return True
            else:
                return False

        return super().__contains__(obj)


class MixedEnum(enum.Enum, metaclass=MixedEnumMeta):
    pass


class StrEnum(str, MixedEnum):

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

    @staticmethod
    def member(func=None, *, bound=False):
        if func is None:
            return functools.partial(callable_member, bound=bound)

        return callable_member(func, bound=bound)

    @classmethod
    def membermethod(cls, func):
        return cls.member(func, bound=True)

    def __call__(self, *args, **kwargs):
        return self.value(self, *args, **kwargs)


class callable_member:

    def __init__(self, func, *, bound=False):
        functools.update_wrapper(self, func)
        self.__func__ = func
        self.__bound__ = bound

    def __call__(self, member, *args, **kwargs):
        if self.__bound__:
            return self.__func__(member, *args, **kwargs)
        else:
            return self.__func__(*args, **kwargs)


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
