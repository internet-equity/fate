import functools
import itertools

from descriptors import cachedproperty, classonlymethod

from fate.util.sentinel import Undefined


class NestingConf:
    """Mix-in for nested collections whose members keep track of their
    position in the collection tree.

    """
    class AbortAdoption(Exception):
        pass

    def __adopt_depth__(self, name, mapping):
        depth0 = getattr(self, '__depth__', -1)

        if depth0 is None:
            # abort! we might be mid-loading.
            raise self.AbortAdoption

        depth1 = depth0 + 1

        if mapping.__depth__ is None:
            mapping.__depth__ = depth1
        else:
            assert mapping.__depth__ == depth1

    def __adopt_name__(self, name, mapping):
        if mapping.__name__ is Undefined:
            mapping.__name__ = name
        else:
            assert mapping.__name__ == name

    def __adopt_parent__(self, name, mapping):
        if mapping.__parent__ is None:
            mapping.__parent__ = self
        else:
            assert mapping.__parent__ is self

    def __adopt__(self, name, mapping):
        try:
            self.__adopt_depth__(name, mapping)
            self.__adopt_name__(name, mapping)
            self.__adopt_parent__(name, mapping)
        except self.AbortAdoption:
            return

    def __getitem__(self, key):
        value = super().__getitem__(key)

        if isinstance(value, NestedConf):
            self.__adopt__(key, value)

        return value


class NestedPathProxy:
    """Extensible and sliceable representation of a NestedConf path."""

    def __init__(self, conf, added):
        self.conf = conf
        self.added = added

    def __getitem__(self, item):
        if not isinstance(item, (slice, int)):
            raise TypeError('indices must be integers or slices not ' + item.__class__.__name__)

        if isinstance(item, int) and item < 0:
            raise ValueError('integer indices may not be negative: 0 <= x <= sys.maxsize')

        if self.conf.__name__ is Undefined:
            return None

        # reverse, include self.conf and exclude top-level collection
        parents_headless = self.conf.__parents__[:-1]
        members_path = itertools.chain(reversed(parents_headless), [self.conf])

        slce = (item.start, item.stop, item.step) if isinstance(item, slice) else (item, item + 1)
        members_sliced = itertools.islice(members_path, *slce)

        member_names = (str(conf.__name__) for conf in members_sliced)

        return '.'.join(itertools.chain(member_names, self.added))

    def __str__(self):
        return self[:] or ''


class NestedConf(NestingConf):
    """Mix-in for members of nested collections to keep track of their
    position in the collection tree.

    """
    @classonlymethod
    def nest(cls, parent, name, *args, **kwargs):
        """Construct a new NestedConf nested under `parent`."""
        instance = cls(*args, **kwargs)
        parent.__adopt__(name, instance)
        return instance

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__reset__()

    def __reset__(self):
        self.__depth__ = None
        self.__parent__ = None
        self.__name__ = Undefined

    @property
    def __parents__(self):
        ancestry = []

        node = self

        while (parent := getattr(node, '__parent__', None)):
            node = parent
            ancestry.append(node)

        return ancestry

    @property
    def __root__(self):
        return self.__parents__[-1]

    def __get_path__(self, *added):
        return NestedPathProxy(self, added)

    @property
    def __path__(self):
        return self.__get_path__()[:]


class adopt:
    """Method decorator to __adopt__ dynamically-constructed
    configuration.

    """
    def __init__(self, name, ancestry=0):
        self.name = name
        self.ancestry = ancestry

    def __call__(self, target):
        @functools.wraps(target)
        def wrapped(parent, *args, **kwargs):
            result = target(parent, *args, **kwargs)

            depth = self.ancestry
            while (depth := depth - 1) >= 0:
                parent = parent.__parent__

            parent.__adopt__(self.name, result)

            return result

        return wrapped


class at_depth:
    """Decorator and wrapper descriptor of methods of
    DecoratedNestedConf to specify the position in the nested collection
    tree at which they are to be defined.

    """
    def __init__(self, level):
        self.__level__ = level
        self.__wrapped__ = None
        self.__dict__.update(dict.fromkeys(functools.WRAPPER_ASSIGNMENTS))

    def __call__(self, desc):
        """Decorate a method."""
        if self.__wrapped__ is not None:
            raise TypeError("wrapper already set")

        functools.update_wrapper(self, desc)
        return self

    def __get__(self, instance, cls):
        """Bind wrapped method to the given instance (if any)."""
        if instance is None:
            return self

        if self.__wrapped__ is None:
            raise TypeError("nothing has been wrapped")

        if not self.__present__(instance):
            raise AttributeError("descriptor not defined for object")

        return self.__wrapped__.__get__(instance, cls)

    def __present__(self, instance):
        """Indicate whether instance should define wrapped method."""
        if isinstance(self.__level__, int):
            return self.__level__ == instance.__depth__

        (node0, node1) = (None, instance)

        for name in reversed(self.__level__.split('.')):
            if name != '*' and name != getattr(node1, '__name__', None):
                return False

            (node0, node1) = (node1, getattr(node1, '__parent__', None))
        else:
            return getattr(node0, '__depth__', None) == 0


class DecoratedNestedConf(NestedConf):

    __atdepth_members__ = ()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # determine cls's __atdepth_members__

        # cls will inherit at_depth members from bases
        atdepth_members = set(
            itertools.chain.from_iterable(
                getattr(base, '__atdepth_members__', ()) for base in cls.__bases__
            )
        )
        # but cls will ignore *any* inherited names it overrides
        atdepth_members -= cls.__dict__.keys()

        # cls's effective at_depth members are those inherited plus
        # its own at_depth members
        atdepth_members.update(
            name for (name, obj) in cls.__dict__.items()
            if isinstance(obj, at_depth)
        )

        cls.__atdepth_members__ = tuple(atdepth_members)

    @cachedproperty
    def __atdepth_hidden__(self):
        return frozenset(name for name in self.__atdepth_members__
                         if not getattr(self.__class__, name).__present__(self))

    def __dir__(self):
        # let's try just excluding those defined here
        return [name for name in super().__dir__() if name not in self.__atdepth_hidden__]
