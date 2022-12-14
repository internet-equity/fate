import abc
import itertools

from descriptors import (
    cachedclassproperty,
    classonlymethod,
)


class Resets(abc.ABC):

    def __init__(self):
        self._reset_ = []

    @cachedclassproperty
    def _reset_methods(cls):
        inherited = itertools.chain.from_iterable(
            getattr(base, '_reset_methods', ())
            for base in reversed(cls.__bases__)
        )

        own = filter(None, (getattr(obj, 'reset', None)
                            for obj in cls.__dict__.values()))

        merged = {
            reset.name: reset
            for reset in itertools.chain(inherited, own)
        }

        return tuple(merged.values())

    def _iter_pop_reset(self):
        for reset in self._reset_methods:
            args = (None,) if reset.optional else ()
            yield self.__dict__.pop(reset.name, *args)

    def reset(self):
        """Remove cached property data marked for "reset" from the
        instance.

        """
        try:
            artifact = tuple(self._iter_pop_reset())
        except KeyError as exc:
            raise ValueError(f"cannot reset: {exc} not set")

        self._reset_.append(artifact)


class ResetInfo:

    @classonlymethod
    def resets(cls, obj=None, /, **kwargs):
        reset = cls(**kwargs)

        return reset if obj is None else reset(obj)

    def __init__(self, name=None, *, optional=False):
        self.name = name
        self.optional = optional

    def __repr__(self):
        return f'{self.__class__.__name__}({self.name!r}, optional={self.optional!r})'

    def __call__(self, obj):
        if hasattr(obj, 'reset'):
            raise ValueError("target already decorated")

        obj.reset = self

        if self.name is None:
            target = getattr(obj, '__func__', None) or obj

            if name := target.__name__:
                self.name = name
            else:
                raise TypeError("could not determine target name")

        return obj

resets = ResetInfo.resets
