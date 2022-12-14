import functools

from descriptors import classonlymethod

from fate.util.sentinel import UndefinedType


class ResultIter:

    @classonlymethod
    def storeresult(cls, *c_args, **c_kwargs):
        if len(c_args) == 0 or not callable(c_args[0]):
            def decorator(generator):
                return cls.storeresult(generator, *c_args, **c_kwargs)

            return decorator

        (generator, *c_args) = c_args

        if not callable(generator):
            raise TypeError(f"'{generator.__class__.__name__}' object is not callable")

        @functools.wraps(generator)
        def wrapped(*g_args, **g_kwargs):
            iterator = generator(*g_args, **g_kwargs)
            return cls(iterator, *c_args, **c_kwargs)

        return wrapped

    def __init__(self, iterable, attr='value'):
        self.iterator = iter(iterable)
        self._name_ = attr
        self.__value__ = self._initial_ = UndefinedType()

    @property
    def done(self):
        return self.__value__ is not self._initial_

    @property
    def __value__(self):
        return getattr(self, self._name_)

    @__value__.setter
    def __value__(self, value):
        setattr(self, self._name_, value)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self.iterator)
        except StopIteration as stop:
            self.__value__ = stop.value
            raise

    def __repr__(self):
        return (f'<{self.__class__.__name__}({self.iterator!r}, {self._name_!r}): '
                f'{self.__value__!r}>')


storeresult = ResultIter.storeresult
