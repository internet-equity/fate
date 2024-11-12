import abc
import functools

import descriptors

from fate.conf import types


class CompletingTask(abc.ABC):

    @property
    @abc.abstractmethod
    def path_(self):
        pass

    @abc.abstractmethod
    def ended_(self):
        pass


class TaskConfExt(types.TaskConfDict):
    """Abstract base to classes extending task configuration with
    specific functionality.

    Concrete subclasses must define at least one custom constructor, a
    method which is decorated with TaskConfExt._constructor_. This
    method need only return an instance of the subclass. The method will
    be wrapped as a class-only method and with functionality to ensure
    the constructed instance operates correctly as task configuration.

    """
    _constructors_ = frozenset()

    @staticmethod
    def _link_(extension, task):
        task.__parent__.__adopt__(task.__name__, extension)

    @classmethod
    def _constructor_(bcs, func):
        @functools.wraps(func)
        def wrapped(cls, task, *args, **kwargs):
            instance = func(cls, task, *args, **kwargs)

            if instance.__depth__ is None:
                # force-link the new instance to its parent
                bcs._link_(instance, task)

            return instance

        wrapped._isconstructor_ = True

        return descriptors.classonlymethod(wrapped)

    def __init_subclass__(cls, /, **kwargs):
        super().__init_subclass__(**kwargs)

        cls._constructors_ |= frozenset(
            name for (name, member) in cls.__dict__.items()
            if getattr(getattr(member, '__func__', member), '_isconstructor_', False)
        )

    def __init__(self, *args, **kwargs):
        if not self._constructors_:
            raise TypeError(f"Can't instantiate abstract class {self.__class__.__name__} "
                            "without task adoption-handling constructor(s)")

        super().__init__(*args, **kwargs)

    def __adopt_parent__(self, name, mapping):
        if mapping.__parent__ is None:
            mapping.__parent__ = self
            return

        # we've likely taken over for existing configuration via constructor.
        # rather than insist that child is in our tree, merely check
        # that its tree looks the same.
        assert isinstance(mapping.__parent__, types.TaskConfDict)
        assert mapping.__parent__.__path__ == self.__path__
