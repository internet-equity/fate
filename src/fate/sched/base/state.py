"""Task-state connector"""
from lmdb_dict import CachedLmdbDict

from fate.conf import ConfValueError, StateEncodingError


class BoundTaskStateManager:
    """Interface to a `TaskStateManager` bound to a specified task.

    All methods will return results serialized for consumption by the
    bound task. Methods do not require specification of the requesting
    task.

    """
    def __init__(self, manager, task):
        self.manager = manager
        self.task = task

    def read(self):
        return self.manager.read(self.task)

    def write(self, output):
        self.manager.write(self.task, output)

    def read_all(self):
        return self.manager.read_all(self.task)


class TaskStateManager:
    """Task state input and output in the configured serialization
    format of the requesting task.

    Values are cached in memory to avoid redundant serializations.

    """
    class _AutoEncodingError(Exception):
        pass

    def __init__(self, state_path):
        self.db = CachedLmdbDict(state_path)

        self._l_cache_ = {}
        self._g_cache_ = {}

    @staticmethod
    def _format_dump(task):
        format_ = task.format_['state']

        if format_ == 'auto':
            return task.format_['param']

        return format_

    @classmethod
    def _dump_state(cls, task, data):
        format_ = cls._format_dump(task)

        try:
            dumper = task._Dumper[format_]
        except KeyError:
            raise ConfValueError(
                f'{task.__name__}: unsupported state serialization format: '
                f"{format_!r} (select from: {task._Dumper.__names__})"
            )
        else:
            return dumper(data)

    @classmethod
    def _load_state(cls, task, output):
        format_ = task.format_['state']

        try:
            (data, loader) = task._Loader.autoload(output, format_)
        except task._Loader.NonAutoError:
            pass
        else:
            if loader is None:
                raise cls._AutoEncodingError

            return data

        try:
            loader = task._Loader[format_]
        except KeyError:
            raise ConfValueError(
                task._serializer_error.format(
                    conf_path=f'{task.__name__}.format.state',
                    format_=format_,
                )
            )

        try:
            return loader(output)
        except loader.raises as exc:
            raise StateEncodingError(format_, exc)

    def bind(self, task):
        return BoundTaskStateManager(self, task)

    def read(self, task):
        """Retrieve task's state in its preferred serialization format.

        Returns None if task has stored no state.

        """
        if task.__name__ not in self._l_cache_:
            data = self.db.get(task.__name__, '')

            if isinstance(data, str):
                serialization = data
            else:
                serialization = self._dump_state(task, data)

            self._l_cache_[task.__name__] = serialization

            return serialization

        return self._l_cache_[task.__name__]

    def write(self, task, output):
        """Persist task's written output state.

        Does nothing if written output is empty or if it does not differ
        from cached state.

        """
        if not output or output == self._l_cache_.get(task.__name__):
            return

        # update caches
        self._g_cache_.clear()
        self._l_cache_[task.__name__] = output

        # deserialize output
        try:
            data = self._load_state(task, output)
        except self._AutoEncodingError:
            data = output

        # update db
        self.db[task.__name__] = data

    def read_all(self, task):
        """Serialize all tasks' states in given task's preferred format.

        """
        format_ = self._format_dump(task)

        if format_ not in self._g_cache_:
            serialization = self._dump_state(task, self.db)

            self._g_cache_[format_] = serialization

            return serialization

        return self._g_cache_[format_]
