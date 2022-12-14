import os
import sys
import collections

import loguru

from fate.util.datastructure import (
    adopt,
    at_depth,
    NestedConf,
)

from ..base import Conf

from ..error import ConfTypeError, ConfValueError

from .base import (
    ConfDict,
    ConfList,
    ConfType,
)


class DefaultConf(Conf):

    @property
    def path_(self):
        try:
            return self['path']
        except KeyError:
            return DefaultConfDict.nest(self, 'path')


class DefaultConfType(ConfType):

    _dev_keys_ = frozenset(('level',
                            'file'))

    _log_keys_ = _dev_keys_ | frozenset(('rotation',
                                         'compression',
                                         'retention',
                                         'directory'))

    @at_depth('path')
    @property
    def log_(self):
        log = self.get('log', '/dev/stderr')

        if isinstance(log, (str, collections.abc.Mapping)):
            if isinstance(log, NestedConf):
                log.__reset__()

            log = DefaultConfList.nest(self, 'log', [log])

        if not isinstance(log, collections.abc.Sequence):
            raise ConfTypeError(
                f'{self.__root__.__name__} value @ {self.__path__}.log: expected string, '
                f'mapping or list of same, not value of type {log.__class__.__name__} '
                f'(in {self.__root__.__path__})'
            )

        sinks = []

        for (index, sink) in enumerate(log):
            if isinstance(sink, str):
                if os.path.exists(sink):
                    key = 'directory' if os.path.isdir(sink) else 'file'
                else:
                    key = 'file' if '.' in os.path.basename(sink) else 'directory'

                sink = self.__class__.nest(log, index, {key: sink})

            sink_file = sink.get('file', '')

            allowed_keys = self._dev_keys_ if sink_file.startswith('/dev/') else self._log_keys_

            if bad_log_keys := sink.keys() - allowed_keys:
                sink_path = sink_file or sink.get('directory', '')
                log_id_stanza = f' {sink_path}' if sink_path else ''

                raise ConfTypeError(
                    f'{self.__root__.__name__} value @ {self.__path__}.log: allowed keys for '
                    f'log path{log_id_stanza} are {set(allowed_keys)} not {bad_log_keys} '
                    f'(in {self.__root__.__path__})'
                )

            sinks.append(sink)

        return sinks

    @at_depth('path.log.*')
    @property
    def target_(self):
        if path := self.get('file'):
            if 'directory' in self:
                raise ConfTypeError(
                    f"{self.__root__.__name__} value @ {self.__path__}: log configuration may "
                    f"specify 'file' or 'directory' not both (in {self.__root__.__path__})"
                )

            if path == '/dev/stdout':
                return sys.stdout

            if path == '/dev/stderr':
                return sys.stderr

            return path

        if directory := self.get('directory'):
            return os.path.join(directory, f'{self.__lib__}.{{time}}.log')

        raise ConfTypeError(
            f"{self.__root__.__name__} value @ {self.__path__}: log configuration must "
            f"specify either a 'file' or 'directory' target (in {self.__root__.__path__})"
        )

    @at_depth('path.log.*')
    @property
    @adopt('log', ancestry=1)
    def extra_(self):
        return self.__class__(
            (key, value) for (key, value) in self.items()
            if key != 'file' and key != 'directory'
        )

    @at_depth('path.log.*')
    @property
    def level_(self):
        try:
            return loguru.logger.level(self.level)
        except ValueError:
            raise ConfValueError(
                f'{self.__root__.__name__} value @ {self.__path__}.level: '
                f'unsupported log level: {self.level!r} '
                f'(in {self.__root__.__path__})'
            )


class DefaultConfDict(DefaultConfType, ConfDict):
    """Mapping of data deserialized from defaults configuration files."""


class DefaultConfList(DefaultConfType, ConfList):
    """Defaults configuration list."""
