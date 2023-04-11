import collections
import functools
import logging
import pathlib
import re
from datetime import datetime
from enum import IntEnum

import croniter
import jinja2
from descriptors import classproperty

from fate.util.compat.os import cpu_count
from fate.util.format import Dumper, SLoader
from fate.util.iteration import storeresult
from fate.util.sentinel import Undefined

from fate.util.datastructure import (
    adopt,
    at_depth,
    StrEnum,
)

from .. import template

from ..error import (
    ConfBracketError,
    ConfTypeError,
    ConfValueError,
    ResultEncodingError,
)

from .base import (
    ConfChain,
    ConfDict,
    ConfType,
)


class TaskConfType(ConfType):
    """Generic interface applied to data deserialized from task
    configuration files.

    """
    _Dumper = Dumper
    _Loader = SLoader

    _LogRecord = collections.namedtuple('LogRecord', ('level', 'record'))

    _LogDecodingStatus = collections.namedtuple('LogDecodingStatus', ('format', 'errors'))

    class _DefaultFormat(StrEnum):

        param = 'json'
        log = 'auto'
        result = 'auto'
        state = 'auto'

    class _DefaultScheduling(IntEnum):

        tenancy = (2 * cpu_count() - 1)

    class _ShellExecutable(StrEnum):

        sh = 'sh'
        bash = 'bash'
        python = 'python'

        @classproperty
        def default(cls):
            return cls.sh

        @classproperty
        def options(cls):
            return [str(member) for member in cls]

    class _ScheduleError(StrEnum):

        missing = "{task_name}: schedule requires cron expression like '* * * * *'"
        bad = missing + " not {schedule!r}"

        def self_format(self, task, schedule=None):
            return self.format(task_name=task.__name__, schedule=schedule)

    _serializer_error = ('{conf_path}: unsupported serialization format: {format_!r} '
                         f"(select from: {_Loader.__names__})")

    def __repr__(self):
        repr_ = super().__repr__()

        if (path := self.__path__) is None:
            return repr_

        return f"<{self.__class__.__name__} @ {path}: {repr_}>"

    @property
    def __default__(self):
        return self.__root__.__other__.default

    @at_depth(0)
    @property
    def _croniter(self):
        # cover for croniter_range not directly supporting hash_id
        return functools.partial(croniter.croniter, hash_id=self.__name__)

    @at_depth(0)
    def schedule_iter_(self, t0, t1=None, /, *, max_years_between_matches=None):
        try:
            schedule = self['schedule']
        except KeyError:
            raise ConfTypeError(self._ScheduleError.missing.self_format(self))

        if not isinstance(schedule, str):
            raise ConfTypeError(self._ScheduleError.bad.self_format(self, schedule))

        runs = (self._croniter(schedule, t0,
                               max_years_between_matches=max_years_between_matches)
                if t1 is None
                else croniter.croniter_range(t0, t1, schedule, _croniter=self._croniter))

        try:
            yield from runs
        except croniter.CroniterBadCronError:
            raise ConfValueError(self._ScheduleError.bad.self_format(self, schedule))

    @at_depth(0)
    def schedule_next_(self, *args, **kwargs):
        arglen = len(args)

        if 1 <= arglen <= 2:
            (times, default) = (args, Undefined)
        elif arglen == 3:
            (*times, default) = args
        else:
            raise TypeError(f"expected 2 or 3 arguments got {arglen}")

        series = self.schedule_iter_(*times, **kwargs)
        return next(series) if default is Undefined else next(series, default)

    @at_depth(0)
    def scheduled_(self, *times):
        try:
            next(self.schedule_iter_(*times))
        except StopIteration:
            return False
        else:
            return True

    @at_depth(0)
    @property
    @adopt('scheduling')
    def scheduling_(self):
        return TaskChainMap(
            self.get('scheduling', {}),
            self.__default__.get('scheduling', {}),
            self._DefaultScheduling.__members__,
        )

    @at_depth(0)
    @property
    def exec_(self):
        # check for superfluous/conflicting argumentation
        keys = ('exec', 'shell', 'command')
        if sum(key in self for key in keys) > 1:
            raise ConfTypeError(f"{self.__name__}: ambiguous task configuration: "
                                f"specify 0 or 1 of keys: {keys!r}")

        if 'exec' in self:
            try:
                return template.render_complex(self['exec'])
            except TypeError:
                raise ConfTypeError(f'{self.__name__}: "exec" requires string or list of strings '
                                    f"not: {self['exec']!r}")
            except jinja2.TemplateError as exc:
                raise ConfValueError(f"{exc.__class__.__name__} @ {self.__path__}.exec: {exc}")

        if 'shell' in self:
            if isinstance(self.shell, collections.abc.Mapping) and 'executable' in self.shell:
                executable = self.shell.executable

                if not isinstance(executable, str):
                    raise ConfTypeError(f'{self.__name__}.shell: "executable" expected '
                                        f'string not: {executable!r}')

                if executable not in self._ShellExecutable:
                    raise ConfValueError(f'{self.__name__}.shell: "executable" must be '
                                         f'one of: {tuple(self._ShellExecutable.options)!r}')
            else:
                executable = self._ShellExecutable.default

            script = (self.shell.get('script') if isinstance(self.shell, collections.abc.Mapping)
                      else self.shell)

            if not isinstance(script, str):
                raise ConfTypeError(
                    f'{self.__name__}: "shell" expected string input '
                    f"for '{executable}' command indicated either by a simple string "
                    f"(for '{self._ShellExecutable.default}') or a mapping of string "
                    f"input ('script') and optional base command "
                    f"('executable': {set(self._ShellExecutable.options)!r})"
                )

            if not script:
                raise ConfValueError(
                    f'{self.__name__}: "shell" expected string input '
                    f"for '{executable}' command indicated either by a simple string "
                    f"(for '{self._ShellExecutable.default}') or a mapping of string "
                    f"input ('script') and optional base command "
                    f"('executable': {set(self._ShellExecutable.options)!r})"
                )

            try:
                return (executable, '-c', template.render_str(script))
            except jinja2.TemplateError as exc:
                raise ConfValueError(f"{exc.__class__.__name__} @ {self.__path__}.shell: {exc}")

        command = self['command'] if 'command' in self else self.__name__

        if not isinstance(command, str):
            raise ConfTypeError(f'{self.__name__}: "command" requires string not: {command!r}')

        return f'{self.__lib__}-{command}'

    @at_depth(0)
    @property
    def if_(self):
        if 'if' in self:
            if 'unless' in self:
                raise ConfTypeError(f"{self.__name__}: ambiguous configuration: "
                                    "specify either task 'if' or 'unless' not both")

            (key, negate) = ('if', False)
        elif 'unless' in self:
            (key, negate) = ('unless', True)
        else:
            return True

        expression = self[key]

        if not isinstance(expression, str):
            raise ConfTypeError(f'{self.__name__}: "{key}" requires expression '
                                f"string not: {expression!r}")

        bracket_match = template.variable_pattern.fullmatch(expression.strip())

        target = bracket_match['expr'] if bracket_match else expression

        try:
            evaluation = template.eval_expr(target)
        except jinja2.TemplateError as exc:
            raise ConfValueError(f"{exc.__class__.__name__} @ {self.__path__}.{key}: {exc}")

        predicate = not evaluation if negate else bool(evaluation)

        if bracket_match:
            path = self.__get_path__(key)[1:]
            raise ConfBracketError(path, predicate)

        return predicate

    @at_depth(0)
    @property
    def format_(self):
        return collections.ChainMap(
            self.get('format', {}),
            self.__default__.get('format', {}),
            self._DefaultFormat.__members__,
        )

    @at_depth(0)
    @property
    @adopt('path')
    def path_(self):
        return TaskChainMap(
            self.get('path', {}),
            self.__default__.get('path', {}),
            {'result': self._prefix_.data / 'result'},
        )

    @at_depth(0)
    @property
    def param_(self):
        param = self.get('param', {})

        if isinstance(param, str):
            return param

        format_ = self.format_['param']

        try:
            dumper = self._Dumper[format_]
        except KeyError:
            raise ConfValueError(
                f'{self.__name__}: unsupported serialization format: '
                f"{format_!r} (select from: {self._Dumper.__names__})"
            )
        else:
            return dumper(param)

    _task_log_pattern = re.compile(r'<([1-5])> *(.*)')

    @staticmethod
    def _normalize_log_level(value):
        if isinstance(value, int):
            if value < 10:
                value *= 10

            return logging._levelToName.get(value)

        if isinstance(value, str):
            value = value.upper()

            return value if value in logging._nameToLevel else None

        raise TypeError("log level to normalize may be of type int or str "
                        f"not {value.__class__.__name__}")

    @at_depth(0)
    @storeresult('status')
    def _iter_logs_(self, stderr):
        format_ = self.format_['log']

        status = self._LogDecodingStatus(format_, [])

        for log_line in stderr.split('\0'):
            if not log_line:
                continue

            if level_match := self._task_log_pattern.fullmatch(log_line):
                (record_code, record_text) = level_match.groups()
                record_level = self._normalize_log_level(int(record_code))
            else:
                (record_level, record_text) = (None, log_line)

            try:
                (record_struct, _loader) = self._Loader.autoload(record_text, format_)
            except self._Loader.NonAutoError:
                try:
                    loader = self._Loader[format_]
                except KeyError:
                    raise ConfValueError(
                        self._serializer_error.format(
                            conf_path=f'{self.__name__}.format.log',
                            format_=format_,
                        )
                    )

                try:
                    record_struct = loader(record_text)
                except loader.raises as exc:
                    status.errors.append(exc)
                    record_struct = None

            struct_level = record_struct.get('level') if isinstance(record_struct, dict) else None

            if struct_level:
                if isinstance(struct_level, (int, str)):
                    struct_level = self._normalize_log_level(struct_level)
                else:
                    struct_level = None

            yield self._LogRecord(
                level=(record_level or struct_level or 'INFO'),
                record=(record_struct if isinstance(record_struct, dict) else record_text),
            )

        return status

    @at_depth('*.path')
    def _result_(self, stdout, dt=None):
        result_spec = self.result

        if not result_spec:
            # empty path.result synonymous with /dev/null: quit
            return None

        format_ = self.__parent__.format_['result']

        if format_ == 'auto' and not stdout:
            # auto (unlike mixed) suppresses empty results: quit
            return None

        if dt is None:
            dt = datetime.now()

        stamp = dt.timestamp()
        datestr = dt.strftime('%Y%m%dT%H%M%S')

        result_path = (result_spec if isinstance(result_spec, pathlib.Path)
                       else pathlib.Path(result_spec))

        identifier = result_path / f'result-{stamp:.0f}-{datestr}-{self.__parent__.__name__}'

        try:
            (_struct, loader) = self._Loader.autoload(stdout, format_)
        except self._Loader.NonAutoError:
            if isinstance(format_, str) or not isinstance(format_, collections.abc.Iterable):
                formats = (format_,)
            else:
                formats = format_

            errors = []

            for format1 in formats:
                try:
                    loader = self._Loader[format1]
                except KeyError:
                    raise ConfValueError(
                        self._serializer_error.format(
                            conf_path=f'{self.__parent__.__name__}.format.result',
                            format_=format1,
                        )
                    )

                try:
                    loader(stdout)
                except loader.raises as exc:
                    errors.append(exc)
                else:
                    break
            else:
                raise ResultEncodingError(format_, errors, identifier)

        return identifier.with_suffix(loader.suffix) if loader else identifier


class TaskConfDict(TaskConfType, ConfDict):
    """Mapping of data deserialized from task configuration files."""


class TaskChainMap(TaskConfType, ConfChain):
    """ChainMap of task configuration with fallback to system-level and
    library-level defaults.

    """
