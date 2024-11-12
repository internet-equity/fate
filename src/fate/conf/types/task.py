import collections
import functools
import os.path
import pathlib
import re
import typing
import uuid
from datetime import timedelta
from enum import IntEnum

import croniter
import jinja2
from descriptors import classproperty

from fate.util.compat.os import cpu_count
from fate.util.compat.types import NoneType
from fate.util.format import Dumper, SLoader
from fate.util.sentinel import Undefined
from fate.util.timedelta import parse_timedelta

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
        mixed_hash = missing + (" or, with hashed-name jitter, like 'H H(0-2) * * * H' or, "
                                "with device-unique hashed-name jitter, like 'U U(0-2) * * * U', "
                                "not {schedule!r}")

        def self_format(self, task, schedule=None):
            return self.format(task_name=task.__name__, schedule=schedule)

    _result_format_error = ('{conf_path}: unsupported result format: {format_!r} '
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
    @adopt('schedule')
    def schedule_(self):
        try:
            schedule = self['schedule']
        except KeyError:
            raise ConfTypeError(self._ScheduleError.missing.self_format(self))

        if isinstance(schedule, collections.abc.Mapping):
            meta = self.__class__(schedule)

            try:
                schedule = self['schedule']['expression']
            except KeyError:
                raise ConfTypeError(self._ScheduleError.missing.self_format(self))
        else:
            meta = self.__class__(expression=schedule)

        if not isinstance(schedule, str):
            raise ConfTypeError(self._ScheduleError.bad.self_format(self, schedule))

        hash_id = meta.setdefault('hash', self.__name__)

        if not isinstance(hash_id, str):
            raise ConfTypeError(f'{self.__name__}: schedule.hash requires value of type string '
                                f'not {hash_id.__class__.__name__}: {hash_id}')

        return meta

    @at_depth(0)
    def schedule_iter_(self, t0, t1=None, /, *, max_years_between_matches=None):
        meta = self.schedule_

        if meta.expression.startswith('@'):
            (schedule, hash_unique) = (meta.expression, False)
        else:
            (schedule, hash_unique) = re.subn('U', 'H', meta.expression, flags=re.I)

            if hash_unique and re.search('H', meta.expression, re.I):
                raise ConfValueError(
                    self._ScheduleError.mixed_hash.self_format(self, meta.expression)
                )

        hash_id = meta.hash

        if hash_unique:
            hash_id += f'.{uuid.getnode()}'

        # cover for croniter_range not directly supporting hash_id
        croniter_ = functools.partial(croniter.croniter, hash_id=hash_id)

        runs = (croniter_(schedule, t0, max_years_between_matches=max_years_between_matches)
                if t1 is None
                else croniter.croniter_range(t0, t1, schedule, _croniter=croniter_))

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
    def exec_(self) -> typing.Tuple[str]:
        # check for superfluous/conflicting argumentation
        keys = ('exec', 'shell', 'command')
        if sum(key in self for key in keys) > 1:
            raise ConfTypeError(f"{self.__name__}: ambiguous task configuration: "
                                f"specify 0 or 1 of keys: {keys!r}")

        if 'exec' in self:
            try:
                return tuple(template.render_str_list(self['exec']))
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

        return (f'{self.__lib__}-{command}',)

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

        if isinstance(expression, bool):
            return not expression if negate else expression

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
    def timeout_(self) -> typing.Optional[timedelta]:
        timeout = self.get('timeout', self.__default__.get('timeout'))

        # null, false, '', 0 -> None
        if not timeout:
            return None

        if isinstance(timeout, int):
            # negative, zero -> None
            if timeout <= 0:
                return None

            # positive -> timedelta
            return timedelta(seconds=timeout)

        try:
            # '1w2d3h4m5s' -> timedelta
            return parse_timedelta(timeout)
        except ValueError as exc:
            raise ConfValueError(f'{self.__name__}: timeout: {exc}')
        except TypeError:
            raise ConfTypeError(
                f"{self.__name__}: timeout expects integer (seconds), timedelta string "
                f"(e.g. 1w2d3h4m5s), or falsey/null value, not {timeout.__class__.__name__}"
            )

    @at_depth(0)
    @property
    def format_(self):
        return collections.ChainMap(
            self.get('format', {}),
            self.__default__.get('format', {}),
            self._DefaultFormat.__members__,
        )

    @property
    def _default_path_result(self) -> str:
        return str(
            self._prefix_.data
            / 'result'
            / "result"
              "-{{ task_ended_at.timestamp() | int }}"
              "-{{ task_ended_at.strftime('%Y%m%dT%H%M%S') }}"
              "-{{ task_name }}"
              "{{ label_ext }}"
              "{{ ext }}"
        )

    @at_depth(0)
    @property
    @adopt('path')
    def path_(self):
        return TaskChainMap(
            self.get('path', {}),
            self.__default__.get('path', {}),
            {'result': self._default_path_result},
        )

    @at_depth(0)
    @property
    def param_(self) -> str:
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

    @at_depth('*.path')
    def result_(self, **context) -> str:
        spec = self.result

        if not isinstance(spec, (str, NoneType)):
            raise ConfTypeError(f"{self.__path__}.result: expected string or null not "
                                f"{spec.__class__.__name__}")

        # empty path.result synonymous with /dev/null: quit
        if not spec:
            return ''

        default_path = pathlib.Path(
            template.render_template(self._default_path_result, context).strip()
        )

        if not (path := template.render_template(spec, context, default=default_path).strip()):
            return ''

        (path_dir, path_base) = os.path.split(path)

        if path_dir and path_base:
            # all good
            return path

        # fill in blanks with application default
        (default_dir, default_base) = os.path.split(default_path)

        if path_dir:
            return os.path.join(path_dir, default_base)

        return os.path.join(default_dir, path_base)


class TaskConfDict(TaskConfType, ConfDict):
    """Mapping of data deserialized from task configuration files."""


class TaskChainMap(TaskConfType, ConfChain):
    """ChainMap of task configuration with fallback to system-level and
    library-level defaults.

    """
