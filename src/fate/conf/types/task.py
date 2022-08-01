import collections
import pathlib
from datetime import datetime

from ..datastructure import at_depth, StrEnum
from ..error import ConfTypeError, ConfValueError
from ..format import Dumper, SLoader
from ..path import SystemPrefix

from .base import ConfChain, ConfDict, ConfInterface


class TaskMap(ConfInterface):
    """Mapping applied to data deserialized from task configuration files."""

    _Dumper = Dumper
    _Loader = SLoader

    class _DefaultFormat(StrEnum):

        param = 'json'
        result = 'auto'

    @at_depth(0)
    @property
    def exec_(self):
        if 'exec' in self:
            if 'command' in self:
                raise ConfTypeError("ambiguous configuration: specify either "
                                    "task 'command' or 'exec' not both")

            return self['exec']

        command = self['command'] if 'command' in self else self.__name__

        return f'{self.__lib__}-{command}'

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
    def path_(self):
        return TaskChainMap.nest(self, 'path',
            self.get('path', {}),
            self.__default__.get('path', {}),
            {'result': SystemPrefix.data / self.__lib__ / 'result'},
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

    @at_depth('*.path')
    def result_(self, stdout, dt=None):
        if dt is None:
            dt = datetime.now()

        stamp = dt.timestamp()
        datestr = dt.strftime('%Y%m%dT%H%M%S')

        result_spec = self.result
        result_path = (result_spec if isinstance(result_spec, (pathlib.Path, SystemPrefix))
                       else pathlib.Path(result_spec))

        identifier = result_path / f'result-{stamp:.0f}-{datestr}-{self.__parent__.__name__}'

        format_ = self.__parent__.format_['result']

        if not format_:
            loader = None
        elif format_ == 'auto':
            for loader in self._Loader.__auto__:
                try:
                    result = loader(stdout)
                except loader.raises:
                    pass
                else:
                    # yaml.load treats a string literal as a valid document;
                    # however, this is probably not what's wanted.
                    if not isinstance(result, str):
                        break
            else:
                loader = None
        else:
            try:
                loader = self._Loader[format_]
            except KeyError:
                raise ConfValueError(
                    f'{self.__parent__.__name__}.format.result: unsupported serialization format: '
                    f"{format_!r} (select from: {self._Loader.__names__})"
                )

            try:
                loader(stdout)
            except loader.raises:
                loader = None

        return identifier.with_suffix(loader.suffix) if loader else identifier


class TaskConf(TaskMap, ConfDict):
    pass


class TaskChainMap(TaskMap, ConfChain):
    pass
