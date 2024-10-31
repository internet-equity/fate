from __future__ import annotations

import logging
import re
import typing
from dataclasses import dataclass, field

from fate.conf.error import ConfValueError, LogRecordDecodeError
from fate.util.format import SLoader

if typing.TYPE_CHECKING:
    from fate.util.stream import BufferedOutput


@dataclass
class LogReader:

    stream: typing.Union[bytes, BufferedOutput]
    position: int = field(default=0, init=False)

    @property
    def buffer(self) -> bytes:
        return bytes(self.stream)[self.position:]

    def read(self, final=False):
        position = self.position

        for match in re.finditer(rb'(?P<record>[^\0]+)\0+', self.buffer):
            self.position = position + match.end()
            yield match['record']

        if final and (rem := self.buffer):
            self.position += len(rem)
            yield rem


class LogRecord(typing.NamedTuple):

    level: str
    record: typing.Union[dict, str]

    _Loader = SLoader

    _task_log_pattern = re.compile(r'<([1-5])> *(.*)')

    _deserializer_error = ('{conf_path}: unsupported serialization format: {format!r} '
                           f"(select from: {_Loader.__deserializers__})")

    @staticmethod
    def _normalize_level(value):
        if isinstance(value, int):
            if value < 10:
                value *= 10

            return logging._levelToName.get(value)

        if isinstance(value, str):
            value = value.upper()

            return value if value in logging._nameToLevel else None

        raise TypeError("log level to normalize may be of type int or str "
                        f"not {value.__class__.__name__}")

    @classmethod
    def parse(cls, message: str, format: str, name: str) -> LogRecord:
        if level_match := cls._task_log_pattern.fullmatch(message):
            (record_code, record_text) = level_match.groups()
            record_level = cls._normalize_level(int(record_code))
        else:
            (record_level, record_text) = (None, message)

        try:
            (record_struct, _loader) = cls._Loader.autoload(record_text, format)
        except cls._Loader.NonAutoError:
            try:
                loader = cls._Loader[format]
            except KeyError:
                loader = None

            if loader is None or loader.binary:
                raise ConfValueError(
                    cls._deserializer_error.format(
                        conf_path=f'{name}.format.log',
                        format=format,
                    )
                )

            try:
                record_struct = loader(record_text)
            except loader.raises as exc:
                raise LogRecordDecodeError(
                    format,
                    exc,
                    cls(level=record_level or 'INFO', record=record_text)
                ) from exc

        struct_level = record_struct.get('level') if isinstance(record_struct, dict) else None

        if struct_level:
            if isinstance(struct_level, (int, str)):
                struct_level = cls._normalize_level(struct_level)
            else:
                struct_level = None

        return cls(
            level=record_level or struct_level or 'INFO',
            record=record_struct if isinstance(record_struct, dict) else record_text,
        )
