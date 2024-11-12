from __future__ import annotations

import collections
import datetime
import enum
import io
import pathlib
import tarfile
import typing

from descriptors import classonlymethod

from fate.common.ext import CompletingTask
from fate.conf.error import ConfValueError, OutputEncodeError
from fate.util.format import SLoader
from fate.util.iteration import returnboth


class TaskOutput(typing.NamedTuple):

    value: bytes
    label: str = ''
    ext: str = ''

    Loader = SLoader

    class ArchiveMode(enum.Enum):

        file = enum.auto()
        archive = enum.auto()
        detect = enum.auto()

    modes = (
        *Loader.__formats__,
        ArchiveMode.archive.name,
    )

    class NonArchiveError(ValueError):
        pass

    @staticmethod
    def _split_suffixes(name: str, sep: str = '.') -> typing.Tuple[str]:
        """Split file basename into tuple of name and full extension.

        Unlike os.path.splitext, the entirety of the extension is split,
        from the first incidence of the separator.

        """
        if name.endswith(sep):
            return (name, '')

        (head, target) = (name[0], name[1:]) if name.startswith(sep) else ('', name)

        (body, tail_sep, tail) = target.partition(sep)

        return (head + body, tail_sep + tail)

    @classonlymethod
    def iter_archive(cls,
                     stdout: bytes,
                     archive_marker: str,
                     archive_mode: ArchiveMode) -> typing.Iterator[TaskOutput]:
        if not isinstance(archive_mode, cls.ArchiveMode):
            raise TypeError(f"expected mode of type {cls.__name__}.{cls.ArchiveMode.__name__} "
                            f"not {archive_mode.__class__.__name__}")

        if archive_mode is cls.ArchiveMode.file:
            raise ValueError(f"cannot expand archive in non-archive mode {archive_mode}")

        st_file = io.BytesIO(stdout)

        try:
            archive = tarfile.open(fileobj=st_file)
        except tarfile.TarError:
            raise cls.NonArchiveError("payload not recognizable as tar archive")

        if archive_mode is cls.ArchiveMode.detect:
            try:
                marker = archive.getmember(f'.{archive_marker}')
            except KeyError:
                raise cls.NonArchiveError(f"archive marker '.{archive_marker}' "
                                          f"not detected within tar archive")

            if not marker.isdir():
                raise cls.NonArchiveError(
                    f"tar member found with name of archive marker but should "
                    f"be directory (type {tarfile.DIRTYPE}) not type {marker.type}"
                )

        for member in archive:
            if member.isfile():
                yield cls(archive.extractfile(member).read(),
                          *cls._split_suffixes(member.name))

    @classonlymethod
    @returnboth
    def detect_format(
        cls,
        stdout: bytes,
        formats: typing.Iterable[str],
    ) -> typing.Iterator[Exception]:
        formats = set(formats)

        if unsupported_formats := formats.difference(cls.modes):
            raise ConfValueError(f"unexpected format mode(s) {unsupported_formats!r} "
                                 f"not member(s) of {cls.modes!r}")

        try:
            (_struct, loader) = cls.Loader.autoload(stdout, formats)
        except cls.Loader.NonAutoError:
            pass
        else:
            return loader.suffix if loader else ''

        binary = stdout

        try:
            text = binary.decode()
        except UnicodeDecodeError as exc:
            decode_exc = exc
            text = None
        else:
            decode_exc = None

        for format_ in formats:
            try:
                loader = cls.Loader[format_]
            except KeyError:
                raise ConfValueError(f"unexpected format {format_!r} not a member of "
                                     f"{set(cls.Loader.__members__)!r}")

            encoded = binary if loader.binary else text

            if encoded is None:
                yield decode_exc
                continue

            try:
                decoding = loader(encoded)
            except loader.raises as exc:
                yield exc
            else:
                if isinstance(decoding, cls.Loader.Decoding):
                    loader = decoding.decoder

                return loader.suffix

        return ''

    @classonlymethod
    def parse(
        cls,
        stdout: bytes,
        archive_marker: str,
        mode: typing.Union[str, typing.Iterable[str]] = 'auto',
    ) -> typing.Iterator[TaskOutput]:
        if not mode:
            modes = set()
        elif isinstance(mode, str) or not isinstance(mode, collections.abc.Iterable):
            modes = {mode}
        else:
            modes = set(mode)

        if 'archive' in modes:
            archive_mode = cls.ArchiveMode.archive
            modes.remove('archive')
        elif modes & {'auto', 'mixed'}:
            archive_mode = cls.ArchiveMode.detect
        else:
            archive_mode = cls.ArchiveMode.file

        if 'auto' in modes and not stdout:
            # auto (unlike mixed) suppresses empty results: quit
            return

        if archive_mode is not cls.ArchiveMode.file:
            # if not configured for tar output then check for expandable tar archive
            try:
                yield from cls.iter_archive(stdout, archive_marker, archive_mode)
            except cls.NonArchiveError:
                pass
            else:
                return

        (ext, errors) = cls.detect_format(stdout, modes)

        if errors and not ext:
            raise OutputEncodeError(cls(stdout), mode, *errors)

        yield cls(stdout, ext=ext)


class TaskResult(typing.NamedTuple):

    value: bytes
    path: pathlib.Path

    @classonlymethod
    def compose(cls, task: CompletingTask, output: TaskOutput) -> typing.Optional[TaskResult]:
        path = task.path_.result_(
            ext=output.ext,
            label=output.label,
            label_ext=output.label and f'-{output.label}',
            task_ended_at=datetime.datetime.fromtimestamp(task.ended_()),
            task_name=task.__name__,
        )

        return cls(
            value=output.value,
            path=pathlib.Path(path),
        ) if path else None
