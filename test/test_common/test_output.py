import io
import json
import pytest
import tarfile

import toml

import fate.conf
from fate.common.output import TaskOutput


class TarBytes:

    def __init__(self, compression=''):
        self.fileobj = io.BytesIO()
        self.archive = tarfile.open(fileobj=self.fileobj, mode=f'w:{compression}')

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        return self.archive.__exit__(*args, **kwargs)

    def add_content(self, name: str, content: bytes):
        info = tarfile.TarInfo(name)
        info.size = len(content)
        self.archive.addfile(info, io.BytesIO(content))

    def add_directory(self, name: str):
        info = tarfile.TarInfo(name)
        info.type = tarfile.DIRTYPE
        self.archive.addfile(info)

    def getvalue(self):
        return self.fileobj.getvalue()

    def __bytes__(self):
        return self.getvalue()


class TestIterArchive:

    @classmethod
    def setup_class(cls):
        cls.conf = fate.conf.get()

    @classmethod
    def list_archive(cls, stdout: bytes, *args, **kwargs):
        if not args:
            kwargs.setdefault('archive_marker', cls.conf._lib_)

        return list(
            TaskOutput.iter_archive(stdout, *args, **kwargs)
        )

    def test_bad_archive_mode(self):
        with pytest.raises(TypeError):
            self.list_archive(b'...', archive_mode=None)

    def test_file_archive_mode(self):
        with pytest.raises(ValueError):
            self.list_archive(b'...', archive_mode=TaskOutput.ArchiveMode.file)

    def test_archive_non_archive(self):
        with pytest.raises(TaskOutput.NonArchiveError):
            self.list_archive(b'...', archive_mode=TaskOutput.ArchiveMode.archive)

    def test_detect_non_archive(self):
        with pytest.raises(TaskOutput.NonArchiveError):
            self.list_archive(b'...', archive_mode=TaskOutput.ArchiveMode.detect)

    def test_detect_no_marker(self):
        with TarBytes() as target:
            target.add_content('results.json', b'{"foo": "bar"}')

        with pytest.raises(TaskOutput.NonArchiveError):
            self.list_archive(bytes(target), archive_mode=TaskOutput.ArchiveMode.detect)

    def test_detect_bad_marker(self):
        with TarBytes('gz') as encap:
            encap.add_content('special.json', b'{"alpha": "beta"}')

        with TarBytes() as target:
            target.add_content('.fate', b'')
            target.add_content('results.json', b'{"foo": "bar"}')
            target.add_content('special.json.tar.gz', bytes(encap))

        with pytest.raises(TaskOutput.NonArchiveError):
            self.list_archive(bytes(target), archive_mode=TaskOutput.ArchiveMode.detect)

    def test_detect_marker(self):
        with TarBytes('gz') as encap:
            encap.add_content('special.json', b'{"alpha": "beta"}')

        with TarBytes() as target:
            target.add_directory('.fate')
            target.add_content('results.json', b'{"foo": "bar"}')
            target.add_content('special.json.tar.gz', bytes(encap))

        assert self.list_archive(
            bytes(target),
            archive_mode=TaskOutput.ArchiveMode.detect,
        ) == [
            TaskOutput(value=b'{"foo": "bar"}', label='results', ext='.json'),
            TaskOutput(value=bytes(encap), label='special', ext='.json.tar.gz'),
        ]

    def test_archive(self):
        with TarBytes('gz') as encap:
            encap.add_content('special.json', b'{"alpha": "beta"}')

        with TarBytes() as target:
            target.add_content('results.json', b'{"foo": "bar"}')
            target.add_content('special.json.tar.gz', bytes(encap))

        assert self.list_archive(
            bytes(target),
            archive_mode=TaskOutput.ArchiveMode.archive,
        ) == [
            TaskOutput(value=b'{"foo": "bar"}', label='results', ext='.json'),
            TaskOutput(value=bytes(encap), label='special', ext='.json.tar.gz'),
        ]


class TestDetectFormat:

    def test_bad_format(self):
        with pytest.raises(fate.conf.ConfValueError):
            TaskOutput.detect_format(b'....', ['A4'])

    def test_auto_empty(self):
        assert TaskOutput.detect_format(b'', ['auto']) == ('', [])

    def test_auto_json(self):
        assert TaskOutput.detect_format(b'{"foo": "bar"}', ['auto']) == ('.json', [])

    def test_auto_yaml(self):
        assert TaskOutput.detect_format(b'foo: bar', ['auto']) == ('.yaml', [])

    def test_auto_toml(self):
        assert TaskOutput.detect_format(b'foo = "bar"', ['auto']) == ('.toml', [])

    def test_auto_tar(self):
        with TarBytes() as archive:
            archive.add_content('results.json', b'{"foo": "bar"}')

        assert TaskOutput.detect_format(bytes(archive), ['auto']) == ('.tar', [])

    def test_auto_tar_gz(self):
        with TarBytes('gz') as archive:
            archive.add_content('results.json', b'{"foo": "bar"}')

        assert TaskOutput.detect_format(bytes(archive), ['auto']) == ('.tar.gz', [])

    def test_auto_csv_fail(self):
        assert TaskOutput.detect_format(b'foo,bar', ['auto']) == ('', [])

    def test_format_empty(self):
        assert TaskOutput.detect_format(b'{"foo": "bar"}', ()) == ('', [])

    def test_format_json(self):
        assert TaskOutput.detect_format(b'{"foo": "bar"}', ['json']) == ('.json', [])

    def test_format_json_toml(self):
        (suffix, _errors) = TaskOutput.detect_format(b'{"foo": "bar"}', ('toml', 'json'))
        assert suffix == '.json'

    def test_format_decode_error(self):
        with TarBytes('gz') as archive:
            archive.add_content('results.json', b'{"foo": "bar"}')

        (suffix, errors) = TaskOutput.detect_format(bytes(archive), ['json'])
        assert suffix == ''
        assert len(errors) == 1
        assert isinstance(errors[0], UnicodeDecodeError)

    def test_format_deserialize_error(self):
        (suffix, errors) = TaskOutput.detect_format(b'foo: bar', ('toml', 'json'))
        assert suffix == ''
        assert {type(error) for error in errors} == {json.JSONDecodeError, toml.TomlDecodeError}


class TestParse:

    @classmethod
    def setup_class(cls):
        cls.conf = fate.conf.get()

    @classmethod
    def list_outputs(cls, stdout: bytes, *args, **kwargs):
        if not args:
            kwargs.setdefault('archive_marker', cls.conf._lib_)

        return list(
            TaskOutput.parse(stdout, *args, **kwargs)
        )

    def test_auto_empty(self):
        assert self.list_outputs(b'', mode='auto') == []

    def test_mixed_empty(self):
        assert self.list_outputs(b'', mode='mixed') == [TaskOutput(b'')]

    def test_archive(self):
        with TarBytes() as archive:
            archive.add_directory('.fate')
            archive.add_content('results.json', b'{"foo": "bar"}')

        assert self.list_outputs(bytes(archive)) == [
            TaskOutput(b'{"foo": "bar"}', label='results', ext='.json'),
        ]

    def test_tar(self):
        with TarBytes() as archive:
            archive.add_content('results.json', b'{"foo": "bar"}')

        assert self.list_outputs(bytes(archive)) == [
            TaskOutput(bytes(archive), label='', ext='.tar'),
        ]

    def test_json(self):
        assert self.list_outputs(b'{"foo": "bar"}') == [TaskOutput(b'{"foo": "bar"}', ext='.json')]

    def test_json_toml(self):
        assert self.list_outputs(b'{"foo": "bar"}', mode=('toml', 'json')) == [
            TaskOutput(b'{"foo": "bar"}', ext='.json'),
        ]

    def test_deserialize_error(self):
        with pytest.raises(fate.conf.OutputEncodeError) as ctx:
            self.list_outputs(b'foo: bar', mode=('toml', 'json'))

        assert ctx.value.output == TaskOutput(b'foo: bar')
        assert ctx.value.format == ('toml', 'json')
        assert {type(error) for error in ctx.value.errors} == {json.JSONDecodeError,
                                                               toml.TomlDecodeError}
