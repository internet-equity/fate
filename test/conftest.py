import pytest

from .fixture import ConfFixture


def conf_fixture(format_, /, monkeypatch, tmp_path):
    conf_path = tmp_path / 'conf'
    state_path = tmp_path / 'state'

    monkeypatch.setenv('FATE_PREFIX_CONF', str(conf_path))
    monkeypatch.setenv('FATE_PREFIX_STATE', str(state_path))

    return ConfFixture(conf_path, format_)


@pytest.fixture
def conf_yaml(monkeypatch, tmp_path):
    return conf_fixture('yaml', monkeypatch, tmp_path)


@pytest.fixture
def conf_toml(monkeypatch, tmp_path):
    return conf_fixture('toml', monkeypatch, tmp_path)


@pytest.fixture
def confpatch(monkeypatch, tmp_path):
    return conf_fixture('yaml', monkeypatch, tmp_path)
