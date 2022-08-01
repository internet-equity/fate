"""Support for configuration file formats."""
from functools import partial

import csv
import json
import toml
import yaml
from descriptors import classproperty

from .datastructure import CallableEnum, FileFormatEnum


class ConfigurableYamlLoader(yaml.SafeLoader):
    """YAML SafeLoader supporting alternate mapping classes.

    Mapping objects default to the built-in `dict`. An alternate
    constructor may be specified upon initialization of the loader.

    Note: To use with the default interface, `yaml.load()`, a `partial`
    must be constructed, for example:

        yaml.load(stream, functools.partial(ConfigurableYamlLoader,
                                            dict_=NewDict))

    """
    def __init__(self, stream, dict_=None):
        super().__init__(stream)
        self.dict_ = dict_

    def construct_yaml_map(self, node):
        data = {} if self.dict_ is None else self.dict_()
        yield data
        value = self.construct_mapping(node)
        data.update(value)


ConfigurableYamlLoader.add_constructor('tag:yaml.org,2002:map',
                                       ConfigurableYamlLoader.construct_yaml_map)


class tag:
    """configurable decorator to set given attribute to given value"""

    _undefined_ = object()

    def __init__(self, name, value=_undefined_):
        self.name = name
        self.value = value

    def __call__(self, value_or_target):
        if self.value is self._undefined_:
            return self.__class__(self.name, value_or_target)

        setattr(value_or_target, self.name, self.value)
        return value_or_target

    def __repr__(self):
        value = 'Undefined' if self.value is self._undefined_ else repr(self.value)
        return f"{self.__class__.__name__}({self.name!r}, {value})"


raises = tag('raises')

auto = tag('auto', True)


class _NameList:

    @classproperty
    def __names__(cls):
        return [member.name for member in cls]


class SLoader(_NameList, FileFormatEnum, CallableEnum):

    @CallableEnum.member
    @raises(csv.Error)
    def csv(text):
        return list(csv.reader(text.splitlines()))

    @CallableEnum.member
    @auto
    @raises(json.decoder.JSONDecodeError)
    def json(text):
        return json.loads(text)

    @CallableEnum.member
    @auto
    @raises(toml.decoder.TomlDecodeError)
    def toml(text, dict_=dict):
        return toml.loads(text, _dict=dict_)

    @CallableEnum.member
    @auto
    @raises(yaml.error.YAMLError)
    def yaml(text, dict_=dict):
        conf = yaml.load(text, partial(ConfigurableYamlLoader, dict_=dict_))
        return {} if conf is None else conf

    @classproperty
    def __auto__(cls):
        return [member for member in cls if member.auto]

    @property
    def auto(self):
        return getattr(self.value, 'auto', False)

    @property
    def raises(self):
        return getattr(self.value, 'raises', ())


class Loader(FileFormatEnum, CallableEnum):

    @CallableEnum.member
    def toml(path, dict_=dict):
        return toml.load(path, _dict=dict_)

    @CallableEnum.member
    def yaml(path, dict_=dict):
        with open(path) as fd:
            return SLoader.yaml(fd, dict_=dict_)


class Dumper(_NameList, CallableEnum):

    @CallableEnum.member
    def json(obj):
        return json.dumps(obj)

    @CallableEnum.member
    def toml(obj):
        return toml.dumps(obj)

    @CallableEnum.member
    def yaml(obj):
        return yaml.dump(obj)
