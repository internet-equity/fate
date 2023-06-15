import toml
import yaml

from descriptors import cachedproperty

from fate import conf
from fate.util.log import StructLogger

from .log import LogCapture


class ConfFixture:

    @staticmethod
    def _init_paths_(path_base, extension):
        path_base.mkdir(exist_ok=True)

        for conf_name in ('defaults', 'tasks'):
            conf_path = path_base / f'{conf_name}.{extension}'
            conf_path.touch()

    def __init__(self, path_base, extension):
        self._init_paths_(path_base, extension)

        self.conf = conf.get()
        self.handler = yaml if extension == 'yaml' else toml

    def _set_(self, conf, data):
        with conf.__path__.open('w') as fd:
            self.handler.dump(data, fd)

    def set_tasks(self, data):
        self._set_(self.conf.task, data)

    def set_defaults(self, data):
        self._set_(self.conf.default, data)

    @cachedproperty
    def logger(self):
        return StructLogger(self.conf.default.path_.log_)

    def caplog(self, *args, **kwargs):
        return LogCapture.caplog(self.logger, *args, **kwargs)
