import enum

import fate.conf


class ExitOnError:

    def __init__(self, parser):
        self.parser = parser

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, _traceback):
        if exc_type is None:
            return

        if issubclass(exc_type, fate.conf.MultiConfError):
            paths = ', '.join(exc_value.paths)
            self.parser.exit(64, f'{self.parser.prog}: error: multiple configuration file '
                                 f'formats at overlapping paths: {paths}\n')

        if issubclass(exc_type, fate.conf.ConfSyntaxError):
            self.parser.exit(65, f'{self.parser.prog}: error: could not decode '
                                 f'{exc_value.format.upper()}: {exc_value.decode_err}\n')

        if issubclass(exc_type, fate.conf.NoConfError):
            self.parser.exit(72, f'{self.parser.prog}: error: missing '
                                 f'configuration file (tried: {exc_value})\n')

        if issubclass(exc_type, (fate.conf.ConfTypeError, fate.conf.ConfValueError)):
            self.parser.exit(78, f'{self.parser.prog}: error: {exc_value}\n')


class CommandInterface:

    class CommandStatus(enum.Enum):
        """Status categories of task command return codes."""

        OK = 0
        Retry = 42
        Error = -1  # any other

        @classmethod
        def status(cls, code):
            """Retrieve appropriate status for given return code."""
            value = int(code)

            try:
                return cls(value)
            except ValueError:
                if value > 0:
                    return cls.Error

                raise

        def __str__(self):
            return self.name

    @property
    def conf(self):
        if not self.__parents__:
            # this is the root command
            # retrieve and store conf here
            try:
                conf = self.__dict__['conf']
            except KeyError:
                conf = self.__dict__['conf'] = self.args.__conf__ or fate.conf.get()

            return conf

        # defer to root
        return self.root.conf

    @property
    def exit_on_error(self):
        return ExitOnError(self.args.__parser__)

    @staticmethod
    def write_result(path, contents):
        if path.exists():
            raise FileExistsError(path)

        if not path.parent.exists():
            try:
                path.parent.mkdir(parents=True)
            except NotADirectoryError:
                pass

        if not path.parent.is_dir():
            raise NotADirectoryError(20, 'Not a directory', path.parent)

        with path.open('w') as fd:
            print(contents, end='', file=fd)
