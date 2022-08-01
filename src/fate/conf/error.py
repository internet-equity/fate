class ConfError(Exception):
    pass


class ConfTypeError(TypeError, ConfError):
    pass


class ConfValueError(ValueError, ConfError):
    pass


class NoConfError(LookupError, ConfError):
    pass


class MultiConfError(ValueError, ConfError):

    @property
    def paths(self):
        return [str(path) for path in self.args]
