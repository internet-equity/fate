class ConfError(Exception):
    pass


class ConfSyntaxError(ConfError):

    def __init__(self, format_, decode_err):
        super().__init__(format_, decode_err)
        self.format = format_
        self.decode_err = decode_err


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
