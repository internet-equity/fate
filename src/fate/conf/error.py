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


class LogsDecodingError(ValueError, ConfError):

    def __init__(self, format_, errors, logs):
        super().__init__(format_, errors, logs)
        self.format = format_
        self.errors = errors
        self.logs = logs


class ResultEncodingError(ValueError, ConfError):

    def __init__(self, format_, errors, identifier):
        super().__init__(format_, errors, identifier)
        self.format = format_
        self.errors = errors
        self.identifier = identifier


class StateEncodingError(ValueError, ConfError):

    def __init__(self, format_, error):
        super().__init__(format_, error)
        self.format = format_
        self.error = error


class ConfBracketError(ValueError, ConfError):

    message = ('expression SHOULD NOT be enclosed by brackets '
               'outside of interpolation context')

    def __init__(self, path, evaluation):
        super().__init__(path, evaluation)
        self.path = path
        self.evaluation = evaluation

    def __str__(self):
        return self.message
