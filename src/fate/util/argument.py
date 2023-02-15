import argparse
import enum
import functools
import operator
import os
import pathlib


class ChoiceMapping(argparse.Action):
    """Argparse Action to interpret the `choices` argument as a
    mapping of user-specified choices values to the resulting option
    values.

    """
    def __init__(self, *args, choices, **kwargs):
        super().__init__(*args, choices=choices.keys(), **kwargs)
        self.mapping = choices

    def __call__(self, parser, namespace, value, option_string=None):
        setattr(namespace, self.dest, self.mapping[value])


def access_parent(path):
    access_target = path

    # Path.exists() calls stat() which can raise PermissionError (prematurely)
    while not os.path.exists(access_target) and access_target.name != '':
        access_target = access_target.parent

    return access_target


class PathAccess:
    """Argparse type ensuring filesystem access to given path argument.

    An instance of pathlib.Path is returned.

    """
    class Access(enum.IntFlag):

        r = os.R_OK
        w = os.W_OK

        @property
        def mode(self):
            #
            # note: enum has various internal means of determining contents of
            # a composite flag until proper instance iteration lands in 3.11 or
            # so.
            #
            # rather than worrying about that, here we determine contained names
            # manually. in 3.11 should be even simpler.
            #
            return ''.join(member.name for member in self.__class__ if member in self)

        def ok(self, path):
            return os.access(path, self)

    class PathAccessError(argparse.ArgumentTypeError):
        """Subclass of ArgumentTypeError raised when path permissions
        do not match specified mode.

        """

    def __init__(self, mode, parents=False):
        if isinstance(mode, str):
            self.access = functools.reduce(operator.or_, (self.Access[part] for part in mode))
        elif isinstance(mode, int):
            self.access = self.Access(mode)
        elif isinstance(mode, self.Access):
            self.access = mode
        else:
            raise TypeError('expected access mode of type str, int or Access '
                            'not ' + mode.__class__.__name__)

        self.parents = parents

    def __call__(self, value):
        path = pathlib.Path(value)

        access_target = access_parent(path) if self.parents else path

        if not self.access.ok(access_target):
            raise self.PathAccessError("failed to access path with mode "
                                       f"{self.access.mode}: {path}")

        return path


class PathTypeError(argparse.ArgumentTypeError):
    """Subclass of ArgumentTypeError raised for path of incorrect type."""


class FileAccess(PathAccess):

    PathTypeError = PathTypeError

    def __call__(self, value):
        path = super().__call__(value)

        if self.parents and not path.exists():
            if not access_parent(path).is_dir():
                raise self.PathTypeError(f"path inaccessible: {path}")
        elif not path.is_file():
            raise self.PathTypeError(f"path must be file: {path}")

        return path


class DirAccess(PathAccess):

    PathTypeError = PathTypeError

    def __call__(self, value):
        path = super().__call__(value)

        if self.parents and not path.exists():
            if not access_parent(path).is_dir():
                raise self.PathTypeError(f"path inaccessible: {path}")
        elif not path.is_dir():
            raise self.PathTypeError(f"path must be directory: {path}")

        return path
