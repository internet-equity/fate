"""Inference, and environment variable specification, of relevant
filesystem paths.

"""
import enum
import functools
import operator
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path

from descriptors import cachedproperty, classonlymethod, classproperty

from fate.util.os import system_path


class PrefixProfile(enum.Flag):

    #
    # system: library was installed under a system (root-writable) path
    #
    # (if not set, library was installed under some user path.)
    #
    system = enum.auto()

    #
    # isolated: library was installed into a python virtual environment
    #           for the purpose of isolation
    #
    # (note: this flag may not be set for some virtual environments,
    # which seek only to isolate the library's requirements, but not
    # the tool itself -- e.g., pipx)
    #
    isolated = enum.auto()

    @classproperty
    def empty(cls):
        return cls(0)

    @classonlymethod
    def infer(cls, lib):
        """Compose profile flags appropriate to the installation context.

        Inference is overridden by the process environment variable:

            {LIB}_PREFIX_PROFILE={system,isolated,empty}

        The value of this variable may be a singular profile name, or a
        list of these, delimited by either "," (comma) or "|" (pipe).

        The special value `empty` may be specified to indicate the empty
        flag, (*i.e.*, non-system, non-isolated, or the "user-global"
        profile).

        Note: Any other non-existant profile names specified by the
        environment variable are ignored.

        """
        #
        # check for profile specified by environment variable
        #

        environ_spec = os.getenv(f'{lib}_PREFIX_PROFILE'.upper(), '')

        # environ-given names may be delimited by comma or pipe
        environ_names = re.findall(r'[^ ,|]+', environ_spec)

        # unrecognized environ-given names are simply ignored
        environ_profiles = [cls[name] for name in environ_names if name in cls.__members__]

        if 'empty' in environ_names:
            environ_profiles.append(cls.empty)

        if environ_profiles:
            return functools.reduce(operator.or_, environ_profiles)

        #
        # infer from installation
        #

        # module either installed under a user home directory
        # (and so will use XDG_CONFIG_HOME, etc.)
        # OR appears global
        # (and so will install global)
        location_profile = cls.system if system_path(Path(__file__)) else cls.empty

        if (
            # using system python
            sys.prefix == sys.base_prefix

            # using tox venv: treat as non-isolated
            or 'pipx' in Path(sys.prefix).parts
        ):
            return location_profile

        # looks isolated (a venv)
        # (and so will construct path from `sys.prefix`)
        return location_profile | cls.isolated


def path(method):
    """Wrap a Path-returning method such that:

    * it may be overridden by an environment variable

    * its result (if any) is given a leaf directory named for the
      PrefixPath.lib attribute

    """
    @functools.wraps(method)
    def wrapped(self, *args, **kwargs):
        if override := os.getenv(f'{self.lib}_PREFIX_{method.__name__}'.upper()):
            return Path(override).absolute()

        method_path = method(self, *args, **kwargs)

        return method_path and method_path / self.lib

    return wrapped


def path_property(prop):
    """Wrap a `Path`-returning method as a `path` and as a
    `cachedproperty`.

    See: `path`.

    """
    return cachedproperty(path(prop))

path.property = path_property


@dataclass
class PrefixPaths:
    """Path prefixes appropriate to the environment.

    Overrides to inference and defaults are retrieved from the
    process environment variables:

        {LIB}_PREFIX_PROFILE={system,isolated}

        {LIB}_PREFIX_{FIELD}=path

    """
    lib: str
    profile: PrefixProfile

    @classonlymethod
    def infer(cls, lib):
        profile = PrefixProfile.infer(lib)
        return cls(lib, profile)

    @path.property
    def conf(self):
        """library configuration"""
        if PrefixProfile.system in self.profile:
            return Path(os.sep) / 'etc'

        if PrefixProfile.isolated in self.profile:
            return Path(sys.prefix)

        if xdg_config := os.getenv('XDG_CONFIG_HOME'):
            return Path(xdg_config)

        return Path.home() / '.config'

    @path.property
    def data(self):
        """results directory (default)"""
        if PrefixProfile.system in self.profile:
            return Path(os.sep) / 'var' / 'log'

        if PrefixProfile.isolated in self.profile:
            return Path(sys.prefix)

        if xdg_data := os.getenv('XDG_DATA_HOME'):
            return Path(xdg_data)

        return Path.home() / '.local' / 'share'

    @path.property
    def state(self):
        """library (retry records) and task state"""
        if PrefixProfile.system in self.profile:
            return Path(os.sep) / 'var' / 'lib'

        if PrefixProfile.isolated in self.profile:
            return Path(sys.prefix)

        if xdg_state := os.getenv('XDG_STATE_HOME'):
            return Path(xdg_state)

        return Path.home() / '.local' / 'state'

    @path.property
    def run(self):
        """run (lock) files"""
        if PrefixProfile.system in self.profile:
            return Path(os.sep) / 'run'

        if PrefixProfile.isolated in self.profile:
            return Path(sys.prefix)

        if xdg_runtime := os.getenv('XDG_RUNTIME_DIR'):
            return Path(xdg_runtime)

        return Path.home() / '.local' / 'run'

    @path
    def completions(self, shell_name, force_system=None):
        """shell completion files"""
        dir_name = 'bash-completion' if shell_name == 'bash' else shell_name

        if force_system or (force_system is None and PrefixProfile.system in self.profile):
            return Path(os.sep) / 'usr' / 'share' / dir_name / 'completions'

        # completions must ignore user virtual env (and shouldn't matter)

        data_path = (Path(xdg_data) if (xdg_data := os.getenv('XDG_DATA_HOME'))
                     else Path.home() / '.local' / 'share')

        return data_path / dir_name / 'completions'
