import os
import pathlib
import sys
import typing

from descriptors import classonlymethod

from fate.util.compat.path import is_relative_to


class PrefixPaths(typing.NamedTuple):
    """Collection and constructors of relevant filesystem paths."""

    # library configuration
    conf: pathlib.Path

    # results directory (default)
    data: pathlib.Path

    # library (retry records) and task state
    state: pathlib.Path

    # run (lock) files
    run: pathlib.Path

    @classonlymethod
    def _make_system(cls):
        return cls(
            conf=pathlib.Path('/etc/'),
            data=pathlib.Path('/var/log/'),
            state=pathlib.Path('/var/lib/'),
            run=pathlib.Path('/run/'),
        )

    @classonlymethod
    def _make_user(cls):
        home = pathlib.Path.home()

        return cls(
            conf=(pathlib.Path(xdg_config)
                  if (xdg_config := os.getenv('XDG_CONFIG_HOME'))
                  else home / '.config'),
            data=(pathlib.Path(xdg_data)
                  if (xdg_data := os.getenv('XDG_DATA_HOME'))
                  else home / '.local' / 'share'),
            state=(pathlib.Path(xdg_state)
                  if (xdg_state := os.getenv('XDG_STATE_HOME'))
                  else home / '.local' / 'state'),
            run=(pathlib.Path(xdg_runtime)
                 if (xdg_runtime := os.getenv('XDG_RUNTIME_DIR'))
                 else home / '.local' / 'run'),
        )

    @classonlymethod
    def _make_venv(cls):
        return cls(
            conf=pathlib.Path(sys.prefix),
            data=pathlib.Path(sys.prefix),
            state=pathlib.Path(sys.prefix),
            run=pathlib.Path(sys.prefix),
        )

    @classonlymethod
    def _infer_paths(cls):
        if sys.prefix == sys.base_prefix:
            # using system python

            # compat: Python <3.9
            if is_relative_to(pathlib.Path(__file__), pathlib.Path.home()):
                # module installed under a user home directory
                # use XDG_CONFIG_HOME, etc.
                return cls._make_user()
            else:
                # appears global: install global
                return cls._make_system()
        else:
            # looks like a virtualenv
            # construct path from `sys.prefix`
            return cls._make_venv()

    @classonlymethod
    def _infer(cls, lib):
        """Determine path prefixes appropriate to environment.

        Overrides to inference and defaults are retrieved from the
        process environment variables:

            {LIB}_PREFIX_PROFILE={system,user,venv}

            {LIB}_PREFIX_{FIELD}=path

        """
        environ_profile = os.getenv(f'{lib}_PREFIX_PROFILE'.upper())

        if environ_profile == 'system':
            prefixes = cls._make_system()
        elif environ_profile == 'user':
            prefixes = cls._make_user()
        elif environ_profile == 'venv':
            prefixes = cls._make_venv()
        else:
            prefixes = cls._infer_paths()

        # add lib leaf directory to all defaults
        paths = cls._make(prefix / lib for prefix in prefixes)

        environ_overrides = (
            (field, os.getenv(f'{lib}_PREFIX_{field}'.upper()))
            for field in cls._fields
        )
        replacements = {
            field: pathlib.Path(override).absolute()
            for (field, override) in environ_overrides if override
        }

        return paths._replace(**replacements) if replacements else paths
