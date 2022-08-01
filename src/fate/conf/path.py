import enum
import os
import pathlib
import sys
import typing

from descriptors import classonlymethod


class PrefixPaths(typing.NamedTuple):
    """Collection and constructor of relevant filesystem paths."""

    # library configuration
    conf: pathlib.Path

    # results directory (default)
    data: pathlib.Path

    # library (retry records) and task state
    state: pathlib.Path

    @classonlymethod
    def _find(cls):
        """Determine paths appropriate to environment."""
        if sys.prefix == sys.base_prefix:
            # using system python

            home = pathlib.Path.home()

            if pathlib.Path(__file__).is_relative_to(home):
                # module installed under a user home directory
                # use XDG_CONFIG_HOME, etc.
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
                )
            else:
                # appears global: install global
                return cls(
                    conf=pathlib.Path('/etc/'),
                    data=pathlib.Path('/var/log/'),
                    state=pathlib.Path('/var/lib/'),
                )
        else:
            # looks like a virtualenv
            # construct path from `sys.prefix`
            return cls(
                conf=pathlib.Path(sys.prefix),
                data=pathlib.Path(sys.prefix),
                state=pathlib.Path(sys.prefix),
            )

    def _aspairs(self):
        return list(zip(self._fields, self))


#
# enum and pathlib classes *really* do NOT mix under multiclass inheritance.
#
# (E.g.: pathlib invokes object.__new__(cls) and would have to know when to
# allow this to create a new enum -- upon import -- and when to merely create
# standard paths -- when performing path operations.)
#
# As such, it is NOT worth attempting to fashion a proper Enum of Path members
# which also inherit Path methods.
#
# Instead, we'll construct a standard Enum, from a PrefixPaths collection, with
# a simple interface mixed in to defer to the Enum's `value` --
# (the actual Path) -- for *specified* methods and attributes.
#
# (Notably, especially with this set-up, this is little better than just an
# instance of PrefixPaths. However, Enum does add a little extra functionality.)
#

# We might define interface via __getattr__, but then we couldn't override Enum
# methods; and, this will be more straight-forwardly inspectable.

def defer_to_value(name):
    """Construct a property to defer to the `value` attribute for
    lookups at `name`.

    """
    def func(self):
        return getattr(self.value, name)

    func.__name__ = name

    return property(func)

defer_to_value.targets = (
    '__truediv__', '__rtruediv__', '__str__',
    'absolute', 'anchor', 'as_posix', 'as_uri', 'chmod', 'cwd', 'drive', 'exists', 'expanduser',
    'glob', 'group', 'hardlink_to', 'home', 'is_absolute', 'is_block_device', 'is_char_device',
    'is_dir', 'is_fifo', 'is_file', 'is_mount', 'is_relative_to', 'is_reserved', 'is_socket',
    'is_symlink', 'iterdir', 'joinpath', 'lchmod', 'link_to', 'lstat', 'match', 'mkdir', 'name',
    'open', 'owner', 'parent', 'parents', 'parts', 'read_bytes', 'read_text', 'readlink',
    'relative_to', 'rename', 'replace', 'resolve', 'rglob', 'rmdir', 'root', 'samefile', 'stat',
    'stem', 'suffix', 'suffixes', 'symlink_to', 'touch', 'unlink', 'with_name', 'with_stem',
    'with_suffix', 'write_bytes', 'write_text',
)

PathInterface = type(
    'PathInterface',
    (object,),
    {name: defer_to_value(name) for name in defer_to_value.targets},
)


SystemPrefix = enum.Enum('SystemPrefix',
                         PrefixPaths._find()._aspairs(),
                         module=__name__,
                         type=PathInterface)
