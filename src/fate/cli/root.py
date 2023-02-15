from functools import partial

import argcmdr

import fate.cli.command
from fate.cli.base import Main


def extend_parser(parser, conf=None, banner_path=None, entry_points=None):
    parser.set_defaults(
        __conf__=conf,
        __banner_path__=banner_path,
        __entry_points__=entry_points,
    )


def entrypoint(root, **settings):
    # auto-discover nested commands
    argcmdr.init_package(
        fate.cli.command.__path__,
        fate.cli.command.__name__,
    )

    if isinstance(root, str):
        # lazily look up nested command signature
        names = root.split('.')
        root = fate.cli.command
        for name in names:
            root = getattr(root, name)

    argcmdr.main(root, extend_parser=partial(extend_parser, **settings))


main = partial(entrypoint, Main)

daemon = partial(entrypoint, 'control.Daemon')

serve = partial(entrypoint, 'control.Serve')
