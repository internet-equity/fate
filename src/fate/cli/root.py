from functools import partial

import argcmdr
from descriptors import cachedproperty

import fate.cli.command
import fate.conf


class Fate(argcmdr.RootCommand):
    """manage the periodic execution of commands"""

    @classmethod
    def base_parser(cls):
        parser = super().base_parser()

        # enforce program name when invoked via "python -m fate"
        if parser.prog == '__main__.py':
            parser.prog = 'fate'

        return parser

    @cachedproperty
    def conf(self):
        (args, kwargs) = self.args.__confspec__ or ((), {})
        return fate.conf.get(*args, **kwargs)


def extend_parser(parser, confspec):
    parser.set_defaults(
        __confspec__=confspec,
    )


def main(confspec=None):
    # auto-discover nested commands
    argcmdr.init_package(
        fate.cli.command.__path__,
        fate.cli.command.__name__,
    )

    argcmdr.main(Fate, extend_parser=partial(extend_parser, confspec=confspec))
