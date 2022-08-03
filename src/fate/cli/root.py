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
        if conf := self.args.__conf__:
            return conf

        return fate.conf.get()


def extend_parser(parser, conf):
    parser.set_defaults(
        __conf__=conf,
    )


def main(conf=None):
    # auto-discover nested commands
    argcmdr.init_package(
        fate.cli.command.__path__,
        fate.cli.command.__name__,
    )

    argcmdr.main(Fate, extend_parser=partial(extend_parser, conf=conf))
