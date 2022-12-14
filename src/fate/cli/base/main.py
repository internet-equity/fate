import argcmdr

from .common import CommandInterface


class Main(CommandInterface, argcmdr.RootCommand):
    """manage the periodic execution of commands"""

    @classmethod
    def base_parser(cls):
        parser = super().base_parser()

        # enforce program name when invoked via "python -m fate"
        if parser.prog == '__main__.py':
            parser.prog = 'fate'

        return parser
