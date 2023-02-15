import os.path
import sys

import argcmdr

from .common import CommandInterface


class Main(CommandInterface, argcmdr.RootCommand):
    """manage the periodic execution of commands"""

    @classmethod
    def _new_parser_(cls):
        parser = super()._new_parser_()

        # enforce program name when invoked via "python -m fate"
        if parser.prog == '__main__.py':
            command = os.path.basename(sys.executable)
            parser.prog = f'{command} -m fate'

        return parser
