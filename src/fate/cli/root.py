import argcmdr

import fate.cli.command


class Fate(argcmdr.RootCommand):
    """manage the periodic execution of commands"""

    @classmethod
    def base_parser(cls):
        parser = super().base_parser()

        # enforce program name when invoked via "python -m fate"
        if parser.prog == '__main__.py':
            parser.prog = 'fate'

        return parser


def main():
    # auto-discover nested commands
    argcmdr.init_package(
        fate.cli.command.__path__,
        fate.cli.command.__name__,
    )

    argcmdr.main(Fate)
