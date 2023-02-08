import enum
import os
import sys
from pathlib import Path

import argcomplete

from fate.util.argument import FileAccess, access_parent
from fate.util.compat.argument import BooleanOptionalAction
from fate.util.datastructure import StrEnum
from plumbum import colors

from .. import Main


class StatusSymbol(StrEnum):

    complete   = colors.bold & colors.success | '☑'  # noqa: E221
    failed     = colors.bold & colors.fatal   | '☒'  # noqa: E221
    incomplete = colors.bold & colors.info    | '☐'  # noqa: E221


class EndStatus(enum.Enum):

    complete   = (StatusSymbol.complete,   'installed')  # noqa: E221,E241
    failed     = (StatusSymbol.failed,     'failed')     # noqa: E221,E241
    incomplete = (StatusSymbol.incomplete, 'skipped')    # noqa: E221,E241

    @property
    def symbol(self):
        return self.value[0]

    @property
    def message(self):
        return self.value[1]


class TaskSymbol(StrEnum):

    comp = colors.bold | '↹'


@Main.register
class Init(Main):
    """post-installation initializations"""

    def __init__(self, parser):
        tty_detected = sys.stdin.isatty()
        prompt_default = 'prompt' if tty_detected else 'no prompt'

        parser.add_argument(
            '--prompt',
            default=tty_detected,
            action=BooleanOptionalAction,
            help=f"prompt to confirm actions via TTY (default: {prompt_default})",
        )

    def __call__(self):
        print(colors.title | 'shell completion', end='\n\n')

        self['comp'].delegate()

    class Comp(Main):
        """install shell tab-completion files"""

        script_suffixes = ('', 'd', 's')

        class Shell(StrEnum):

            bash = 'bash'
            fish = 'fish'
            tcsh = 'tcsh'

            @classmethod
            def get_choices(cls):
                return sorted(str(member) for member in cls)

            @classmethod
            def get_default(cls):
                login_shell = os.getenv('SHELL')

                if not login_shell:
                    return None

                shell_path = Path(login_shell)

                if not shell_path.is_file():
                    return None

                shell_name = shell_path.name

                return cls.__members__.get(shell_name)

        def __init__(self, parser):
            shell_default = self.Shell.get_default()
            parser.add_argument(
                '--shell',
                choices=self.Shell.get_choices(),
                default=shell_default,
                help="shell for which to install completion "
                     + ("(default: %(default)s)" if shell_default else "(required)"),
                required=shell_default is None,
            )

            target = parser.add_mutually_exclusive_group()
            target.add_argument(
                '--system',
                default=None,
                dest='system_profile',
                action='store_true',
                help="force system-wide installation (default: inferred)",
            )
            target.add_argument(
                '--user',
                default=None,
                dest='system_profile',
                action='store_false',
                help="force user-only installation (default: inferred)",
            )
            target.add_argument(
                'path',
                nargs='?',
                type=FileAccess('rw', parents=True),
                help="force installation to file at path (default: inferred)",
            )

        def __call__(self, args, parser):
            """install shell completion"""
            # determine installation path
            if args.path:
                completions_path = args.path
            else:
                completions_path = self.conf._prefix_.completions(args.shell, args.system_profile)

                if completions_path.exists():
                    access_target = completions_path

                    if access_target.is_dir():
                        parser.print_usage(sys.stderr)
                        parser.exit(71, f'{parser.prog}: fatal: inferred path is '
                                        f'extant directory: {completions_path}\n')
                else:
                    access_target = access_parent(completions_path)

                    if not access_target.is_dir():
                        parser.print_usage(sys.stderr)
                        parser.exit(71, f'{parser.prog}: fatal: inferred path is '
                                        f'inaccessible: {completions_path}\n')

                if not os.access(access_target, os.R_OK | os.W_OK):
                    parser.print_usage(sys.stderr)
                    parser.exit(73, f'{parser.prog}: fatal: inferred path is '
                                    f'not read-writable: {completions_path}\n')

            # determine file contents
            entry_points = args.__entry_points__ or [f'{self.conf._lib_}{suffix}'
                                                     for suffix in self.script_suffixes]

            contents = argcomplete.shellcode(entry_points, shell=args.shell)

            # check file status
            try:
                up_to_date = completions_path.read_text() == contents
            except FileNotFoundError:
                file_exists = up_to_date = False
            else:
                file_exists = True

            # print status line
            print(StatusSymbol.complete if up_to_date else StatusSymbol.incomplete,
                  TaskSymbol.comp,
                  colors.underline & colors.dim | str(completions_path),
                  sep='  ')

            lines = 1

            if up_to_date:
                status = EndStatus.complete
            else:
                if args.prompt:
                    lines += 2

                    print(
                        '\n_ [Y|n]',
                        'update' if file_exists else 'install',
                        'shell completion?',
                        end='\r',  # return
                    )

                    with colors:
                        colors.underline()  # must be reset by context manager

                        try:
                            while (do_install := input().lower() or 'y') not in 'yn':
                                pass
                        except KeyboardInterrupt:
                            # treat as input of "n"
                            do_install = 'n'
                            print('\r', do_install, ~colors.underline, ' ', sep='')
                        else:
                            if do_install == 'y':
                                # set empty
                                print('\033[F', 'Y', sep='')

                else:
                    do_install = 'y'

                if do_install == 'y':
                    try:
                        completions_path.parent.mkdir(parents=True,
                                                      exist_ok=True)
                        completions_path.write_text(contents)
                    except OSError:
                        status = EndStatus.failed
                    else:
                        status = EndStatus.complete
                else:
                    status = EndStatus.incomplete

            # update status line
            print(
                f'\033[{lines}F',                                     # jump to ☐
                status.symbol,                                        # reset symbol
                '\033[{}C'.format(5 + len(str(completions_path))),    # jump to end
                f': {args.shell} shell completion {status.message}',  # set message
                sep='',
                end=('\n' * lines),                                   # return to bottom
            )
