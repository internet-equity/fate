import abc
import enum
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import argcomplete

from fate.util.abstract import abstractmember
from fate.util.argument import ChoiceMapping, DirAccess, FileAccess
from fate.util.compat.argument import BooleanOptionalAction
from fate.util.datastructure import StrEnum
from fate.util.format import Loader
from fate.util.term import getch
from plumbum import colors

from .. import exit_on_error, Main


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
    conf = colors.bold | '⚙'


@dataclass
class TaskPrompt(abc.ABC):

    identifier: str
    description: str
    path: Path
    exists: bool = True
    syncd: bool = True

    update_action = abstractmember()


class PathOverwrite(TaskPrompt):

    update_action = 'overwrite'


class PathUpdate(TaskPrompt):

    update_action = 'update'


class InitCommand(Main, metaclass=abc.ABCMeta):

    description = abstractmember()
    path_access = abstractmember()

    def check_access(self, path):
        try:
            self.path_access(path)
        except self.path_access.PathTypeError:
            extant_type = 'directory' if isinstance(self.path_access, FileAccess) else 'file'

            self.parser.print_usage(sys.stderr)
            self.parser.exit(71, f'{self.parser.prog}: fatal: inferred path is '
                                 f'extant {extant_type}: {path}\n'
                                 if path.exists() else
                                 f'{self.parser.prog}: fatal: inferred path is '
                                 f'inaccessible: {path}\n')
        except self.path_access.PathAccessError:
            self.parser.print_usage(sys.stderr)
            self.parser.exit(73, f'{self.parser.prog}: fatal: inferred path is '
                                 f'not read-writable: {path}\n')

    @abc.abstractmethod
    def execute(self):
        yield from ()

    @exit_on_error
    def __call__(self, args):
        executor = self.delegate('execute')

        prompt = next(executor)

        print(StatusSymbol.complete if prompt.syncd else StatusSymbol.incomplete,
              TaskSymbol[prompt.identifier],
              colors.underline & colors.dim | str(prompt.path),
              sep='  ')

        lines = 1

        if args.prompt and not prompt.syncd:
            lines += 2

            print(
                '\n_ [Y|n]',
                colors.warn[prompt.update_action] if prompt.exists else 'install',
                f'{prompt.description}?',
                end='\r',  # return
            )

            while (do_install := getch().lower()) not in 'yn\r\x03\x04':
                pass

            if do_install == '\r':
                # set empty
                do_install = 'y'
            elif do_install in '\x03\x04':
                # treat ^C and ^D as input of "n"
                do_install = 'n'

            print(colors.underline | do_install.upper())
        else:
            do_install = 'y'

        status = executor.send(do_install == 'y')

        # update status line
        print(
            f'\033[{lines}F',                                     # jump to ☐
            status.symbol,                                        # reset symbol
            '\033[{}C'.format(5 + len(str(prompt.path))),         # jump to end
            f': {prompt.description} {status.message}',           # set message
            sep='',
            end=('\n' * lines),                                   # return to bottom
        )


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
        for (index, subcommand) in enumerate(self):
            print('' if index == 0 else '\n',
                  colors.title | subcommand.description,
                  sep='',
                  end='\n\n')

            subcommand.call()


@Init.register
class Conf(InitCommand):
    """install configuration files"""

    description = 'default configuration'
    path_access = DirAccess('rw', parents=True)

    @dataclass
    class FormatPreference:

        name: str

        @property
        def suffix(self):
            return '.' + self.name

        def select(self, suffix):
            return suffix == self.suffix

        def __str__(self):
            return self.name

    def __init__(self, parser):
        formats = {loader.name: self.FormatPreference(loader.name) for loader in Loader}
        parser.add_argument(
            '--format',
            action=ChoiceMapping,
            choices=formats,
            default=formats['toml'],
            help="configuration format to prefer (default: %(default)s)",
        )

        parser.add_argument(
            'path',
            nargs='?',
            type=self.path_access,
            help="force installation to directory path (default: inferred)",
        )

    def execute(self, args, parser):
        if args.path:
            conf_prefix = args.path
        else:
            conf_prefix = self.conf._prefix_.conf

            self.check_access(conf_prefix)

        update_paths = {}

        prompt = PathOverwrite('conf', self.description, path=None)

        for conf in self.conf:
            builtins = {path.suffix: path for path in conf._iter_builtins_()}

            formats_builtin = sorted(builtins, key=args.format.select, reverse=True)

            try:
                format_builtin = formats_builtin[0]
            except IndexError:
                parser.print_usage(sys.stderr)
                parser.exit(70, f"{parser.prog}: fatal: no built-in for "
                                f"conf file '{conf.__name__}'")

            if extant := conf._get_path_(conf_prefix):
                if template := builtins.get(extant.suffix):
                    update_paths[extant] = template
                    prompt.syncd = prompt.syncd and template.read_text() == extant.read_text()
                else:
                    parser.print_usage(sys.stderr)
                    parser.exit(70, f'{parser.prog}: fatal: no built-in template for format '
                                    f'{extant.suffix[1:]} of existing conf file: {extant}')
            else:
                prompt.exists = prompt.syncd = False

                template = builtins[format_builtin]
                target_path = conf_prefix / template.name
                update_paths[target_path] = template

        pseudo_name = '{%s}' % ','.join(path.name for path in update_paths)
        prompt.path = conf_prefix / pseudo_name

        confirmed = yield prompt

        if prompt.syncd:
            yield EndStatus.complete
        elif (args.prompt and confirmed) or (not args.prompt and not prompt.exists):
            try:
                conf_prefix.mkdir(parents=True, exist_ok=True)

                for (target_path, source_path) in update_paths.items():
                    with target_path.open('wb') as t_fd, source_path.open('rb') as s_fd:
                        t_fd.writelines(s_fd)
            except OSError:
                yield EndStatus.failed
            else:
                yield EndStatus.complete
        else:
            yield EndStatus.incomplete


@Init.register
class Comp(InitCommand):
    """install shell tab-completion files"""

    description = 'shell completion'
    path_access = FileAccess('rw', parents=True)

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
            type=self.path_access,
            help="force installation to file at path (default: inferred)",
        )

    def execute(self, args):
        """install shell completion"""
        # determine installation path
        if args.path:
            completions_path = args.path
        else:
            completions_path = self.conf._prefix_.completions(args.shell, args.system_profile)

            self.check_access(completions_path)

        # determine file contents
        entry_points = args.__entry_points__ or [f'{self.conf._lib_}{suffix}'
                                                 for suffix in self.script_suffixes]

        contents = argcomplete.shellcode(entry_points, shell=args.shell)

        # check file status and prepare prompt
        prompt = PathUpdate('comp', f'{args.shell} {self.description}', completions_path)

        try:
            prompt.syncd = completions_path.read_text() == contents
        except FileNotFoundError:
            prompt.exists = prompt.syncd = False
        else:
            prompt.exists = True

        # delegate prompt to controller
        confirmed = yield prompt

        # complete execution
        if prompt.syncd:
            yield EndStatus.complete
        elif confirmed:
            try:
                completions_path.parent.mkdir(parents=True,
                                              exist_ok=True)
                completions_path.write_text(contents)
            except OSError:
                yield EndStatus.failed
            else:
                yield EndStatus.complete
        else:
            yield EndStatus.incomplete
