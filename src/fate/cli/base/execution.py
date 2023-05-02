import argparse
import functools
import re
import textwrap

import argcmdr
import plumbum.commands.base

from .common import CommandInterface


class OneOffExecutor(CommandInterface, argcmdr.Local):
    """Base class for Fate commands that execute tasks.

    Subclasses must define `get_command` to specify the task name
    (if any) and command to execute.

    """
    redirection_command_types = (
        plumbum.commands.base.StdinRedirection,
        plumbum.commands.base.StdinDataRedirection,
    )

    @staticmethod
    def print_output(name, text):
        """Print report value text formatted appropriately for its
        length (number of lines).

        """
        if '\n' in text:
            print(f'{name}:', textwrap.indent(text, '  '), sep='\n\n')
        else:
            print(f'{name}:', text)

    @classmethod
    def print_report(cls, name, command, retcode, stdout, stderr):
        """Print a report of task command execution outcomes."""
        print('Name:', '-' if name is None else name)

        # If we're composing the command with "echo" or otherwise providing
        # stdin then let's not include that in the report:
        cmd = command.cmd if isinstance(command, cls.redirection_command_types) else command

        print('Command:', cmd)

        print()

        if retcode is None:
            print('Status: Dry Run')
            return

        print('Status:', cls.CommandStatus.status(retcode), f'(Exit code {retcode})')

        print()

        cls.print_output('Result', stdout if stdout else '-')

        if stderr:
            print()

            # make fate task logging separators -- null byte -- visual
            stderr_formatted = stderr.replace('\0', '\n\n').strip() + '\n'
            cls.print_output('Logged (standard error)', stderr_formatted)

    def __init__(self, parser):
        super().__init__(parser)

        # argcmdr built-in arguments (@)
        # fate added-in arguments (%)

        # (@) never print commands to be executed
        #     (we handle this in the report):
        parser.set_defaults(
            show_commands=False,
        )

        # (@) look up but do not actually execute commands during dry run:
        parser.add_argument(
            '-d', '--dry-run',
            action='store_false',
            dest='execute_commands',
            help="do not execute command",
        )

        # (%) copy stdout results to given path:
        parser.add_argument(
            '-o', '--stdout',
            metavar='path',
            type=argparse.FileType('w'),
            help="write command result to path",
        )

        # (%) copy stderr output to given path:
        parser.add_argument(
            '-e', '--stderr',
            metavar='path',
            type=argparse.FileType('w'),
            help="write command standard error to path",
        )

        # (@) pass command output (stdout and stderr) through (to terminal):
        parser.add_argument(
            '-p', '--print-output',
            action='store_true',
            default=False,
            dest='foreground',
            help="print command output (in addition to report)",
        )

        # (%) silence fate command execution report:
        parser.add_argument(
            '--no-report',
            action='store_false',
            dest='report',
            help="do not print command report",
        )

    def __call__(self, args):
        with self.exit_on_error:
            super().__call__(args)

    def get_command(self, args):
        """Determine task name (if any) and command to execute
        from CLI argumentation.

        Returns either just a command to execute -- plumbum
        `BaseCommand` -- or a tuple of the task name and the
        command -- `(str, BaseCommand)`.

        """
        super(argcmdr.Local, self).__call__(args)

    def prepare(self, args, parser):
        """Execute and report on task command execution."""
        try:
            command_spec = self.delegate('get_command')

            if command_spec is None:
                return

            if send := getattr(command_spec, 'send', None):
                command_args = next(command_spec)
            else:
                command_args = command_spec
        except self.local.CommandNotFound as exc:
            hint = ('\nhint: whitespace in program name suggests a misconfiguration'
                    if re.search(r'\s', exc.program) else '')
            parser.exit(127, f'{parser.prog}: error: {exc.program}: '
                             f'command not found on path{hint}\n')

        if isinstance(command_args, (list, tuple)):
            (task_name, command) = command_args
        else:
            (task_name, command) = (None, command_args)

        # it's assumed that even if stdin is set to a TTY it's purposeful
        # here; so, indicate to task.param.read() not to worry about it:
        bound = command.with_env(FATE_READ_TTY_PARAM='1')

        result = yield bound

        if send:
            try:
                send(result)
            except StopIteration:
                pass
            else:
                raise ValueError("get_command() generated more than one command")

        (retcode, stdout, stderr) = result

        if args.stdout and stdout is not None:
            print(stdout, end='', file=args.stdout)
            stdout = f'[See {args.stdout.name}]'

        if args.stderr and stderr is not None:
            print(stderr, end='', file=args.stderr)
            stderr = f'[See {args.stderr.name}]'

        if args.report:
            self.print_report(task_name, command, retcode, stdout, stderr)

    # Raise no exceptions for command return codes:
    prepare.retcode = None


"""Decorator to manufacture OneOffExecutor commands from a simple
function defining method `get_command`.

"""
runcmd = functools.partial(argcmdr.cmd, base=OneOffExecutor, method_name='get_command')
