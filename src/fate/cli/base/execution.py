import argparse
import functools
import os
import re
import shutil
import subprocess
import textwrap
import typing

import argcmdr

from .common import CommandInterface


class OneOffExecutor(CommandInterface, argcmdr.Command):
    """Base class for Fate commands that execute tasks directly.

    Subclasses must define `get_command` to specify the task name
    (if any) and command to execute.

    """
    class Command(typing.NamedTuple):

        args: typing.Sequence[str]
        name: str = ''
        stdin: bytes = b''

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
    def print_report(cls, command, retcode, stdout, stderr):
        """Print a report of task command execution outcomes."""
        print('Name:', command.name or '-')

        print('Command:', *command.args)

        print()

        print('Status:', cls.CommandStatus.status(retcode), f'(Exit code {retcode})')

        print()

        try:
            output = stdout.decode()
        except UnicodeDecodeError:
            output = "<binary or bad output>"

        cls.print_output('Result', output or '-')

        if stderr:
            print()

            try:
                logs = stderr.decode()
            except UnicodeDecodeError:
                stderr_formatted = "<could not character-decode stderr logs>"
            else:
                # make fate task logging separators -- null byte -- visual
                stderr_formatted = logs.replace('\0', '\n\n').strip() + '\n'

            cls.print_output('Logged (standard error)', stderr_formatted)

    def __init__(self, parser):
        super().__init__(parser)

        parser.add_argument(
            '-o', '--stdout',
            metavar='path',
            type=argparse.FileType('w'),
            help="write command result to path",
        )
        parser.add_argument(
            '-e', '--stderr',
            metavar='path',
            type=argparse.FileType('w'),
            help="write command standard error to path",
        )
        parser.add_argument(
            '--no-report',
            action='store_false',
            dest='report',
            help="do not print command report",
        )

    def __call__(self, args, parser):
        """Execute and report on task command execution."""
        with self.exit_on_error:
            command_spec = self.delegate('get_command')

            if send := getattr(command_spec, 'send', None):
                command = next(command_spec)
            else:
                command = command_spec

            (program, *command_args) = command.args

            executable = shutil.which(program)

            if executable is None:
                hint = ('\nhint: whitespace in program name suggests a misconfiguration'
                        if re.search(r'\s', program) else '')
                parser.exit(127, f'{parser.prog}: error: {program}: '
                                 f'command not found on path{hint}\n')

            result = subprocess.run(
                [executable] + command_args,

                input=command.stdin,

                capture_output=True,

                # it's assumed that even if stdin is set to a TTY it's purposeful
                # here; so, indicate to task.param.read() not to worry about it:
                env=dict(os.environ, FATE_READ_TTY_PARAM='1'),
            )

            if send:
                try:
                    send(result)
                except StopIteration:
                    pass
                else:
                    raise ValueError("get_command() generated more than one command")

            if args.stdout:
                args.stdout.write(result.stdout)
                stdout = f'[See {args.stdout.name}]'
                args.stdout.close()
            else:
                stdout = result.stdout

            if args.stderr:
                args.stderr.write(result.stderr)
                stderr = f'[See {args.stderr.name}]'
                args.stderr.close()
            else:
                stderr = result.stderr

            if args.report:
                self.print_report(command, result.returncode, stdout, stderr)

    def get_command(self, args):
        """Determine task name (if any) and command to execute
        from CLI argumentation.

        As a simple method, returns a OneOffExecutor.Command. As a
        generator method, yields only a single element -- the Command --
        and receives the execution result.

        """
        super(argcmdr.Command, self).__call__(args)


"""Decorator to manufacture OneOffExecutor commands from a simple
function defining method `get_command`.

"""
runcmd = functools.partial(argcmdr.cmd, base=OneOffExecutor,
                           binding=True, method_name='get_command')
