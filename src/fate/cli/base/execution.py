import argparse
import functools
import os
import re
import shutil
import subprocess
import sys
import textwrap
import time
import typing

import argcmdr

from fate.common.ext import CompletingTask, TaskConfExt
from fate.common.output import TaskOutput
from fate.conf import OutputEncodeError
from fate.util.term import snip

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
        format_result: typing.Sequence[str] = ('auto',)
        timeout: typing.Optional[int] = None

    @staticmethod
    def snip_many(iterable, maxlen, cast=str):
        contents = ', '.join(cast(item) for item in iterable)
        if len(contents) > maxlen - 2:
            contents = contents[:maxlen - 6] + ' ...'
        return f'({contents})'

    @staticmethod
    def print_output(name, text):
        """Print report value text formatted appropriately for its
        length (number of lines).

        """
        if '\n' in text:
            print(f'{name}:', textwrap.indent(text.strip(), '  '), sep='\n\n')
        else:
            print(f'{name}:', text)

    @staticmethod
    def present_outputs(outputs):
        for task_output in outputs:
            if task_output.label:
                label = '{}{}'.format(task_output.label, task_output.ext)
            elif task_output.ext:
                label = f'[{task_output.ext}]'
            else:
                label = '<unrecognized>'

            try:
                text = snip(task_output.value.strip().decode(), 300)
            except UnicodeDecodeError:
                text = "<non-text output>"

            if '\n' in text:
                yield label + '\n\n' + textwrap.indent(text, '  ')
            else:
                yield f'{label}: {text}'

    @classmethod
    def print_report(cls, command, retcode, outputs, stderr, error):
        """Print a report of task command execution outcomes."""
        print('Name:', command.name or '-')

        print()

        cls.print_output('Command', ' '.join(command.args))

        print()

        if error:
            if isinstance(error, subprocess.TimeoutExpired):
                status = ('Timeout', f'({error.timeout}s)')
            else:
                raise NotImplementedError(error)
        else:
            status = (cls.CommandStatus.select(retcode), f'(Exit code {retcode})')

        print('Status:', *status)

        print()

        output = '\n\n'.join(cls.present_outputs(outputs))

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

            try:
                result = subprocess.run(
                    [executable] + command_args,

                    input=command.stdin,

                    timeout=command.timeout,

                    capture_output=True,

                    # it's assumed that even if stdin is set to a TTY it's purposeful
                    # here; so, indicate to task.param.read() not to worry about it:
                    env=dict(os.environ, FATE_READ_TTY_PARAM='1'),
                )
            except subprocess.TimeoutExpired as exc:
                result = None
                returncode = None

                error = exc
                stdout = exc.stdout or b''
                stderr = exc.stderr or b''
            else:
                error = None

                returncode = result.returncode
                stdout = result.stdout
                stderr = result.stderr

            try:
                outputs = (*TaskOutput.parse(stdout, self.conf._lib_, command.format_result),)
            except OutputEncodeError as exc:
                outputs = (exc.output,)

                print("Warn: bad result encoding for configured format",
                      f'{exc.format!r}:',
                      "path suffix ignored:",
                      self.snip_many(exc.errors, 65, repr),
                      file=sys.stderr)
                print()

            if send:
                try:
                    send((result, outputs))
                except StopIteration:
                    pass
                else:
                    raise ValueError("get_command() generated more than one command")

            if args.stdout:
                args.stdout.write(stdout)
                stdout = f'[See {args.stdout.name}]'
                outputs = (TaskOutput(stdout, '<stored>'),)
                args.stdout.close()

            if args.stderr:
                args.stderr.write(stderr)
                stderr = f'[See {args.stderr.name}]'
                args.stderr.close()

            if args.report:
                self.print_report(command, returncode, outputs, stderr, error)

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


class CompletedDebugTask(TaskConfExt, CompletingTask):

    @TaskConfExt._constructor_
    def complete(cls, task, *args, **kwargs):
        return cls(task, *args, **kwargs)

    def __init__(self, data, ended=0):
        super().__init__(data)
        self._ended_ = ended or time.time()

    def ended_(self):
        return self._ended_
