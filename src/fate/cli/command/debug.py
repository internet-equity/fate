import argcmdr
import argparse
import os.path
import sys

from fate.conf import ResultEncodingError

from .. import Main, runcmd


READABLE = argparse.FileType('r')


def path_or_text(value):
    """Guess whether to use given value as-is or to treat as a
    filesystem path (from which to read a value).

    Returns either the given value OR an `open()` file descriptor (via
    `FileType`).

    """
    if value.startswith('{') or '\n' in value:
        return value

    if value == '-' or os.path.sep in value or os.path.exists(value):
        return READABLE(value)

    return value


@Main.register
class Debug(argcmdr.Command):
    """ad-hoc execution commands"""

    @runcmd('arguments', metavar='command-arguments', nargs=argparse.REMAINDER,
            help="command arguments (optional)")
    @runcmd('command', help="program to execute")
    @runcmd('-i', '--stdin', metavar='path|text', type=path_or_text,
            help="set standard input (parameterization) for command to given "
                 "path or text (specify '-' to pass through stdin)")
    def execute(context, args, parser):
        """execute an arbitrary program as an ad-hoc task"""
        try:
            cmd = context.local[args.command][args.arguments]
        except context.local.CommandNotFound:
            parser.print_usage(sys.stderr)
            raise

        if hasattr(args.stdin, 'read'):
            return cmd < args.stdin

        if args.stdin is not None:
            return cmd << args.stdin

        return cmd

    @runcmd('task', help="name of configured task to run")
    @runcmd('-i', '--stdin', metavar='path|text', type=path_or_text,
            help="override standard input (parameterization) for task to given "
                 "path or text (specify '-' to pass through stdin) "
                 "(default: from configuration)")
    @runcmd('--record', action='store_true', help="record task result")
    def run(context, args, parser):
        """run a configured task ad-hoc"""
        try:
            spec = context.conf.task[args.task]
        except KeyError:
            parser.error(f"task not found: '{args.task}'")

        if isinstance(exec_ := spec.exec_, str):
            cmd = context.local[exec_]
        else:
            (root, *arguments) = exec_
            cmd = context.local[root][arguments]

        if hasattr(args.stdin, 'read'):
            bound = cmd < args.stdin
        elif args.stdin is not None:
            bound = cmd << args.stdin
        else:
            bound = cmd << spec.param_

        (retcode, stdout, stderr) = yield (args.task, bound)

        if args.record and retcode == 0:
            try:
                result_path = spec.path_._result_(stdout)
            except ResultEncodingError as exc:
                result_path = exc.identifier

                print(f"result does not appear to be encoded as {exc.format}:",
                      "will write to file without suffix",
                      file=sys.stderr)

            try:
                context.write_result(result_path, stdout)
            except NotADirectoryError as exc:
                print('cannot record result: path or sub-path is not a directory:',
                      exc.filename,
                      file=sys.stderr)
