import argcmdr
import argparse
import os.path
import sys

from fate.conf import ResultEncodingError

from .. import Main, runcmd


READABLE = argparse.FileType('rb')


def path_to_bytes(value: str) -> bytes:
    """Guess whether to use given value as-is or to treat as a
    filesystem path (from which to read a value).

    Returns bytes derived either from the given value OR the result of
    reading a file at that path (via `FileType`).

    """
    if not value.startswith('{') and '\n' not in value and (
        value == '-' or os.path.sep in value or os.path.exists(value)
    ):
        with READABLE(value) as file:
            return file.read()

    return value.encode()


@Main.register
class Debug(argcmdr.Command):
    """ad-hoc execution commands"""

    @runcmd('arguments', metavar='command-arguments', nargs=argparse.REMAINDER,
            help="command arguments (optional)")
    @runcmd('command', help="program to execute")
    @runcmd('-i', '--stdin', metavar='path|text', type=path_to_bytes,
            help="set standard input (parameterization) for command to given "
                 "path or text (specify '-' to pass through stdin)")
    def execute(context, args):
        """execute an arbitrary program as an ad-hoc task"""
        return context.Command(
            [args.command] + args.arguments,
            stdin=args.stdin or b'',
        )

    @runcmd('task', help="name of configured task to run")
    @runcmd('-i', '--stdin', metavar='path|text', type=path_to_bytes,
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

        result = yield context.Command(
            args=spec.exec_,
            name=args.task,
            stdin=spec.param_.encode() if args.stdin is None else args.stdin,
        )

        if args.record and result.returncode == 0:
            try:
                result_path = spec.path_._result_(result.stdout)
            except ResultEncodingError as exc:
                result_path = exc.identifier

                print(f"result does not appear to be encoded as {exc.format}:",
                      "will write to file without suffix",
                      file=sys.stderr)

            try:
                context.write_result(result_path, result.stdout)
            except NotADirectoryError as exc:
                print('cannot record result: path or sub-path is not a directory:',
                      exc.filename,
                      file=sys.stderr)
