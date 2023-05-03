"""Task controller, i.e. scheduling, commands."""
import contextlib
import datetime
import os
import random
import re
import shutil
import sys
import time

import plumbum
from descriptors import cachedproperty

from fate import sched
from fate.conf import LogsDecodingError, ResultEncodingError
from fate.util.argument import ChoiceMapping
from fate.util.compat import resources
from fate.util.compat.path import readlink
from fate.util.lazy import lazy_id
from fate.util.log import StructLogger
from fate.util.os import pid_exists

from .. import Main


def snip(text, length=36, ellipsis=' ...'):
    if len(text) <= length:
        return text

    return text[:length - len(ellipsis)] + ellipsis


class ControlCommand(Main):
    """Base command class for concrete implementations of the task
    controller command.

    """
    # default package resource path at which to look up command banner files
    banner_path = 'fate.cli.include.banner'

    # registered task schedulers
    schedulers = {
        scheduler.module_short: scheduler
        for scheduler in (sched.TieredTenancyScheduler,)
    }

    class LockConflict(Exception):
        """Indicate a conflicting (blocking) lock."""

    def __init__(self, parser):
        parser.add_argument(
            '--force',
            action='store_true',
            help='ignore run locks',
        )
        parser.add_argument(
            '--log-level',
            choices=('debug', 'info', 'warning', 'error', 'critical'),
            metavar='name',
            help="override log targets' configured levels with one of: {%(choices)s}",
        )
        parser.add_argument(
            '--no-banner',
            action='store_false',
            dest='show_banner',
            help='suppress banner',
        )
        parser.add_argument(
            '--no-prune',
            action='store_false',
            dest='prune_runs',
            help='do not remove stale run directories',
        )

        parser.add_argument(
            '--scheduler',
            action=ChoiceMapping,
            choices=self.schedulers,
            default=next(iter(self.schedulers.values())),
            help='specify task scheduler (only current option and default: %(default)s)',
        )

    @cachedproperty
    def conf_paths(self):
        """A set of the process's configuration file paths."""
        return {conf.__path__ for conf in self.conf}

    @cachedproperty
    def run_path(self):
        """The directory path at which the process stores run-time information."""
        return self.conf._prefix_.run / f'run-{os.getpid()}'

    # Regular expression with which to extract PID from run directory name
    run_path_pattern = re.compile(r'run-(\d+)')

    @cachedproperty
    def logger(self):
        """The logger through which logs are emitted under normal operation."""
        return StructLogger(self.conf.default.path_.log_, self.args.log_level)

    @property
    def safe_logger(self):
        """A flexibly-constructed logger supporting log-emission despite
        exceptions in the creation of the full-featured logger.

        This logger *may* not conform to user's log configuration but
        should otherwise perform to expectations.

        If the situation is too dire to recover -- and a logger cannot
        be constructed -- this property may be `None`.

        """
        try:
            return self.logger.safe()
        except Exception:
            return None

    @property
    def exit_stack(self):
        """Context manager handling exceptions otherwise uncaught by
        the command.

        * Configuration-related exceptions are caught to exit the
          process with an appropriate error message and exit code
          (see `exit_on_error`).

        * All other uncaught exceptions are logged and the process
          exited with an appropriate error message and appropriate exit
          code.

          Note: This latter exception-handling is disabled when display
          of tracebacks is enabled, and when a logger (of any kind)
          could not be constructed.

        """
        stack = contextlib.ExitStack()

        if not self.args.traceback and (logger := self.safe_logger):
            log_catch = logger.bind(exc_info=True).catch(
                level='CRITICAL',
                onerror=self.onerror,
                message='fatal exception of type {record[exception].type.__name__}',
            )
            stack.enter_context(log_catch)

        stack.enter_context(self.exit_on_error)

        return stack

    def onerror(self, exc):
        """Exit the process with a generic message and exit code
        reflecting the given uncaught exception.

        """
        error = str(exc)
        error_msg = error and f': {error}'
        error_name = exc.__class__.__name__

        self.parser.exit(1, f'{self.parser.prog}: fatal: {error_name}{error_msg}\n')

    def __call__(self, args, parser):
        """Execute the command."""
        with self.exit_stack:
            try:
                self.set_lock()

                try:
                    self.check_lock(args.prune_runs, args.force)
                except self.LockConflict as exc:
                    parser.error(str(exc))

                if args.show_banner:
                    self.show_banner(args.__banner_path__)

                background = getattr(args, 'background', False)

                if getattr(args, 'continual', False):
                    self.daemon(args.scheduler, background=background)
                elif not background:
                    self.serve(args.scheduler)
                else:
                    raise NotImplementedError('cannot background during non-continual service')
            finally:
                self.remove_lock()

    def daemon(self, scheduler, background=False):
        """Schedule tasks continually as a full-featured daemon."""
        if background:
            raise NotImplementedError('background')

        next_check = 0  # no initial wait

        while True:
            if (time_sleep := next_check - time.time()) > 0:
                time.sleep(time_sleep)

            run_info = self.serve(scheduler)

            next_check = run_info.next

    def serve(self, scheduler):
        """Schedule tasks that are due for execution.

        Note: This method does *not* block to wait for tasks to become
        due.

        """
        logger = self.logger.set(session=lazy_id())

        completed_tasks = scheduler(self.conf, logger)()

        for completed_task in completed_tasks:
            self.finish_task(completed_task, logger)

        logger.info(execution_count=completed_tasks.info.count,
                    scheduled_next=datetime.datetime.fromtimestamp(completed_tasks.info.next))

        return completed_tasks.info

    def finish_task(self, task, session_log):
        """Handle a ScheduledTask whose execution has completed."""
        logger = session_log.set(task=task.__name__)

        # Check for in-process exception

        if exc := task.exception():
            # Nothing more to do than to report the error
            logger.error(f'command not found on path: {exc.program}'
                         if isinstance(exc, plumbum.CommandNotFound) else str(exc))

            return

        # Pass through task's logs (subprocess stderr)

        try:
            log_records = task.logs()
        except LogsDecodingError as exc:
            log_records = exc.logs

            logger.warning(records=len(exc.errors),
                           format=exc.format,
                           error=str(exc.errors[0]),
                           msg="bad log encoding for configured format: "
                               "record(s) treated as plain text")

        for log_record in log_records:
            logger.log(*log_record)

        # Check on task subprocess exit status

        status = self.CommandStatus.status(task.returncode)

        status_record = {
            'status': str(status),
            'exitcode': task.returncode,
        }

        if status is self.CommandStatus.Error:
            logger.error(status_record, stdout=snip(task.stdout), stderr=snip(task.stderr))
        else:
            logger.info(status_record)

        if status is self.CommandStatus.OK:
            # Write task result (subprocess stdout)
            try:
                result_path = task.path_.result_()
            except ResultEncodingError as exc:
                result_path = exc.identifier

                logger.warning(format=exc.format,
                               error=(str(exc.errors[0]) if len(exc.errors) == 1
                                      else [str(error) for error in exc.errors]),
                               msg="bad result encoding for configured format: "
                                   "path suffix ignored")

            if result_path:
                try:
                    self.write_result(result_path, task.stdout)
                except NotADirectoryError as exc:
                    logger.error(f'cannot record result: '
                                 f'path or sub-path is not a directory: {exc.filename}')
                except PermissionError as exc:
                    logger.error(f'cannot record result: permission denied: {exc.filename}')
            else:
                logger.debug('result empty or record disabled')

    def set_lock(self):
        """Set command's run-time lock.

        The lock indicates the command's configuration via symbolic
        links such that these may dictate a lock conflict.

        """
        self.logger.debug(run_path=self.run_path, msg="setting lock")

        lock_path = self.run_path / 'conf'
        lock_path.mkdir(parents=True)

        for conf_path in self.conf_paths:
            link_path = lock_path / conf_path.name
            link_path.symlink_to(conf_path)

        self.logger.debug(lock_path=lock_path, msg="lock set")

    def check_lock(self, prune_runs, force):
        """Check for conflicting locks.

        The discovery of any lock indicating a command running with the
        same set of configuration raises a LockConflict exception,
        dependent upon the argument `force`.

        Stale run directories, belonging to non-existent processes, are
        removed, dependent upon the argument `prune_runs`.

        """
        for run_path in self.run_path.parent.glob('run-*'):
            if run_path == self.run_path:
                continue

            name_match = self.run_path_pattern.fullmatch(run_path.name)

            if not name_match:
                continue

            pid = int(name_match.group(1))

            logger = self.logger.set(run_path=run_path)

            if not pid_exists(pid):
                if prune_runs:
                    shutil.rmtree(run_path)

                    logger.debug("dead run directory: pruned")
                else:
                    logger.debug("dead run directory: ignored")

                continue

            lock_path = run_path / 'conf'

            if not lock_path.is_dir():
                logger.debug("concurrent run")
                continue

            conf_paths = {readlink(path) for path in lock_path.iterdir() if path.is_symlink()}

            if conf_paths == self.conf_paths:
                if force:
                    logger.warning("conflicting run: ignored")
                else:
                    logger.critical("conflicting run: fatality")

                    raise self.LockConflict(
                        f"conflict: PID {pid} refers to an instance that is already running "
                        f"against the same configuration (specify --force to ignore)"
                    )
            else:
                logger.debug("concurrent run")

    def remove_lock(self):
        """Remove the process's lock and run directory tree."""
        try:
            shutil.rmtree(self.run_path)
        except FileNotFoundError:
            pass

    @classmethod
    def show_banner(cls, banner_path=None):
        """Write the contents of a random command banner file to
        standard output.

        A custom package resource path may be specified in lieu of the
        default look-up path.

        """
        for path in (banner_path, cls.banner_path):
            if path and (banner_names := resources.contents(path)):
                break
        else:
            return

        banner_name = random.choice(banner_names)

        with resources.open_text(path, banner_name) as banner_text:
            sys.stdout.writelines(banner_text)


@Main.register
class Control(ControlCommand):
    """execute scheduled commands"""

    def __init__(self, parser):
        super().__init__(parser)
        parser.add_argument(
            '--continual',
            action='store_true',
            help="run task scheduler until signaled to stop",
        )
        parser.add_argument(
            '--background',
            action='store_true',
            help="run task scheduler in background (fork) during continual "
                 "operation (see --continual)",
        )

    def __call__(self, args, parser):
        if args.background and not args.continual:
            parser.error('cannot background during non-continual service')

        super().__call__(args, parser)


class Daemon(ControlCommand):
    """launch the command-execution scheduler daemon"""

    def __init__(self, parser):
        super().__init__(parser)
        parser.add_argument(
            '--foreground',
            dest='background',
            action='store_false',
            help="run task scheduler daemon in foreground (do not fork)",
        )
        parser.set_defaults(
            continual=True,
        )


class Serve(ControlCommand):
    """execute scheduled commands"""

    def __init__(self, parser):
        super().__init__(parser)
        parser.add_argument(
            '--continual',
            action='store_true',
            help="run task scheduler until signaled to stop",
        )
        parser.set_defaults(
            background=False,
        )
