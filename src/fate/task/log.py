"""Task logging integrated with the Fate scheduler's own logging.

Logging is provided by and configurable through `TaskLogger`.

A module-level instance of `TaskLogger` is lazily constructed and
accessible at `logger`.

Additionally, the logging methods of this module-level instance are made
dynamically available at the module level.

For example, `TaskLogger`:

    from fate.task.log import TaskLogger

    my_logger = TaskLogger(format='toml')

    my_logger.info({'ears': 'floppy'})

Module-level logger:

    from fate.task.log import logger

    logger.info({'ears': 'floppy'})

Module-level methods:

    from fate.task import log

    log.info({'ears': 'floppy'})

"""
import logging
import sys
import threading

from fate.util.format import Dumper


def __dir__():
    """Ensure "logger", and its methods, are listed, (even if it doesn't
    *yet* exist).

    """
    members = globals().keys() | _DYNAMIC_NAMES
    return sorted(members)


def __getattr__(name):
    """Lazily construct a default TaskLogger under the name "logger" and
    provide module-level access to its logging methods.

    """
    if name in _DYNAMIC_NAMES:
        context = globals()

        with threading.Lock():
            if 'logger' in context:
                logger = context['logger']
            else:
                logger = context['logger'] = TaskLogger()

        return logger if name == 'logger' else getattr(logger, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class TaskLogger:
    """Logging for Fate-compliant tasks."""

    def __init__(self, *,
                 end='\0',
                 file=sys.stderr,
                 format='json',
                 insert_level=False,
                 prepend_level=True):
        self.end = end
        self.file = file
        self.format = format
        self.insert_level = insert_level
        self.prepend_level = prepend_level

        try:
            self.dump = Dumper[self.format]
        except KeyError:
            raise ValueError(f'unsupported format: {format!r} not in {set(Dumper.__members__)}')

    def log(self, level, arg=None, /, **kwargs):
        """Write a record to the configured file.

        A mappping may be specified as a single positional dict argument
        and/or keyword arguments. Mappings are serialized according to
        the configured format.

        Alternatively, a string may be specified as a single positional
        argument. Keyword arguments are interpolated via format() /
        format_map().

        Examples:

            logger.log('DEBUG', transmogrified=True, output="radical")

            logger.log('WARNING', {'problems': 6}, radical=False)

            logger.log('CRITICAL', "Where have all the {plural_noun} gone?", plural_noun='cowboys')

        """
        level_code = logging._nameToLevel.get(level)

        if not level_code:
            raise ValueError(f"unrecognized log level '{level}'")

        if arg is None:
            arg = {}

        if isinstance(arg, dict):
            struct_level = {'level': level.lower()} if self.insert_level else {}

            struct = {**struct_level, **arg, **kwargs}

            message = self.dump(struct)
        elif isinstance(arg, str):
            message = arg.format_map(kwargs) if kwargs else arg
        else:
            raise TypeError('log expects at most one positional argument of '
                            f'type dict or str not {arg.__class__.__name__}')

        if self.prepend_level:
            level_ordinal = level_code // 10
            message = f'<{level_ordinal}> {message}'

        print(message, end=self.end, file=self.file)

    def critical(self, *args, **kwargs):
        """Write a CRITICAL record to the configured file.

        See log() for more information.

        """
        self.log('CRITICAL', *args, **kwargs)

    def error(self, *args, **kwargs):
        """Write an ERROR record to the configured file.

        See log() for more information.

        """
        self.log('ERROR', *args, **kwargs)

    def warning(self, *args, **kwargs):
        """Write a WARNING record to the configured file.

        See log() for more information.

        """
        self.log('WARNING', *args, **kwargs)

    warn = warning

    def info(self, *args, **kwargs):
        """Write an INFO record to the configured file.

        See log() for more information.

        """
        self.log('INFO', *args, **kwargs)

    def debug(self, *args, **kwargs):
        """Write a DEBUG record to the configured file.

        See log() for more information.

        """
        self.log('DEBUG', *args, **kwargs)


_LOGGER_NAMES = {name for name in TaskLogger.__dict__ if not name.startswith('_')}

_DYNAMIC_NAMES = {'logger'} | _LOGGER_NAMES

__all__ = {'TaskLogger'} | _DYNAMIC_NAMES
