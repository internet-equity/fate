import collections
import os.path
import sys
import traceback
import wcwidth
from functools import partial

import loguru
from descriptors import cachedproperty

from fate.util.lazy import lazy_id

from . import encoder


class StructLogger:

    # structured record serialization function
    encode_record = encoder.dump_structured_log_record

    # serialized mapping base spec
    record_base = (
        # note: not easy to support colors when side-stepping
        # default message handling
        #
        # ('time', '<g>{time:YYYY-MM-DD HH:mm:ss.SSS}</>'),
        # ('level', '<lvl>{extra[level_lower]}</>'),

        # (serialization key, context key OR string for format_map)
        ('time', '{time:YYYY-MM-DD HH:mm:ss.SSS}'),
        ('level', 'level_lower'),
        ('event', 'event_id'),
    )

    def __init__(self, sinks, log_level=None):
        self._sinks = sinks
        self.log_level = log_level

    # callables passed to loguru

    @staticmethod
    def _sink_format_(target, level, context):
        format_ = '{extra[serialized]}\n'

        if target is sys.stderr:
            if target.isatty():
                stanza = '{level.icon}'

                # unicode icon cool but not monospace...
                if wcwidth.wcswidth(context['level'].icon) == 1:
                    stanza += ' '
            else:
                stanza = '<{extra[level_ordinal]}>'

            format_ = stanza + ' ' + format_

        if level.no <= 10:
            format_ += '{exception}'

        return format_

    @classmethod
    def _patch_record_(cls, context):
        extra = context['extra']

        # update "extra" in order to make these available to loguru format
        # string as well as to below "env"

        if exception := context['exception']:
            exc_stack = traceback.StackSummary.extract(traceback.walk_tb(exception.traceback))
            exc_frame = exc_stack[-1]
        else:
            exc_stack = exc_frame = None

        extra.update(
            level_lower=context['level'].name.lower(),
            level_ordinal=context['level'].no // 10,
            event_id=lazy_id(),
            exc_stack=exc_stack,
            exc_frame=exc_frame,
        )

        # allow reference to "extra" without subscription
        env = collections.ChainMap(context, extra)

        # build record to serialize
        record = {
            key: spec.format_map(env) if '{' in spec else env[spec]
            for (key, spec) in cls.record_base
        }

        if exc_frame and extra.get('exc_info', False):
            record.update(
                exc_line='{}:{}'.format(os.path.basename(exc_frame.filename), exc_frame.lineno),
                exc_frame=exc_frame.name,
            )

        if 'struct_extra' in extra:
            record.update(extra['struct_extra'])

        if 'struct' in extra:
            record.update(extra['struct'])

        if message := context['message']:
            if 'msg' in record:
                raise TypeError('conflict: log event specifies both text '
                                '"message" and struct "msg"')

            record['msg'] = message

        extra['serialized'] = cls.encode_record(record)

    # underlying loguru logger (lazily constructed)

    @cachedproperty
    def _base_logger(self):
        """The *unconfigured* and underlying Logger.

        Ensures (default) sink(s) only cleared once.

        """
        # NOTE: this level of loguru configuration may
        # NOTE: defeat thread-safety (tho not a current concern)

        loguru.logger.remove()  # clear default sink
        return loguru.logger

    def _clear_base_logger(self):
        """Clear the cache of the _base_logger."""
        try:
            del self._base_logger
        except AttributeError:
            pass

    @cachedproperty
    def _logger(self):
        """The *configured* and underlying Logger."""
        for sink in self._sinks:
            extra = sink.extra_

            if self.log_level:
                extra['level'] = self.log_level.upper()
            else:
                extra.setdefault('level', 'INFO')

            level_ = extra.level_

            del extra['level']

            self._add_sink(sink.target_, level_, **extra)

        return self._patched_logger()

    # helpers

    def _add_sink(self, target, level, format=None, **extra):
        if format is None:
            format = partial(self._sink_format_, target, level)

        self._base_logger.add(
            target,
            level=level.name,
            format=format,
            **extra
        )

    def _patched_logger(self):
        return self._base_logger.opt(depth=1).patch(self._patch_record_)

    # interface methods

    def safe(self, fallback_message="failed to construct logger as configured: using safe logger"):
        try:
            self._logger
        except Exception:
            # start over with base logger
            # (i.e. *ensure* default sink -- and any partially-configured sinks -- cleared)
            self._clear_base_logger()

            log_level = self.log_level.upper() if self.log_level else 'DEBUG'
            self._add_sink(sys.stderr, self._base_logger.level(log_level))

            logger = self._patched_logger()

            if fallback_message:
                logger.error(fallback_message)

            return logger
        else:
            return self

    def clone(self, **kwargs):
        instance = self.__class__(self._sinks, self.log_level)
        if logger := self.__dict__.get('_logger'):
            instance.__dict__['_logger'] = logger
        instance.__dict__.update(**kwargs)
        return instance

    def bind(self, **kwargs):
        return self.clone(_logger=self._logger.bind(**kwargs))

    def opt(self, **kwargs):
        return self.clone(_logger=self._logger.opt(**kwargs))

    def set(self, **kwargs):
        # loguru methods such as bind() *do not* merge settings;
        # rather, each call overrides the last.
        #
        # we won't be like that: set() merges its data on each invocation.
        #
        extra = self._logger._options[8]
        struct_extra = extra.get('struct_extra', {})
        data = {**struct_extra, **kwargs}
        return self.bind(struct_extra=data)

    @staticmethod
    def _make_struct(data=None, **kwargs):
        if data:
            if isinstance(data, str):
                struct = {'msg': data}
            else:
                struct = dict(data)

            struct.update(kwargs)

            return struct

        return kwargs

    def log(self, level, data=None, **kwargs):
        self._logger.log(level.upper(), '', struct=self._make_struct(data, **kwargs))

    def debug(self, data=None, **kwargs):
        self._logger.debug('', struct=self._make_struct(data, **kwargs))

    def info(self, data=None, **kwargs):
        self._logger.info('', struct=self._make_struct(data, **kwargs))

    def warning(self, data=None, **kwargs):
        self._logger.warning('', struct=self._make_struct(data, **kwargs))

    def error(self, data=None, **kwargs):
        self._logger.error('', struct=self._make_struct(data, **kwargs))

    def exception(self, data=None, **kwargs):
        self._logger.exception('', struct=self._make_struct(data, **kwargs))

    def critical(self, data=None, **kwargs):
        self._logger.critical('', struct=self._make_struct(data, **kwargs))

    def catch(self, *args, **kwargs):
        return self._logger.catch(*args, **kwargs)
