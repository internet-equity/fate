import contextlib
import re

import loguru


class LogCapture:

    @classmethod
    @contextlib.contextmanager
    def caplog(cls, *args, **kwargs):
        log_capture = cls(*args, **kwargs)

        try:
            yield log_capture
        finally:
            log_capture.detach()

    def __init__(self, logger, level='DEBUG'):
        if not isinstance(level, loguru._logger.Level):
            level = logger._base_logger.level(level)

        self.capture = []
        self.logger = logger
        self.handler_id = self.logger._add_sink(self.capture.append, level=level)

    def detach(self):
        self.logger._remove_sink(self.handler_id)

    def __len__(self):
        return len(self.capture)

    def __iter__(self):
        yield from self.capture

    @staticmethod
    def _field_pattern(key, value, partial=False):
        if isinstance(value, (int, float)):
            if partial:
                raise TypeError

            return re.compile(rf'\b{key}={value}\b')

        if isinstance(value, str):
            if partial:
                return re.compile(rf'\b{key}=[\'"][^\'"]*{value}[^\'"]*[\'"][\b\n]')

            return re.compile(rf'\b{key}=[\'"]{value}[\'"]\s')

        raise TypeError

    def field_contains(self, **pairs):
        for message in self.capture:
            if all(
                self._field_pattern(key, text, partial=True).search(message)
                for (key, text) in pairs.items()
            ):
                return True
        else:
            return False

    def field_equals(self, count=None, /, **pairs):
        found = self.field_count(**pairs)

        return found > 0 if count is None else found == count

    def field_count(self, **pairs):
        found = 0

        for message in self.capture:
            for (key, value) in pairs.items():
                if not self._field_pattern(key, value).search(message):
                    break
            else:
                found += 1

        return found

    def message_contains(self, text):
        return any(text in message for message in self.capture)

    def message_equals(self, text):
        return any(text == message for message in self.capture)
