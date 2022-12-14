import pathlib
import typing
from functools import partial

import toml

from fate.util.lazy import lstr


class TomlStrValue:

    _as_str_ = ()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dump_funcs.update(self._dump_funcs_str_)

    @property
    def _dump_funcs_str_(self):
        return dict.fromkeys(self._as_str_, self._value_as_str_)

    def _value_as_str_(self, value):
        return self.dump_funcs[str](str(value))


class TomlTerse:

    class InlineTableFormatters(typing.NamedTuple):

        sep: typing.Iterable[str] = (', ', ' = ')
        close: typing.Iterable[str] = ('{ ', ' }')
        end: str = '\n'

    def dump_inline_table_formatted(self, section,
                                    formatters=InlineTableFormatters(),
                                    recursive_formatters=None):
        """Preserve inline table in its compact syntax instead of expanding
        into subsection.

        https://github.com/toml-lang/toml#user-content-inline-table

        Unlike the built-in `dump_inline_table`, seperators may be
        customized, for example to omit superfluous whitespace.

        Also unlike the built-in, keys are cast to str, rather than
        presumed to be this type.

        By default, configured formatting is not applied to nested
        dictionaries; these are dumped with formatting compatible to
        that of the built-in method (*not* specially "formatted").
        Specify argument `recursive_formatters=True` to apply formatting
        configured by `formatters` to nested dictionaries as well; or,
        use `recursive_formatters` to specify an alternative set of
        formatters.

        """
        if not isinstance(section, dict):
            return str(self.dump_value(section))

        (
            (sep_item, sep_pair),
            (close0, close1),
            end,
        ) = formatters

        if hasattr(recursive_formatters, '__iter__'):
            dump_value = partial(self.dump_inline_table_formatted,
                                 formatters=recursive_formatters,
                                 recursive_formatters=True)
        elif recursive_formatters:
            dump_value = partial(self.dump_inline_table_formatted,
                                 formatters=formatters,
                                 recursive_formatters=True)
        else:
            dump_value = self.dump_inline_table_formatted

        values = (
            str(key) + sep_pair + dump_value(val)
            for (key, val) in section.items()
        )
        return close0 + sep_item.join(values) + close1 + end


class TomlLoggingEncoder(TomlTerse, TomlStrValue, toml.TomlEncoder):

    _as_str_ = (lstr, pathlib.PosixPath)


toml_logging_encoder = TomlLoggingEncoder()

inline_formatters_terse = TomlLoggingEncoder.InlineTableFormatters(sep=(' ', '='),
                                                                   close=('', ''),
                                                                   end='')

nested_formatters_terse = TomlLoggingEncoder.InlineTableFormatters(sep=(' ', '='),
                                                                   close=('{', '}'),
                                                                   end='')


def dump_structured_log_record(struct):
    return toml_logging_encoder.dump_inline_table_formatted(
        struct,
        inline_formatters_terse,
        nested_formatters_terse,
    )
