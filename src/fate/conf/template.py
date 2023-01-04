import functools
import re
import os

from jinja2 import sandbox


environ = sandbox.ImmutableSandboxedEnvironment()

environ.globals.update(
    env=os.environ,
)


variable_pattern = re.compile(r'{{(?P<expr>.*?)}}')


def eval_expr(string, **context):
    expr = environ.compile_expression(string)
    return expr(**context)


def _render_match(match, **context):
    evaluation = eval_expr(match['expr'], **context)
    return str(evaluation)


def render_str(string, **context):
    replacer = functools.partial(_render_match, **context) if context else _render_match
    return variable_pattern.sub(replacer, string)


def render_complex(contents, **context):
    if isinstance(contents, list):
        return [render_complex(content, **context) for content in contents]

    if isinstance(contents, str):
        return render_str(contents, **context)

    raise TypeError("expected str or list of str not " + contents.__class__.__name__)
