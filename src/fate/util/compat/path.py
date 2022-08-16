def _is_relative_to(path, *other):
    """Return True if the path is relative to another path or False.
    """
    try:
        path.relative_to(*other)
        return True
    except ValueError:
        return False


# added to pathlib in 3.9
def is_relative_to(path, *other):
    try:
        is_relative_to = path.is_relative_to
    except AttributeError:
        return _is_relative_to(path, *other)
    else:
        return is_relative_to(*other)
