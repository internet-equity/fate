# added to pathlib in 3.9
def _is_relative_to(path, *other):
    """Return True if the path is relative to another path or False.
    """
    try:
        path.relative_to(*other)
        return True
    except ValueError:
        return False


def is_relative_to(path, *other):
    try:
        is_relative_to = path.is_relative_to
    except AttributeError:
        return _is_relative_to(path, *other)
    else:
        return is_relative_to(*other)


# added to pathlib in 3.9
def _readlink(path):
    target = path._accessor.readlink(path)
    return path._from_parts((target,))


def readlink(path):
    try:
        readlink = path.readlink
    except AttributeError:
        return _readlink(path)
    else:
        return readlink()
