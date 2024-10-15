import datetime
import re


TIMEDELTA_PATTERN = re.compile(
    r'(?:(?P<weeks>\d+)w)?'
    r'(?:(?P<days>\d+)d)?'
    r'(?:(?P<hours>\d+)h)?'
    r'(?:(?P<minutes>\d+)m)?'
    r'(?:(?P<seconds>\d+)s)?'
)


def parse_timedelta(text: str) -> datetime.timedelta:
    match = TIMEDELTA_PATTERN.fullmatch(text)

    if not match:
        raise ValueError(f"unsupported timedelta pattern: {text}")

    params = {key: int(value) for (key, value) in match.groupdict().items() if value is not None}

    return datetime.timedelta(**params)


RESOLUTION = datetime.timedelta(milliseconds=1)

MIN = datetime.timedelta(microseconds=1)


def human_readable(delta: datetime.timedelta, resolution: datetime.timedelta = RESOLUTION) -> str:
    # timedelta.__str__ is decent but we can do better (and mirror parse_timedelta)
    #
    # days are fine
    days = delta.days
    #
    # hours and minutes must be extracted from seconds
    (hours, seconds) = divmod(delta.seconds, 3600)
    (minutes, seconds) = divmod(seconds, 60)
    #
    # milliseconds may be extracted from microseconds
    if resolution == MIN:
        (milliseconds, microseconds) = divmod(delta.microseconds, 1000)
    elif resolution == RESOLUTION:
        (milliseconds, microseconds) = (round(delta.microseconds / 1000), 0)
    else:
        raise ValueError(f"unexpected resolution: {resolution}")

    result = ''

    for (value, tag) in (
        (days, 'd'),
        (hours, 'h'),
        (minutes, 'm'),
        (seconds, 's'),
        (milliseconds, 'ms'),
        (microseconds, 'μs'),
    ):
        result += f'{value}{tag}' if value else ''

    if result:
        return result

    # guard against empty (sub-resolution) result
    return '0μs' if resolution == MIN else '0ms'
