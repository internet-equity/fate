from descriptors import cachedproperty

from .event import event_id


class LazyStr(str):

    def __new__(cls, *_args, **_kwargs):
        # ensure str ignores arguments and
        # never instantiates anything more than empty ''
        return super().__new__(cls)

    def __init__(self, func, *args, **kwargs):
        self.__func__ = func
        self.__args__ = args
        self.__kwargs__ = kwargs

        # set lazily:
        # self.__value__

    @cachedproperty
    def __value__(self):
        value = self.__func__(*self.__args__, **self.__kwargs__)

        if not isinstance(value, str):
            raise TypeError(f'{self.__class__.__name__} expected str but got {value!r}')

        return value

    @property
    def __cached__(self):
        return self.__dict__.get('__value__')

    def __repr__(self):
        args_spec = (
            ', ' + ', '.join(repr(arg) for arg in self.__args__)
        ) if self.__args__ else ''

        kwargs_spec = (
            ', ' + ', '.join(f'{key}={value!r}' for (key, value) in self.__kwargs__.items())
        ) if self.__kwargs__ else ''

        cached = self.__cached__

        cached_spec = 'UNKNOWN' if cached is None else repr(cached)

        return (f'<{self.__class__.__name__}({self.__func__!r}{args_spec}{kwargs_spec}): '
                f'{cached_spec}>')

    def __str__(self):
        return self.__value__

    # ...and the rest...

    def __add__(self, other):
        return self.__value__.__add__(other)

    def __contains__(self, item):
        return self.__value__.__contains__(item)

    def __eq__(self, other):
        return self.__value__.__eq__(other)

    def __format__(self, format_spec):
        return self.__value__.__format__(format_spec)

    def __ge__(self, other):
        return self.__value__.__ge__(other)

    def __getitem__(self, key):
        return self.__value__.__getitem__(key)

    def __gt__(self, other):
        return self.__value__.__gt__(other)

    def __iter__(self):
        return self.__value__.__iter__()

    def __le__(self, other):
        return self.__value__.__le__(other)

    def __len__(self):
        return self.__value__.__len__()

    def __lt__(self, other):
        return self.__value__.__lt__(other)

    def __mod__(self, other):
        return self.__value__.__mod__(other)

    def __mul__(self, other):
        return self.__value__.__mul__(other)

    def __ne__(self, other):
        return self.__value__.__ne__(other)

    def __rmod__(self, other):
        return self.__value__.__rmod__(other)

    def __rmul__(self, other):
        return self.__value__.__rmul__(other)

    def capitalize(self):
        return self.__value__.capitalize()

    def casefold(self):
        return self.__value__.casefold()

    def center(self, *args, **kwargs):
        return self.__value__.center(*args, **kwargs)

    def count(self, *args, **kwargs):
        return self.__value__.count(*args, **kwargs)

    def encode(self, *args, **kwargs):
        return self.__value__.encode(*args, **kwargs)

    def endswith(self, *args, **kwargs):
        return self.__value__.endswith(*args, **kwargs)

    def expandtabs(self, *args, **kwargs):
        return self.__value__.expandtabs(*args, **kwargs)

    def find(self, *args, **kwargs):
        return self.__value__.find(*args, **kwargs)

    def format(self, *args, **kwargs):
        return self.__value__.format(*args, **kwargs)

    def format_map(self, *args, **kwargs):
        return self.__value__.format_map(*args, **kwargs)

    def index(self, *args, **kwargs):
        return self.__value__.index(*args, **kwargs)

    def isalnum(self, *args, **kwargs):
        return self.__value__.isalnum(*args, **kwargs)

    def isalpha(self, *args, **kwargs):
        return self.__value__.isalpha(*args, **kwargs)

    def isascii(self, *args, **kwargs):
        return self.__value__.isascii(*args, **kwargs)

    def isdecimal(self, *args, **kwargs):
        return self.__value__.isdecimal(*args, **kwargs)

    def isdigit(self, *args, **kwargs):
        return self.__value__.isdigit(*args, **kwargs)

    def isidentifier(self, *args, **kwargs):
        return self.__value__.isidentifier(*args, **kwargs)

    def islower(self, *args, **kwargs):
        return self.__value__.islower(*args, **kwargs)

    def isnumeric(self, *args, **kwargs):
        return self.__value__.isnumeric(*args, **kwargs)

    def isprintable(self, *args, **kwargs):
        return self.__value__.isprintable(*args, **kwargs)

    def isspace(self, *args, **kwargs):
        return self.__value__.isspace(*args, **kwargs)

    def istitle(self, *args, **kwargs):
        return self.__value__.istitle(*args, **kwargs)

    def isupper(self, *args, **kwargs):
        return self.__value__.isupper(*args, **kwargs)

    def join(self, *args, **kwargs):
        return self.__value__.join(*args, **kwargs)

    def ljust(self, *args, **kwargs):
        return self.__value__.ljust(*args, **kwargs)

    def lower(self, *args, **kwargs):
        return self.__value__.lower(*args, **kwargs)

    def lstrip(self, *args, **kwargs):
        return self.__value__.lstrip(*args, **kwargs)

    def maketrans(self, *args, **kwargs):
        return self.__value__.maketrans(*args, **kwargs)

    def partition(self, *args, **kwargs):
        return self.__value__.partition(*args, **kwargs)

    def removeprefix(self, *args, **kwargs):
        return self.__value__.removeprefix(*args, **kwargs)

    def removesuffix(self, *args, **kwargs):
        return self.__value__.removesuffix(*args, **kwargs)

    def replace(self, *args, **kwargs):
        return self.__value__.replace(*args, **kwargs)

    def rfind(self, *args, **kwargs):
        return self.__value__.rfind(*args, **kwargs)

    def rindex(self, *args, **kwargs):
        return self.__value__.rindex(*args, **kwargs)

    def rjust(self, *args, **kwargs):
        return self.__value__.rjust(*args, **kwargs)

    def rpartition(self, *args, **kwargs):
        return self.__value__.rpartition(*args, **kwargs)

    def rsplit(self, *args, **kwargs):
        return self.__value__.rsplit(*args, **kwargs)

    def rstrip(self, *args, **kwargs):
        return self.__value__.rstrip(*args, **kwargs)

    def split(self, *args, **kwargs):
        return self.__value__.split(*args, **kwargs)

    def splitlines(self, *args, **kwargs):
        return self.__value__.splitlines(*args, **kwargs)

    def startswith(self, *args, **kwargs):
        return self.__value__.startswith(*args, **kwargs)

    def strip(self, *args, **kwargs):
        return self.__value__.strip(*args, **kwargs)

    def swapcase(self, *args, **kwargs):
        return self.__value__.swapcase(*args, **kwargs)

    def title(self, *args, **kwargs):
        return self.__value__.title(*args, **kwargs)

    def translate(self, *args, **kwargs):
        return self.__value__.translate(*args, **kwargs)

    def upper(self, *args, **kwargs):
        return self.__value__.upper(*args, **kwargs)

    def zfill(self, *args, **kwargs):
        return self.__value__.zfill(*args, **kwargs)


lstr = LazyStr


def lazy_id():
    return lstr(event_id)
