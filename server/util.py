from datetime import datetime
from typing import List, TypeVar, Union
from itertools import chain


T = TypeVar('T')
U = TypeVar('U')


def mentions_fursuit(text: str) -> bool:
    text = text.replace('\n', ' ').lower()
    return 'fursuit' in text or 'murrsuit' in text


def parse_datetime(s: str) -> datetime:
    formats = [
        r'%Y-%m-%dT%H:%M:%S.%fZ',
        r'%Y-%m-%dT%H:%M:%S.%f',
        r'%Y-%m-%dT%H:%M:%SZ',
        r'%Y-%m-%dT%H:%M:%S',
        r'%Y-%m-%dT%H:%M:%S.%f+00:00',
        r'%Y-%m-%dT%H:%M:%S+00:00',
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    raise ValueError(f'failed to parse datetime string "{s}"')


def interleave(sep: T, xs: List[U]) -> List[Union[U, T]]:
    return (
        [] if not xs else
        [xs[0], *chain.from_iterable((sep, i) for i in xs[1:])]
    )
