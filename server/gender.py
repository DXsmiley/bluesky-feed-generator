from typing import List, Literal, Union
from dataclasses import dataclass
import re
import unicodedata


Gender = Literal['unknown', 'non-furry', 'boy', 'enby', 'girl']
ManualGender = Union[Gender, Literal['not-looked-at']]


@dataclass
class Vibes:
    emoji: List[str]
    words: List[str]
    regexes: List[str]


girl_vibes = Vibes(
    emoji=['♀', '⚢'],
    words=[
        'she',
        'her',
        'hers',
        'f',
        'woman',
        'female',
        'girl',
        'transgirl',
        'tgirl',
        'transwoman',
        'puppygirl',
        'doggirl',
        'catgirl',
        'lesbian',
        'sapphic',
        'gal',
    ],
    regexes=[
        r'\b\d\df\b'
    ]
)


boy_vibes = Vibes(
    emoji=['♂️'],
    words=[
        'he',
        'him',
        'his',
        'm',
        'man',
        'male',
        'boy',
        'boi',
        'femboy',
        'dogboy',
        'catboy',
        'tboy',
        'tboi',
        'transman',
        'transmale',
        'transmasc',
        'himbo',
        'boyo',
    ],
    regexes=[
        r'\b\d\dm\b'
    ]
)


enby_vibes = Vibes(
    emoji=[],
    words=[
        'they',
        'them',
        'enby',
        'nb',
        'nonbinary',
        'non-binary',
        'thembo',
        'any pronouns',
        'any pronounce',
        'any prns',
        'genderfluid',
        'genderflux',
        'agender',
        'agendered',
        # Intentionally not separate words
        'it/its',
    ],
    regexes=[
        r'\b\d\dnb\b'
    ]
)


def _test_vibes(vibes: Vibes, t: str) -> Union['re.Match[str]', str, None]:
    for i in vibes.emoji:
        if i in t:
            return i
    regex = r'\b(' + '|'.join(vibes.words) + r')\b'
    if m := re.search(regex, t):
        return m
    for i in vibes.regexes:
        if m := re.search(i, t):
            return m
    return None


def guess_gender_reductive(text: str) -> Gender:
    t = (
        unicodedata.normalize('NFKC', text)
        .replace('\n', ' ')
        .replace("'", '')
        .replace('’', '')
        .replace('丨', ' ')
        .replace('/', ' ')
        .lower()
    )
    g = _test_vibes(girl_vibes, t) is not None
    b = _test_vibes(boy_vibes, t) is not None
    n = _test_vibes(enby_vibes, t) is not None
    if g and b:
        return 'enby'
    if g:
        return 'girl'
    if b:
        return 'boy'
    if n:
        return 'enby'
    return 'unknown'
