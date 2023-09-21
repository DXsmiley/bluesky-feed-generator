from typing import List, Union
from dataclasses import dataclass
import re
import unicodedata


@dataclass
class VibeCheck:
    fem: bool
    enby: bool
    masc: bool


@dataclass
class Vibes:
    emoji: List[str]
    words: List[str]
    regexes: List[str]


girl_vibes = Vibes(
    emoji=["♀", "⚢"],
    words=[
        "she",
        "her",
        "hers",
        "f",
        "woman",
        "female",
        "girl",
        "girls",
        "transgirl",
        "tgirl",
        "transwoman",
        "puppygirl",
        "doggirl",
        "catgirl",
        "lesbian",
        "sapphic",
        "gal",
    ],
    regexes=[r"\b\d\df\b"],
)


boy_vibes = Vibes(
    emoji=["♂️"],
    words=[
        "he",
        "him",
        "his",
        "m",
        "man",
        "men",
        "male",
        "boy",
        "boys",
        "boi",
        "femboy",
        "dogboy",
        "catboy",
        "tboy",
        "tboi",
        "transman",
        "transmale",
        "transmasc",
        "himbo",
        "boyo",
    ],
    regexes=[r"\b\d\dm\b"],
)


enby_vibes = Vibes(
    emoji=[],
    words=[
        "they",
        "them",
        "enby",
        "nb",
        "nonbinary",
        "non-binary",
        "thembo",
        "any pronouns",
        "any pronounce",
        "any prns",
        "genderfluid",
        "genderflux",
        "agender",
        "agendered",
        # Intentionally not separate words
        "it/its",
    ],
    regexes=[r"\b\d\dnb\b"],
)


def _test_vibes(vibes: Vibes, t: str) -> Union["re.Match[str]", str, None]:
    for i in vibes.emoji:
        if i in t:
            return i
    regex = r"\b(" + "|".join(vibes.words) + r")\b"
    if m := re.search(regex, t):
        return m
    for i in vibes.regexes:
        if m := re.search(i, t):
            return m
    return None


def vibecheck(text: str) -> VibeCheck:
    t = (
        unicodedata.normalize("NFKC", text)
        .replace("\n", " ")
        .replace("'", "")
        .replace("’", "")
        .replace("丨", " ")
        .replace("/", " ")
        .lower()
    )
    return VibeCheck(
        fem=_test_vibes(girl_vibes, t) is not None,
        enby=_test_vibes(enby_vibes, t) is not None,
        masc=_test_vibes(boy_vibes, t) is not None,
    )
