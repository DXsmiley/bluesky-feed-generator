import asyncio
from datetime import datetime
from typing import List, TypeVar, Union, Callable, Dict, Coroutine, Any, Optional
from itertools import chain
from collections import defaultdict


K = TypeVar("K")
T = TypeVar("T")
U = TypeVar("U")


def mentions_fursuit(text: str) -> bool:
    text = text.replace("\n", " ").lower()
    return "fursuit" in text or "murrsuit" in text


def parse_datetime(s: str) -> datetime:
    formats = [
        r"%Y-%m-%dT%H:%M:%S.%fZ",
        r"%Y-%m-%dT%H:%M:%S.%f",
        r"%Y-%m-%dT%H:%M:%SZ",
        r"%Y-%m-%dT%H:%M:%S",
        r"%Y-%m-%dT%H:%M:%S.%f+00:00",
        r"%Y-%m-%dT%H:%M:%S+00:00",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    raise ValueError(f'failed to parse datetime string "{s}"')


def interleave(sep: T, xs: List[U]) -> List[Union[U, T]]:
    return [] if not xs else [xs[0], *chain.from_iterable((sep, i) for i in xs[1:])]


def groupby(f: Callable[[T], K], ts: List[T]) -> Dict[K, List[T]]:
    d: Dict[K, List[T]] = defaultdict(list)
    for i in ts:
        d[f(i)].append(i)
    return d


def ensure_string(s: str) -> str:
    assert isinstance(s, str)
    return s


async def sleep_on(event: asyncio.Event, timeout: float) -> bool:
    try:
        await asyncio.wait_for(event.wait(), timeout)
        return True
    except asyncio.TimeoutError:
        return False
    except asyncio.CancelledError:
        return False


async def join_unless(queue: 'asyncio.Queue[Any]', event: asyncio.Event):
    if not event.is_set():
        queue_task = asyncio.create_task(queue.join())
        wait_task = asyncio.create_task(event.wait())
        done, pending = await asyncio.wait(
            [queue_task, wait_task],
            return_when=asyncio.FIRST_COMPLETED
        )
        # Dunno if we need to do something about the "done" things?
        for i in pending:
            try:
                if i.cancel():
                    await i
            except asyncio.CancelledError:
                pass


async def wait_interruptable(c: Coroutine[Any, Any, T], event: asyncio.Event) -> Optional[T]:
    async def _f() -> None:
        await event.wait()
    c_task = asyncio.create_task(c)
    wait_task = asyncio.create_task(_f())
    done, pending = await asyncio.wait(
        [c_task, wait_task],
        return_when=asyncio.FIRST_COMPLETED
    )
    for i in pending:
        try:
            if i.cancel():
                await i
        except asyncio.CancelledError:
            pass
    for i in done:
        return i.result()
    return None
