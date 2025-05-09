import asyncio
from datetime import datetime
from datetime import timezone
import dateutil
import types
from typing import ClassVar, Protocol, Type, List, TypeVar, Union, Callable, Dict, Coroutine, Any, Optional, Iterable, AsyncIterable
import dateutil.parser
from typing_extensions import TypeGuard, LiteralString
from itertools import chain
from collections import defaultdict
from atproto_client.models.base import ModelBase
from atproto_client.models.dot_dict import DotDict
from pydantic.fields import FieldInfo

from atproto_client.models.utils import is_record_type as _is_record_type


K = TypeVar("K")
T = TypeVar("T")
U = TypeVar("U")



class ModelWithDiscriminator(Protocol):
    model_fields: ClassVar[Dict[str, FieldInfo]]
    @property
    def py_type(self) -> LiteralString: ...


Model = TypeVar('Model', bound=ModelWithDiscriminator)


class HasAMainModel(Protocol[Model]):
    Main: Type[Model]


class HasARecordModel(Protocol[Model]):
    Record: Type[Model]


def is_record_type(model: Union[None, ModelBase, DotDict], expected_type: Union[HasAMainModel[Model], HasARecordModel[Model]]) -> TypeGuard[Model]:
    assert isinstance(expected_type, types.ModuleType)
    # This is a wild hack
    if hasattr(expected_type, 'Main'):
        expected_type.Record = expected_type.Main
    return isinstance(model, (ModelBase, DotDict)) and _is_record_type(model, expected_type)


def mentions_fursuit(text: str) -> bool:
    text = text.replace("\n", " ").lower()
    return "fursuit" in text or "murrsuit" in text


def parse_datetime(s: str) -> datetime:
    return dateutil.parser.parse(s).astimezone(timezone.utc)
    # formats = [
    #     r"%Y-%m-%dT%H:%M:%S.%fZ",
    #     r"%Y-%m-%dT%H:%M:%S.%f",
    #     r"%Y-%m-%dT%H:%M:%SZ",
    #     r"%Y-%m-%dT%H:%M:%S",
    #     r"%Y-%m-%dT%H:%M:%S.%f+00:00",
    #     r"%Y-%m-%dT%H:%M:%S+00:00",
    #     r"%Y-%m-%dT%H:%M:%S.%f+0000",
    #     r"%Y-%m-%dT%H:%M:%S+0000",
    # ]
    # for fmt in formats:
    #     try:
    #         return datetime.strptime(s, fmt).astimezone(timezone.utc)
    #     except ValueError:
    #         pass
    # raise ValueError(f'failed to parse datetime string "{s}"')


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


async def sleep_on(event: Optional[asyncio.Event], timeout: float) -> bool:
    if event is None:
        await asyncio.sleep(timeout)
        return False
    try:
        await asyncio.wait_for(event.wait(), timeout)
        return True
    except asyncio.TimeoutError:
        return False
    except asyncio.CancelledError:
        return False
    

class Joinable(Protocol):
    async def join(self) -> None: ...


async def join_unless(joinable: Joinable, event: asyncio.Event):
    if not event.is_set():
        queue_task = asyncio.create_task(joinable.join())
        wait_task = asyncio.create_task(event.wait())
        _, pending = await asyncio.wait(
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


def chunkify(xs: Iterable[T], chunk_size: int) -> Iterable[List[T]]:
    chunk: List[T] = []
    for i in xs:
        chunk.append(i)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if len(chunk) > 0:
        yield chunk


async def achunkify(ai: AsyncIterable[T], chunk_size: int) -> AsyncIterable[List[T]]:
    chunk: List[T] = []
    async for i in ai:
        chunk.append(i)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if len(chunk) > 0:
        yield chunk


async def alist(ai: AsyncIterable[T]) -> List[T]:
    return [i async for i in ai]


B32CHARS = 'abcdefghijklmnopqrstuvwxyz234567'


def b32_decode(s: str) -> int:
    return sum((32 ** i) * B32CHARS.index(v) for i, v in enumerate(s[::-1]))


def b32_encode(x: int) -> str:
    s = ''
    while x > 0:
        s += B32CHARS[x % 32]
        x //= 32
    return s[::-1]


def previous_rkey(rkey: str) -> str:
    return b32_encode(b32_decode(rkey) - 1)


def next_rkey(rkey: str) -> str:
    return b32_encode(b32_decode(rkey) + 1)
