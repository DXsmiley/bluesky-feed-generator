import sys

import asyncio
import typing as t
from typing import Coroutine, Any, Callable, List, TypeVar, Generic, Union, Optional
from typing_extensions import TypedDict, TypeGuard
import traceback
from datetime import datetime

from atproto import CAR, AtUri, models
from atproto.exceptions import FirehoseError
# from atproto.firehose.client import AsyncFirehoseClient as AsyncFirehoseR
from atproto_firehose import parse_subscribe_repos_message, AsyncFirehoseSubscribeReposClient
from atproto_firehose.client import aconnect, _MAX_MESSAGE_SIZE_BYTES
from atproto_client.models.utils import get_or_create
from atproto_client.models.common import XrpcError
from atproto_client.models.base import ModelBase
from atproto_client.models.dot_dict import DotDict
from atproto_client.models.com.atproto.sync import subscribe_repos
from foxfeed.util import is_record_type

from termcolor import cprint

from foxfeed.util import parse_datetime, Model, HasARecordModel
from foxfeed.logger import logger
from foxfeed.database import Database

import time


if t.TYPE_CHECKING:
    from atproto_firehose.models import MessageFrame


T = TypeVar("T")


class CreateOp(TypedDict, Generic[T]):
    uri: str
    cid: str
    author: str
    record: T


class DeleteOp(TypedDict):
    uri: str


class OpsPosts(TypedDict, Generic[T]):
    created: List[CreateOp[T]]
    deleted: List[DeleteOp]


class OpsByType(TypedDict):
    posts: OpsPosts[models.AppBskyFeedPost.Record]
    reposts: OpsPosts[None]
    likes: OpsPosts[models.AppBskyFeedLike.Record]
    follows: OpsPosts[models.AppBskyGraphFollow.Record]


OPERATIONS_CALLBACK_TYPE = Callable[[Database, OpsByType], Coroutine[Any, Any, None]]


def _get_ops_by_type(commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> OpsByType:
    operation_by_type: OpsByType = {
        "posts": {"created": [], "deleted": []},
        "reposts": {"created": [], "deleted": []},
        "likes": {"created": [], "deleted": []},
        "follows": {"created": [], "deleted": []},
    }

    assert isinstance(commit.blocks, bytes)

    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        uri = AtUri.from_str(f"at://{commit.repo}/{op.path}")

        record_raw_data = None if op.cid is None else car.blocks.get(op.cid)
        record = None if record_raw_data is None else get_or_create(record_raw_data, strict=False)

        if record is not None and not isinstance(record, (ModelBase, DotDict)):
            continue

        def check(r: Union[ModelBase, DotDict, None], expected_type: HasARecordModel[Model]) -> TypeGuard[Model]:
            return (
                uri.collection == expected_type.Record.model_fields['py_type'].default
                and is_record_type(r, expected_type)
            )
        
        def check_delete(expected_type: HasARecordModel[Model]) -> bool:
            return uri.collection == expected_type.Record.model_fields['py_type'].default

        if op.action == "update":
            if check(record, models.AppBskyActorProfile):
                pass
            elif check(record, models.AppBskyFeedGenerator):
                pass
            elif check(record, models.AppBskyGraphList):
                pass
            else:
                # cprint(f'updated something else idk {uri.collection}', 'red', force_color=True)
                # print(record)
                pass

        elif op.action == "create":
            if op.cid is None:
                print('Create where op.cid is None, this is weird')
            elif check(record, models.AppBskyFeedLike):
                operation_by_type["likes"]["created"].append(
                    {
                        "uri": str(uri),
                        "cid": str(op.cid),
                        "author": commit.repo,
                        "record": record,
                    }
                )
            elif check(record, models.AppBskyFeedPost):
                operation_by_type["posts"]["created"].append(
                    {
                        "uri": str(uri),
                        "cid": str(op.cid),
                        "author": commit.repo,
                        "record": record,
                    }
                )
            elif check(record, models.AppBskyGraphFollow):
                operation_by_type["follows"]["created"].append(
                    {
                        "uri": str(uri),
                        "cid": str(op.cid),
                        "author": commit.repo,
                        "record": record,
                    }
                )
            elif check(record, models.AppBskyFeedRepost):
                pass
            elif check(record, models.AppBskyGraphBlock):
                pass
            elif check(record, models.AppBskyGraphList):
                pass
            elif check(record, models.AppBskyGraphListitem):
                pass
            elif check(record, models.AppBskyGraphListblock):
                pass
            elif check(record, models.AppBskyActorProfile):
                pass
            elif check(record, models.AppBskyFeedGenerator):
                pass
            else:
                pass
                # cprint(f'created something else idk {uri.collection}', 'red', force_color=True)
                # print(record)

        elif op.action == "delete":
            if check_delete(models.AppBskyFeedLike):
                operation_by_type["likes"]["deleted"].append({"uri": str(uri)})
            elif check_delete(models.AppBskyFeedPost):
                operation_by_type["posts"]["deleted"].append({"uri": str(uri)})
            elif check_delete(models.AppBskyGraphFollow):
                operation_by_type["follows"]["deleted"].append({"uri": str(uri)})
            elif check_delete(models.AppBskyFeedRepost):
                pass
            elif check_delete(models.AppBskyGraphListitem):
                pass
            elif check_delete(models.AppBskyGraphBlock):
                pass
            elif check_delete(models.AppBskyGraphListblock):
                pass
            else:
                # cprint(f'Deleted something else idk {uri.collection}', 'red', force_color=True)
                pass

        else:
            cprint(f'Unknown op.action {op.action}', 'red', force_color=True)

    return operation_by_type


async def run(
    db: Database,
    name: str,
    operations_callback: OPERATIONS_CALLBACK_TYPE,
    stream_stop_event: asyncio.Event
) -> None:
    while not stream_stop_event.is_set():
        try:
            await _run(db, name, operations_callback, stream_stop_event)
        except asyncio.CancelledError:
            raise
        except KeyboardInterrupt:
            raise
        except FirehoseError as e:
            logger.info(f"Got FirehoseError: {e}")
            if e.__context__ and e.__context__.args:
                xrpc_error = e.__context__.args[0]
                if (
                    isinstance(xrpc_error, XrpcError)
                    and xrpc_error.error == "ConsumerTooSlow"
                ):
                    logger.warn("Reconnecting to Firehose due to ConsumerTooSlow...")
                    continue

            raise e
    print("Finished run(...) due to stream stop event")


class AFSRCpatched(AsyncFirehoseSubscribeReposClient):
    def _get_async_client(self):
        return aconnect(self._websocket_uri, max_size=_MAX_MESSAGE_SIZE_BYTES, close_timeout=0.2)


async def _run(
    db: Database,
    name: str,
    operations_callback: OPERATIONS_CALLBACK_TYPE,
    stream_stop_event: asyncio.Event,
) -> None:
    state = await db.subscriptionstate.find_first(where={"service": name})
    print('Starting firehose state:', None if state is None else state.model_dump_json())
    params = subscribe_repos.Params(cursor=state.cursor if state else None)
    client = AFSRCpatched(params)
    message_count = [0]
    message_count_time = [time.time()]
    prev_time: List[Optional[datetime]] = [None]

    messages_to_process: 'asyncio.Queue[MessageFrame]' = asyncio.Queue(maxsize=200)

    async def process_message(message: "MessageFrame") -> None:
        commit = parse_subscribe_repos_message(message)

        if isinstance(commit, subscribe_repos.Info):
            print('Info', commit.model_dump_json())
        else:
            if isinstance(commit, subscribe_repos.Tombstone):
                pass # print('Tombstone', commit.model_dump_json())
            elif isinstance(commit, subscribe_repos.Handle):
                pass # print('Handle', commit.model_dump_json())
            elif isinstance(commit, subscribe_repos.Migrate):
                pass # print('Migrate', commit.model_dump_json())
            elif isinstance(commit, subscribe_repos.Info):
                pass # print('Info', commit.model_dump_json())
            elif isinstance(commit, subscribe_repos.Account):
                pass # print('Account', commit.model_dump_json())
            elif isinstance(commit, subscribe_repos.Identity):
                pass # print('Identity', commit.model_dump_json())
            elif isinstance(commit, subscribe_repos.RepoOp):
                pass # print('RepoOp', commit.model_dump_json())
            elif isinstance(commit, subscribe_repos.Commit):
                ops = _get_ops_by_type(commit)
                await operations_callback(db, ops)
                pass
            else:
                # Should never reach here
                assert False
            
            if commit.seq % 500 == 0:
                t = time.time()
                elapsed = t - message_count_time[0]
                rate = int(500 / elapsed)
                message_count_time[0] = t

                client.update_params({'cursor': commit.seq})
                await db.subscriptionstate.upsert(
                    where={'service': name},
                    data={
                        'create': {'service': name, 'cursor': commit.seq},
                        'update': {'cursor': commit.seq},
                    }
                )
                stream_time = parse_datetime(commit.time)
                lag = datetime.utcnow() - stream_time
                lag_minutes = int(lag.total_seconds()) // 60
                stream_elapsed = 0 if prev_time[0] is None else (stream_time - prev_time[0]).seconds
                stream_rate = stream_elapsed / elapsed
                prev_time[0] = stream_time
                if lag_minutes != 0:
                    cprint(f'Firehose is lagging | commit {commit.seq} | {messages_to_process.qsize()} items in queue | {rate:4d}/s | {stream_rate:.2f} | {lag_minutes // 60} hours {lag_minutes % 60} minutes behind', 'cyan', force_color=True)

    async def process_messages_forever() -> None:
        while True:
            message = await messages_to_process.get()
            try:
                if not stream_stop_event.is_set():
                    await process_message(message)
            except Exception as e:
                await on_error_handler(e)
            finally:
                messages_to_process.task_done()

    async def on_message_handler(message: "MessageFrame") -> None:
        await messages_to_process.put(message)

    async def on_error_handler(exception: BaseException) -> None:
        if isinstance(exception, KeyboardInterrupt):
            # This has the potential to bubble waaaaaay up the stack so IDK if this is smart
            # It shouldn't really be occuring here anyway during production tho TBH
            print('KeyboardInterrupt during data stream message handler, shutting down')
            stream_stop_event.set()
        elif not stream_stop_event.is_set():
            print("Error in data stream message handler:", file=sys.stderr)
            traceback.print_exception(type(exception), exception, exception.__traceback__)
        else:
            print('Error in data stream message handler, not reporting due to stop signal')

    async def ender() -> None:
        await stream_stop_event.wait()
        print('ender() for stream stop event!')
        await client.stop()

    num_workers = 4
    workers = [asyncio.create_task(process_messages_forever()) for _ in range(num_workers)]
    end_w = asyncio.create_task(ender())
    await client.start(on_message_handler, on_error_handler)
    await messages_to_process.join()
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, end_w, return_exceptions=True)

