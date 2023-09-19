import sys

import asyncio
import typing as t
from typing import Coroutine, Any, Callable, List, TypeVar, Generic, Optional
from typing_extensions import TypedDict
import traceback
from datetime import datetime

from atproto import CAR, AtUri, models
from atproto.exceptions import FirehoseError
from server.firehose.client import AsyncFirehoseSubscribeReposClient
from atproto.firehose import parse_subscribe_repos_message
from atproto.xrpc_client.models.utils import get_or_create, is_record_type
from atproto.xrpc_client.models.common import XrpcError
from atproto.xrpc_client.models.com.atproto.sync import subscribe_repos

from termcolor import cprint

from server.util import parse_datetime, wait_interruptable

# from atproto.xrpc_client.models.unknown_type import UnknownRecordType

from server.logger import logger
from server.database import Database

if t.TYPE_CHECKING:
    from atproto.firehose.models import MessageFrame


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
    posts: OpsPosts[models.AppBskyFeedPost.Main]
    reposts: OpsPosts[None]
    likes: OpsPosts[models.AppBskyFeedLike.Main]
    follows: OpsPosts[models.AppBskyGraphFollow.Main]


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

        # print(uri.collection, op.action)

        if op.action == "update":
            if not op.cid:
                continue
            # not supported yet
            # print('update!!!')
            record_raw_data = car.blocks.get(op.cid)
            if not record_raw_data:
                continue
            record = get_or_create(record_raw_data, strict=False)
            # print(record)
            if record is not None:
                if uri.collection == models.ids.AppBskyActorProfile and is_record_type(
                    record, models.AppBskyActorProfile
                ):
                    # print('profile update!')
                    # print(record)
                    pass
                elif uri.collection == models.ids.AppBskyFeedGenerator and is_record_type(
                    record, models.AppBskyFeedGenerator
                ):
                    pass
                elif uri.collection == models.ids.AppBskyGraphList and is_record_type(
                    record, models.AppBskyGraphList
                ):
                    pass
                else:
                    cprint(f'updated something else idk {uri.collection}', 'red', force_color=True)
                    print(record)

        elif op.action == "create":
            if not op.cid:
                continue

            record_raw_data = car.blocks.get(op.cid)
            if not record_raw_data:
                continue

            record = get_or_create(record_raw_data, strict=False)
            # assert isinstance(record, ModelBase)

            if record is not None:
                if uri.collection == models.ids.AppBskyFeedLike and is_record_type(
                    record, models.AppBskyFeedLike
                ):
                    operation_by_type["likes"]["created"].append(
                        {
                            "uri": str(uri),
                            "cid": str(op.cid),
                            "author": commit.repo,
                            "record": record,
                        }
                    )
                elif uri.collection == models.ids.AppBskyFeedPost and is_record_type(
                    record, models.AppBskyFeedPost
                ):
                    operation_by_type["posts"]["created"].append(
                        {
                            "uri": str(uri),
                            "cid": str(op.cid),
                            "author": commit.repo,
                            "record": record,
                        }
                    )
                elif uri.collection == models.ids.AppBskyGraphFollow and is_record_type(
                    record, models.AppBskyGraphFollow
                ):
                    operation_by_type["follows"]["created"].append(
                        {
                            "uri": str(uri),
                            "cid": str(op.cid),
                            "author": commit.repo,
                            "record": record,
                        }
                    )
                elif uri.collection == models.ids.AppBskyFeedRepost and is_record_type(
                    record, models.AppBskyFeedRepost
                ):
                    pass
                elif uri.collection == models.ids.AppBskyGraphBlock and is_record_type(
                    record, models.AppBskyGraphBlock
                ):
                    pass
                elif uri.collection == models.ids.AppBskyGraphList and is_record_type(
                    record, models.AppBskyGraphList
                ):
                    pass
                elif uri.collection == models.ids.AppBskyGraphListitem and is_record_type(
                    record, models.AppBskyGraphListitem
                ):
                    pass
                elif uri.collection == models.ids.AppBskyActorProfile and is_record_type(
                    record, models.AppBskyActorProfile
                ):
                    pass
                elif uri.collection == models.ids.AppBskyFeedGenerator and is_record_type(
                    record, models.AppBskyFeedGenerator
                ):
                    pass
                else:
                    cprint(f'created something else idk {uri.collection}', 'red', force_color=True)
                    print(record)

        elif op.action == "delete":
            if uri.collection == models.ids.AppBskyFeedLike:
                operation_by_type["likes"]["deleted"].append({"uri": str(uri)})
            elif uri.collection == models.ids.AppBskyFeedPost:
                operation_by_type["posts"]["deleted"].append({"uri": str(uri)})
            elif uri.collection == models.ids.AppBskyGraphFollow:
                operation_by_type["follows"]["deleted"].append({"uri": str(uri)})
            elif uri.collection == models.ids.AppBskyFeedRepost:
                pass
            elif uri.collection == models.ids.AppBskyGraphListitem:
                pass
            elif uri.collection == models.ids.AppBskyGraphBlock:
                pass
            else:
                cprint(f'Deleted something else idk {uri.collection}', 'red', force_color=True)

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


async def _run(
    db: Database,
    name: str,
    operations_callback: OPERATIONS_CALLBACK_TYPE,
    stream_stop_event: asyncio.Event,
) -> None:
    state = await db.subscriptionstate.find_first(where={"service": name})
    print('Starting firehose state:', None if state is None else state.model_dump_json())
    params = subscribe_repos.Params(cursor=state.cursor if state else None)
    client = AsyncFirehoseSubscribeReposClient(params)

    messages_to_process: 'asyncio.Queue[MessageFrame]' = asyncio.Queue(maxsize=20)

    async def process_message(message: "MessageFrame") -> None:
        commit = parse_subscribe_repos_message(message)

        if isinstance(commit, subscribe_repos.Info):
            print('Info', commit.model_dump_json())
        else:
            if isinstance(commit, subscribe_repos.Tombstone):
                print('Tombstone', commit.model_dump_json())
            elif isinstance(commit, subscribe_repos.Handle):
                print('Handle', commit.model_dump_json())
            elif isinstance(commit, subscribe_repos.Migrate):
                print('Migrate', commit.model_dump_json())
            elif isinstance(commit, subscribe_repos.Commit):
                ops = _get_ops_by_type(commit)
                await operations_callback(db, ops)
            else:
                # Should never reach here
                assert False
            
            if commit.seq % 5000 == 0:
                client.update_params({'cursor': commit.seq})
                await db.subscriptionstate.upsert(
                    where={'service': name},
                    data={
                        'create': {'service': name, 'cursor': commit.seq},
                        'update': {'cursor': commit.seq},
                    }
                )
                lag = datetime.now() - parse_datetime(commit.time)
                lag_minutes = int(lag.total_seconds()) // 60
                # num_tasks = len(client._on_message_tasks)
                if lag_minutes != 0:
                    cprint(f'Firehose is lagging | commit {commit.seq} | {messages_to_process.qsize()} items in queue | {lag_minutes // 60} hours {lag_minutes % 60} minutes behind', 'cyan', force_color=True)

    async def process_messages_forever() -> None:
        while True:
            message = await messages_to_process.get()
            try:
                if not stream_stop_event.is_set():
                    await process_message(message)
            except Exception as e:
                on_error_handler(e)
            finally:
                messages_to_process.task_done()

    async def on_message_handler(message: "MessageFrame") -> None:
        await messages_to_process.put(message)

    def on_error_handler(exception: BaseException) -> None:
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
        client.stop()

    worker = asyncio.create_task(process_messages_forever())
    end_w = asyncio.create_task(ender())
    await client.start(on_message_handler, on_error_handler)
    await messages_to_process.join()
    worker.cancel()
    await asyncio.gather(worker, end_w, return_exceptions=True)

