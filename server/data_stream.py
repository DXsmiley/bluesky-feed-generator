import threading
import typing as t
from typing import Callable, List, Optional, TypeVar, Generic
from typing_extensions import Never, TypedDict

from atproto import CAR, AtUri, models
from atproto.exceptions import FirehoseError
from atproto.firehose import FirehoseSubscribeReposClient, parse_subscribe_repos_message
from atproto.xrpc_client.models import get_or_create, is_record_type
from atproto.xrpc_client.models.base import ModelBase #, DotDict
from atproto.xrpc_client.models.common import XrpcError
# from atproto.xrpc_client.models.unknown_type import UnknownRecordType

from server.logger import logger
from server.database import SubscriptionState

if t.TYPE_CHECKING:
    from atproto.firehose import MessageFrame


T = TypeVar('T')


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


def _get_ops_by_type(commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> OpsByType:
    operation_by_type: OpsByType = {
        'posts': {'created': [], 'deleted': []},
        'reposts': {'created': [], 'deleted': []},
        'likes': {'created': [], 'deleted': []},
        'follows': {'created': [], 'deleted': []},
    }

    assert isinstance(commit.blocks, bytes)

    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        uri = AtUri.from_str(f'at://{commit.repo}/{op.path}')

        if op.action == 'update':
            # not supported yet
            continue

        if op.action == 'create':
            if not op.cid:
                continue

            record_raw_data = car.blocks.get(op.cid)
            if not record_raw_data:
                continue

            record = get_or_create(record_raw_data, strict=False)
            # assert isinstance(record, ModelBase)

            if uri.collection == models.ids.AppBskyFeedLike and is_record_type(record, models.AppBskyFeedLike):
                # assert isinstance(record, models.AppBskyFeedLike.Main)
                operation_by_type['likes']['created'].append({'uri': str(uri), 'cid': str(op.cid), 'author': commit.repo, 'record': record})
            elif uri.collection == models.ids.AppBskyFeedPost and is_record_type(record, models.AppBskyFeedPost):
                # assert isinstance(record, models.AppBskyFeedPost.Main)
                operation_by_type['posts']['created'].append({'uri': str(uri), 'cid': str(op.cid), 'author': commit.repo, 'record': record})
            elif uri.collection == models.ids.AppBskyGraphFollow and is_record_type(record, models.AppBskyGraphFollow):
                # assert isinstance(record, models.AppBskyGraphFollow.Main)
                operation_by_type['follows']['created'].append({'uri': str(uri), 'cid': str(op.cid), 'author': commit.repo, 'record': record})

        if op.action == 'delete':
            if uri.collection == models.ids.AppBskyFeedLike:
                operation_by_type['likes']['deleted'].append({'uri': str(uri)})
            if uri.collection == models.ids.AppBskyFeedPost:
                operation_by_type['posts']['deleted'].append({'uri': str(uri)})
            if uri.collection == models.ids.AppBskyGraphFollow:
                operation_by_type['follows']['deleted'].append({'uri': str(uri)})

    return operation_by_type


def run(name: str, operations_callback: Callable[[OpsByType], None], stream_stop_event: Optional[threading.Event] = None) -> Never:
    while True:
        try:
            _run(name, operations_callback, stream_stop_event)
        except FirehoseError as e:
            if e.__context__ and e.__context__.args:
                xrpc_error = e.__context__.args[0]
                if isinstance(xrpc_error, XrpcError) and xrpc_error.error == 'ConsumerTooSlow':
                    logger.warn('Reconnecting to Firehose due to ConsumerTooSlow...')
                    continue

            raise e


def _run(name: str, operations_callback: Callable[[OpsByType], None], stream_stop_event: Optional[threading.Event] = None) -> None:
    state = SubscriptionState.prisma().find_first(
        where={'service': {'equals': name}}
    )

    params = None
    if state:
        params = models.ComAtprotoSyncSubscribeRepos.Params(cursor=state.cursor)

    client = FirehoseSubscribeReposClient(params)

    if not state:
        SubscriptionState.prisma().create(data={'service': name, 'cursor': 0})

    def on_message_handler(message: 'MessageFrame') -> None:

        # stop on next message if requested
        if stream_stop_event is not None and stream_stop_event.is_set():
            client.stop()
            return

        commit = parse_subscribe_repos_message(message)
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            return

        # update stored state every ~20 events
        if commit.seq % 20 == 0:
            # ok so I think name should probably be unique or something????
            SubscriptionState.prisma().update_many(
                data={
                    'cursor': commit.seq
                },
                where={
                    'service': name
                }
            )
            # SubscriptionState.update(cursor=commit.seq).where(SubscriptionState.service == name).execute()

        ops = _get_ops_by_type(commit)
        operations_callback(ops)

    client.start(on_message_handler)
