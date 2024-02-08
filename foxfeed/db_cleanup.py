import asyncio
from datetime import datetime, timedelta
from foxfeed.database import make_database_connection, Database
from foxfeed.algos.generators import LOOKBACK_HARD_LIMIT
from foxfeed.metrics import METRICS_MAXIMUM_LOOKBACK
from prisma.bases import _PrismaModel

from typing import Awaitable, Protocol, Callable, TypeVar, Generic, Optional, List


POST_MAX_AGE = timedelta(days=60)


T = TypeVar('T')
Where = TypeVar('Where', contravariant=True)
WhereUnique = TypeVar('WhereUnique', contravariant=True)

Model = TypeVar('Model', bound=_PrismaModel)
ModelCo = TypeVar('ModelCo', bound=_PrismaModel, covariant=True)


class FindMany(Protocol, Generic[Where, Model]):
    async def __call__(
        self,
        take: Optional[int] = None,
        skip: None = None,
        where: Optional[Where] = None,
        cursor: None = None,
        include: None = None,
        order: None = None,
        distinct: None = None,
    ) -> List[Model]: ...


class DeleteMany(Protocol, Generic[Where]):
    async def __call__(self, where: Where) -> int: ...


class Delete(Protocol, Generic[WhereUnique, ModelCo]):
    async def __call__(self, where: WhereUnique, include: None = None) -> Optional[ModelCo]: ...


class Table(Protocol, Generic[Where, WhereUnique, Model]):
    @property
    def find_many(self) -> FindMany[Where, Model]: ...
    @property
    def delete_many(self) -> DeleteMany[Where]: ...
    @property
    def delete(self) -> Delete[WhereUnique, Model]: ...



async def drop(description: str, f: Awaitable[int]) -> None:
    start = datetime.now()
    print(description)
    num_rows = await f
    end = datetime.now()
    seconds = (end - start).total_seconds()
    print(f'> dropped {num_rows} rows in {seconds:.0f} seconds')


async def drop_limited(
        end_at: datetime,
        description: str,
        table: Table[Where, WhereUnique, Model],
        condition: Where,
        get_id: Callable[[Model], WhereUnique]
    ):
    CHUNK_SIZE = 1000
    while True:
        start = datetime.utcnow()
        if start > end_at:
            break
        print(description)
        found: List[Model] = await table.find_many(where=condition, take=CHUNK_SIZE)
        for i in found:
            await table.delete(where=get_id(i))
        end = datetime.utcnow()
        seconds = (end - start).total_seconds()
        print(f'> dropped {len(found)} rows in {seconds:.0f} seconds')
        if len(found) < CHUNK_SIZE:
            break


async def main():
    now = datetime.utcnow()
    end_at = now + timedelta(minutes=1)
    db = await make_database_connection(timeout=300)

    print('Cleaning up the database...')

    postscore_max_version = await db.postscore.find_first(order={'version': 'desc'})
    if postscore_max_version is not None:
        await drop(
            'Deleting postscores',
            db.postscore.delete_many(where={'version': {'not': postscore_max_version.version}})
        )

    await drop(
        'Deleting servedblocks',
        db.servedblock.delete_many(where={'when': {'lt': now - METRICS_MAXIMUM_LOOKBACK}})
    )

    await drop(
        'Deleting servedposts',
        db.servedpost.delete_many(where={'when': {'lt': now - METRICS_MAXIMUM_LOOKBACK}})
    )

    await drop(
        'Deleting blueskyclientsessions',
        db.blueskyclientsession.delete_many(where={'created_at': {'lt': now - timedelta(days=7)}})
    )

    # Can't do this while we're trying to complete our graph trees
    await drop_limited(
        end_at,
        'Deleting old likes',
        db.like,
        {'created_at': {'lt': now - POST_MAX_AGE}},
        lambda x: {'uri': x.uri},
    )

    await drop_limited(
        end_at,
        'Deleting old posts',
        db.post,
        {'indexed_at': {'lt': now - POST_MAX_AGE}},
        lambda x: {'uri': x.uri}
    )

    await drop_limited(
        end_at,
        'Deleting old likes from accounts outside the main cluster',
        db.like,
        {
            'created_at': {'lt': now - LOOKBACK_HARD_LIMIT * 2},
            'liker': {
                'is': {
                    'OR': [
                        {'is_external_to_network': True},
                        {'manual_include_in_fox_feed': False},
                        {'flagged_for_manual_review': True},
                        {'is_muted': True},
                    ]
                }
            }
        },
        lambda x: {'uri': x.uri},
    )

    await drop_limited(
        end_at,
        'Deleting old posts from accounts outside the main cluster',
        db.post,
        {
            'indexed_at': {'lt': now - LOOKBACK_HARD_LIMIT * 2},
            'author': {
                'is': {
                    'OR': [
                        {'is_external_to_network': True},
                        {'manual_include_in_fox_feed': False},
                        {'flagged_for_manual_review': True},
                        {'is_muted': True},
                    ]
                }
            },
        },
        lambda x: {'uri': x.uri},
    )


if __name__ == '__main__':
    asyncio.run(main())
