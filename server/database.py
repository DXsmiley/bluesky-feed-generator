import prisma
from prisma.types import HttpConfig, DatasourceOverride, ActorWhereInput

from typing import Optional, Tuple


Database = prisma.Prisma
Post = prisma.models.Post
SubscriptionState = prisma.models.SubscriptionState
Actor = prisma.models.Actor
PostScore = prisma.models.PostScore


async def make_database_connection(url: Optional[str] = None, timeout: int = 10, log_queries: bool = False) -> Database:
    db = prisma.Prisma(
        connect_timeout=timeout,
        http=HttpConfig(timeout=timeout),
        datasource=(None if url is None else DatasourceOverride(url=url)),
        log_queries=log_queries
    )
    await db.connect()
    return db


care_about_storing_user_data_preemptively: ActorWhereInput = {
    'is_muted': False,
    'OR': [
        {'manual_include_in_fox_feed': True},
        {'manual_include_in_fox_feed': None}
    ]
}


user_is_in_fox_feed: ActorWhereInput = {
    'is_muted':  False,
    'flagged_for_manual_review': False,
    'OR': [
        {'manual_include_in_fox_feed': True},
        {'manual_include_in_fox_feed': None}
    ]
}


user_is_in_vix_feed: ActorWhereInput = {
    'is_muted': False,
    'flagged_for_manual_review': False,
    'OR': [
        {
            'manual_include_in_vix_feed': True
        },
        {
            'manual_include_in_vix_feed': None,
            'autolabel_masc_vibes': False,
            'autolabel_fem_vibes': True,
        }
    ]
}
