import prisma
from prisma.types import HttpConfig, DatasourceOverride, ActorWhereInput
# import psycopg.conninfo
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
# import psycopg


Database = prisma.Prisma
Post = prisma.models.Post
SubscriptionState = prisma.models.SubscriptionState
Actor = prisma.models.Actor
PostScore = prisma.models.PostScore


async def make_database_connection(
    url: Optional[str] = None, timeout: int = 30, log_queries: bool = False
) -> Database:
    db = prisma.Prisma(
        connect_timeout=timeout,
        http=HttpConfig(timeout=timeout),
        datasource=(None if url is None else DatasourceOverride(url=url)),
        log_queries=log_queries,
    )
    await db.connect()
    # assert url is not None
    # pg = await psycopg.AsyncConnection.connect(url)
    # print(pg)
    return db


care_about_storing_user_data_preemptively: ActorWhereInput = {
    "is_muted": False,
    "OR": [{"manual_include_in_fox_feed": True}, {"manual_include_in_fox_feed": None, "is_external_to_network": False}],
}


user_is_in_fox_feed: ActorWhereInput = {
    "AND": [
        care_about_storing_user_data_preemptively,
        {"flagged_for_manual_review": False},
    ]
}


user_is_in_vix_feed: ActorWhereInput = {
    "AND": [
        user_is_in_fox_feed,
        {
            "OR": [
                {"manual_include_in_vix_feed": True},
                {
                    "manual_include_in_vix_feed": None,
                    "autolabel_masc_vibes": False,
                    "autolabel_fem_vibes": True,
                },
            ]
        },
    ]
}


class ScorePostsOutputModel(BaseModel):
    uri: str
    author: str
    indexed_at: datetime
    score: float
    labels: Optional[List[str]]
    author_is_fem: bool


class ScoreByInteractionOutputModel(BaseModel):
    uri: str
    score: int

class FindUnlinksOutputModel(BaseModel):
    uri: str
