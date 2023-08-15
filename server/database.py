import prisma
from prisma.types import HttpConfig

db = prisma.Prisma(
    connect_timeout=10,
    auto_register=True,
    http=HttpConfig(
        timeout=10,
    ),
    # log_queries=True
)

db.connect()

Post = prisma.models.Post
SubscriptionState = prisma.models.SubscriptionState
Actor = prisma.models.Actor

# db = peewee.SqliteDatabase('feed_database.db')


# class BaseModel(peewee.Model):
#     class Meta:
#         database = db


# class Post(BaseModel):
#     uri = peewee.CharField(index=True)
#     cid = peewee.CharField()
#     reply_parent = peewee.CharField(null=True, default=None)
#     reply_root = peewee.CharField(null=True, default=None)
#     indexed_at = peewee.DateTimeField(default=datetime.now)


# class SubscriptionState(BaseModel):
#     service = peewee.CharField(unique=True)
#     cursor = peewee.IntegerField()


# if db.is_closed():
#     db.connect()
#     db.create_tables([Post, SubscriptionState])
