release: prisma db push --accept-data-loss
web: prisma generate && PORT=$PORT python -m foxfeed --admin --no-firehose
firehose: prisma generate && python -m foxfeed --firehose
