release: prisma db push --accept-data-loss
web: PORT=$PORT DATABASE_URL=$DATABASE_URL python -m server --admin-panel --no-firehose
firehose: python -m server.firehose
