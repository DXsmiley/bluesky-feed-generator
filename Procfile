release: prisma db push --accept-data-loss
web: prisma generate && PORT=$PORT DATABASE_URL=$DATABASE_URL python -m server --admin-panel --no-scores
scoreposts: prisma generate && python -m server.algos.score_task --forever
