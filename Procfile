release: prisma db push --accept-data-loss && python -m scripts.db_cleanup
web: prisma generate && PORT=$PORT DATABASE_URL=$DATABASE_URL python -m server --admin-panel --no-scores
scoreposts: prisma generate && ./run_score_task_forever.sh
