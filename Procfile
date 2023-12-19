release: prisma db push --accept-data-loss # && python -m scripts.db_cleanup
web: prisma generate && PORT=$PORT python -m foxfeed --admin
