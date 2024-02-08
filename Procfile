release: prisma db push --accept-data-loss && python -m foxfeed.db_cleanup
web: prisma generate && PORT=$PORT python -m foxfeed --admin
db-cleanup: prisma generate && python -m foxfeed.db_cleanup --forever
