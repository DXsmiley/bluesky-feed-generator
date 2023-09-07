import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

logging.getLogger("httpx").setLevel(logging.WARNING)
# logging.getLogger('prisma').setLevel(logging.DEBUG)
# logging.getLogger('sqlite3').setLevel(logging.DEBUG)
