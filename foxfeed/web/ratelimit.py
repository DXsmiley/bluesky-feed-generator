from aiohttp import web
from datetime import datetime, timedelta

class Ratelimit:
    def __init__(self, timespan: timedelta, limit: int):
        self.timespan = timespan
        self.timeout = datetime.now()
        self.limit = limit
        self.count = 0

    def _check(self) -> bool:
        now = datetime.now()
        if now > self.timeout:
            self.timeout = now + self.timespan
            self.count = 0
        self.count += 1
        return self.count <= self.limit

    def check_raising(self):
        if not self._check():
            raise web.HTTPTooManyRequests()
