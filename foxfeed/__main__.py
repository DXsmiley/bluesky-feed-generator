import foxfeed.monkeypatch

import sys
from foxfeed.web.app import create_and_run_webapp, Services
from typing import List
from foxfeed import config

ARGS = {
    "--no-scraper": "Disable the website scraper",
    "--no-firehose": "Disable the firehose consumer",
    "--no-scores": "Disable the score task",
    "--log-db-queries": "Log queries made to the database",
    "--admin-panel": "Enable admin panel",
}


def main(args: List[str]) -> int:
    if "--help" in args:
        print("Arguments:\n")
        for k, v in ARGS.items():
            print(f"  {k:20} {v}")
        print("")
        return 0

    error = False
    for i in args:
        if i not in ARGS:
            print("Unknown flag", i)
    if error:
        return 1

    create_and_run_webapp(
        port=config.PORT,
        db_url=config.DB_URL,
        services=Services(
            scraper="--no-scraper" not in args,
            firehose="--no-firehose" not in args,
            scores="--no-scores" not in args,
            log_db_queries="--log-db-queries" in args,
            admin_panel="--admin-panel" in args,
        ),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
