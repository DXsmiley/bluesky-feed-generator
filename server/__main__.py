import server.monkeypatch

import sys
import os
from server.app import create_and_run_webapp, Services
from typing import List

ARGS = {
    '--no-scraper': 'Disable the website scraper',
    '--no-firehose': 'Disable the firehose consumer',
    '--no-scores': 'Disable the score task',
    '--log-db-queries': 'Log queries made to the database',
    '--admin-panel': 'Enable admin panel',
}

def main(args: List[str]) -> int:
    if '--help' in args:
        print('Arguments:\n')
        for k, v in ARGS.items():
            print(f'  {k:20} {v}')
        print('')
        return 0
    
    error = False
    for i in args:
        if i not in ARGS:
            print('Unknown flag', i)
    if error:
        return 1

    port = int(os.environ.get('PORT', 8000))
    db_url = os.environ.get('DATABASE_URL', None)
    create_and_run_webapp(
        port=port,
        db_url=db_url,
        services=Services(
            scraper='--no-scraper' not in args,
            firehose='--no-firehose' not in args,
            scores='--no-scores' not in args,
            log_db_queries='--log-db-queries' in args,
            admin_panel='--admin-panel' in args,
        )
    )
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))