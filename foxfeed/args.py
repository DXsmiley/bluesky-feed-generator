from dataclasses import dataclass
from typing import List, Union, Optional, TypeVar


T = TypeVar('T')


HELP = '''\

Run the server with `python -m foxfeed`, all services enabled by default.
If at least one service is manually enabled, then all other services are disabled by default.

Services:

    --server --no-server      Enable or disable the webserver
    --scraper --no-scraper    Enable or disable the scraper
    --firehose --no-firehose  Enable or disable the firehose
    --scores --no-scores      Enable or disable post scoring (feed generation)

Settings:

    --admin --no-admin        Enable the admin panel (requires the webserver)
    --log-db-queries          Log database queries
    --forever                 Run services forever, enabled by default if at least perpetual service is enabled

Unimplemented flags:

    --firehose-start-from     Select the firehose start time, one of "oldest", "present" or "default"
    --skip-full-scrape        Skip the extensive scrape that happens when the server boots
    --dont-refresh-posts      Disable post refreshing as part of post scoring

'''


@dataclass
class Args:
    webserver: bool
    scraper: bool
    firehose: bool
    scores: bool
    
    log_db_queries: bool
    admin_panel: bool
    dont_require_admin_login: bool

    forever: bool


def take(args: List[str], tflag: str, fflag: Optional[str] = None, *, default: T = None) -> Union[bool, T]:
    if fflag is not None and fflag in args and tflag in args:
        raise ValueError(f'Contradictory flags {tflag} and {fflag} both present')
    elif tflag in args:
        args.remove(tflag)
        return True
    elif fflag is not None and fflag in args:
        args.remove(fflag)
        return False
    else:
        return default
    

def defaulting(value: Optional[T], default: T) -> T:
    return default if value is None else value


def parse_args(args_original: List[str]) -> Union[int, Args]:
    # Make a copy since we're gonna mutate this list :(

    args = list(args_original)

    if '--help' in args:
        print(HELP)
        return 0

    forever_flag = take(args, '--forever', default=False)
    log_db_queries = take(args, '--log-db-queries', default=False)
    admin_panel = take(args, '--admin', '--no-admin', default=False)

    webserver_flag = take(args, '--server', '--no-server')
    scraper_flag = take(args, '--scraper', '--no-scraper')
    firehose_flag = take(args, '--firehose', '--no-firehose')
    scores_flag = take(args, '--scores', '--no-scores')

    dral_flag = take(args, '--admin-without-login', default=False)

    # If there are no "service enabled" flags, then all services are enabled by default
    service_default = (
        webserver_flag is not True
        and scraper_flag is not True
        and firehose_flag is not True
        and scores_flag is not True
    )

    webserver = defaulting(webserver_flag, service_default)
    scraper = defaulting(scraper_flag, service_default)
    firehose = defaulting(firehose_flag, service_default)
    scores = defaulting(scores_flag, service_default)

    forever = (
        forever_flag
        or webserver
        or firehose
    )

    if args != []:
        print('Unknown arguments:', ' '.join(args))
        return 1

    return Args(
        webserver=webserver,
        scraper=scraper,
        firehose=firehose,
        scores=scores,
        log_db_queries=log_db_queries,
        admin_panel=admin_panel,
        forever=forever,
        dont_require_admin_login=dral_flag
    )