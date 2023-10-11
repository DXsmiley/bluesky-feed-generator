import re
from typing import Iterator

OUTPUT = './foxfeed/gen/db.py'

INPUT = [
    ('score_posts', 'score_posts.sql', 'ScorePostsOutputModel'),
    ('score_by_interactions', 'score_by_interaction.sql', 'ScoreByInteractionOutputModel'),
]

HEADDER = '''
# This is kinda weird and really bad sorry

from datetime import datetime
import foxfeed.database
from typing import List, Union

Arg = Union[str, int, float, bool, datetime]

def escape(a: Arg) -> str:
    if isinstance(a, bool):
        return 'TRUE' if a else 'FALSE'
    if isinstance(a, str):
        assert "'" not in a
        return "'" + a + "'"
    if isinstance(a, int):
        return str(a)
    if isinstance(a, float):
        return str(a)
    if isinstance(a, datetime):
        return "'" + a.isoformat().split('.')[0] + "'::timestamp"
'''

from datetime import datetime

d = datetime.now()
d.isoformat

def codegen_for_query(function_name: str, output_model: str, sql: str) -> Iterator[str]:
    arguments = [i[1:] for i in re.findall(r'(?::)\w+', sql)]
    # Put the sql in a big global variable
    yield f'{function_name}_sql_query = """'
    yield re.sub(r':\w+', lambda m: f'{{{m[0][1:]}}}', sql)
    yield '"""'
    yield ''
    # Function signature
    yield f'async def {function_name}('
    yield '    db: foxfeed.database.Database,'
    yield '    *,'
    for i in sorted(set(arguments)):
        yield f'    {i}: Arg,'
    yield f') -> List[foxfeed.database.{output_model}]:'
    # Function body
    yield f'    query = {function_name}_sql_query.format('
    for i in sorted(set(arguments)):
        yield f'        {i} = escape({i}),'
    yield '    )'
    yield f'    result = await db.query_raw(query, model=foxfeed.database.{output_model}) # type: ignore'
    yield '    return result'
    yield ''


def codegen() -> Iterator[str]:
    yield HEADDER
    for function_name, filename, output_model in INPUT:
        with open(filename) as f:
            yield from codegen_for_query(function_name, output_model, f.read())


if __name__ == '__main__':
    code = '\n'.join(codegen())
    with open(OUTPUT, 'w') as f:
        f.write(code)

