# Database logs are kinda hard to read so we pipe them through this to be nicer

import json
from termcolor import cprint

while True:
    line = input()
    try:
        blob = json.loads(line)
        query = blob.get('fields', {}).get('query', None)
        if query:
            cprint(query, 'cyan')
        print(json.dumps(blob, indent=4))
    except:
        print(line, end='')
