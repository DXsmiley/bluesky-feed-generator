from server.database import Actor, db
import json
import re
from typing import Any, Optional


def is_girl(description: Optional[str]) -> bool:
    if description is None:
        return False
    desc = description.replace('\n', ' ').lower()
    # he/him results in False (to catch cases of he/she/they)
    if re.search(r'\bhe\b', desc):
        return False
    if re.search(r'\bhim\b', desc):
        return False
    # she/her pronouns
    if re.search(r'\bshe\b', desc):
        return True
    if re.search(r'\bher\b', desc):
        return True
    # Emoji
    if '♀️' in desc or '⚢' in desc:
        return True
    # look for cases of "25F" or something similar
    if re.search(r'\b(\d\d)?f\b', desc):
        return True
    # they/them intentionally not considered,
    # but if we've seen nothing by now we bail
    return False


def load():
    with open('known_furries.json') as f:
        blob = json.load(f)

    db.post.delete_many()
    db.actor.delete_many()

    for x, i in enumerate(blob['furries']):
        if x % 100 == 0:
            print(f'Loading furries into DB: {x} / {len(blob["furries"])}')
        db.actor.create(
            data={
                'did': i['did'],
                'handle': i['handle'],
                'description': i['description'],
                'displayName': i['displayName'],
                'in_fox_feed': True,
                'in_vix_feed': is_girl(i['description']),
            }
        )

    print('\ndone loading furries')


if __name__ == '__main__':
    load()
    
