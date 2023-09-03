from server import html
from server.html import Node, head, style, img, div, h3, p, a, UnescapedString
import re
from typing import List, Tuple, Union
from prisma.models import Post, Actor
from server.util import interleave


base_css = '''
body {
    margin: 0px;
    text-align: justify;
    font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;
}

.body {
    max-width: 800px;
    margin: 40px auto;
}

button + button {
    margin-left: 5px;
}

img {
    object-fit: contain;
}

.profile {
    float: right;
    border-radius: 50%;
    margin-left: 10px;
}

.toast {
    border: 2px solid black;
    margin: 10px;
    padding: 5px;
    box-shadow: 5px 5px #888888;
}

.toastbox {
    position: fixed;
}
'''


base_script = '''
function toast(string) {
    let toast = document.createElement('div');
    toast.setAttribute('class', 'toast');
    toast.textContent = string;
    let box = document.getElementById('toastbox');
    box.appendChild(toast);
    window.setTimeout((() => box.removeChild(toast)), 10000);
}

async function mark(did, in_fox_feed, in_vix_feed, gender) {
    const data = {
        'did': did,
        'in_fox_feed': in_fox_feed,
        'in_vix_feed': in_vix_feed,
        'gender': gender,
    };
    const response = await fetch(
        '/admin/mark',
        {
            method: 'POST',
            cache: 'no-cache',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data)
        }
    );
    const text = await response.text();
    toast(`${response.status} ${response.statusText} - ${text}`);
}

async function boost(uri) {
    const data = {
        'uri': uri
    };
    const response = await fetch(
        '/admin/boost',
        {
            method: 'POST',
            cache: 'no-cache',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(data)
        }
    );
    const text = await response.text();
    toast(`${response.status} ${response.statusText} - ${text}`);
}
'''

_navbar = [
    a(href='/')('home'),
    a(href='/stats')('stats'),
    a(href='/feed')('feeds'),
    a(href='/quickflag')('quickflag'),
]

navbar = div(*interleave(' | ', _navbar))


def wrap_body(*n: Union[Node, None]) -> Node:
    return html.html(
        head(
            style(base_css),
            html.script(base_script),
        ),
        html.body(
            div(class_='toastbox', id_='toastbox'),
            div(class_='body')(navbar, *n)
        )
    )


def post(post_: Post) -> Node:
    text = re.sub(r'\n+', ' • ', post_.text, re.MULTILINE)
    mainline = p(
        img(src=post_.author.avatar, width="30px", height="30px", class_="profile") if post_.author and post_.author.avatar else None,
        # html.button('boost', onclick=UnescapedString(f"boost('{post_.uri}')")),
        ' ',
        "?" if not post_.author else a(post_.author.handle, href='/user/' + post_.author.handle),
        ' - ',
        '[' + ' '.join(post_.labels) + ']',
        ' - ',
        text
    )
    images = div(*[a(href=url, target="_blank")(img(src=url, width='100px', height='80px')) for url in [post_.m0, post_.m1, post_.m2, post_.m3] if url is not None])
    return div(mainline, images, class_='post')


def feeds_page(names: List[str]) -> Node:
    ls = [p(a(href=f'/feed/{i}')(i)) for i in names]
    return wrap_body(h3('feeds'), *ls)


def feed_page(feed_name: str, full_posts: List[Post]) -> Node:
    return wrap_body(h3(feed_name), *[post(i) for i in full_posts])


def stats_page(stats: List[Tuple[str, int]]) -> Node:
    ls = [p(f'{n} {s}') for n, s in stats]
    return wrap_body(h3('stats'), *ls)


def user_controls(did: str) -> Node:
    return div(
        # these are kind the only categories we "care about" right now
        html.button('mark as nonfurry', onclick=UnescapedString(f"mark('{did}', false, false, 'non-furry')")),
        html.button('mark as furry', onclick=UnescapedString(f"mark('{did}', true, false, 'unknown')")),
        html.button('mark as furry girl', onclick=UnescapedString(f"mark('{did}', true, true, 'girl')")),
    )


def user_main(user: Actor, posts: List[Post]) -> Node:
    hline = [
        a('☁️', href='https://bsky.app/profile/' + user.handle, target="_blank"),
        '🚩' if user.flagged_for_manual_review else None,
        f'{user.displayName} ({user.handle})' if user.displayName else user.handle
    ]
    return div(
        h3(*interleave(' • ', [i for i in hline if i is not None])),
        (img(src=user.avatar, width="150px", height="150px", class_="profile") if user.avatar else None),
        p(user.description),
        # user_controls(user.did),
        p(f'Muted: {user.is_muted}'),
        p(f'Furrylist verified: {user.is_furrylist_verified}'),
        p(f'In fox feed: {user.manual_include_in_fox_feed}'),
        p(f'In vix feed: {user.manual_include_in_vix_feed}'),
        h3(f'{len(posts)} posts') if posts else None,
        *[post(i) for i in posts]
    )

def user_page(user: Actor, posts: List[Post]) -> Node:
    return wrap_body(user_main(user, posts))


def quickflag_page(users: List[Actor]) -> Node:
    return wrap_body(
        h3('Quickflag'),
        *[user_main(i, i.posts or []) for i in users],
        h3('(end)'),
        a('refresh page for more users', href='/quickflag')
    )
