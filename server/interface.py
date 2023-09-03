from server import html
from server.html import Node, head, style, img, div, h3, p, span, a, UnescapedString
import re
from typing import List, Tuple, Union, TypeVar, Optional
from prisma.models import Post, Actor
from server.util import interleave


T = TypeVar('T')


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
            Node('link', [], {'rel': 'stylesheet', 'href': '/static/admin-style.css'}),
            Node('script', [], {'src': '/static/admin-script.js'})
        ),
        html.body(
            div(class_='toastbox', id_='toastbox'),
            div(class_='body')(navbar, *n)
        )
    )


# TODO: I *really* don't like how this works, I think it's awful and I hate it
def toggle_foxfeed(handle: str, did: str, current_value: Optional[bool]) -> Node:
    return Node('span', [
        html.button(
            "Exclude",
            class_='togglestrip' + (' selected' if current_value is False else ''),
            id_=f'{handle}-ff-false',
            onclick=UnescapedString(f"set_include_in_fox_feed('{handle}', '{did}', false)")
        ),
        html.button(
            "Shrug",
            class_='togglestrip' + (' selected' if current_value is None else ''),
            id_=f'{handle}-ff-null',
            onclick=UnescapedString(f"set_include_in_fox_feed('{handle}', '{did}', null)")
        ),
        html.button(
            "Include",
            class_='togglestrip' + (' selected' if current_value is True else ''),
            id_=f'{handle}-ff-true',
            onclick=UnescapedString(f"set_include_in_fox_feed('{handle}', '{did}', true)")
        )
    ], {})


def toggle_vixfeed(handle: str, did: str, current_value: Optional[bool]) -> Node:
    return Node('span', [
        html.button(
            "Exclude",
            class_='togglestrip' + (' selected' if current_value is False else ''),
            id_=f'{handle}-vf-false',
            onclick=UnescapedString(f"set_include_in_vix_feed('{handle}', '{did}', false)")
        ),
        html.button(
            "Shrug",
            class_='togglestrip' + (' selected' if current_value is None else ''),
            id_=f'{handle}-vf-null',
            onclick=UnescapedString(f"set_include_in_vix_feed('{handle}', '{did}', null)")
        ),
        html.button(
            "Include",
            class_='togglestrip' + (' selected' if current_value is True else ''),
            id_=f'{handle}-vf-true',
            onclick=UnescapedString(f"set_include_in_vix_feed('{handle}', '{did}', true)")
        )
    ], {})


def post(post_: Post) -> Node:
    text = re.sub(r'\n+', ' • ', post_.text, re.MULTILINE)
    mainline = p(
        img(src=post_.author.avatar, width="30px", height="30px", class_="profile") if post_.author and post_.author.avatar else None,
        html.button('scan', onclick=UnescapedString(f"scan_likes('{post_.uri}')")),
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
        p(
            span(class_='marker' if not user.autolabel_fem_vibes else 'marker pink'),
            span(class_='marker' if not user.autolabel_nb_vibes else 'marker yellow'),
            span(class_='marker' if not user.autolabel_masc_vibes else 'marker blue'),
            span('Verified', class_='pill') if user.is_furrylist_verified else None,
            span('Muted', class_='pill') if user.is_muted else None,
        ),
        p(f'In fox feed: ', toggle_foxfeed(user.handle, user.did, user.manual_include_in_fox_feed)),
        p(f'In vix feed: ', toggle_vixfeed(user.handle, user.did, user.manual_include_in_vix_feed)),
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


def admin_login_page() -> Node:
    return wrap_body(
        h3('Admin Login'),
        Node('form', [], {'method': 'post'})(
            div(Node('input', [], {'type': 'password', 'name': 'password', 'required': '1'})),
            div(Node('button', ['Login'], {'type': 'submit'}))
        )
    )


def admin_done_login_page() -> Node:
    return wrap_body(h3('Logged in'), p(':)'))
