from foxfeed.web import html
from foxfeed.web.html import Node, head, img, div, h3, h4, p, span, a, UnescapedString
import re
from typing import List, Tuple, Union, TypeVar, Optional, Callable
from prisma.models import Post, Actor, ScheduledPost
from foxfeed.util import interleave, groupby
from foxfeed.metrics import FeedMetrics, FeedMetricsSlice
from foxfeed import image


T = TypeVar("T")


_navbar = [
    a(href="/")("home"),
    a(href="/stats")("stats"),
    a(href="/feed")("feeds"),
    a(href="/pinned_posts")("pins"),
    a(href="/user/puppyfox.bsky.social")("me"),
    a(href="/quickflag")("quickflag"),
    a(href="/schedule")("schedule"),
]

navbar = div(*interleave(" | ", _navbar))


def wrap_body(title: str, *n: Union[Node, None]) -> Node:
    return html.html(
        head(
            Node("title", [title], {}),
            Node("link", [], {"rel": "stylesheet", "href": "/static/admin-style.css"}),
            Node("script", [], {"src": "/static/admin-script.js"}),
        ),
        html.body(
            div(class_="toastbox", id_="toastbox"), div(class_="body")(navbar, *n)
        ),
    )


# TODO: I *really* don't like how this works, I think it's awful and I hate it
def toggle_foxfeed(
    enabled: bool, handle: str, did: str, current_value: Optional[bool]
) -> Node:
    return span(
        *(
            html.button(
                name,
                class_="togglestrip" + (" selected" if current_value is pyvalue else ""),
                id_=f"{handle}-ff-{jsvalue}",
                disabled=not enabled,
                onclick=UnescapedString(
                    f"set_include_in_fox_feed('{handle}', '{did}', {jsvalue})"
                ),
            )
            for (name, pyvalue, jsvalue) in [
                ("Exclude", False, 'false'),
                ("Shrug", None, 'null'),
                ("Include", True, 'true'),
            ]
        )
    )


def toggle_vixfeed(
    enabled: bool, handle: str, did: str, current_value: Optional[bool]
) -> Node:
    return span(
        *(
            html.button(
                name,
                class_="togglestrip" + (" selected" if current_value is pyvalue else ""),
                id_=f"{handle}-vf-{jsvalue}",
                disabled=not enabled,
                onclick=UnescapedString(
                    f"set_include_in_vix_feed('{handle}', '{did}', {jsvalue})"
                ),
            )
            for (name, pyvalue, jsvalue) in [
                ("Exclude", False, 'false'),
                ("Shrug", None, 'null'),
                ("Include", True, 'true'),
            ]
        )
    )


def toggle_post_pin(
    uri: str, current_value: bool
) -> Node:
    return span(
        *(
            html.button(
                name,
                class_="togglestrip" + (" selected" if current_value is pyvalue else ""),
                id_=f"{uri}-pinned-{jsvalue}",
                # disabled=not enabled,
                onclick=UnescapedString(
                    f"set_post_pinned('{uri}', {jsvalue})"
                ),
            )
            for (name, pyvalue, jsvalue) in [
                ("-", False, 'false'),
                ("Pin", True, 'true'),
            ]
        )
    )


def post(enable_admin_controls: bool, post_: Post, quote: Optional[Post] = None, colour: str = 'white') -> Node:
    text = re.sub(r"\n+", " • ", post_.text, re.MULTILINE)
    profile_image = (
        img(src=post_.author.avatar, width="30px", height="30px", class_="profile")
        if post_.author and post_.author.avatar else None
    )
    scan_button = (
        html.button("scan", onclick=UnescapedString(f"scan_likes('{post_.uri}')"))
        if enable_admin_controls else None
    )
    toggle_pin_buttons = (
        toggle_post_pin(post_.uri, post_.is_pinned) if enable_admin_controls
        else '📌' if post_.is_pinned
        else None
    )
    author_name = (
        a(post_.author.handle, href="/user/" + post_.author.handle)
        if post_.author else None
    )
    mainline = p(
        profile_image,
        scan_button,
        toggle_pin_buttons,
        author_name,
        " [" + " ".join(post_.labels) + "] ",
        text,
        style=f'background-color: {colour};'
    )
    images = div(
        *[
            a(href=url, target="_blank")(img(src=url, width="100px", height="80px"))
            for url in [post_.m0, post_.m1, post_.m2, post_.m3]
            if url is not None
        ]
    )
    qq = (
        None if quote is None else
        div(
            post(False, quote),
            style='padding-left: 20px; border-left: 4px solid lightgrey;'
        )
    )
    return div(mainline, images, qq, class_="post")


def feeds_page(names: List[str]) -> Node:
    ls = [p(a(href=f"/feed/{i}")(i)) for i in names]
    return wrap_body("Fox Feed - Feeds", h3("Feeds"), *ls)


def feed_page(
    enable_admin_controls: bool, feed_name: str, full_posts: List[Tuple[Optional[Post], Optional[Post]]], next_cursor: Optional[str]
) -> Node:
    return wrap_body(
        f"Fox Feed - Feeds - {feed_name}",
        h3(feed_name),
        a(href=f"/feed/{feed_name}/stats")(p("stats")),
        *[
            p('(post was deleted)') if i is None
            else post(enable_admin_controls, i, q)
            for i, q in full_posts
        ],
        a(href=f"/feed/{feed_name}?cursor={next_cursor}")(p("next page")) if next_cursor else None,
    )


def feed_timetravel_page(
    cols: List[List[Optional[Post]]]
) -> Node:
    just_posts = [j for i in cols for j in i if j is not None]
    repeat_posters = [
        k
        for (k, v) in groupby(lambda p: p.authorId, just_posts).items()
        if len(v) > 1
    ]
    colours = {
        k: f'hsl({255 * i // len(repeat_posters):d}, 60%, 90%)'
        for i, k in enumerate(repeat_posters)
    }
    return html.html(
        html.head(
            Node("link", [], {"rel": "stylesheet", "href": "/static/admin-style.css"}),
        ),
        html.body(
            div(class_="timetravel-container")(
                *[
                    div(class_="timetravel-column")(
                        *[
                            p('(post was deleted)') if i is None
                            else post(False, i, colour=colours.get(i.authorId, 'white'))
                            for i in c
                        ]
                    )
                    for c in cols
                ]
            )
        )
    )


def post_list_page(
    enable_admin_controls: bool, name: str, full_posts: List[Post]
) -> Node:
    return wrap_body(
        "Fox Feed - {name}",
        h3(name),
        *[post(enable_admin_controls, i) for i in full_posts],
    )


def feed_metric_row(
    name: str,
    metrics: FeedMetrics,
    value: Callable[[FeedMetricsSlice], Union[int, float]],
) -> Node:
    values = [(i.start, value(i)) for i in metrics.timesliced]
    maximum = max([v for _, v in values] + [1])
    return div(
        h4(name),
        div(class_="column-graph-row")(
            *[
                div(
                    class_="column-graph-bar",
                    id_=f"col-{metrics.feed_name}-{hash(name)}-{i}",
                )
                for i, (_, _) in enumerate(values)
            ]
        ),
        *[
            html.style(
                f"""
                #col-{metrics.feed_name}-{hash(name)}-{i} {{
                    height: {0 if v == 0 else max(2, int(40 * v / maximum))}px;
                }}
                #col-{metrics.feed_name}-{hash(name)}-{i}:hover::after {{
                    content: "{v} - {s}";
                }}
                """
            )
            for i, (s, v) in enumerate(values)
        ],
    )


def feed_metrics_page(metrics: FeedMetrics) -> Node:
    return wrap_body(
        "Fox Feed - Metrics",
        h3(metrics.feed_name),
        feed_metric_row("attributed likes", metrics, lambda x: x.attributed_likes),
        feed_metric_row("requests", metrics, lambda x: x.num_requests),
        feed_metric_row("posts served", metrics, lambda x: x.posts_served),
        feed_metric_row("unique viewers", metrics, lambda x: x.unique_viewers),
    )


def stats_page(stats: List[Tuple[str, int]], metrics: FeedMetrics) -> Node:
    ls = [p(f"{n} {s}") for n, s in stats]
    return wrap_body(
        "Fox Feed - Stats",
        h3("stats"),
        *ls,
        h3("accumulated feed metrics"),
        feed_metric_row("attributed likes", metrics, lambda x: x.attributed_likes),
        feed_metric_row("requests", metrics, lambda x: x.num_requests),
        feed_metric_row("posts served", metrics, lambda x: x.posts_served),
        feed_metric_row("unique viewers", metrics, lambda x: x.unique_viewers),
    )


def user_main(enable_admin_controls: bool, user: Actor, posts: List[Post]) -> Node:
    hline = [
        a("☁️", href="https://bsky.app/profile/" + user.handle, target="_blank"),
        "🚩" if user.flagged_for_manual_review and enable_admin_controls else None,
        f"{user.displayName} ({user.handle})" if user.displayName else user.handle,
    ]
    admin_controls = [
        p(
            span(class_="marker" if not user.autolabel_fem_vibes else "marker pink"),
            span(class_="marker" if not user.autolabel_nb_vibes else "marker yellow"),
            span(class_="marker" if not user.autolabel_masc_vibes else "marker blue"),
            span("Verified", class_="pill") if user.is_furrylist_verified else None,
            span("Muted", class_="pill") if user.is_muted else None,
            span("External", class_="pill") if user.is_external_to_network else None,
        ),
        p(
            f"In fox feed: ",
            toggle_foxfeed(
                enable_admin_controls,
                user.handle,
                user.did,
                user.manual_include_in_fox_feed,
            ),
        ),
        p(
            f"In vix feed: ",
            toggle_vixfeed(
                enable_admin_controls,
                user.handle,
                user.did,
                user.manual_include_in_vix_feed,
            ),
        )
    ]
    return div(
        h3(*interleave(" • ", [i for i in hline if i is not None])),
        (
            img(src=user.avatar, width="150px", height="150px", class_="profile")
            if user.avatar
            else None
        ),
        p(user.description),
        p(f'{user.following_count} following, ', Node('b', [str(user.follower_count)], {}), ' followers'),
        *(admin_controls if enable_admin_controls else []),
        h3(f"{len(posts)} posts") if posts else None,
        *[post(enable_admin_controls, i) for i in posts],
    )


def user_page(enable_admin_controls: bool, user: Actor, posts: List[Post]) -> Node:
    return wrap_body(
        f"Fox Feed - User - {user.handle}",
        user_main(enable_admin_controls, user, posts)
    )


def media_experiment_page(name: str, media: List[Tuple[float, str, Optional[str]]]):
    return wrap_body(
        f"Fox Feed - Experiment Results - {name}",
        h3("Experiment results for ", name),
        div(
            *[
                div(
                    a(href=(url or ''), target="_blank")(img(src=(url or ''), width="100px", height="80px")),
                    p(f'{score:.3f} : {comment}')
                )
                for score, comment, url in media
            ],
            style='display: flex; flex-wrap: wrap;'
        )
    )


def quickflag_page(enable_admin_controls: bool, users: List[Actor]) -> Node:
    return wrap_body(
        "Fox Feed - Quickflag",
        h3("Quickflag"),
        *[user_main(enable_admin_controls, i, i.posts or []) for i in users],
        h3("(end)"),
        a("refresh page for more users", href="/quickflag"),
    )


def data_to_small_dataurl(b: bytes) -> str:
    img = image.from_bytes(b)
    img = image.scale_down(img, 96)
    return image.to_dataurl(img)


def scheduled_posts_page(posts: List[ScheduledPost]) -> Node:
    return wrap_body(
        "Fox Feed - Scheduled Posts",
        h3("New Post"),
        html.form(action="/schedule", method='POST', enctype="multipart/form-data")(
            # html.input_(type=''),
            p('Text'),
            html.textarea(name='text'),
            html.br,
            p('Image alt-text'),
            html.textarea(name='alt-text'),
            html.br,
            html.br,
            html.input_(type_='file', name='image',  accept="image/png, image/jpeg"),
            html.br,
            html.br,
            *html.radio_button_set(
                'maturity',
                [
                    {'label': 'General', 'value': 'none'},
                    {'label': 'Nudity', 'value': 'nudity'},
                    {'label': 'Sexual', 'value': 'sexual'},
                    {'label': 'Porn', 'value': 'porn'},
                ]
            ),
            html.br,
            html.input_(type_='submit'),
        ),
        h3("Scheduled Posts"),
        *[
            div(
                p(
                    html.button("cancel", onclick=UnescapedString(f'cancel_post({i.id})')),
                    span(i.status, class_="pill"),
                    span(i.label, class_="pill") if i.label is not None else None,
                    ' ' + re.sub(r"\n+", " • ", i.text, re.MULTILINE),
                ),
                *[
                    div(
                        img(src=data_to_small_dataurl(m.data.decode())),
                        p(m.alt_text)
                    )
                    for m in (i.media or [])
                ]
            )
            for i in posts
        ]
    )


admin_login_page = (
    wrap_body(
        "Fox Feed - Admin Login",
        h3("Admin Login"),
        Node("form", [], {"method": "post"})(
            div(
                Node(
                    "input",
                    [],
                    {"type": "password", "name": "password", "required": "1"},
                )
            ),
            div(Node("button", ["Login"], {"type": "submit"})),
        ),
    )
)


admin_login_page_disabled = (
    wrap_body(
        "Fox Feed - Admin Login",
        h3("Admin Login"),
        p("Admin tools are currently disabled"),
    )
)


def admin_done_login_page() -> Node:
    return wrap_body(
        "Fox Feed - Admin Login",
        h3("Logged in"),
        p(":)")
    )
