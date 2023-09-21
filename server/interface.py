from server import html
from server.html import Node, head, img, div, h3, h4, p, span, a, UnescapedString
import re
from typing import List, Tuple, Union, TypeVar, Optional, Callable
from prisma.models import Post, Actor
from server.util import interleave
from server.metrics import FeedMetrics, FeedMetricsSlice

T = TypeVar("T")


_navbar = [
    a(href="/")("home"),
    a(href="/stats")("stats"),
    a(href="/feed")("feeds"),
    a(href="/pinned_posts")("pins"),
    a(href="/user/puppyfox.bsky.social")("me"),
    a(href="/quickflag")("quickflag"),
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


def post(enable_admin_controls: bool, post_: Post) -> Node:
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
    )
    images = div(
        *[
            a(href=url, target="_blank")(img(src=url, width="100px", height="80px"))
            for url in [post_.m0, post_.m1, post_.m2, post_.m3]
            if url is not None
        ]
    )
    return div(mainline, images, class_="post")


def feeds_page(names: List[str]) -> Node:
    ls = [p(a(href=f"/feed/{i}")(i)) for i in names]
    return wrap_body("Fox Feed - Feeds", h3("Feeds"), *ls)


def feed_page(
    enable_admin_controls: bool, feed_name: str, full_posts: List[Post]
) -> Node:
    return wrap_body(
        f"Fox Feed - Feeds - {feed_name}",
        h3(feed_name),
        a(href=f"/feed/{feed_name}/stats")(p("stats")),
        *[post(enable_admin_controls, i) for i in full_posts],
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
