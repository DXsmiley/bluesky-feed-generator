from typing import List, Dict, Union, Iterator, TypedDict
from html import escape


class UnescapedString:
    def __init__(self, string: str):
        self.string = string


class Node:
    def __init__(
        self,
        tag: str,
        children: List[Union["Node", str, UnescapedString, None]],
        attrs: Dict[str, Union[str, UnescapedString, bool]],
    ):
        self.tag = tag
        self.children = children
        self.attrs = attrs

    def __call__(
        self,
        *children: Union["Node", str, UnescapedString, None],
        **attrs: Union[str, UnescapedString, bool],
    ) -> "Node":
        return Node(self.tag, self.children + list(children), {**self.attrs, **attrs})

    def render(self) -> Iterator[str]:
        yield "<" + self.tag
        for k, v in self.attrs.items():
            if isinstance(v, bool):
                if v:
                    yield f' {k.strip("_")} '
            elif isinstance(v, UnescapedString):
                yield f' {k.rstrip("_")}="{v.string}" '
            else:
                yield f' {k.rstrip("_")}="{escape(v)}" '
        yield ">"
        for c in self.children:
            if isinstance(c, Node):
                yield from c.render()
            if isinstance(c, str):
                yield escape(c)
            if isinstance(c, UnescapedString):
                yield c.string
        yield "</" + self.tag + ">"

    def __str__(self) -> str:
        return "".join(self.render())


html = Node("html", [], {})
body = Node("body", [], {})
head = Node("head", [], {})

div = Node("div", [], {})
img = Node("img", [], {})
p = Node("p", [], {})
a = Node("a", [], {})
h1 = Node("h1", [], {})
h2 = Node("h2", [], {})
h3 = Node("h3", [], {})
h4 = Node("h4", [], {})
br = UnescapedString("<br>")
button = Node("button", [], {})
span = Node("span", [], {})
form = Node("form", [], {})
input_ = Node("input", [], {})
textarea = Node("textarea", [], {})
label = Node("label", [], {})


class RadioButtonDef(TypedDict):
    value: str
    label: str


def radio_button_set(name: str, buttons: List[RadioButtonDef]) -> List[Union[Node, UnescapedString]]:
    return [
        j
        for i in buttons
        for j in
        [
            input_(type_='radio', name=name, value=i['value']),
            label(i['label']),
            br,
        ]
    ]



def style(css: str) -> Node:
    return Node("style", [UnescapedString(css)], {})


def script(js: str) -> Node:
    return Node("script", [UnescapedString(js)], {})
