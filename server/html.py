from typing import List, Dict, Union, Iterator
from html import escape


class UnescapedString:
    def __init__(self, string: str):
        self.string = string


class Node:
    def __init__(self, tag: str, children: List[Union['Node', str, UnescapedString, None]], attrs: Dict[str, str]):
        self.tag = tag
        self.children = children
        self.attrs = attrs

    def __call__(self, *children: Union['Node', str, UnescapedString, None], **attrs: str) -> 'Node':
        return Node(self.tag, self.children + list(children), {**self.attrs, **attrs})

    def render(self) -> Iterator[str]:
        yield '<' + self.tag
        for k, v in self.attrs.items():
            yield f' {k.rstrip("_")}="{escape(v)}" '
        yield '>'
        for c in self.children:
            if isinstance(c, Node):
                yield from c.render()
            if isinstance(c, str):
                yield escape(c)
            if isinstance(c, UnescapedString):
                yield c.string
        yield '</' + self.tag + '>'

    def __str__(self) -> str:
        return ''.join(self.render())


html = Node('html', [], {})
body = Node('body', [], {})
head = Node('head', [], {})
div = Node('div', [], {})
img = Node('img', [], {})
p = Node('p', [], {})
a = Node('a', [], {})
h1 = Node('h1', [], {})
h2 = Node('h2', [], {})
h3 = Node('h3', [], {})
h4 = Node('h4', [], {})
br = UnescapedString('<br>')

def style(css: str) -> Node:
    return Node('style', [UnescapedString(css)], {})