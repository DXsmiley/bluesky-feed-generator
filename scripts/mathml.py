# mathml sucks so we express equations in a form that's nicer

from typing import Union, List, Tuple

NodeType = Union['Node', int, float, str, None]

class Node:
    def __init__(self, s: NodeType):
        if isinstance(s, Node):
            self.s = s.s
        elif isinstance(s, str):
            self.s = s
        elif isinstance(s, int):
            self.s = f'<mn>{s}</mn>'
        elif isinstance(s, float):
            self.s = f'<mn>{s}</mn>'
        else:
            self.s = '???'
    def __str__(self) -> str: return self.s
    def __add__(self, other: 'NodeType') -> 'Node': return Node(f'{self}<mo>+</mo>{Node(other)}')
    def __radd__(self, other: 'NodeType') -> 'Node': return Node(f'{Node(other)}<mo>+</mo>{self}')
    def __sub__(self, other: 'NodeType') -> 'Node': return Node(f'{self}<mo>-</mo>{Node(other)}')
    def __rsub__(self, other: 'NodeType') -> 'Node': return Node(f'{Node(other)}<mo>-</mo>{self}')
    def __mul__(self, other: 'NodeType') -> 'Node': return Node(f'{self}<mo>∙</mo>{Node(other)}')
    def __rmul__(self, other: 'NodeType') -> 'Node': return Node(f'{Node(other)}<mo>∙</mo>{self}')
    def __lt__(self, other: 'NodeType') -> 'Node': return Node(f'{self}<mo>&lt;</mo>{Node(other)}')
    def __ge__(self, other: 'NodeType') -> 'Node': return Node(f'{self}<mo>&geq;</mo>{Node(other)}')
    def __pow__(self, other: 'NodeType') -> 'Node': return Node(f'<msup>{self}{Node(other)}</msup>')
    def __truediv__(self, other: 'NodeType') -> 'Node': return Node(f'<mfrac>{self}{Node(other)}</mfrac>')
    def __rtruediv__(self, other: 'NodeType') -> 'Node': return Node(f'<mfrac>{Node(other)}{self}</mfrac>')
    def __eq__(self, other: 'NodeType') -> 'Node': return Node(f'{self}<mo>=</mo>{Node(other)}')
    def __call__(self, other: 'NodeType') -> 'Node': return Node(f'<mrow>{self}<mo>(</mo><mrow>{Node(other)}</mrow><mo>)</mo></mrow>')

def i_(s: str) -> Node:
    return Node(f'<mi>{s}</mi>')

def p_(s: NodeType) -> Node:
    return Node(f'<mrow><mo>(</mo><mrow>{Node(s)}</mrow><mo>)</mo></mrow>')

def if_(cs: List[Tuple[NodeType, NodeType]]) -> Node:
    rs = ''.join(
        f'<mtr><mtd>{Node(expr)}</mtd><mtd><mtext>if</mtext><mo> </mo>{Node(cond)}</mtd></mtr>'
        for expr, cond in cs
    )
    return Node(f'<mrow><mo>&#123;</mo><mrow><mtable>{rs}</mtable></mrow></mrow>')

def m_(s: NodeType):
    print(f'<math>{s}</math>')

alpha = i_('α')
beta = i_('β')
gamma = i_('γ')
atan = i_('tan') ** -1
T = i_('T')
L = i_('L')
F = i_('F')
x = i_('x')
y = i_('y')

m_(x == T / beta)

m_(y == p_(L ** gamma + 5) * p_(0.7 + -0.1 *  atan(F / 800)) * if_([
    (1 / (x ** alpha), x >= 1),
    (2 - 1 / (p_(2 - x) ** alpha), x < 1),
]))
