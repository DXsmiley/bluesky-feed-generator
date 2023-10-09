from PIL import Image as pil
import io
import base64
from typing import Literal

Image = pil.Image

open = pil.open


def from_bytes(b: bytes) -> Image:
    return pil.open(io.BytesIO(b))


def scale_down(img: Image, max_sidelength: int) -> Image:
    w, h = img.size
    scale_factor = max_sidelength / max(w, h)
    if scale_factor < 1:
        img = img.resize((int(w * scale_factor), int(h * scale_factor)))
    return img


def to_bytes(img: Image, format: Literal['PNG']) -> bytes:
    bts = io.BytesIO()
    img.save(bts, format=format)
    return bts.getvalue()


def to_dataurl(img: Image) -> str:
    bts = io.BytesIO()
    img.save(bts, format='PNG')
    b64 = base64.b64encode(bts.getvalue()).decode('utf-8')
    return f'data:image/png;base64,{b64}'
