"""Telecharge une image et la redimensionne (ratio conserve) dans OUTPUT_DIR."""

from io import BytesIO
from pathlib import Path

import httpx
from PIL import Image, UnidentifiedImageError

from shared.payloads import ResizeImagePayload
from worker.tasks import PermanentError, TaskContext

MAX_BYTES = 15 * 1024 * 1024


def _download(url: str) -> bytes:
    with httpx.Client(timeout=15, follow_redirects=True) as client, client.stream("GET", url) as resp:
        if 400 <= resp.status_code < 500:
            # 404, 403... reessayer ne changera rien.
            raise PermanentError(f"HTTP {resp.status_code} sur {url}")
        resp.raise_for_status()  # 5xx -> erreur transitoire -> retry
        data = bytearray()
        for chunk in resp.iter_bytes():
            data.extend(chunk)
            if len(data) > MAX_BYTES:
                raise PermanentError("image trop volumineuse (> 15 Mo)")
        return bytes(data)


def handle(payload: ResizeImagePayload, ctx: TaskContext) -> dict:
    raw = _download(str(payload.url))
    try:
        img = Image.open(BytesIO(raw))
        img.load()
    except (UnidentifiedImageError, OSError) as exc:
        raise PermanentError(f"fichier non reconnu comme image: {exc}") from exc

    original = img.size
    img.thumbnail((payload.width, payload.height))
    out_dir = Path(ctx.settings.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    # Nom = id de tache : un retry reecrit le meme fichier (idempotent).
    filename = f"{ctx.task_id}.png"
    img.save(out_dir / filename, format="PNG")
    return {"output": f"/outputs/{filename}", "original_size": list(original), "size": list(img.size)}
