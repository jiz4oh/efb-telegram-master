# coding=utf-8

import logging
import queue
import shutil
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

from PIL import Image

from .db import DatabaseManager

logger = logging.getLogger(__name__)


def _phash_hex(path: str, hash_size: int = 8, highfreq_factor: int = 4) -> Tuple[str, int, int, int]:
    img = Image.open(path).convert("L")
    w, h = img.size
    img = img.resize((hash_size * highfreq_factor, hash_size * highfreq_factor), Image.Resampling.LANCZOS)
    pixels = list(img.getdata())
    size = hash_size * highfreq_factor

    # 2D DCT (naive, but small matrix: 32x32 by default)
    import math
    coeff = [[0.0 for _ in range(size)] for _ in range(size)]
    c = [math.sqrt(1.0 / size)] + [math.sqrt(2.0 / size) for _ in range(1, size)]

    for u in range(size):
        for v in range(size):
            s = 0.0
            for x in range(size):
                for y in range(size):
                    p = pixels[x * size + y]
                    s += p * math.cos(((2 * x + 1) * u * math.pi) / (2 * size)) * math.cos(((2 * y + 1) * v * math.pi) / (2 * size))
            coeff[u][v] = c[u] * c[v] * s

    low = []
    for u in range(hash_size):
        for v in range(hash_size):
            low.append(coeff[u][v])

    # Exclude DC term from median threshold
    vals = sorted(low[1:])
    median = vals[len(vals) // 2]

    bits = 0
    for v in low:
        bits = (bits << 1) | (1 if v > median else 0)

    return f"{bits:016x}", w, h, Path(path).stat().st_size


def _hamming_distance_hex(a: str, b: str) -> int:
    return (int(a, 16) ^ int(b, 16)).bit_count()


@dataclass
class ImageIndexTask:
    path: str
    index_path: str
    tg_file_id: Optional[str]
    tg_file_unique_id: Optional[str]
    tg_media_type: str
    mime: Optional[str]


class ImageDedupeManager:
    def __init__(self, db: DatabaseManager, max_distance: int = 8, queue_size: int = 2000, enabled: bool = False):
        self.db = db
        self.max_distance = max_distance
        self.enabled = enabled
        self.queue: queue.Queue[Optional[ImageIndexTask]] = queue.Queue(maxsize=queue_size)
        self.worker: Optional[threading.Thread] = None
        self.temp_dir: Optional[tempfile.TemporaryDirectory] = None

        if not self.enabled:
            return

        self.temp_dir = tempfile.TemporaryDirectory(prefix="etm-image-dedupe-")
        self.worker = threading.Thread(target=self._run, name="etm-image-phash-worker", daemon=True)
        self.worker.start()

    def stop_worker(self):
        if not self.enabled or self.worker is None:
            return
        if self.worker.is_alive():
            self.queue.put(None)
            self.worker.join(timeout=3)
        if not self.worker.is_alive() and self.temp_dir is not None:
            self.temp_dir.cleanup()
            self.temp_dir = None

    @staticmethod
    def _remove_index_file(path: Optional[str]):
        if not path:
            return
        try:
            Path(path).unlink()
        except FileNotFoundError:
            pass
        except OSError:
            logger.warning("failed to remove image index file: %s", path)

    def try_find_similar_file_id(self, path: str, tg_media_type: str = "photo") -> Optional[str]:
        if not self.enabled:
            return None
        try:
            phash, _, _, _ = _phash_hex(path)
            prefix = phash[:4]
            candidates = self.db.find_recent_image_candidates_by_prefix(prefix, tg_media_type=tg_media_type, limit=300)
            for row in candidates:
                if _hamming_distance_hex(phash, row.phash) <= self.max_distance:
                    return row.tg_file_id
        except Exception:
            logger.exception("find similar image failed: %s", path)
        return None

    def enqueue_index(self, path: str, tg_file_id: Optional[str], tg_file_unique_id: Optional[str], tg_media_type: str = "photo", mime: Optional[str] = None):
        if not self.enabled or not path or self.temp_dir is None:
            return

        index_path = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=self.temp_dir.name,
                suffix=Path(path).suffix,
                delete=False,
            ) as index_file:
                index_path = index_file.name
            shutil.copyfile(path, index_path)
        except (OSError, shutil.Error):
            self._remove_index_file(index_path)
            logger.exception("image index preparation failed: %s", path)
            return

        task = ImageIndexTask(
            path=path,
            index_path=index_path,
            tg_file_id=tg_file_id,
            tg_file_unique_id=tg_file_unique_id,
            tg_media_type=tg_media_type,
            mime=mime,
        )
        try:
            self.queue.put_nowait(task)
        except queue.Full:
            self._remove_index_file(index_path)
            logger.warning("image index queue full, drop: %s", path)

    def _run(self):
        while True:
            task = self.queue.get()
            try:
                if task is None:
                    return
                if not Path(task.index_path).exists():
                    continue
                phash, width, height, file_size = _phash_hex(task.index_path)
                self.db.upsert_image_phash(
                    path=task.path,
                    phash=phash,
                    phash_prefix=phash[:4],
                    tg_file_id=task.tg_file_id,
                    tg_file_unique_id=task.tg_file_unique_id,
                    tg_media_type=task.tg_media_type,
                    mime=task.mime,
                    width=width,
                    height=height,
                    file_size=file_size,
                )
            except Exception:
                logger.exception("image index worker failed")
            finally:
                if task is not None:
                    self._remove_index_file(task.index_path)
                self.queue.task_done()
