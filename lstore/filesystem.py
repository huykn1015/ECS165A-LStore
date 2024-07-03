import os
from lstore import config
from lstore.config import PAGE_SIZE
from lstore.page import PageId


def get_page_path(db_dir: str, page_id: PageId) -> str:
    """Return the filename to which the page belongs to."""
    return os.path.join(
        db_dir,
        page_id.table_name,
        str(page_id.raw_column_index),
        "base" if page_id.is_base else "tail",
    )


def read_pages(db_dir: str, page_id: PageId, count: int) -> list[bytearray]:
    """
    Read a page and prefetch some more pages. Might return fewer than (prefetch_count + 1)
    pages when we run out of pages to prefetch.
    """
    if count == 0:
        return []
    path = get_page_path(db_dir, page_id)
    with open(path, "rb") as f:
        f.seek(0, 2)
        file_size = f.tell()
        f.seek(page_id.page_index * PAGE_SIZE)
        cur_pos = f.tell()
        blocks_to_read = min([(file_size - cur_pos) // PAGE_SIZE, count])
        if blocks_to_read <= 0:
            return []
        data = f.read(blocks_to_read * PAGE_SIZE)
    return chop(data)


def create_column_file(path: str):
    """
    Used for creating database column files upon Database.open().
    Use get_page_path() (and a dummy PageId) for the path argument.
    """
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            pass
            f.write(b"\x00" * PAGE_SIZE)


def chop(m: bytes, bs: int = config.PAGE_SIZE) -> list[bytearray]:
    return [bytearray(m[i : i + bs]) for i in range(0, len(m), bs)]


def write_page(db_dir: str, page_id: PageId, data: bytearray):
    path = get_page_path(db_dir, page_id)
    with open(path, "r+b") as f:
        f.seek(page_id.page_index * PAGE_SIZE)
        f.write(data)
        f.flush()
        os.fsync(f)
