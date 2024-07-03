from collections import OrderedDict, defaultdict
from contextlib import contextmanager
from threading import Condition, RLock
from lstore import config

from lstore.config import (
    BUFFERPOOL_EVICT_TIMEOUT,
    BUFFERPOOL_LOCK_TIMEOUT,
    BUFFERPOOL_MAX_FRAMES,
    BUFFERPOOL_PREFER_READ,
)
from lstore.filesystem import read_pages, write_page
from lstore.page import PageId, page_range
from lstore.rwlock import ReadPreferringRWLock, ReadersWriterLock, WritePreferringRWLock


def getDefaultRWLock() -> ReadersWriterLock:
    if BUFFERPOOL_PREFER_READ:
        return ReadPreferringRWLock(BUFFERPOOL_LOCK_TIMEOUT)
    else:
        return WritePreferringRWLock(BUFFERPOOL_LOCK_TIMEOUT)


class Frame:
    def __init__(
        self, bufferpool: "Bufferpool", data: bytearray, page_id: PageId
    ) -> None:
        self._lock = getDefaultRWLock()
        self._data = data
        self._page_id = page_id
        self._bufferpool = bufferpool

    @contextmanager
    def read(self):
        """Yield a readonly copy of the buffer, or False if lock acquisition timed out."""
        if self._lock.acquire_read():
            try:
                yield memoryview(self._data)
            finally:
                self._lock.release_read()
        else:
            yield False

    @contextmanager
    def write(self):
        """Yield a writable buffer, or False if lock acquisition timed out."""
        if self._lock.acquire_write():
            try:
                self._bufferpool.mark_dirty(self._page_id)
                yield self._data
            finally:
                self._lock.release_write()
        else:
            yield False

    def acquire_read(self) -> memoryview:
        self._lock.acquire_read()
        return memoryview(self._data)

    def release_read(self):
        self._lock.release_read()

    def acquire_write(self) -> bytearray:
        self._lock.acquire_write()
        self._bufferpool.mark_dirty(self._page_id)
        return self._data

    def release_write(self):
        self._lock.release_write()

    @property
    def page_id(self):
        return self._page_id


class Bufferpool:
    """Page buffer management that uses LRU page eviction policy."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._lock = RLock()
        self._cond = Condition(self._lock)  # pool not full
        self._frames: OrderedDict[PageId, Frame] = OrderedDict()
        self._dirty: set[PageId] = set()
        self._pins: dict[PageId, int] = defaultdict(int)

    @contextmanager
    def read(self, page_id: PageId):
        """
        Convenience method to pin, read, and unpin a frame in one go.

        Example:
        ```py
        with bufferpool.read(page_id) as data:
            if not data:
                return False
            page = pagedir.get_data_page(page_id, data)
            # Read the data...
        # The frame is automatically unpinned.
        return True
        ```
        """
        frame = self.pin(page_id)
        try:
            with frame.read() as data:
                yield data
        finally:
            self.unpin(page_id)

    @contextmanager
    def write(self, page_id: PageId):
        """Convenience method to pin, modify, and unpin a frame in one go.

        Example:
        ```py
        with bufferpool.write(page_id) as data:
            if not data:
                return False
            page = pagedir.get_data_page(page_id, data)
            # Read or modify the data...
        # The frame is automatically unpinned.
        return True
        ```
        """
        frame = self.pin(page_id)
        try:
            with frame.write() as data:
                yield data
        finally:
            self.unpin(page_id)

    def pin(self, page_id: PageId) -> Frame:
        """
        Pin frame associated with the page ID (fetching it if not already in
        bufferpool).
        """
        with self._lock:
            if page_id in self._frames:
                self._frames.move_to_end(page_id)
                self._pins[page_id] += 1
            else:
                if not self.has_capacity():
                    self._evict_lru()
                self.fetch(page_id, 1)
                self._pins[page_id] = 1
            return self._frames[page_id]

    def fetch(self, start: PageId, count: int = 1):
        """
        Fetch 1 to `count` pages into pool without pinning them.
        `count` is reduced to one if there isn't enough space for prefetching.
        """
        with self._lock:
            if not self.has_capacity() or count == 0:
                return
            # Run fetch if any page within fetch range isn't already in bufferpool
            # For more optimization, we can fetch only the ones that we actually need.
            available_slots = config.BUFFERPOOL_MAX_FRAMES - len(self._frames)
            count = max(min(available_slots - 10, count), 1)
            if all(pid in self._frames for pid in page_range(start, count)):
                return
            pages = read_pages(self._db_path, start, count)
            if len(pages) == 0:
                pages.append(bytearray(b"\x00" * config.PAGE_SIZE))
            for idx, page_id in enumerate(page_range(start, min(count, len(pages)))):
                if page_id in self._frames:
                    continue
                self._frames[page_id] = Frame(
                    self,
                    pages[idx],
                    page_id,
                )

    def unpin(self, page_id: PageId):
        """Unpin a frame."""
        with self._lock:
            pin_count = self._pins[page_id]
            self._pins[page_id] = max(pin_count - 1, 0)
            if pin_count < 1:
                self._cond.notify_all()

    def mark_dirty(self, page_id: PageId):
        """
        Mark a frame as dirty.
        Note that calling Bufferpool.write() and Frame.write() already marks the
        page dirty, so normally you don't need to call this manually.
        """
        with self._lock:
            self._dirty.add(page_id)

    def has_capacity(self) -> bool:
        """Check if the bufferpool has capacity for at least one new frame."""
        with self._lock:
            return len(self._frames) < config.BUFFERPOOL_MAX_FRAMES

    def _evict_lru(self):
        with self._lock:
            lru_page_id, _ = next(iter(self._frames.items()))
            if not self._cond.wait_for(
                lambda: self._pins[lru_page_id] == 0, BUFFERPOOL_EVICT_TIMEOUT
            ):
                raise RuntimeError("LRU page did not unpin in time")
            self.evict_page(lru_page_id)

    def evict_page(self, page_id: PageId):
        with self._lock:
            if self._pins[page_id] != 0:
                raise ValueError("Pinned page cannot be evicted!")
            if page_id in self._dirty:
                write_page(self._db_path, page_id, self._frames[page_id]._data)
            del self._frames[page_id]
            del self._pins[page_id]

    def close(self):
        """Evict all frames and lock indefinitely. Used when closing the database."""
        self._lock.acquire()
        self._evict_all()

    def _evict_all(self):
        while len(self._frames) != 0:
            self._evict_lru()

    def release(self):
        """Manually release previously acquired lock."""
        self._lock.release()

    def checkpoint(self):
        """Cause all dirty pages (at this moment of checkpoint) to be flushed to disk."""
        with self._lock:
            dirty = self._dirty.copy()
            for page in dirty:
                self._cond.wait_for(lambda: self._pins[page] == 0)
                self.evict_page(page)
