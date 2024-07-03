from abc import ABC, abstractmethod
from contextlib import contextmanager
from threading import Condition, RLock, get_ident


# See https://en.wikipedia.org/wiki/Readers%E2%80%93writer_lock
class ReadersWriterLock(ABC):
    @abstractmethod
    def acquire_read(self) -> bool:
        pass

    @abstractmethod
    def release_read(self):
        pass

    @abstractmethod
    def acquire_write(self) -> bool:
        pass

    @abstractmethod
    def release_write(self):
        pass

    @contextmanager
    def read(self):
        try:
            if not self.acquire_read():
                raise RuntimeError("Failed to acquire read lock")
            yield
        finally:
            self.release_read()

    @contextmanager
    def write(self):
        try:
            if not self.acquire_write():
                raise RuntimeError("Failed to acquire write lock")
            yield
        finally:
            self.release_write()

    @property
    @abstractmethod
    def locked(self) -> bool:
        pass


class ReadPreferringRWLock(ReadersWriterLock):
    def __init__(self, timeout=None) -> None:
        self._num_readers = 0
        self._read_lock = RLock()
        self._exclusive_lock = RLock()
        self._timeout = timeout if timeout else -1

    def acquire_read(self) -> bool:
        if not self._read_lock.acquire(timeout=self._timeout):
            return False
        self._num_readers += 1
        if self._num_readers == 1:
            self._exclusive_lock.acquire()
        self._read_lock.release()
        return True

    def acquire_write(self) -> bool:
        return self._exclusive_lock.acquire(timeout=self._timeout)

    def release_read(self):
        self._read_lock.acquire()
        self._num_readers -= 1
        if self._num_readers == 0:
            self._exclusive_lock.release()
        self._read_lock.release()

    def release_write(self):
        self._exclusive_lock.release()

    @property
    def locked(self):
        can_lock = self._exclusive_lock.acquire(False)
        if can_lock:
            self._exclusive_lock.release()
            return True
        return False


class WritePreferringRWLock(ReadersWriterLock):
    def __init__(self, timeout=None) -> None:
        self._lock = RLock()
        self._num_readers_active = 0
        self._num_writers_waiting = 0
        self._is_writer_active = False
        self._write_thread_id = -1
        self._recursion_level = -1
        self._timeout = timeout
        self._cond = Condition(self._lock)

    def acquire_read(self) -> bool:
        self._lock.acquire()
        satisfied = self._cond.wait_for(
            lambda: self._num_writers_waiting == 0 and not self._is_writer_active,
            self._timeout,
        )
        if satisfied:
            self._num_readers_active += 1
        self._lock.release()
        return satisfied

    def release_read(self):
        self._lock.acquire()
        self._num_readers_active -= 1
        if self._num_readers_active == 0:
            self._cond.notify_all()
        self._lock.release()

    def acquire_write(self) -> bool:
        self._lock.acquire()
        if self._write_thread_id == get_ident():
            self._recursion_level += 1
            return True
        self._num_writers_waiting += 1
        satisfied = self._cond.wait_for(
            lambda: self._num_readers_active == 0 and not self._is_writer_active,
            self._timeout,
        )
        self._num_writers_waiting -= 1
        if satisfied:
            self._write_thread_id = get_ident()
            self._recursion_level = 1
            self._is_writer_active = True
        self._lock.release()
        return satisfied

    def release_write(self):
        self._lock.acquire()
        self._recursion_level -= 1
        if self._recursion_level == 0:
            self._is_writer_active = False
            self._write_thread_id = -1
            self._cond.notify_all()
        self._lock.release()

    @property
    def locked(self):
        return (
            self._is_writer_active
            or self._num_readers_active > 0
            or self._num_writers_waiting > 0
        )
