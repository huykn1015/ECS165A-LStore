import os
import pickle
from lstore.rwlock import WritePreferringRWLock

TRACKER_PATH = "xact_aborted"


class TransactionTracker:
    def __init__(self) -> None:
        self._lock = WritePreferringRWLock()
        # True = aborted, False = committed, absent = pending
        self._aborted: dict[int, bool] = {}

    def load(self, path: str):
        path = os.path.join(path, TRACKER_PATH)
        if not os.path.exists(path):
            return
        with self._lock.write():
            with open(path, "rb") as f:
                self._aborted = pickle.load(f)
            assert isinstance(self._aborted, dict)

    def save(self, path: str):
        path = os.path.join(path, TRACKER_PATH)
        with self._lock.read():
            with open(path, "wb") as f:
                pickle.dump(self._aborted, f)

    def mark_committed(self, timestamp: int):
        with self._lock.write():
            if timestamp in self._aborted:
                raise ValueError("Transaction already present in tracker!")
            self._aborted[timestamp] = False

    def mark_aborted(self, timestamp: int):
        with self._lock.write():
            if timestamp in self._aborted:
                raise ValueError("Transaction already present in tracker!")
            self._aborted[timestamp] = True

    def is_committed(self, timestamp: int):
        with self._lock.read():
            return self._aborted.get(timestamp, True) == False

    def is_aborted(self, timestamp: int):
        with self._lock.read():
            return self._aborted.get(timestamp, False) == True

    def is_maybe_pending(self, timestamp: int):
        with self._lock.read():
            return timestamp not in self._aborted
