from time import time_ns
from typing import TYPE_CHECKING, Callable

from lstore.transaction_tracker import TransactionTracker

if TYPE_CHECKING:
    from lstore.table import Table
    from lstore.pagedir import PageDirectory

StoredQuery = tuple[Callable, str, tuple]


class Transaction:
    def __init__(self, start_time_override=-1):
        """Creates a transaction object."""
        self.queries: list[StoredQuery] = []
        self.affected_rids: list[int] = []
        self.start_time = start_time_override
        self.__xact_tracker: TransactionTracker | None = None
        self.__pagedirs: dict[str, "PageDirectory"] = {}
        pass

    def add_query(self, query, table: "Table", *args):
        """
        Adds the given query to this transaction

        Example:
        ```py
        q = Query(grades_table)
        t = Transaction()
        t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
        ```
        """
        if self.__xact_tracker is None:
            self.__xact_tracker = table.xact_tracker
        if table.name not in self.__pagedirs:
            self.__pagedirs[table.name] = table.page_directory
        self.queries.append((query, table.name, args))
        # use grades_table for aborting

    # If you choose to implement this differently this method must still return
    # True if transaction commits or False on abort
    def run(self) -> bool:
        if self.start_time == -1:
            self.start_time = time_ns()
        for query, table_name, args in self.queries:
            # TODO: need to retrieve the inserted base/tail RID (for
            # committing), so probably need to use special query methods
            result = query(*args, xact=self)
            # If the query has failed the transaction should abort
            if result is False:
                return self.abort()
        return self.commit()

    def abort(self) -> bool:
        try:
            if self.__xact_tracker is None:
                return False
            self.__xact_tracker.mark_aborted(self.start_time)
            for pagedir in self.__pagedirs.values():
                pagedir.notify_resolve(self.affected_rids)
            return False
        except Exception:
            return False

    def commit(self) -> bool:
        try:
            if self.__xact_tracker is None:
                return True
            self.__xact_tracker.mark_committed(self.start_time)
            for pagedir in self.__pagedirs.values():
                pagedir.notify_resolve(self.affected_rids)
            return True
        except Exception:
            return False
