from contextlib import contextmanager
import os
import shutil
from tempfile import TemporaryDirectory, mkdtemp
from threading import Lock
from lstore import config
from lstore.bufferpool import Bufferpool
from lstore.config import MAX_TABLE_NAME_LEN, NUM_META_COLS
from lstore.filesystem import create_column_file, get_page_path
from lstore.page import PageId
from lstore.table import Table
from lstore.transaction_tracker import TransactionTracker
from lstore.wal import WriteAheadLog


class Database:
    def __init__(self):
        self.tables: dict[str, Table] = {}

        # To be initialized in Database.open():
        self.path: str = str(mkdtemp())
        self._to_delete = self.path
        self.bufferpool: Bufferpool = Bufferpool(self.path)
        self.wal: WriteAheadLog = WriteAheadLog(self.path)
        self.aborted: dict[int, bool] = {}
        self.xact_tracker = TransactionTracker()
        self._lock = Lock()

    def __del__(self):
        if self._to_delete is not None:
            shutil.rmtree(self._to_delete, ignore_errors=True)

    def open(self, path: str):
        self.path = path
        if os.path.isfile(path):
            raise ValueError("Database base directory cannot be a file")
        if not os.path.isdir(path):
            os.makedirs(path, 0o750)
        self.bufferpool = Bufferpool(path)
        self.wal = WriteAheadLog(path)
        if self.__is_existing_db(path):
            self.xact_tracker.load(self.path)
            self.__init_tables(path)
        xacts = self.wal.recover(self.tables)
        if xacts:
            for xact in xacts:
                if not xact.run():
                    raise RuntimeError("Logged transaction could not be replayed!")
            self.bufferpool.checkpoint()
            self.wal.checkpoint()
        open(os.path.join(self.path, config.DB_MARKER), "w").close()

    def __is_existing_db(self, path: str) -> bool:
        if not os.path.exists(path) or not os.path.isdir(path):
            return False
        for entry in os.scandir(path):
            if entry.name == config.DB_MARKER:
                return True
        return False

    def __init_tables(self, path: str):
        for table in os.scandir(path):
            unique_cols = set()
            ranged_cols = set()
            if not table.is_dir():
                # Not a table subdirectory
                continue
            num_data_cols = 0
            key = None
            for col in os.scandir(table.path):
                if not col.is_dir():
                    # Not a column subdirectory
                    continue
                raw_col_idx = int(col.name)
                if raw_col_idx < config.NUM_META_COLS:
                    # No indices for meta columns
                    continue
                num_data_cols += 1
                if os.path.exists(os.path.join(col.path, "key")):
                    key = raw_col_idx - config.NUM_META_COLS
                elif os.path.exists(os.path.join(col.path, "unique")):
                    unique_cols.add(raw_col_idx - config.NUM_META_COLS)
                elif os.path.exists(os.path.join(col.path, "ranged")):
                    ranged_cols.add(raw_col_idx - config.NUM_META_COLS)
            assert num_data_cols > 0
            assert key is not None
            self.tables[table.name] = Table(
                table.name,
                num_data_cols,
                key,
                unique_cols,
                ranged_cols,
                self.bufferpool,
                self.path,
                self.wal,
                self.xact_tracker,
                True,
            )

    def close(self):
        for table in self.tables.values():
            table.drain_merge()
            table.page_directory.save()
        self.bufferpool.close()
        self.wal.close()
        self.xact_tracker.save(self.path)
        del self.path
        del self.bufferpool
        del self.wal
        self.tables = {}

    def create_table(self, name: str, num_columns: int, key_index: int) -> Table:
        """
        Creates a new table.
        :param str name: Table name
        :param int num_columns: Number of Columns: all columns are integer
        :param int key: Index of table key in columns
        """
        if len(name) > MAX_TABLE_NAME_LEN:
            raise ValueError("Table name too long!")
        # TODO: modify (create files on disk, etc)
        if name in self.tables:
            raise ValueError(f"Table '{name}' already exists.")
        if not (0 <= key_index < num_columns):
            raise ValueError("Key index is not in range.")

        for raw_col in range(NUM_META_COLS + num_columns):
            bp_path = get_page_path(self.path, PageId(name, raw_col, True, 0))
            create_column_file(bp_path)
            tp_path = get_page_path(self.path, PageId(name, raw_col, False, 0))
            create_column_file(tp_path)
        open(
            os.path.join(self.path, name, str(key_index + config.NUM_META_COLS), "key"),
            "w",
        ).close()
        table = Table(
            name,
            num_columns,
            key_index,
            set(),
            set(),
            self.bufferpool,
            self.path,
            self.wal,
            self.xact_tracker,
        )
        self.tables[name] = table
        return table

    def drop_table(self, name: str):
        """Deletes the specified table."""
        # TODO: modify (delete from disk, abort transactions, etc)
        if name in self.tables:
            del self.tables[name]
            # TODO: might want to remove at closing instead of here
            # prevent pending transactions from failing
            shutil.rmtree(os.path.join(self.path, name))

    def get_table(self, name: str) -> Table:
        """Returns table with the passed name."""
        return self.tables[name]


@contextmanager
def tempdb():
    with TemporaryDirectory() as tmpdir:
        db = Database()
        db.open(tmpdir)
        try:
            yield db
        finally:
            db.close()
