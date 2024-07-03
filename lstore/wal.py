from dataclasses import dataclass, field
from io import BufferedRandom
import io
import os
from threading import RLock

from bitarray import bitarray
from lstore import config
from lstore.query import Query, QueryType
from lstore.table import Table
from lstore.transaction import StoredQuery, Transaction


class WriteAheadLogEntry:
    def __init__(self, xact: Transaction) -> None:
        self._xact = xact


@dataclass
class RedoQuery:
    query_type: QueryType
    table_name: str
    insert: list[int] = field(default_factory=list)
    update: list[int | None] = field(default_factory=list)
    key: int = 0  # used for update, delete, and increment
    inc_column: int = 0  # used for increment

    def __post_init__(self):
        if QueryType.is_readonly(self.query_type):
            raise RuntimeError("Readonly query cannot be a RedoQuery")

    def to_stored_query(self, table: Table) -> StoredQuery:
        assert table.name == self.table_name
        q = Query(table)
        match self.query_type:
            case QueryType.INSERT:
                return (q.insert, self.table_name, tuple(self.insert))
            case QueryType.UPDATE:
                return (q.update, self.table_name, (self.key, *self.update))
            case QueryType.INCREMENT:
                return (q.increment, self.table_name, (self.key, self.inc_column))
            case QueryType.DELETE:
                return (q.delete, self.table_name, (self.key,))
            case _:
                raise RuntimeError("Unexpected query type for RedoQuery")


@dataclass
class RedoTransaction:
    start_time: int = 0
    queries: list[RedoQuery] = field(default_factory=list)

    def to_xact(self, tables: dict[str, Table]) -> Transaction:
        xact = Transaction(self.start_time)
        xact.queries = [q.to_stored_query(tables[q.table_name]) for q in self.queries]
        return xact


class WriteAheadLogParser:
    def __init__(self, f: BufferedRandom) -> None:
        assert "b" in f.mode
        self._f = f
        # used for restoring file cursor after parsing
        self._orig_pos = self._f.tell()
        self._f.seek(0, io.SEEK_END)
        self._fsize = self._f.tell()
        self._f.seek(self._orig_pos)

    def parse(self) -> list[RedoTransaction]:
        """WAL = TRANSACTION..."""
        self._f.seek(0)
        xacts: list[RedoTransaction] = []
        while self._f.tell() < self._fsize:
            xacts.append(self._parse_xact())

        # restore cursor position
        self._f.seek(self._orig_pos)
        return xacts

    def _parse_xact(self) -> RedoTransaction:
        """TRANSACTION = START_TIME NUM_QUERIES QUERY..."""
        xact = RedoTransaction()
        xact.start_time = self.__parse_uint(config.DATA_SIZE)
        num_queries = self.__parse_uint(config.N_QUERY_ENCODING)
        for _ in range(num_queries):
            xact.queries.append(self._parse_query())
        return xact

    def _parse_query(self) -> RedoQuery:
        """QUERY = QUERY_TYPE TABLE [ INSERT | UPDATE | INCREMENT | DELETE ]"""
        query = RedoQuery(self.__parse_query_type(), self.__parse_str())
        match query.query_type:
            case QueryType.INSERT:
                return self._parse_insert(query)
            case QueryType.UPDATE:
                return self._parse_update(query)
            case QueryType.INCREMENT:
                return self._parse_increment(query)
            case QueryType.DELETE:
                return self._parse_delete(query)
            case _:
                raise RuntimeError("Unexpected query type in WAL")

    def _parse_insert(self, query: RedoQuery) -> RedoQuery:
        """INSERT = NUM_COLS COL..."""
        num_cols = self.__parse_uint(config.COL_ENCODING)
        for _ in range(num_cols):
            query.insert.append(self.__parse_int(config.DATA_SIZE))
        return query

    def _parse_update(self, query: RedoQuery) -> RedoQuery:
        """UPDATE = KEY TABLE_NUM_COLS ENCODING COL..."""
        query.key = self.__parse_int(config.DATA_SIZE)
        table_num_cols = self.__parse_uint(config.COL_ENCODING)
        bitarray_size = (table_num_cols + 7) // 8
        ba = bitarray(endian="little")
        ba.frombytes(self._f.read(bitarray_size))
        ba = ba[:table_num_cols]
        for flag in ba:
            if flag == 0:
                query.update.append(None)
                continue
            query.update.append(self.__parse_int(config.DATA_SIZE))
        return query

    def _parse_increment(self, query: RedoQuery) -> RedoQuery:
        """INCREMENT = KEY INC_COLUMN"""
        query.key = self.__parse_int(config.DATA_SIZE)
        query.inc_column = self.__parse_uint(config.COL_ENCODING)
        return query

    def _parse_delete(self, query: RedoQuery) -> RedoQuery:
        """DELETE = KEY"""
        query.key = self.__parse_int(config.DATA_SIZE)
        return query

    def __parse_int(self, num_bytes: int, signed=True) -> int:
        b = self._f.read(num_bytes)
        return int.from_bytes(b, "little", signed=signed)

    def __parse_uint(self, num_bytes: int) -> int:
        return self.__parse_int(num_bytes, False)

    def __parse_query_type(self) -> QueryType:
        i = self.__parse_uint(1)
        return QueryType(i)

    def __parse_str(self) -> str:
        n_chars = self.__parse_uint(config.MAX_TABLE_SIZE_INT)
        return self._f.read(n_chars).decode("utf-8")


class TransactionSerializer:
    def __init__(self, xact: Transaction) -> None:
        self._x = xact
        self._buf = bytearray()

    def serialize(self) -> bytes:
        self.__serialize_uint(self._x.start_time, config.DATA_SIZE)
        self.__serialize_uint(len(self._x.queries), config.N_QUERY_ENCODING)
        for q in self._x.queries:
            self._serialize_query(q)
        return bytes(self._buf)

    def _serialize_query(self, q: StoredQuery):
        query_type: QueryType = q[0].query_type
        table = q[1]
        if QueryType.is_readonly(query_type):
            return
        self.__serialize_uint(query_type, 1)
        self.__serialize_str(table)
        match query_type:
            case QueryType.INSERT:
                self._serialize_insert(q[2])
            case QueryType.UPDATE:
                self._serialize_update(q[2])
            case QueryType.INCREMENT:
                self._serialize_increment(q[2])
            case QueryType.DELETE:
                self._serialize_delete(q[2])

    def _serialize_insert(self, q: tuple):
        self.__serialize_uint(len(q), config.COL_ENCODING)
        for col in q:
            self.__serialize_int(col, config.DATA_SIZE)

    def _serialize_update(self, q: tuple):
        key = q[0]
        cols = q[1:]
        self.__serialize_int(key, config.DATA_SIZE)
        self.__serialize_uint(len(cols), config.COL_ENCODING)
        ba = bitarray(endian="little")
        for col in cols:
            ba.append(0 if col is None else 1)
        self._buf += ba.tobytes()
        for col in cols:
            if col is None:
                continue
            self.__serialize_int(col, config.DATA_SIZE)

    def _serialize_increment(self, q: tuple):
        key = q[0]
        inc_column = q[1]
        self.__serialize_int(key, config.DATA_SIZE)
        self.__serialize_uint(inc_column, config.COL_ENCODING)

    def _serialize_delete(self, q: tuple):
        key = q[0]
        self.__serialize_int(key, config.DATA_SIZE)

    def __serialize_int(self, value: int, num_bytes: int, signed=True):
        self._buf += value.to_bytes(num_bytes, "little", signed=signed)

    def __serialize_uint(self, value: int, num_bytes: int):
        self.__serialize_int(value, num_bytes, False)

    def __serialize_str(self, s: str):
        if len(s) > config.MAX_TABLE_NAME_LEN:
            raise ValueError("Table name too long")
        self.__serialize_uint(len(s), config.MAX_TABLE_SIZE_INT)
        self._buf += s.encode("utf-8")


class WriteAheadLog:
    def __init__(self, db_path: str) -> None:
        self._lock = RLock()
        self._filename = os.path.join(db_path, "wal")
        if not os.path.exists(self._filename):
            open(self._filename, "wb").close()
        self._f = open(self._filename, "r+b")

    def log(self, xact: Transaction):
        with self._lock:
            self._f.write(TransactionSerializer(xact).serialize())
            self.__force_write()

    def checkpoint(self):
        """
        Tell WAL that a bufferpool checkpoint just took place (i.e. all
        transactions have been saved). This empties the WAL.
        """
        with self._lock:
            self._f.truncate(0)
            self._f.seek(0)
            self.__force_write()

    def __force_write(self):
        self._f.flush()
        os.fsync(self._f)

    def recover(self, tables: dict[str, Table]) -> list[Transaction]:
        """Retrieve all logged transactions in the WAL for redoing."""
        with self._lock:
            redo_xacts = WriteAheadLogParser(self._f).parse()
            return [xact.to_xact(tables) for xact in redo_xacts]

    def close(self):
        """
        Lock the WAL and delete the WAL file (assumes a clean database shutdown
        where bufferpool has been saved).
        """
        self._lock.acquire()
        self.checkpoint()
        self._f.close()
        # Keep lock to prevent access
