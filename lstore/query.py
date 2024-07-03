from enum import IntEnum
from typing import Literal
from lstore.config import NUM_META_COLS
from lstore.record import Record
from lstore.table import Table
from bitarray import bitarray
import sys
import time
import traceback
from lstore.transaction import Transaction


class QueryType(IntEnum):
    INSERT = 1
    UPDATE = 2
    INCREMENT = 3
    DELETE = 4
    _READONLY = 5
    SELECT = 5
    SUM = 6

    @classmethod
    def is_readonly(cls, val: "QueryType"):
        return val > cls._READONLY


def query(query_type: QueryType):
    def wrapper(func):
        setattr(func, "query_type", query_type)
        return func

    return wrapper


class Query:
    def __init__(self, table: Table):
        """
        Creates a Query object that can perform different queries on the specified table.
        - Queries that fail must return False.
        - Queries that succeed should return the result or True.
        - Any query that crashes (due to exceptions) should return False.
        """
        self.table = table
        pass

    @query(QueryType.DELETE)
    def delete(self, *args, **kwargs) -> bool:
        """
        internal Method
        Read a record with specified RID
        Returns True upon succesful deletion
        Return False if record doesn't exist or is locked due to 2PL
        """
        try:
            return self.__delete_impl(*args, **kwargs)
        except Exception:
            return False

    def __delete_impl(self, primary_key, xact: Transaction | None = None) -> bool:
        key_lookup = self.table.index.locate(self.table.key, primary_key)
        if len(key_lookup) != 1:
            return False
        key = key_lookup[0]
        rec = self.select(key, self.table.key, [1] * self.table.num_columns)
        assert rec is not False
        if len(rec) == 0:
            return False
        rec = rec[0]
        timestamp = xact.start_time if xact is not None else time.time_ns()
        tail_rid = self.table.delete_record(key_lookup[0], timestamp)
        if tail_rid is False:
            return False

        key_index = self.table.index.indices[self.table.key]
        assert key_index is not None
        key_index.remove(key_lookup[0], None)

        # Update indices
        for col, column_index in enumerate(self.table.index.indices):
            if column_index is None:
                continue
            column_index.remove(rec.columns[col], rec.base_rid)

        if xact is None:
            # Implicitly log as transaction
            xact = Transaction(timestamp)
            xact.add_query(self.delete, self.table, primary_key)
            xact.affected_rids.append(tail_rid)
            xact.commit()
            self.table.wal.log(xact)
        else:
            # Add affected RID to outer transaction
            xact.affected_rids.append(tail_rid)
        return True

    @query(QueryType.INSERT)
    def insert(self, *args, **kwargs) -> bool:
        """
        Insert a record with specified columns
        - Return True upon succesful insertion
        - Returns False if insert fails for whatever reason
        """
        try:
            return self.__insert_impl(*args, **kwargs)
        except Exception:
            return False

    def __insert_impl(self, *columns, xact: Transaction | None = None) -> bool:
        if len(columns) != self.table.num_columns:
            return False

        # Check if key exists? Reject if it does.
        key_lookup = self.table.index.locate(self.table.key, columns[self.table.key])
        if len(key_lookup) > 0:
            return False

        # Append record to base page
        base_rec = Record(
            self.table.key, [0] * (self.table.num_columns + NUM_META_COLS)
        )
        # NOTE: Endian is important! Page enforces native byteorder.
        base_rec.schema_encoding = bitarray(
            "0" * self.table.num_columns, endian=sys.byteorder
        )
        base_rec.timestamp = xact.start_time if xact is not None else time.time_ns()
        base_rec.columns = list(columns)
        base_rid = self.table.add_base_record(base_rec)
        if base_rid is False:
            return False

        # Add base record to index
        key_index = self.table.index.indices[self.table.key]
        if not key_index:
            raise RuntimeError("No index defined on key column.")
        for col_idx, col_value in enumerate(columns):
            index = self.table.index.indices[col_idx]
            if index is None:
                continue
            index.add(col_value, base_rid)

        if xact is None:
            # Implicitly log as transaction
            xact = Transaction(base_rec.timestamp)
            xact.affected_rids.append(base_rid)
            xact.add_query(self.insert, self.table, *columns)
            xact.commit()
            self.table.wal.log(xact)
        else:
            # Add affected RID to outer transaction
            xact.affected_rids.append(base_rid)
        return True

    @query(QueryType.SELECT)
    def select(self, *args, **kwargs) -> list[Record] | Literal[False]:
        """
        Read matching record with specified search key.
        Assume that select will never be called on a key that doesn't exist.

        :param search_key: the value you want to search based on
        :param search_key_index: the column index you want to search based on
        :param projected_columns_index: what columns to return. array of 1 or 0 values.
        :return: a list of Record objects upon success
        :return: False if record locked by TPL
        """
        try:
            return self.__select_impl(*args, **kwargs)
        except Exception as e:
            traceback.print_exc()
            return False

    def __select_impl(
        self,
        search_key: int,
        search_key_index: int,
        projected_column_indices: list[int],
    ) -> list[Record]:
        results: list[Record] = []
        # Try to select using index
        index = self.table.index.indices[search_key_index]
        if index is None:
            # linear scan
            for rec in self.table.records():
                if rec.columns[search_key_index] == search_key:
                    rec.filter_columns(projected_column_indices)
                    results.append(rec)
            return results

        base_rids = self.table.index.locate(search_key_index, search_key)
        for base_rid in base_rids:
            rec = self.table.get_latest_record(base_rid)
            if not rec:
                continue
            rec.filter_columns(projected_column_indices)
            results.append(rec)
        return results

    @query(QueryType.SELECT)
    def select_version(self, *args, **kwargs) -> list[Record] | Literal[False]:
        """
        Read matching record with specified search key.
        Assume that select will never be called on a key that doesn't exist.

        :param search_key: the value you want to search based on
        :param search_key_index: the column index you want to search based on
        :param projected_columns_index: what columns to return. array of 1 or 0 values.
        :param relative_version: the relative version of the record you need to retreive.
        :return: a list of Record objects upon success
        :return: False if record locked by TPL
        """
        try:
            return self.__select_version_impl(*args, **kwargs)
        except Exception:
            return False

    def __select_version_impl(
        self,
        search_key: int,
        search_key_index: int,
        projected_column_indices: list[int],
        relative_version: int,
    ) -> list[Record] | Literal[False]:
        if relative_version == 0:
            return self.select(search_key, search_key_index, projected_column_indices)
        results: list[Record] = []
        for rec in self.table.records(relative_version):
            if rec.columns[search_key_index] == search_key:
                rec.filter_columns(projected_column_indices)
                results.append(rec)
        return results

    @query(QueryType.UPDATE)
    def update(self, *args, **kwargs) -> bool:
        """
        Update a record with specified key and columns
        :return: True if update is succesful
        :return: False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
        """
        try:
            return self.__update_impl(*args, **kwargs)
        except Exception:
            return False

    def __update_impl(
        self,
        primary_key: int,
        *columns,
        xact: Transaction | None = None,
    ) -> bool:
        # check if record exists -> get base ID if record exists
        key_lookup = self.table.index.locate(self.table.key, primary_key)
        if len(key_lookup) != 1:
            return False
        base_rid = key_lookup[0]
        if all(column is None for column in columns):
            return True
        # select latest version so that we can update the index later
        original_rec = self.select(
            primary_key, self.table.key, [1] * self.table.num_columns
        )
        assert original_rec is not False
        original_rec = original_rec[0]

        update_rec = Record(
            self.table.key, [0] * (self.table.num_columns + NUM_META_COLS)
        )
        update_rec.base_rid = base_rid
        if len(columns) != self.table.num_columns:
            return False
        update_rec.columns = list(columns)
        update_rec.timestamp = xact.start_time if xact is not None else time.time_ns()
        tail_rid = self.table.add_tail_record(update_rec)

        if tail_rid is False:
            return False

        # Update index
        for col, original_new in enumerate(zip(original_rec.columns, columns)):
            original_value, new_value = original_new
            index = self.table.index.indices[col]
            if index is None:
                continue
            if new_value is None or original_value == new_value:
                continue
            index.remove(original_value, base_rid)
            index.add(new_value, base_rid)

        if xact is None:
            # Implicitly
            xact = Transaction(update_rec.timestamp)
            xact.add_query(self.update, self.table, primary_key, *columns)
            xact.affected_rids.append(tail_rid)
            xact.commit()
            self.table.wal.log(xact)
        else:
            xact.affected_rids.append(tail_rid)
        return True

    @query(QueryType.SUM)
    def sum(self, *args, **kwargs) -> int | Literal[False]:
        """
        This function is only called on the primary key.

        :param int start_range: Start of the key range to aggregate
        :param int end_range: End of the key range to aggregate
        :param int aggregate_column_index: Index of desired column to aggregate
        :return: the summation of the given range upon success
        :return: False if no record exists in the given range
        """
        try:
            return self.__sum_impl(*args, **kwargs)
        except Exception:
            return False

    def __sum_impl(
        self, start_range: int, end_range: int, aggregate_column_index: int
    ) -> int | Literal[False]:
        if (
            0 > aggregate_column_index
            or aggregate_column_index > self.table.num_columns - 1
        ):
            return False
        index = self.table.index.indices[self.table.key]
        sum = 0
        found = False
        if index:
            rids = index.locate_range(start_range, end_range)
            if len(rids) == 0:
                return False
            found = True
            for rid in rids:
                rec = self.table.get_record_version(rid, 0)
                if rec:
                    sum += rec.columns[aggregate_column_index]
        else:
            records = self.table.records()
            for rec in records:
                t = rec.columns[aggregate_column_index]
                if start_range <= rec.columns[self.table.key] <= end_range:
                    found = True
                    sum += t
        if found:
            return sum
        return False

    @query(QueryType.SUM)
    def sum_version(self, *args, **kwargs) -> int | Literal[False]:
        """
        This function is only called on the primary key.

        :param int start_range: Start of the key range to aggregate
        :param int end_range: End of the key range to aggregate
        :param int aggregate_columns: Index of desired column to aggregate
        :param relative_version: the relative version of the record you need to retreive.
        :return: the summation of the given range upon success
        :return: False if no record exists in the given range
        """
        try:
            return self.__sum_version_impl(*args, **kwargs)
        except Exception:
            return False

    def __sum_version_impl(
        self,
        start_range: int,
        end_range: int,
        aggregate_column_index: int,
        relative_version: int,
    ) -> int | Literal[False]:
        if (
            0 > aggregate_column_index
            or aggregate_column_index > self.table.num_columns - 1
        ):
            return False
        if relative_version == 0:
            return self.sum(start_range, end_range, aggregate_column_index)
        sum = 0
        found = False
        records = self.table.records(relative_version)
        for rec in records:
            t = rec.columns[self.table.key]
            if start_range <= t <= end_range:
                found = True
                sum += rec.columns[aggregate_column_index]
        if found:
            return sum
        return False

    @query(QueryType.INCREMENT)
    def increment(self, *args, **kwargs) -> bool:
        """
        Incremenets one column of the record.
        This implementation should work if your select and update queries already work.
        :param key: the primary of key of the record to increment
        :param column: the column to increment
        :return: True is increment is successful
        :return: False if no record matches key or if target record is locked by 2PL.
        """
        try:
            return self.__increment_impl(*args, **kwargs)
        except Exception:
            return False

    def __increment_impl(
        self, key: int, column: int, xact: Transaction | None = None
    ) -> bool:
        r = self.select(key, self.table.key, [1] * self.table.num_columns)
        if r is not False:
            r = r[0]
            updated_columns: list[None | int] = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            # to prevent Query.update() from implicitly adding a transaction
            timestamp = xact.start_time if xact is not None else time.time_ns()
            has_xact_override = xact is not None
            if xact is None:
                xact = Transaction(timestamp)
            result = self.update(key, *updated_columns, xact=xact)
            if result is True and not has_xact_override:
                xact.add_query(self.increment, self.table, key, column)
                xact.commit()
                self.table.wal.log(xact)
            return result
        return False
