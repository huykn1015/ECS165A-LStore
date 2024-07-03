from abc import ABC, abstractmethod
from multiprocessing import Value
from typing import Type, TYPE_CHECKING
from sortedcontainers import SortedDict, SortedSet

if TYPE_CHECKING:
    from lstore.table import Table


class ColumnIndex(ABC):
    @abstractmethod
    def populate(self, table: "Table", column: int):
        pass

    @abstractmethod
    def add(self, value: int, rid: int):
        pass

    @abstractmethod
    def locate(self, value: int) -> list[int]:
        pass

    @abstractmethod
    def locate_range(self, begin: int, end: int) -> list[int]:
        pass

    @abstractmethod
    def remove(self, value: int, rid: int | None):
        pass


class UniqueKeyViolationError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("Unique key violation")


class RangedIndex(ColumnIndex):
    def __init__(self, is_unique: bool = False) -> None:
        self._index = SortedDict()
        self._is_unique = is_unique

    def __add(self, value: int, rid: int):
        if self._is_unique and value in self._index:
            raise UniqueKeyViolationError()
        if value not in self._index:
            self._index[value] = SortedSet()
        self._index[value].add(rid)

    def populate(self, table: "Table", column: int):
        # TODO: use TableReader
        for rec in table.records():
            value = rec.columns[column]
            self.__add(value, rec.base_rid)

    def add(self, value: int, rid: int):
        self.__add(value, rid)

    def locate(self, value: int) -> list[int]:
        return list(self._index.get(value, []))

    def locate_range(self, begin: int, end: int) -> list[int]:
        ret = [
            rid
            for value in self._index.irange(begin, end)
            for rid in self._index[value]
        ]
        return ret

    def remove(self, value: int, rid: int | None):
        """
        Remove a value & RID pair from index. An rid of None removes all RIDs
        associated with the value.
        """
        rids = self._index.get(value)
        if rids is None:
            return
        if self._is_unique or rid is None:
            del self._index[value]
        else:
            rids.remove(rid)


class UniqueIndex(RangedIndex):
    def __init__(self) -> None:
        super().__init__(True)


class Index:
    """
    A data strucutre holding indices for various columns of a table. Key column
    should be indexd by default, other columns can be indexed through this
    object. Indices are usually B-Trees, but other data structures can be used
    as well.
    """

    def __init__(
        self,
        table: "Table",
        unique_cols: set[int] = set(),
        ranged_cols: set[int] = set(),
    ):
        """One index for each table. All are empty initially."""
        self.indices: list[ColumnIndex | None] = [None] * table.num_columns
        self.table = table
        self.create_index(self.table.key, UniqueIndex)
        for unique_col in unique_cols:
            if unique_col == table.key:
                continue
            self.create_index(unique_col, RangedIndex)
        for ranged_col in ranged_cols:
            if ranged_col == table.key:
                continue
            self.create_index(ranged_col, RangedIndex)

    def locate(self, column: int, value: int) -> list[int]:
        """
        Returns the location of all records with the given value on column "column".
        :return: list[int] of RIDs (or empty list if none is found)
        """
        index = self.indices[column]
        if not index:
            raise RuntimeError(f"Index not defined on column {column}")
        return index.locate(value)

    def locate_range(self, begin: int, end: int, column: int):
        """
        Returns the RIDs of all records with values in column "column" between "begin" and "end".
        """
        index = self.indices[column]
        if not index:
            raise RuntimeError(f"Index not defined on column {column}")
        return index.locate_range(begin, end)

    def create_index(
        self, column_number: int, CustomIndex: Type[ColumnIndex] = RangedIndex
    ):
        """Create index on specific column"""
        if self.indices[column_number] is not None:
            raise ValueError(f"Index already exists for column {column_number}.")
        index = CustomIndex()
        self.indices[column_number] = index
        index.populate(self.table, column_number)

    def drop_index(self, column_number: int):
        """Drop index of specific column"""
        if column_number == self.table.key:
            raise ValueError("Cannot delete primary key index.")
        self.indices[column_number] = None
