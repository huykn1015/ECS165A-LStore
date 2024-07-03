from abc import ABC, abstractmethod
from copy import copy
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, Literal
import sys

from bitarray import bitarray

from lstore import config

if TYPE_CHECKING:
    from lstore.pagedir import PageDirectory


@dataclass
class PageId:
    """
    Page identifier, used in bufferpool.
    Not necessarily a property of Page objects.
    """

    # Name of the table to which the page belongs
    table_name: str
    # Index of column to which the page belongs
    raw_column_index: int
    # Whether it's a base or tail page
    is_base: bool
    # Position of page within the column's file.
    # This is equal to the conceptual page index.
    # It is assumed that PageIds that only differ in page indices reside in the
    # same file.
    page_index: int

    # Enables using PageId as dictionary key
    def __hash__(self):
        return self.to_tuple().__hash__()

    def to_tuple(self):
        return (self.table_name, self.raw_column_index, self.is_base, self.page_index)


@dataclass
class PageLocation:
    # Don't subclass PageId to prevent hash issues
    table_name: str
    raw_column_index: int
    is_base: bool
    page_index: int
    offset: int

    def id(self) -> PageId:
        return PageId(
            self.table_name, self.raw_column_index, self.is_base, self.page_index
        )

    @staticmethod
    def from_id(id: PageId, offset: int):
        return PageLocation(
            id.table_name, id.raw_column_index, id.is_base, id.page_index, offset
        )


def page_range(page_id: PageId, count: int):
    """Iterate over page indices within the same file. [page_id.page_index, end)"""
    if count == 0:
        return
    cur_page = copy(page_id)
    for i in range(page_id.page_index, page_id.page_index + count):
        cur_page.page_index = i
        yield copy(cur_page)


def columns_of(page_id: PageId, num_raw_cols: int):
    if num_raw_cols == 0:
        return
    cur_page = copy(page_id)
    for i in range(num_raw_cols):
        cur_page.raw_column_index = i
        yield copy(cur_page)


class PageType(Enum):
    DATA = auto()
    SCHEMA_ENCODING = auto()
    MAPPING = auto()


class Page(ABC):
    def __init__(
        self, page_id: PageId, data: bytearray | memoryview, pagedir: "PageDirectory"
    ) -> None:
        super().__init__()
        self._page_id = page_id
        # TODO: remove in-memory database compatibility (remove if/else)
        # Pretend it's a bytearray, even though it might be a memoryview
        self._data = data if data else bytearray(config.PAGE_SIZE)
        self._pagedir = pagedir
        assert len(self._data) == config.PAGE_SIZE

    @property
    def page_id(self) -> PageId:
        return self._page_id

    @abstractmethod
    def has_capacity(self) -> bool:
        """:return: whether there is any space left for new data."""
        pass

    @property
    def num_records(self) -> int:
        """Number of records in the page"""
        return self._pagedir.get_num_records(self._page_id)

    @num_records.setter
    def num_records(self, value: int):
        self._pagedir.set_num_records(self._page_id, value)

    @property
    def page_size(self) -> int:
        """Page size."""
        return config.PAGE_SIZE

    @property
    def endian(self) -> Literal["little", "big"]:
        """Endianness to use during (de)serialization"""
        # Try to use native endianness.
        return sys.byteorder

    @property
    @abstractmethod
    def page_type(self) -> PageType:
        pass


class AlignedPage(Page):
    """Parent class for all pages with fixed-size data elements."""

    def __init__(
        self,
        page_id: PageId,
        col_size: int,
        data: bytearray | memoryview,
        pagedir: "PageDirectory",
    ):
        super().__init__(page_id, data, pagedir)
        self._col_size = col_size

    def has_capacity(self) -> bool:
        return (
            self.num_records * self.col_size < self.page_size
            and self._pagedir.has_capacity(self.page_id)
        )

    @property
    def col_size(self) -> int:
        return self._col_size

    @property
    def max_records(self):
        return int(self.page_size // self.col_size)

    def alloc(self) -> int | None:
        if not self.has_capacity():
            return None
        offset = self.num_records
        self.num_records += 1
        return offset

    def _write_bytes(self, value: bytes, offset: int):
        assert len(value) == self.col_size
        begin = offset * self.col_size
        end = begin + self.col_size
        self._data[begin:end] = value

    def _read_bytes(self, offset: int):
        begin = offset * self.col_size
        end = begin + self.col_size
        return self._data[begin:end]


class DataPage(AlignedPage):
    def __init__(
        self, page_id: PageId, data: bytearray | memoryview, pagedir: "PageDirectory"
    ):
        super().__init__(page_id, config.DATA_SIZE, data, pagedir)

    def add(self, value: int) -> int | None:
        """
        Add a new int to the page.

        :param int value: signed 64-bit integer
        :return: record offset if write succeeded
        :return: None if no capacity left or integer overflow.
        :raise: OverflowError if value doesn't fit in COL_SIZE bytes.
        """
        offset = self.alloc()
        if offset is None:
            return None
        self.write(value, offset)
        return offset

    def read(self, offset: int) -> int:
        """Read self.COL_SIZE bytes from the offset as a signed integer."""
        return int.from_bytes(self._read_bytes(offset), self.endian, signed=True)

    def write(self, value: int, offset: int):
        """
        Set data at the offset to the given bitarray.
        """
        self._write_bytes(
            value.to_bytes(config.DATA_SIZE, self.endian, signed=True), offset
        )

    @property
    def page_type(self):
        return PageType.DATA


class SchemaEncodingPage(AlignedPage):
    def __init__(
        self,
        page_id: PageId,
        num_columns: int,
        data: bytearray | memoryview,
        pagedir: "PageDirectory",
    ):
        col_size = self.get_col_size(num_columns)
        super().__init__(page_id, col_size, data, pagedir)
        self._num_columns = num_columns

    @staticmethod
    def get_col_size(num_columns: int):
        return int((num_columns + 7) // 8)

    def add(self, value: bitarray) -> int | None:
        """
        Add a new int to the page.

        :param int value: signed 64-bit integer
        :return: record offset if write succeeded
        :return: None if no capacity left or integer overflow.
        :raise: OverflowError if value doesn't fit in COL_SIZE bytes.
        """
        offset = self.alloc()
        if offset is None:
            return None
        self.write(value, offset)
        return offset

    def write(self, value: bitarray, offset: int):
        """
        Set data at the offset to the given bitarray.
        """
        assert value.endian() == self.endian
        self._write_bytes(value.tobytes(), offset)

    def read(self, offset: int) -> bitarray:
        """
        Read a bitarray from the offset. Defaults to using 8 * self.COL_SIZE as
        the bitarray length.
        """
        data = self._read_bytes(offset)
        result = bitarray(endian=self.endian)
        result.frombytes(data)
        result = result[: self._num_columns]
        return result

    @property
    def page_type(self):
        return PageType.SCHEMA_ENCODING
