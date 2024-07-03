from lstore import config
from lstore.config import (
    INDIRECTION_COLUMN,
    NUM_META_COLS,
    RID_COLUMN,
    SCHEMA_ENCODING_COLUMN,
    TIMESTAMP_COLUMN,
)


from bitarray import bitarray


class Record:
    def __init__(self, key_col: int, raw_columns: list = []):
        # 0 means invalid (no record will ever have 0 as base RID due to the offset)
        self._base_rid: int = 0
        self.key_col = key_col
        # Make sure that the metadata columns are there
        if len(raw_columns) < NUM_META_COLS:
            raw_columns.extend((NUM_META_COLS - len(raw_columns)) * [0])
        self.raw_columns = raw_columns
        if not isinstance(self.schema_encoding, bitarray):
            self.schema_encoding = bitarray()
        self._is_deleted = False

    @property
    def columns(self) -> list[int]:
        """User-defined data columns."""
        return self.raw_columns[NUM_META_COLS:]

    @columns.setter
    def columns(self, value: list):
        """Allow setting data columns directly (without touching metadata)."""
        self.raw_columns[NUM_META_COLS:] = value

    @property
    def key(self) -> int:
        """Key of the record."""
        return self.columns[self.key_col]

    @property
    def indirection(self) -> int | None:
        indirection = self.raw_columns[INDIRECTION_COLUMN]
        return indirection if indirection != 0 else None

    @indirection.setter
    def indirection(self, rid: int | None):
        self.raw_columns[INDIRECTION_COLUMN] = rid if rid else 0

    @property
    def rid(self) -> int:
        """
        RID of record (may be a tail record RID, which is not globally unique).
        """
        return self.raw_columns[RID_COLUMN]

    @rid.setter
    def rid(self, value: int):
        self.raw_columns[RID_COLUMN] = value

    @property
    def timestamp(self) -> int:
        return self.raw_columns[TIMESTAMP_COLUMN]

    @timestamp.setter
    def timestamp(self, value: int):
        self.raw_columns[TIMESTAMP_COLUMN] = value

    @property
    def schema_encoding(self) -> bitarray:
        return self.raw_columns[SCHEMA_ENCODING_COLUMN]

    @schema_encoding.setter
    def schema_encoding(self, value: bitarray):
        self.raw_columns[SCHEMA_ENCODING_COLUMN] = value

    @property
    def base_rid(self) -> int:
        """Base RID of this record. Could be None if it wasn't set correctly."""
        return self.raw_columns[config.BASE_RID_COLUMN]

    @base_rid.setter
    def base_rid(self, rid: int):
        self.raw_columns[config.BASE_RID_COLUMN] = rid

    @property
    def is_deleted(self) -> bool:
        # return not self.is_base_record and self.schema_encoding.count(0) == len(
        #     self.schema_encoding
        # )
        return self._is_deleted

    @is_deleted.setter
    def is_deleted(self, value: bool):
        self._is_deleted = value

    @property
    def is_base_record(self) -> bool:
        return self.rid == self.base_rid

    def filter_columns(self, projected_column_indices: list[int]):
        """
        Remove the columns that are not needed. This should be
        deprecated from milestone 2 onward so that we aren't reading
        unneeded columns.
        """
        result = []
        for value, flag in zip(self.columns, projected_column_indices):
            if flag != 1:
                continue
            result.append(value)
        self.columns = result
