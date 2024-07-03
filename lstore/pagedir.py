from collections import defaultdict
import copy
from typing import TYPE_CHECKING
import pickle
import os
from bitarray import bitarray
from lstore import config
from lstore.page import DataPage, PageId, PageLocation, SchemaEncodingPage, columns_of
from lstore.rwlock import WritePreferringRWLock

if TYPE_CHECKING:
    from lstore.table import Table


class PageDirectory:
    """Track all pages and base record locations for a table."""

    def __init__(
        self,
        table: "Table",
        load: bool = False,
    ) -> None:
        self.table = table

        self._num_raw_cols = table.num_columns + config.NUM_META_COLS

        # Unused conceptual page indices (used for PageId.page_index).
        # Each page is only uesd for a single page range.
        # Physical pages that belong to the same conceptual page share the
        # same page_index.
        self._unused_tp_idx = 0
        self._unused_bp_idx = 0

        # Queue of full conceptual pages to be merged
        # [tp1, tp2, tp3, ...]
        # Latest stable pages are added at the end
        # Use list(columns_of(page_id)) to get data and metadata columns for a
        # conceptual page.
        self._merge_queue: list[PageId] = []

        # The last conceptual base page allocated for each page range
        # Mapping from page range index to conceptual base page index
        self._range_last_bp: dict[int, int] = {}
        # The last conceptual tail page allocated for a conceptual base page index
        # Mapping from conceptual base page index to conceptual tail page index
        self._con_bp_last_con_tp: dict[int, int | None] = {}
        # TODO: maybe merge bp and tail dicts into their own data structure?
        # Maps conceptual tail page index to conceptual base page index
        self._con_tp_owner: dict[int, int] = {}
        # Number of resolved records in a conceptual base page
        self._con_bp_num_resolved: dict[int, int] = defaultdict(int)
        # Number of resolved records in a conceptual tail page
        self._con_tp_num_resolved: dict[int, int] = defaultdict(int)

        # mapping of tail RID to page offsets (data columns only)
        # metadata offsets are calculated on-demand
        self._offsets: dict[int, list[PageLocation]] = {}

        # RID allocation tracking
        self._base_rid_begin = config.BASE_RID_BEGIN
        self._unused_base_rid = self._base_rid_begin
        self._tail_rid_begin: int = config.TAIL_RID_BEGIN
        self._unused_tail_rid = self._tail_rid_begin

        # track capacities of physical pages
        self._num_records: dict[PageId, int] = defaultdict(int)
        # track capacities of conceptual pages
        self._con_bp_num_records: dict[int, int] = defaultdict(int)
        self._con_tp_num_records: dict[int, int] = defaultdict(int)
        # records per physical page when storing schema encodings
        self._se_recs_per_page = int(
            config.PAGE_SIZE // int((self.table.num_columns + 7) // 8)
        )
        # records per physical page when storing regular data (ints)
        self._data_recs_per_page = int(config.PAGE_SIZE // config.DATA_SIZE)
        # maximum size of a conceptual page
        self._con_page_max_recs = min(self._se_recs_per_page, self._data_recs_per_page)
        # TPS for each base RID
        self._tps: dict[int, int] = {}

        # Page directory modification has single-threaded performance... Oh well...
        self._lock = WritePreferringRWLock()

        if load:
            self.load()

    def clear_merge_queue(self) -> list[PageId]:
        with self._lock.write():
            mq = self._merge_queue.copy()
            self._merge_queue.clear()
            return mq

    def notify_resolve(self, affected_rids: list[int]):
        """
        Mark RIDs as resolved (either committed or aborted) and update the merge
        queue accordingly.
        """
        with self._lock.write():
            for rid in affected_rids:
                self.__mark_resolved(rid)

    def __mark_resolved(self, rid: int):
        page_id = self._offsets[rid][0]
        if page_id.is_base:
            # Only increment resolve count.
            self._con_bp_num_resolved[page_id.page_index] += 1
            return

        self._con_tp_num_resolved[page_id.page_index] += 1

        # Check if tail page is stable
        tp_idx = page_id.page_index
        bp_idx = self._con_tp_owner[page_id.page_index]
        if self._con_bp_num_resolved[bp_idx] != self._con_page_max_recs:
            # Base page not stable
            return
        if self._con_tp_num_resolved[tp_idx] != self._con_page_max_recs:
            # Tail page not stable
            return
        # TODO: To optimize a bit, we can also delete tail page resolve count.
        self._merge_queue.append(page_id.id())

    def has_capacity(self, page_id: PageId):
        """
        Returns whether or not the conceptual page (that consists the physical
        page_id) has capacity.
        """
        with self._lock.read():
            return self.__has_capacity(page_id)

    def __has_capacity(self, page_id: PageId):
        """
        Returns whether or not the conceptual page (that consists the physical
        page_id) has capacity.
        """
        if page_id.is_base:
            return (
                self._con_bp_num_records[page_id.page_index] < self._con_page_max_recs
            )
        else:
            return (
                self._con_tp_num_records[page_id.page_index] < self._con_page_max_recs
            )

    def set_num_records(self, page_id: PageId, value: int):
        """Used by Page"""
        # Don't really need to care about thread-safety for incrementing, since
        # no two threads would possess modify a page at the same time.
        with self._lock.write():
            self.__set_num_records(page_id, value)

    def __set_num_records(self, page_id: PageId, value: int):
        # Don't really need to care about thread-safety for incrementing, since
        # no two threads would possess modify a page at the same time.
        assert value <= self._con_page_max_recs
        self._num_records[page_id] = value
        if page_id.is_base:
            prev_con_size = self._con_bp_num_records[page_id.page_index]
            self._con_bp_num_records[page_id.page_index] = max(value, prev_con_size)
        else:
            prev_con_size = self._con_tp_num_records[page_id.page_index]
            self._con_tp_num_records[page_id.page_index] = max(value, prev_con_size)

    def get_num_records(self, page_id: PageId):
        """Used by Page"""
        with self._lock.read():
            return self.__get_num_records(page_id)

    def __get_num_records(self, page_id: PageId):
        """Used by Page"""
        return self._num_records[page_id]

    def get_data_page(self, page_id: PageId, data: bytearray | memoryview) -> DataPage:
        return DataPage(page_id, data, self)

    def get_schema_encoding_page(
        self, page_id: PageId, data: bytearray | memoryview
    ) -> SchemaEncodingPage:
        return SchemaEncodingPage(page_id, self.table.num_columns, data, self)

    def is_base_rec(self, rid: int) -> bool:
        return self._base_rid_begin <= rid < self._unused_base_rid

    def get_base_record_location(self, base_rid: int) -> tuple[int, list[PageLocation]]:
        """
        Retrieve page offsets associated with page record (including metadata
        columns).
        """
        with self._lock.read():
            if not (self._base_rid_begin <= base_rid < self._unused_base_rid):
                raise ValueError("Invalid base RID")
            return (self._tps[base_rid], copy.deepcopy(self._offsets[base_rid]))

    def get_tail_record_location(self, tail_rid: int) -> list[PageLocation]:
        with self._lock.read():
            if not (self._tail_rid_begin >= tail_rid > self._unused_tail_rid):
                raise ValueError("Invalid tail RID")
            return copy.deepcopy(self._offsets[tail_rid])

    def notify_merge(self, base_rid: int, offsets: list[PageLocation], tps: int):
        """Notify the page directory of a merged base page."""
        with self._lock.write():
            assert len(offsets) == self.table.num_columns
            self._offsets[base_rid][config.NUM_META_COLS :] = copy.deepcopy(offsets)
            self._tps[base_rid] = tps

    def alloc_base_rid(self) -> tuple[int, list[PageLocation]]:
        """Return a new base RID and a list of writable page offsets (metadata included)."""
        with self._lock.write():
            # allocate base RID
            rid = self._unused_base_rid
            self._unused_base_rid += 1
            # calculate page range ID for the base RID
            base_rec_idx = rid - config.BASE_RID_BEGIN
            range_id = int(base_rec_idx // self._con_page_max_recs)  # TODO:
            # ensure range exists (create entry in _page_ranges if needed)
            self.__ensure_range_exists(range_id)
            # check if the conceptual base page has enough capacity
            bp_idx = self.__get_range_bp_idx(range_id)
            offsets = self.__alloc_bp_space(bp_idx)
            self._offsets[rid] = offsets
            self._tps[rid] = self._tail_rid_begin + 1
            return (rid, offsets)

    def __ensure_range_exists(self, range_id: int):
        """
        Makes sure that self._page_ranges have entry for range_id.
        Allocates base/tail page index as needed.
        """
        if range_id not in self._range_last_bp:
            self._range_last_bp[range_id] = self._unused_bp_idx
            self._unused_bp_idx += 1
        if range_id not in self._con_bp_last_con_tp:
            self._con_bp_last_con_tp[range_id] = None

    def __get_range_bp_idx(self, range_id: int) -> int:
        """Get the next non-full base page index"""
        bp_idx = self._range_last_bp[range_id]
        if self._con_bp_num_records[bp_idx] < self._con_page_max_recs:
            return bp_idx
        else:
            bp_idx = self._unused_bp_idx
            self._unused_bp_idx += 1
            self._range_last_bp[range_id] = bp_idx
            return bp_idx

    def __alloc_bp_space(self, bp_idx: int) -> list[PageLocation]:
        """Allocate space (+1 record) for all columns (metadata included)"""
        start = PageLocation(self.table.name, 0, True, bp_idx, 0)
        offsets = []
        for bp in columns_of(start.id(), self.table.num_raw_cols):
            n = self._num_records[bp]
            self.__set_num_records(bp, n + 1)
            offsets.append(PageLocation.from_id(bp, n))
        return offsets

    def alloc_merge_locations(self) -> list[PageLocation]:
        """Allocate page offsets (data columns only) for merging a base page"""
        with self._lock.write():
            bp_idx = self._unused_bp_idx
            self._unused_bp_idx += 1
            start = PageId(self.table.name, 0, True, bp_idx)
            offsets = []
            for bp in columns_of(start, self.table.num_raw_cols):
                self.__set_num_records(bp, self._con_page_max_recs)
                if bp.raw_column_index < config.NUM_META_COLS:
                    continue
                offsets.append(PageLocation.from_id(bp, 0))
            return offsets

    def alloc_tail_rid(
        self, base_rid: int, schema_encoding: bitarray
    ) -> tuple[int, list[PageLocation]]:
        """Return a new tail RID and a list of writable page offsets."""
        with self._lock.write():
            # allocate tail RID
            rid = self._unused_tail_rid
            if rid % config.MERGE_INTERVAL == config.MERGE_INTERVAL - 1:
                self.table.notify_merge()
            self._unused_tail_rid -= 1
            # calculate page range ID for the base RID
            base_rec_idx = base_rid - config.BASE_RID_BEGIN
            range_id = int(base_rec_idx // self._con_page_max_recs)
            # ensure range exists (create entry in _page_ranges if needed)
            self.__ensure_range_exists(range_id)
            # allocate tp index
            bp_idx = self._offsets[base_rid][0].page_index
            tp_idx = self.__get_tp_idx(bp_idx)
            offsets = self.__alloc_tp_space(tp_idx, schema_encoding)
            self._offsets[rid] = offsets
            return (rid, offsets)

    def is_aborted_xact(self, timestamp: int):
        if timestamp == 0:
            return False
        return self.table.xact_tracker.is_aborted(timestamp)

    def is_pending_xact(self, timestamp: int):
        return self.table.xact_tracker.is_maybe_pending(timestamp)

    def __alloc_tp_space(
        self, tp_idx: int, schema_encoding: bitarray
    ) -> list[PageLocation]:
        """Allocate space (+1 record) for all columns (metadata included)"""
        start = PageLocation(self.table.name, 0, False, tp_idx, 0)
        offsets = []
        for tp in columns_of(start.id(), self.table.num_raw_cols):
            raw_col_idx = tp.raw_column_index
            col_idx = raw_col_idx - config.NUM_META_COLS
            if raw_col_idx >= config.NUM_META_COLS and schema_encoding[col_idx] == 0:
                continue
            n = self.__get_num_records(tp)
            self.__set_num_records(tp, n + 1)
            offsets.append(PageLocation.from_id(tp, n))
        return offsets

    def __get_tp_idx(self, bp_idx: int) -> int:
        """Get the next non-full tail page index"""
        tp_idx = self._con_bp_last_con_tp[bp_idx]
        if (
            tp_idx is not None
            and self._con_tp_num_records[tp_idx] < self._con_page_max_recs
        ):
            return tp_idx
        else:
            tp_idx = self._unused_tp_idx

            self._unused_tp_idx += 1
            self._con_bp_last_con_tp[bp_idx] = tp_idx
            self._con_tp_owner[tp_idx] = bp_idx
            return tp_idx

    def load(self):
        path = os.path.join(self.table.path, "")
        # READ THESE VALUES FROM TXT FILE

        if not os.path.exists(path + "values.txt"):
            return

        with open(path + "values.txt", "r") as infile:
            lines = infile.readlines()
            self._unused_tp_idx = int(lines[0])
            self._unused_bp_idx = int(lines[1])
            self._unused_base_rid = int(lines[2])
            self._unused_tail_rid = int(lines[2])

        # READ THESE VALUES FROM PICKLE FILES
        # track capacities of physical pages
        with open(path + "num_records.pickle", "rb") as infile:
            self._num_records = pickle.load(infile)
        with open(path + "offsets.pickle", "rb") as infile:
            self._offsets = pickle.load(infile)
        with open(path + "con_bp_num_records.pickle", "rb") as infile:
            self._con_bp_num_records = pickle.load(infile)
        with open(path + "con_tp_num_records.pickle", "rb") as infile:
            self._con_tp_num_records = pickle.load(infile)
        with open(path + "tps.pickle", "rb") as infile:
            self._tps = pickle.load(infile)
        with open(path + "merge_queue.pickle", "rb") as infile:
            self._merge_queue = pickle.load(infile)
        with open(path + "range_last_bp.pickle", "rb") as infile:
            self._range_last_bp = pickle.load(infile)
        with open(path + "con_bp_last_con_tp.pickle", "rb") as infile:
            self._con_bp_last_con_tp = pickle.load(infile)
        with open(path + "con_bp_num_resolved.pickle", "rb") as infile:
            self._con_bp_num_resolved = pickle.load(infile)
        with open(path + "con_tp_num_resolved.pickle", "rb") as infile:
            self._con_tp_num_resolved = pickle.load(infile)

    def save(self):
        path = os.path.join(self.table.path, "")
        with self._lock.read():
            with open(path + "values.txt", "w") as outfile:
                outfile.write(str(self._unused_tp_idx) + "\n")
                outfile.write(str(self._unused_bp_idx) + "\n")
                outfile.write(str(self._unused_base_rid) + "\n")
                outfile.write(str(self._unused_tail_rid))
            with open(path + "num_records.pickle", "wb") as outfile:
                pickle.dump(self._num_records, outfile)
            with open(path + "offsets.pickle", "wb") as outfile:
                pickle.dump(self._offsets, outfile)
            with open(path + "con_bp_num_records.pickle", "wb") as outfile:
                pickle.dump(self._con_bp_num_records, outfile)
            with open(path + "con_tp_num_records.pickle", "wb") as outfile:
                pickle.dump(self._con_tp_num_records, outfile)
            with open(path + "tps.pickle", "wb") as outfile:
                pickle.dump(self._tps, outfile)
            with open(path + "merge_queue.pickle", "wb") as outfile:
                pickle.dump(self._merge_queue, outfile)
            with open(path + "range_last_bp.pickle", "wb") as outfile:
                pickle.dump(self._range_last_bp, outfile)
            with open(path + "con_bp_last_con_tp.pickle", "wb") as outfile:
                pickle.dump(self._con_bp_last_con_tp, outfile)
            with open(path + "con_bp_num_resolved.pickle", "wb") as outfile:
                pickle.dump(self._con_bp_num_resolved, outfile)
            with open(path + "con_tp_num_resolved.pickle", "wb") as outfile:
                pickle.dump(self._con_tp_num_resolved, outfile)

    def serialize(self) -> bytes:
        # TODO: implement
        with self._lock.read():
            raise NotImplementedError()

    @staticmethod
    def deserialize() -> "PageDirectory":
        # TODO: implement
        raise NotImplementedError()

    def base_rids(self):
        yield from range(self._base_rid_begin, self._unused_base_rid)
