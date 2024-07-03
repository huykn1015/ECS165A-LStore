import copy
import os
import sys
from threading import Lock, Thread
from time import time_ns
from typing import TYPE_CHECKING, Literal

from lstore.bufferpool import Bufferpool, Frame
from lstore.index import Index
from lstore.pagedir import PageDirectory

from bitarray import bitarray
from lstore import config

from lstore.record import Record

from lstore.page import (
    DataPage,
    PageId,
    PageLocation,
    columns_of,
)
from lstore.transaction_tracker import TransactionTracker

if TYPE_CHECKING:
    from lstore.wal import WriteAheadLog


class Table:
    def __init__(
        self,
        name: str,
        num_columns: int,
        key: int,
        unique_cols: set[int],
        ranged_cols: set[int],
        bufferpool: Bufferpool,
        db_path: str,
        wal: "WriteAheadLog",
        xact_tracker: TransactionTracker,
        load: bool = False,
    ):
        """
        :param string name: Table name
        :param int num_columns: Number of Columns: all columns are integer
        :param int key: Index of table key in columns
        """
        self.xact_tracker = xact_tracker
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.num_raw_cols = num_columns + config.NUM_META_COLS
        self.path = os.path.join(db_path, name)
        self.bufferpool = bufferpool
        self.page_directory = PageDirectory(self, load)
        self.index = Index(self, unique_cols, ranged_cols)
        self.wal = wal
        self.__merge_thread: None | Thread = None
        self.__allow_merge = True
        self.__merge_lock = Lock()

    def notify_merge(self):
        with self.__merge_lock:
            if not self.__allow_merge:
                return
            if self.__merge_thread is not None and self.__merge_thread.is_alive():
                return
            self.__merge_thread = Thread(target=lambda: self.__merge())
            self.__merge_thread.start()

    def drain_merge(self):
        """Disable merging and wait for any merge thread to join."""
        with self.__merge_lock:
            self.__allow_merge = False
            if self.__merge_thread is not None and self.__merge_thread.is_alive():
                self.__merge_thread.join()

    def __get_tail_pages(self, tail_page_location: list[PageId]):
        """Gets all tail pages associated with tail_page_info tuple of PageOffsets"""
        tail_pages = []
        for page_info in tail_page_location:
            with self.bufferpool.read(page_info) as data:
                if not data:
                    return False
                if page_info.raw_column_index == config.SCHEMA_ENCODING_COLUMN:
                    page = self.page_directory.get_schema_encoding_page(page_info, data)
                    tail_pages.append(page)
                    continue
                page = self.page_directory.get_data_page(page_info, data)
                tail_pages.append(page)
        return tail_pages

    def __get_base_pages(
        self,
        old_base_page_location: list[PageLocation],
        new_base_page_location: list[PageLocation],
    ):
        """Creates a copy of all base pages from base_page_info list of PageOffsets"""
        base_pages = []
        frames = []
        for col, old_page_info in enumerate(old_base_page_location):

            with self.bufferpool.read(old_page_info.id()) as data:
                if not data:
                    return False, False

                # if old_page_info.raw_column_index == config.SCHEMA_ENCODING_COLUMN:
                #     page = self.page_directory.get_schema_encoding_page(
                #         old_page_info.id(), data[:]
                #     )
                #     base_pages.append(page)
                #     continue

                # if page is not a meta data column, create copy in bufferpool
                if old_page_info.raw_column_index >= config.NUM_META_COLS:
                    frame = self.bufferpool.pin(
                        new_base_page_location[col - config.NUM_META_COLS].id()
                    )
                    buffer = frame.acquire_write()
                    buffer[:] = data[:]
                    page = self.page_directory.get_data_page(
                        new_base_page_location[col - config.NUM_META_COLS].id(), buffer
                    )
                    frames.append(frame)
                    base_pages.append(page)
                    continue
                # page = self.page_directory.get_data_page(old_page_info.id(), data[:])
                # base_pages.append(page)
                base_pages.append(None)
        return base_pages, frames

    def __merge_page(
        self, tail_page_info, tail_pages, base_pages, b_page_offsets, rec_idx
    ):
        """Merge conceptual tail page into copy of concpetual base page"""
        # record to be merged BID and TID
        tid = tail_pages[config.RID_COLUMN].read(rec_idx)
        bid = tail_pages[config.BASE_RID_COLUMN].read(rec_idx)
        tp_offsets = self.page_directory.get_tail_record_location(tid)[
            config.NUM_META_COLS :
        ]
        # get write offsets for the record
        cumulative_schema = tail_pages[config.SCHEMA_ENCODING_COLUMN].read(rec_idx)
        # loop through data columns and write to base page copy
        write_cols = []
        for col in range(self.num_columns):
            if cumulative_schema[col] == 0:
                continue
            tp_offset = tp_offsets.pop(0)
            t_page = tail_pages[col + config.NUM_META_COLS]
            bp_offset = b_page_offsets[col + config.NUM_META_COLS]
            b_page = base_pages[col + config.NUM_META_COLS]
            assert isinstance(t_page, DataPage)
            assert isinstance(b_page, DataPage)
            # read from tail page and write into base page
            data = t_page.read(tp_offset.offset)
            write_cols.append(data)
            b_page.write(data, bp_offset.offset)

    def __merge(self):

        # set of all BID already merged this cycle
        # merge latest TID first to avoid having to do extra work merging tail records
        # that will be merged over this merge cycle
        merged_base_rid: set[int] = set()
        # identify commited tail records
        # loop throught each conceptual tail page at a time

        # go in reverse order so latest stable pages are processes first
        # tail page is tuple of pageID
        merge_queue = self.page_directory.clear_merge_queue()
        for tail_page_id in merge_queue[::-1]:
            # load pages of conceptual tail page into bufferpool
            tail_page_locations = list(columns_of(tail_page_id, self.num_raw_cols))
            tail_pages = self.__get_tail_pages(tail_page_locations)
            if not tail_pages:
                continue
            # create a copy of base pages and load into bufferpool
            bid_page = tail_pages[config.BASE_RID_COLUMN]
            tid_page = tail_pages[config.RID_COLUMN]
            assert isinstance(bid_page, DataPage)
            assert isinstance(tid_page, DataPage)
            # copy of old base pages
            max_rec_bid = bid_page.read(self.page_directory._data_recs_per_page - 1)

            # load base pages from memory
            tps, base_page_location = self.page_directory.get_base_record_location(
                max_rec_bid
            )
            new_base_page_location = self.page_directory.alloc_merge_locations()
            base_pages, frames = self.__get_base_pages(
                base_page_location, new_base_page_location
            )
            if not base_pages:
                continue
            base_page_location[config.NUM_META_COLS :] = new_base_page_location
            # combine pages into base page
            # loops from last record in conceptual tail page to the first
            for rec_idx in range(self.page_directory._data_recs_per_page - 1, -1, -1):
                merge_bid = bid_page.read(rec_idx)
                merge_tid = tid_page.read(rec_idx)

                # get base page offset for the record
                tps, base_record_location = (
                    self.page_directory.get_base_record_location(merge_bid)
                )

                # if bid has already been merged this merge cycle, skip
                if merge_bid in merged_base_rid:
                    continue
                merged_base_rid.add(merge_bid)

                # make a copy of the new base pages PageId
                new_base_record_location = copy.deepcopy(new_base_page_location)

                # copy old base page offset into new base page offset
                for i in range(self.num_columns):
                    new_base_record_location[i].offset = base_record_location[
                        i + config.NUM_META_COLS
                    ].offset
                base_record_location[config.NUM_META_COLS :] = new_base_record_location

                self.__merge_page(
                    tail_page_locations,
                    tail_pages,
                    base_pages,
                    base_record_location,
                    rec_idx,
                )
                self.page_directory.notify_merge(
                    merge_bid, base_record_location[config.NUM_META_COLS :], merge_tid
                )

            # free frames
            if frames:
                for i, frame in enumerate(frames):
                    assert isinstance(frame, Frame)
                    frame.release_write()
                    self.bufferpool.unpin(new_base_page_location[i].id())
            # self.bufferpool.checkpoint()
        # Allow Python GC to auto deallocate in-memory pages.
        # To optimize, notify bufferpool to evict outdated pages.

    def __get_base_rid(self, tid: int) -> int | Literal[False]:
        page_indx = self.page_directory.get_tail_record_location(tid)
        base_rid_info = page_indx[config.SCHEMA_ENCODING_COLUMN]
        with self.bufferpool.read(base_rid_info.id()) as data:
            if not data:
                return False
            page = self.page_directory.get_data_page(base_rid_info.id(), data)
            return page.read(base_rid_info.offset)

    def __get_tail_schema_encoding(self, tid: int) -> bitarray | Literal[False]:
        page_indx = self.page_directory.get_tail_record_location(tid)
        schema_info = page_indx[config.SCHEMA_ENCODING_COLUMN]
        with self.bufferpool.read(schema_info.id()) as data:
            if not data:
                return False
            page = self.page_directory.get_schema_encoding_page(schema_info.id(), data)
            return page.read(schema_info.offset)

    def __get_tail_timestamp(self, tid: int) -> int:
        page_indx = self.page_directory.get_tail_record_location(tid)
        timestamp_info = page_indx[config.TIMESTAMP_COLUMN]
        with self.bufferpool.read(timestamp_info.id()) as data:
            if not data:
                return False
            page = self.page_directory.get_data_page(timestamp_info.id(), data)
        try:
            timestamp = page.read(timestamp_info.offset)
            return timestamp
        except Exception:
            return 0

    def __get_tail_indirection(self, tid: int) -> int:
        page_indx = self.page_directory.get_tail_record_location(tid)
        indirection_info = page_indx[config.INDIRECTION_COLUMN]
        with self.bufferpool.read(indirection_info.id()) as data:
            if not data:
                return False
            page = self.page_directory.get_data_page(indirection_info.id(), data)
        try:
            indirection = page.read(indirection_info.offset)
            return indirection
        except Exception:
            return 0

    def add_base_record(self, rec: Record) -> Literal[False] | int:
        """Add a base record. Returns False if page range is full, or base RID otherwise."""
        base_rid, page_indx = self.page_directory.alloc_base_rid()
        if base_rid is None:
            return False
        rec.base_rid = base_rid
        rec.rid = base_rid
        rec.indirection = None
        rec.schema_encoding = bitarray([0] * self.num_columns, endian=sys.byteorder)
        raw_cols = rec.raw_columns
        for page_info in page_indx:
            with self.bufferpool.write(page_info.id()) as data:
                if not data:
                    return False
                if page_info.raw_column_index == config.SCHEMA_ENCODING_COLUMN:
                    page = self.page_directory.get_schema_encoding_page(
                        page_info.id(), data
                    )
                    page.write(rec.schema_encoding, page_info.offset)
                    continue
                page = self.page_directory.get_data_page(page_info.id(), data)
                page.write(raw_cols[page_info.raw_column_index], page_info.offset)
        return rec.base_rid

    def update_base_record(
        self, base_rid: int, indirection: int, schema_encoding: bitarray
    ) -> bool:
        tps, base_pages_indx = self.page_directory.get_base_record_location(base_rid)
        indirection_page_info = base_pages_indx[config.INDIRECTION_COLUMN]
        schema_encoding_page_info = base_pages_indx[config.SCHEMA_ENCODING_COLUMN]
        with self.bufferpool.write(indirection_page_info.id()) as data:
            if not data:
                return False
            page = self.page_directory.get_data_page(indirection_page_info.id(), data)
            page.write(indirection, indirection_page_info.offset)
        with self.bufferpool.write(schema_encoding_page_info.id()) as data:
            if not data:
                return False
            page = self.page_directory.get_schema_encoding_page(
                schema_encoding_page_info.id(), data
            )
            page.write(schema_encoding, schema_encoding_page_info.offset)
        return True

    def add_original_copy(self, rec: Record):
        if not rec.base_rid:
            return False
        schema_encoding = bitarray("1" * self.num_columns, endian=sys.byteorder)
        new_tid, page_indx = self.page_directory.alloc_tail_rid(
            rec.rid, schema_encoding
        )
        rec.indirection = rec.base_rid
        latest_cols = rec.raw_columns
        latest_cols[config.RID_COLUMN] = new_tid
        latest_cols[config.SCHEMA_ENCODING_COLUMN] = schema_encoding
        for page_info in page_indx:
            with self.bufferpool.write(page_info.id()) as data:
                if not data:
                    return False
                if page_info.raw_column_index == config.SCHEMA_ENCODING_COLUMN:
                    page = self.page_directory.get_schema_encoding_page(
                        page_info.id(), data
                    )
                    page.write(schema_encoding, page_info.offset)
                    continue
                page = self.page_directory.get_data_page(page_info.id(), data)
                page.write(latest_cols[page_info.raw_column_index], page_info.offset)
        self.update_base_record(
            rec.base_rid,
            new_tid,
            bitarray("0" * self.num_columns, endian=sys.byteorder),
        )

    def add_tail_record(self, rec: Record) -> int | Literal[False]:
        if not rec.base_rid:
            return False

        latest_rec = self.get_latest_record(rec.base_rid)
        if not latest_rec:
            raise Exception("Latest version not found")
        base_rec = self.__get_base_record(rec.base_rid)
        if not base_rec:
            raise Exception("base rec not found version not found")
        # if latest rec is base rec, create copy of base rec
        if latest_rec.rid == rec.base_rid:
            self.add_original_copy(base_rec)
            latest_rec = self.get_latest_record(rec.base_rid)
            if not latest_rec:
                raise Exception("Latest version not found")

        base_rec = self.__get_base_record(rec.base_rid)
        if not base_rec:
            raise Exception("base rec not found version not found")
        latest_cols = latest_rec.columns
        schema_encoding = bitarray(endian=sys.byteorder)
        rec.indirection = latest_rec.rid
        new_cols = rec.columns
        for i in range(len(new_cols)):
            if new_cols[i] is not None or (
                new_cols[i] != latest_cols[i] and rec.columns[i] != None
            ):
                schema_encoding.append(1)
            else:
                schema_encoding.append(0)
            if (schema_encoding[i] or base_rec.schema_encoding[i]) and new_cols[
                i
            ] is None:
                new_cols[i] = latest_cols[i]
        cumulative_schema = schema_encoding | base_rec.schema_encoding
        # for i in range(len(new_cols)):
        #     if cumulative_schema[i] == 1 and new_cols[i] is None:
        #         new_cols[i] = latest_cols[i]
        new_tid, page_indx = self.page_directory.alloc_tail_rid(
            base_rec.rid, cumulative_schema
        )

        rec.rid = new_tid
        rec.columns = new_cols
        rec.indirection = latest_rec.rid
        new_raw_cols = rec.raw_columns
        rec.schema_encoding = cumulative_schema
        for page_info in page_indx:
            with self.bufferpool.write(page_info.id()) as data:
                if not data:
                    return False
                if page_info.raw_column_index == config.SCHEMA_ENCODING_COLUMN:
                    page = self.page_directory.get_schema_encoding_page(
                        page_info.id(), data
                    )
                    page.write(cumulative_schema, page_info.offset)
                    continue
                page = self.page_directory.get_data_page(page_info.id(), data)
                page.write(new_raw_cols[page_info.raw_column_index], page_info.offset)
        self.update_base_record(rec.base_rid, new_tid, cumulative_schema)
        return rec.rid

    def __get_base_record(self, rid: int) -> Record | Literal[False]:
        tps, base_pages_indx = self.page_directory.get_base_record_location(rid)
        raw_cols = []
        for page_info in base_pages_indx:
            with self.bufferpool.read(page_info.id()) as data:
                if not data:
                    return False
            if page_info.raw_column_index == config.SCHEMA_ENCODING_COLUMN:
                page = self.page_directory.get_schema_encoding_page(
                    page_info.id(), data
                )
                raw_cols.append(
                    bitarray(page.read(page_info.offset), endian=sys.byteorder)
                )
                continue
            page = self.page_directory.get_data_page(page_info.id(), data)
            raw_cols.append(page.read(page_info.offset))
        return Record(self.key, raw_cols)

    def get_latest_record(self, base_rid: int) -> Record | Literal[False]:
        return self.get_record_version(base_rid, 0)

    def get_record_version(
        self, base_rid: int, version: int = 0
    ) -> Record | Literal[False]:
        if version > 0:
            raise ValueError("Invalid Version")
        base_rec = self.__get_base_record(base_rid)
        if not base_rec:
            return False
        # if self.page_directory.
        base_rec.base_rid = base_rid
        latest_tid = base_rec.indirection
        if (
            not latest_tid
            or latest_tid == base_rec.rid
            or latest_tid >= self.page_directory._tps[base_rid]
        ):
            return base_rec
        latest_encoding = self.__get_tail_schema_encoding(latest_tid)
        if not latest_encoding:
            raise Exception("Tail record encoding not found")
        if latest_encoding.count(0) == len(latest_encoding):
            result = Record(self.key, [None] * self.num_raw_cols)
            result.base_rid = base_rid
            result.is_deleted = True
            return result

        # loop until appropriate version
        # while (not correct version and latest tid) or if record was aborted
        latest_timestamp = self.__get_tail_timestamp(latest_tid)
        while (version < 0 and latest_tid) or self.page_directory.is_aborted_xact(
            latest_timestamp
        ):
            # if the record was merged or is a base record
            if (
                self.page_directory.is_base_rec(latest_tid)
                or latest_tid >= self.page_directory._tps[base_rid]
            ):
                return base_rec
            latest_tid = self.__get_tail_indirection(latest_tid)
            latest_timestamp = self.__get_tail_timestamp(latest_tid)
            version += 1
        if not latest_tid or latest_tid == base_rid:
            return base_rec

        rec = Record(self.key, [])
        rec.rid = latest_tid
        rec.base_rid = base_rid
        latest_cols = [0] * self.num_raw_cols
        cols_needed = base_rec.schema_encoding

        updated_column_locations = self.page_directory.get_tail_record_location(
            latest_tid
        )[config.NUM_META_COLS :]

        # get schema for columns changed this update
        for col in range(self.num_columns):
            if cols_needed[col]:
                # column was updated in current tail record, and not changed in later tail records
                page_info = updated_column_locations.pop()
                with self.bufferpool.read(page_info.id()) as data:
                    if not data:
                        return False
                    page = self.page_directory.get_data_page(page_info.id(), data)
                latest_cols[page_info.raw_column_index] = page.read(page_info.offset)
        # get remaining columns from base record
        base_cols = base_rec.columns
        for i in range(self.num_columns):
            if cols_needed[i] == 0:
                latest_cols[i + config.NUM_META_COLS] = base_cols[i]

        # set columns of return record
        rec.raw_columns = latest_cols
        rec.indirection = base_rec.indirection
        rec.base_rid = base_rid
        rec.schema_encoding = base_rec.schema_encoding
        return rec

    def delete_record(self, base_rid: int, timestamp: int) -> int | Literal[False]:
        delete_indicator = Record(self.key, [0] * self.num_raw_cols)
        delete_indicator.base_rid = base_rid
        delete_indicator.schema_encoding = bitarray(
            "0" * self.num_columns, endian=sys.byteorder
        )
        delete_indicator.timestamp = timestamp
        return self.add_tail_record(delete_indicator)

    def records(self, version=0):
        for base_rid in self.page_directory.base_rids():
            rec = self.get_record_version(base_rid, version)
            if rec and not rec.is_deleted and rec.base_rid != 0:
                yield rec
