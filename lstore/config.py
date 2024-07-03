# ===== Page configuration =====
PAGE_SIZE = 4096

DATA_SIZE = 8
MAP_SIZE_BYTES = 2
MAP_PAGE_BYTES = 4
MAP_OFFSET_BYTES = 2

# ===== Record configuration =====
# Metadata columns
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_RID_COLUMN = 4

# Total number of metadata columns. Keep updated.
NUM_META_COLS = 5


# ===== PageRange configuration =====
# Max number of base pages per column.
RANGE_MAX_BP_PER_COLUMN = 16
# Max number of base records in a page range.
RANGE_MAX_BASE_RECS = 8192

# ===== PageDirectory configuration =====
# Prevent ambiguous 0 vs None for indirection pointer
BASE_RID_BEGIN = 1000
TAIL_RID_BEGIN: int = 2**63 - 1

# ===== Bufferpool configuration =====
# Whether to prefer read or write in a readers-writer lock
BUFFERPOOL_PREFER_READ = True
BUFFERPOOL_LOCK_TIMEOUT = -1  # -1 means wait indefinitely
BUFFERPOOL_EVICT_TIMEOUT = 10  # wait 10 seconds for LRU page to be evicted
# By default, allow 1 GiB worth of frames before evicting
BUFFERPOOL_MAX_FRAMES = 1 * 1024 * 1024 * 1024 // PAGE_SIZE

# ===== Database configuration =====
# max number of bytes used to encode table name size
MAX_TABLE_SIZE_INT = 2
# max length of table name
MAX_TABLE_NAME_LEN = 2 ** (MAX_TABLE_SIZE_INT * 8) - 1
# database marker
DB_MARKER = "ratiodb"

# ===== Table configuration =====
MERGE_INTERVAL = 513  # merge every 10,000 records

# ===== WAL configuration =====
COL_ENCODING = 2
N_QUERY_ENCODING = 8
