from tempfile import TemporaryDirectory
import pytest
from lstore.db import Database, tempdb
from lstore.index import RangedIndex, UniqueIndex, UniqueKeyViolationError
from lstore.query import Query
from lstore import config


def rid(seq: int):
    return seq + config.BASE_RID_BEGIN


class TestCreateIndex:
    def test_create_pk_index(self):
        with tempdb() as db:
            grades = db.create_table("grades", 5, 0)
            assert isinstance(grades.index.indices[0], UniqueIndex)

    def test_dupe_key(self):
        with tempdb() as db:
            grades = db.create_table("grades", 5, 0)
            query = Query(grades)
            assert query.insert(42, 100, 99, 98, 97)
            assert grades.index.indices[0]
            with pytest.raises(UniqueKeyViolationError):
                grades.index.indices[0].add(42, 1001)

    def test_create_nonkey_index(self):
        with tempdb() as db:
            grades = db.create_table("grades", 5, 0)
            query = Query(grades)

            assert query.insert(0, 100, 99, 98, 97)  # rid(0)
            assert query.insert(2, 88, 72, 82, 99)  # rid(1)
            assert query.insert(1, 52, 72, 82, 99)  # rid(2)

            grades.index.create_index(1)
            assert isinstance(grades.index.indices[1], RangedIndex)
            assert not isinstance(grades.index.indices[1], UniqueIndex)
            assert grades.index.indices[1] is not None
            assert grades.index.indices[1].locate(100) == [rid(0)]
            assert grades.index.indices[1].locate(52) == [rid(2)]
            assert grades.index.indices[1].locate_range(60, 100) == [rid(1), rid(0)]
            assert grades.index.indices[1].locate_range(40, 90) == [rid(2), rid(1)]

    def test_remove_key(self):
        with tempdb() as db:
            grades = db.create_table("grades", 5, 0)
            query = Query(grades)

            assert query.insert(0, 100, 99, 98, 97)  # rid(0)
            assert query.insert(2, 88, 72, 82, 99)  # rid(1)
            assert query.insert(1, 52, 72, 82, 99)  # rid(2)

            assert grades.index.indices[0]
            assert grades.index.indices[0].locate(0) == [rid(0)]
            grades.index.indices[0].remove(0, None)
            assert grades.index.indices[0].locate(0) == []
