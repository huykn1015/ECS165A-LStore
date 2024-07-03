from tempfile import TemporaryDirectory
from lstore.db import Database, tempdb
from lstore.query import Query


class TestIncrement:
    def test_increment(self):
        with tempdb() as db:
            test = db.create_table("test", 5, 0)
            assert "test" in db.tables

            query = Query(test)

            query.insert(0, 100, 1, 2, 3)
            query.insert(1, 105, 1, 2, 3)
            query.insert(2, 108, 1, 2, 3)

            query.increment(0, 1)
            rec = list(test.records())[0]
            assert rec.columns[1] == 101

            query.increment(1, 1)
            rec = list(test.records())[1]
            assert rec.columns[1] == 106

            query.increment(2, 2)
            rec = list(test.records())[2]
            assert rec.columns[2] == 2
