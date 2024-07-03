from re import A
from lstore.db import tempdb
from lstore.query import Query
from lstore.record import Record

from random import randint, seed


class TestInsert:
    def test_insert(self):
        with tempdb() as db:
            grades = db.create_table("Grades", 5, 0)
            query = Query(grades)
            columns = [42, 1, 2, 3, 4]
            assert query.insert(*columns)
            assert len(grades.page_directory._range_last_bp) == 1
            rec = next(grades.records())
            assert rec.columns == columns

    def test_insert_many_cols(self):
        with tempdb() as db:
            grades = db.create_table("Grades", 100, 0)
            query = Query(grades)
            assert query.insert(*(i for i in range(100)))
            assert query.insert(*(i for i in range(1, 101)))
            assert query.insert(*(i for i in range(2, 102)))
            assert sum(1 for _ in grades.records()) == 3

    def test_insert_dupe_key(self):
        with tempdb() as db:
            grades = db.create_table("Grades", 5, 0)
            query = Query(grades)
            assert query.insert(*(i for i in range(5)))
            assert not query.insert(*(i for i in range(5)))

    def test_insert_bench(self):
        # From __main__.py, changed number_of_records to 10000
        with tempdb() as db:
            grades = db.create_table("Grades", 5, 0)
            query = Query(grades)
            records = {}
            number_of_records = 10000
            seed(3562901)
            for _ in range(0, number_of_records):
                key = 92106429 + randint(0, number_of_records)
                # skip duplicate keys
                while key in records:
                    key = 92106429 + randint(0, number_of_records)
                records[key] = [
                    key,
                    randint(0, 20),
                    randint(0, 20),
                    randint(0, 20),
                    randint(0, 20),
                ]
                assert query.insert(*records[key])
            assert sum(1 for _ in grades.records()) == number_of_records


class TestUpdate:
    def test_update(self):
        with tempdb() as db:
            grades = db.create_table("Grades", 5, 0)
            query = Query(grades)
            columns = [42, 1, 2, 3, 4]
            assert query.insert(*columns)
            assert len(grades.page_directory._range_last_bp) == 1
            rec = next(grades.records())
            assert rec.columns == columns

            columns = [None, None, 2, 3, 5]
            assert query.update(42, *columns)
            rec = next(grades.records())
            columns = [42, 1, 2, 3, 5]
            assert rec.columns == columns

            columns = [42, 3, 2, 3, 4]
            assert query.update(42, *columns)
            rec = next(grades.records())
            assert rec.columns[1] == 3


class TestSelect:
    def test_select(self):
        with tempdb() as db:
            grades = db.create_table("Grades", 5, 0)
            query = Query(grades)
            for i in range(10):
                columns = [i, 2 * i, 3 * i, 4 * i, 5 * i]
                assert query.insert(*columns)
                columns[0] += 100
                assert query.insert(*columns)
            assert len(grades.page_directory._con_bp_last_con_tp) == 1
            assert query.select(4, 1, [1, 1, 1, 1, 1])[0].columns == [2, 4, 6, 8, 10]
            assert query.select(4, 1, [1, 1, 1, 1, 1])[1].columns == [102, 4, 6, 8, 10]
            assert query.select(45, 4, [1, 1, 0, 0, 1])[0].columns == [9, 18, 45]
            assert query.select(45, 4, [1, 1, 1, 0, 1])[1].columns == [109, 18, 27, 45]

    def test_select_none_found(self):
        with tempdb() as db:
            grades = db.create_table("Grades", 5, 0)
            query = Query(grades)
            for i in range(10):
                columns = [i, 2 * i, 3 * i, 4 * i, 5 * i]
                assert query.insert(*columns)
                columns[0] += 100
                assert query.insert(*columns)
            assert len(grades.page_directory._con_bp_last_con_tp) == 1
            assert len(query.select(5, 1, [1, 1, 1, 1, 1])) == 0
            assert len(query.select(40, 0, [1, 1, 0, 1, 1])) == 0


class TestSum:
    def test_sum(self):
        with tempdb() as db:
            grades = db.create_table("Grades", 5, 0)
            query = Query(grades)
            for i in range(10):
                columns = [i, 2 * i, 3 * i, 4 * i, 5 * i]
                assert query.insert(*columns)
            assert query.sum(0, 4, 0) == 10
            assert query.sum(0, 8, 1) == 72
            assert query.sum(0, 8, 2) == 108
            assert query.sum(0, 9, 3) == 180
            assert query.sum(0, 3, 4) == 30

    def test_sum_none_in_range(self):
        with tempdb() as db:
            grades = db.create_table("Grades", 5, 0)
            query = Query(grades)
            for i in range(10):
                columns = [i, 2 * i, 3 * i, 4 * i, 5 * i]
                assert query.insert(*columns)
            assert query.sum(11, 20, 0) == False
            assert query.sum(-13, -1, 0) == False
            assert query.sum(0, 10, 5) == False
            assert query.sum(-1241, 424, 14) == False
            assert query.sum(-1241, 424, -13) == False

    def test_sum_exceed_range(self):
        with tempdb() as db:
            grades = db.create_table("Grades", 5, 0)
            query = Query(grades)
            for i in range(10):
                columns = [i, 2 * i, 3 * i, 4 * i, 5 * i]
                assert query.insert(*columns)
            assert query.sum(5, 134143, 0) == 35
            assert query.sum(0, 1141412321, 1) == 90
            assert query.sum(-314141, 1141412321, 2) == 135
            assert query.sum(-1241, 8, 0) == 36
