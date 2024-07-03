from tempfile import TemporaryDirectory, mkdtemp
from lstore.db import Database, tempdb


class TestDatabase:
    def test_create_table(self):
        with tempdb() as db:
            db.create_table("test", 5, 0)
            assert "test" in db.tables

    def test_create_tables(self):
        with tempdb() as db:
            table_names = [f"test_{i}" for i in range(10)]
            for name in table_names:
                db.create_table(name, 5, 0)
            assert all(name in db.tables for name in table_names)

    def test_create_table_many_cols(self):
        with tempdb() as db:
            db.create_table("test", 100, 0)

    def test_get_table(self):
        with tempdb() as db:
            db.create_table("test", 5, 0)
            assert db.get_table("test").name == "test"

    def test_drop_table(self):
        with tempdb() as db:
            db.create_table("test", 5, 0)
            db.drop_table("test")
            assert "test" not in db.tables
