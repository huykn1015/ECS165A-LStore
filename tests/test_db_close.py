from tempfile import TemporaryDirectory, mkdtemp
from lstore.db import Database


class TestDBClose:
    def test_db_close(self):
        with TemporaryDirectory() as tmpdir:
            db = Database()
            db.open(tmpdir)
            assert db.create_table("Grades", 5, 0)
            assert "Grades" in db.tables
            assert db.create_table("Grade", 10, 3)
            assert "Grade" in db.tables
            assert db.create_table("Grad", 6, 1)
            assert "Grad" in db.tables
            assert db.create_table("Gra", 16, 12)
            assert "Gra" in db.tables
            db.close()
            db.open(tmpdir)
            assert "Grades" in db.tables
            assert "Grade" in db.tables
            assert "Grad" in db.tables
            assert "Gra" in db.tables

            assert db.tables["Grades"].name == "Grades"
            assert db.tables["Grades"].num_columns == 5
            assert db.tables["Grades"].key == 0

            assert db.tables["Grade"].name == "Grade"
            assert db.tables["Grade"].num_columns == 10
            assert db.tables["Grade"].key == 3

            assert db.tables["Grad"].name == "Grad"
            assert db.tables["Grad"].num_columns == 6
            assert db.tables["Grad"].key == 1

            assert db.tables["Gra"].name == "Gra"
            assert db.tables["Gra"].num_columns == 16
            assert db.tables["Gra"].key == 12

            db.close()
