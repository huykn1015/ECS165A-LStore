from lstore.page import DataPage, Page, PageId
import pytest

# NOTE: can't test page directly without mock bufferpool

# class TestDataPage:
#     def test_write_once(self):
#         p = DataPage(PageId("a", 0, True, 0))
#         assert p.add(1) == 0
#         assert p.num_records == 1
#         assert p.has_capacity()

#     def test_read(self):
#         p = DataPage()
#         assert p.add(1) == 0
#         assert p.read(0) == 1

#         assert p.add(2) == 1
#         assert p.read(1) == 2

#     def test_write_multiple(self):
#         p = DataPage()
#         for i in range(10):
#             assert p.add(i * 2 + 3) == i
#             assert p.num_records == i + 1
#             assert p.has_capacity()

#     def test_negative(self):
#         p = DataPage()
#         assert p.add(-42) == 0
#         assert p.read(0) == -42

#     def test_overflow(self):
#         p = DataPage()
#         assert p.add(1) == 0
#         with pytest.raises(OverflowError):
#             assert p.add(111111111111111111111111) == False
