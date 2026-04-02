"""Tests for CouchbasePaginator."""

import pytest
from unittest.mock import MagicMock

from django_couchbase_orm.paginator import CouchbasePaginator, Page


class MockQuerySet:
    """Simple mock QuerySet for paginator tests."""

    def __init__(self, items):
        self._items = items

    def count(self):
        return len(self._items)

    def __getitem__(self, key):
        if isinstance(key, slice):
            mock_qs = MockQuerySet(self._items[key])
            mock_qs._execute = lambda: self._items[key]
            return mock_qs
        return self._items[key]

    def __iter__(self):
        return iter(self._items)

    def __len__(self):
        return len(self._items)


class TestCouchbasePaginator:
    def test_basic(self):
        items = list(range(50))
        p = CouchbasePaginator(MockQuerySet(items), per_page=10)
        assert p.count == 50
        assert p.num_pages == 5
        assert list(p.page_range) == [1, 2, 3, 4, 5]

    def test_page(self):
        items = list(range(25))
        p = CouchbasePaginator(MockQuerySet(items), per_page=10)
        page = p.page(1)
        assert len(page) == 10
        assert page.number == 1
        assert page.has_next
        assert not page.has_previous

    def test_last_page(self):
        items = list(range(25))
        p = CouchbasePaginator(MockQuerySet(items), per_page=10)
        page = p.page(3)
        assert len(page) == 5
        assert not page.has_next
        assert page.has_previous

    def test_single_page(self):
        items = list(range(5))
        p = CouchbasePaginator(MockQuerySet(items), per_page=10)
        assert p.num_pages == 1
        page = p.page(1)
        assert not page.has_next
        assert not page.has_previous
        assert not page.has_other_pages

    def test_empty(self):
        p = CouchbasePaginator(MockQuerySet([]), per_page=10)
        assert p.count == 0
        assert p.num_pages == 1
        page = p.page(1)
        assert len(page) == 0
        assert not page

    def test_page_navigation(self):
        items = list(range(30))
        p = CouchbasePaginator(MockQuerySet(items), per_page=10)
        page = p.page(2)
        assert page.next_page_number == 3
        assert page.previous_page_number == 1

    def test_next_page_on_last_raises(self):
        items = list(range(10))
        p = CouchbasePaginator(MockQuerySet(items), per_page=10)
        page = p.page(1)
        with pytest.raises(ValueError, match="No next page"):
            page.next_page_number

    def test_previous_page_on_first_raises(self):
        items = list(range(10))
        p = CouchbasePaginator(MockQuerySet(items), per_page=10)
        page = p.page(1)
        with pytest.raises(ValueError, match="No previous page"):
            page.previous_page_number

    def test_invalid_page_zero(self):
        p = CouchbasePaginator(MockQuerySet(list(range(10))), per_page=10)
        with pytest.raises(ValueError, match="at least 1"):
            p.page(0)

    def test_invalid_page_too_high(self):
        p = CouchbasePaginator(MockQuerySet(list(range(10))), per_page=10)
        with pytest.raises(ValueError, match="does not exist"):
            p.page(5)

    def test_invalid_page_string(self):
        p = CouchbasePaginator(MockQuerySet(list(range(10))), per_page=10)
        with pytest.raises(ValueError, match="integer"):
            p.page("abc")

    def test_start_end_index(self):
        items = list(range(25))
        p = CouchbasePaginator(MockQuerySet(items), per_page=10)
        page1 = p.page(1)
        assert page1.start_index == 1
        assert page1.end_index == 10
        page3 = p.page(3)
        assert page3.start_index == 21
        assert page3.end_index == 25

    def test_empty_page_indexes(self):
        p = CouchbasePaginator(MockQuerySet([]), per_page=10)
        page = p.page(1)
        assert page.start_index == 0
        assert page.end_index == 0

    def test_repr(self):
        p = CouchbasePaginator(MockQuerySet(list(range(30))), per_page=10)
        page = p.page(2)
        assert "Page 2 of 3" in repr(page)

    def test_iter(self):
        items = [1, 2, 3]
        p = CouchbasePaginator(MockQuerySet(items), per_page=10)
        page = p.page(1)
        assert list(page) == [1, 2, 3]

    def test_per_page_minimum(self):
        p = CouchbasePaginator(MockQuerySet(list(range(10))), per_page=0)
        assert p.per_page == 1

    def test_count_cached(self):
        qs = MockQuerySet(list(range(10)))
        p = CouchbasePaginator(qs, per_page=5)
        _ = p.count
        _ = p.count  # second call should use cache
        assert p._count == 10
