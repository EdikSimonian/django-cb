"""Django-style Paginator for Couchbase QuerySets."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from django_cb.queryset.queryset import QuerySet


class Page:
    """A single page of results from a Paginator."""

    def __init__(self, object_list: list, number: int, paginator: CouchbasePaginator):
        self.object_list = object_list
        self.number = number
        self.paginator = paginator

    def __repr__(self) -> str:
        return f"<Page {self.number} of {self.paginator.num_pages}>"

    def __len__(self) -> int:
        return len(self.object_list)

    def __iter__(self):
        return iter(self.object_list)

    def __bool__(self) -> bool:
        return len(self.object_list) > 0

    @property
    def has_next(self) -> bool:
        return self.number < self.paginator.num_pages

    @property
    def has_previous(self) -> bool:
        return self.number > 1

    @property
    def has_other_pages(self) -> bool:
        return self.has_next or self.has_previous

    @property
    def next_page_number(self) -> int:
        if not self.has_next:
            raise ValueError("No next page.")
        return self.number + 1

    @property
    def previous_page_number(self) -> int:
        if not self.has_previous:
            raise ValueError("No previous page.")
        return self.number - 1

    @property
    def start_index(self) -> int:
        if not self.object_list:
            return 0
        return (self.number - 1) * self.paginator.per_page + 1

    @property
    def end_index(self) -> int:
        if not self.object_list:
            return 0
        return self.start_index + len(self.object_list) - 1


class CouchbasePaginator:
    """Paginator for Couchbase QuerySets.

    Usage:
        paginator = CouchbasePaginator(Beer.objects.filter(abv__gte=5), per_page=20)
        page = paginator.page(1)
        for beer in page:
            print(beer.name)
    """

    def __init__(self, queryset: QuerySet, per_page: int = 20):
        self.queryset = queryset
        self.per_page = max(1, per_page)
        self._count: int | None = None

    @property
    def count(self) -> int:
        """Total number of objects across all pages."""
        if self._count is None:
            self._count = self.queryset.count()
        return self._count

    @property
    def num_pages(self) -> int:
        """Total number of pages."""
        if self.count == 0:
            return 1
        return math.ceil(self.count / self.per_page)

    @property
    def page_range(self) -> range:
        """Range of valid page numbers (1-indexed)."""
        return range(1, self.num_pages + 1)

    def page(self, number: int) -> Page:
        """Return a Page object for the given 1-indexed page number."""
        number = self._validate_number(number)
        offset = (number - 1) * self.per_page
        results = list(self.queryset[offset : offset + self.per_page])
        return Page(results, number, self)

    def _validate_number(self, number) -> int:
        """Validate and coerce the page number."""
        try:
            number = int(number)
        except (TypeError, ValueError):
            raise ValueError(f"Page number must be an integer, got {number!r}.")
        if number < 1:
            raise ValueError("Page number must be at least 1.")
        if number > self.num_pages:
            raise ValueError(f"Page {number} does not exist. Max is {self.num_pages}.")
        return number
