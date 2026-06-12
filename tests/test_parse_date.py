"""Unit tests for stock_analyzer.parse_date — robust article-date parsing."""
from datetime import date, datetime

from stock_analyzer import parse_date


def test_rfc822_gmt():
    assert parse_date("Wed, 29 Apr 2026 15:18:00 GMT") == date(2026, 4, 29)


def test_iso_with_microseconds():
    assert parse_date("2026-04-29 15:40:27.846215") == date(2026, 4, 29)


def test_plain_iso_date():
    assert parse_date("2026-04-29") == date(2026, 4, 29)


def test_datetime_object_passthrough():
    assert parse_date(datetime(2026, 4, 29, 15, 0)) == date(2026, 4, 29)


def test_date_object_passthrough():
    assert parse_date(date(2026, 4, 29)) == date(2026, 4, 29)


def test_unparseable_returns_none():
    # Critical: an unparseable date must NOT silently fall back to "today",
    # which would assign wrong dates to articles in the correlation analysis.
    assert parse_date("not a real date at all") is None


def test_none_input_returns_none():
    assert parse_date(None) is None
