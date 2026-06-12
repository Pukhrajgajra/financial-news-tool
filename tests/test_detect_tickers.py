"""Unit tests for scraper.detect_tickers — the keyword-based ticker tagger."""
import pytest

from scraper import detect_tickers


@pytest.mark.parametrize(
    "text, expected",
    [
        ("Apple unveils new iPhone at fall event", "AAPL"),
        ("Tesla recalls Cybertrucks over software glitch", "TSLA"),
        ("BREAKING: Microsoft Azure outage hits users", "MSFT"),
        ("NVIDIA's new Blackwell GPU sells out", "NVDA"),
        ("Jerome Powell signals a pause on interest rate hikes", "FED"),
        ("The S&P 500 closed at a record high on Wall Street", "SPY"),
    ],
)
def test_single_ticker_detected(text, expected):
    assert detect_tickers(text) == expected


def test_multiple_tickers_follow_dict_order():
    # AAPL is defined before TSLA in TICKER_KEYWORDS, so the order is deterministic.
    assert detect_tickers("Both Apple and Tesla shares climbed today") == "AAPL,TSLA"


def test_no_match_returns_general():
    assert detect_tickers("Local bakery wins community award") == "GENERAL"


def test_matching_is_case_insensitive():
    assert detect_tickers("APPLE INC RELEASES A NEW IPHONE") == "AAPL"


def test_ev_keyword_is_space_guarded():
    # ' ev ' carries deliberate surrounding spaces so it doesn't fire on words
    # that merely contain the letters 'ev', like 'developers' or 'seven'.
    assert detect_tickers("Developers gathered for the conference") == "GENERAL"


@pytest.mark.xfail(
    strict=True,
    reason="bare 'meta' keyword substring-matches unrelated words like 'metabolism'; "
    "a known limitation of naive keyword matching (would need word-boundary regex)",
)
def test_meta_does_not_match_metabolism():
    # Documents a real false positive: today this returns 'META'. If the matcher is
    # ever upgraded to word boundaries, this test will pass and the strict xfail will
    # flag it (XPASS -> failure), prompting removal of the marker.
    assert detect_tickers("metabolism research published in journal") == "GENERAL"
