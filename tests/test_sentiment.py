"""
Smoke tests for nlp_processor.analyze_sentiment.

These check directional correctness — clearly positive text scores positive,
clearly negative text scores negative — rather than exact polarity values, so
they stay robust across TextBlob versions.

nlp_processor loads a spaCy model at import time. If that model isn't installed,
the whole module is skipped (with a reason) instead of failing the run.
"""
import pytest

try:
    from nlp_processor import analyze_sentiment
except Exception as exc:  # missing spaCy model / textblob corpora / etc.
    pytest.skip(f"nlp_processor unavailable: {exc}", allow_module_level=True)


def test_positive_text_scores_positive():
    score, label = analyze_sentiment("This is wonderful, excellent, and absolutely fantastic news")
    assert label == "positive"
    assert score > 0.1


def test_negative_text_scores_negative():
    score, label = analyze_sentiment("This is terrible, awful, and a complete disaster")
    assert label == "negative"
    assert score < -0.1


def test_neutral_text_scores_neutral():
    # No opinion words -> polarity near 0.0 -> neutral.
    score, label = analyze_sentiment("The report contains twelve numbered pages")
    assert label == "neutral"
    assert -0.1 <= score <= 0.1


def test_label_is_consistent_with_thresholds():
    # Whatever the score, the label must match the documented ±0.1 thresholds.
    score, label = analyze_sentiment("The committee will meet on the third floor")
    if score > 0.1:
        assert label == "positive"
    elif score < -0.1:
        assert label == "negative"
    else:
        assert label == "neutral"
