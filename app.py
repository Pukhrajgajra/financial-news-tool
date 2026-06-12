"""
app.py — Financial News Sentiment Dashboard
===========================================

A user-facing view over the financial-news pipeline. It answers three
questions a person watching a set of stocks actually has:

    1. What's the news mood on each ticker right now?
    2. Which headlines are driving that mood?
    3. Does positive news line up with the next day's price move?

Run it from the project root:

    pip install streamlit
    streamlit run app.py

Data source
-----------
The dashboard reads from your PostgreSQL database when it can reach it
(reusing db_config.py). If the database isn't available — e.g. a fresh
clone with nothing loaded yet — it falls back to the bundled CSV exports
so the page still renders something real. A badge at the top tells you
which source you're looking at.
"""

from __future__ import annotations

from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st
from scipy import stats

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

APP_DIR = Path(__file__).resolve().parent
CORRELATION_CSV = APP_DIR / "correlation_dashboard.csv"
RECENT_CSV = APP_DIR / "dashboard_data.csv"

# Sentiment is the whole point of the tool, so green/red here encodes meaning,
# not decoration: red = negative, grey = neutral, green = positive.
POSITIVE = "#1a7f5a"
NEGATIVE = "#c0392b"
NEUTRAL = "#8a8f98"
INK = "#14213d"
DIVERGING = "redyellowgreen"  # Altair scheme for the -1..+1 sentiment axis

LABEL_COLORS = {"positive": POSITIVE, "negative": NEGATIVE, "neutral": NEUTRAL}

# Columns the rest of the app relies on, in both data paths.
CORR_COLUMNS = [
    "ticker", "sentiment_score", "sentiment_label", "price_date",
    "close_price", "next_day_close", "next_day_pct_change", "title", "source",
]
RECENT_COLUMNS = ["title", "source", "published_at", "sentiment_score", "sentiment_label"]


# --------------------------------------------------------------------------- #
# Data loading
# --------------------------------------------------------------------------- #

def _load_from_db() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Pull both frames from PostgreSQL. Raises if the DB can't be reached."""
    import psycopg2  # imported lazily so the CSV path works without psycopg2

    from db_config import get_db_config  # reuse the project's own config

    conn = psycopg2.connect(**get_db_config())
    try:
        corr = pd.read_sql(
            """
            SELECT sc.ticker, sc.sentiment_score, sc.sentiment_label,
                   sc.price_date, sc.close_price, sc.next_day_close,
                   sc.next_day_pct_change, a.title, a.source
            FROM sentiment_correlations sc
            JOIN articles a ON sc.article_id = a.id
            ORDER BY sc.price_date DESC;
            """,
            conn,
        )
        recent = pd.read_sql(
            """
            SELECT a.title, a.source, a.published_at,
                   s.score AS sentiment_score, s.label AS sentiment_label
            FROM articles a
            JOIN sentiment_scores s ON a.id = s.article_id
            ORDER BY a.published_at DESC
            LIMIT 200;
            """,
            conn,
        )
    finally:
        conn.close()
    return corr, recent


def _load_from_csv() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fall back to the exported CSVs that ship with the repo."""
    corr = pd.read_csv(CORRELATION_CSV) if CORRELATION_CSV.exists() else pd.DataFrame(columns=CORR_COLUMNS)
    recent = pd.read_csv(RECENT_CSV) if RECENT_CSV.exists() else pd.DataFrame(columns=RECENT_COLUMNS)
    return corr, recent


@st.cache_data(ttl=600, show_spinner=False)
def load_data() -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    Return (correlation_df, recent_df, source_label).

    Tries the live database first and quietly falls back to CSV so the
    page never shows a stack trace to someone who just wants to look at it.
    """
    try:
        corr, recent = _load_from_db()
        source = "live"
    except Exception:
        corr, recent = _load_from_csv()
        source = "sample"

    # Normalise types so the charts behave the same on both paths.
    for frame in (corr, recent):
        if "sentiment_score" in frame:
            frame["sentiment_score"] = pd.to_numeric(frame["sentiment_score"], errors="coerce")
        if "sentiment_label" in frame:
            frame["sentiment_label"] = frame["sentiment_label"].astype(str).str.lower()
    if "next_day_pct_change" in corr:
        corr["next_day_pct_change"] = pd.to_numeric(corr["next_day_pct_change"], errors="coerce")
    if "published_at" in recent:
        recent["published_at"] = pd.to_datetime(recent["published_at"], errors="coerce", utc=True)

    return corr, recent, source


def pearson(df: pd.DataFrame) -> tuple[float, float, int] | None:
    """Pearson r, p-value and N for sentiment vs next-day move. None if too sparse."""
    clean = df.dropna(subset=["sentiment_score", "next_day_pct_change"])
    if len(clean) < 3 or clean["sentiment_score"].nunique() < 2:
        return None
    r, p = stats.pearsonr(clean["sentiment_score"], clean["next_day_pct_change"])
    return r, p, len(clean)


# --------------------------------------------------------------------------- #
# Small UI helpers
# --------------------------------------------------------------------------- #

def style_headlines(df: pd.DataFrame):
    """Tint each row's sentiment cell so mood is scannable at a glance."""
    def tint(label: str) -> str:
        color = LABEL_COLORS.get(str(label).lower(), NEUTRAL)
        return f"background-color: {color}22; color: {color}; font-weight: 600;"

    return df.style.map(tint, subset=["Sentiment"])


def inject_css() -> None:
    st.markdown(
        f"""
        <style>
        .stApp {{ background: #fbfbfd; }}
        h1, h2, h3 {{ color: {INK}; }}
        div[data-testid="stMetric"] {{
            background: #ffffff;
            border: 1px solid #ececf1;
            border-radius: 12px;
            padding: 16px 18px;
        }}
        div[data-testid="stMetricValue"] {{ color: {INK}; }}
        .source-badge {{
            display: inline-block; padding: 3px 10px; border-radius: 999px;
            font-size: 0.78rem; font-weight: 600; letter-spacing: .02em;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


# --------------------------------------------------------------------------- #
# Page
# --------------------------------------------------------------------------- #

def main() -> None:
    st.set_page_config(page_title="News Sentiment Dashboard", page_icon="📈", layout="wide")
    inject_css()

    corr, recent, source = load_data()

    st.title("Financial News Sentiment")
    st.caption("How the news is talking about your stocks — and whether the mood tracks the next day's price.")

    if source == "live":
        st.markdown('<span class="source-badge" style="background:#1a7f5a22;color:#1a7f5a;">● Live database</span>',
                    unsafe_allow_html=True)
    else:
        st.markdown('<span class="source-badge" style="background:#8a8f9822;color:#5b606b;">● Sample data (database not connected)</span>',
                    unsafe_allow_html=True)

    if corr.empty:
        st.warning("No analysed articles yet. Run `python scheduler.py` to scrape and score some news, "
                   "then refresh this page.")
        st.stop()

    # ---- Sidebar filters -------------------------------------------------- #
    st.sidebar.header("Filters")
    tickers = sorted(t for t in corr["ticker"].dropna().unique() if t and t != "GENERAL")
    chosen = st.sidebar.multiselect("Tickers", tickers, default=tickers)
    mood = st.sidebar.radio("News mood", ["All", "Positive", "Neutral", "Negative"], horizontal=False)

    view = corr[corr["ticker"].isin(chosen)] if chosen else corr.iloc[0:0]
    if mood != "All":
        view = view[view["sentiment_label"] == mood.lower()]

    if view.empty:
        st.info("Nothing matches those filters yet. Widen the ticker list or set mood back to **All**.")
        st.stop()

    # ---- Headline metrics ------------------------------------------------- #
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Articles analysed", f"{len(view):,}")
    c2.metric("Average sentiment", f"{view['sentiment_score'].mean():+.2f}")
    pos_share = (view["sentiment_label"] == "positive").mean() * 100
    c3.metric("Positive news", f"{pos_share:.0f}%")
    c4.metric("Tickers covered", f"{view['ticker'].nunique()}")

    st.divider()

    # ---- Watchlist pulse + correlation ------------------------------------ #
    left, right = st.columns([1, 1.1])

    with left:
        st.subheader("Watchlist pulse")
        st.caption("Average news sentiment per ticker. Green leans positive, red leans negative.")
        pulse = view.groupby("ticker", as_index=False)["sentiment_score"].mean()
        pulse_chart = (
            alt.Chart(pulse)
            .mark_bar(cornerRadiusEnd=4)
            .encode(
                x=alt.X("sentiment_score:Q", title="Average sentiment",
                        scale=alt.Scale(domain=[-0.5, 0.5])),
                y=alt.Y("ticker:N", sort="-x", title=None),
                color=alt.Color("sentiment_score:Q",
                                scale=alt.Scale(scheme=DIVERGING, domain=[-0.4, 0.4]),
                                legend=None),
                tooltip=[alt.Tooltip("ticker:N", title="Ticker"),
                         alt.Tooltip("sentiment_score:Q", title="Avg sentiment", format="+.3f")],
            )
            .properties(height=max(220, 28 * len(pulse)))
        )
        st.altair_chart(pulse_chart, use_container_width=True)

    with right:
        st.subheader("Does the mood move the price?")
        result = pearson(view)
        if result:
            r, p, n = result
            verdict = "statistically significant" if p < 0.05 else "not statistically significant"
            st.caption(f"Each dot is one article. Pearson r = **{r:+.2f}**, p = **{p:.3f}**, "
                       f"N = **{n}** — {verdict} at the 0.05 level.")
        else:
            st.caption("Each dot is one article. Too few points in this view to compute a correlation.")

        scatter_base = alt.Chart(view.dropna(subset=["sentiment_score", "next_day_pct_change"])).encode(
            x=alt.X("sentiment_score:Q", title="Article sentiment"),
            y=alt.Y("next_day_pct_change:Q", title="Next-day price change (%)"),
        )
        dots = scatter_base.mark_circle(size=70, opacity=0.45, color=INK).encode(
            tooltip=[alt.Tooltip("ticker:N", title="Ticker"),
                     alt.Tooltip("sentiment_score:Q", title="Sentiment", format="+.2f"),
                     alt.Tooltip("next_day_pct_change:Q", title="Next-day %", format="+.2f"),
                     alt.Tooltip("title:N", title="Headline")],
        )
        trend = scatter_base.transform_regression(
            "sentiment_score", "next_day_pct_change"
        ).mark_line(color=NEGATIVE, strokeDash=[5, 4])
        st.altair_chart((dots + trend).properties(height=320), use_container_width=True)
        st.caption("Exploratory only sentiment is one of many drivers of price, and articles sharing a "
                   "price date aren't fully independent observations. Read this as a pattern, not a signal to trade on.")

    st.divider()

    # ---- Latest headlines ------------------------------------------------- #
    st.subheader("Latest headlines")
    headlines = recent if not recent.empty else view
    table = (
        headlines.loc[:, [c for c in ["title", "source", "sentiment_score", "sentiment_label"] if c in headlines]]
        .head(25)
        .rename(columns={
            "title": "Headline", "source": "Source",
            "sentiment_score": "Score", "sentiment_label": "Sentiment",
        })
    )
    if "Sentiment" in table:
        table["Sentiment"] = table["Sentiment"].str.capitalize()
        styled = table.style.map(
            lambda v: f"background-color: {LABEL_COLORS.get(str(v).lower(), NEUTRAL)}22; "
                      f"color: {LABEL_COLORS.get(str(v).lower(), NEUTRAL)}; font-weight: 600;",
            subset=["Sentiment"],
        )
        if "Score" in table:
            styled = styled.format({"Score": "{:+.2f}"})
        st.dataframe(styled, use_container_width=True, hide_index=True)
    else:
        st.dataframe(table, use_container_width=True, hide_index=True)

    # ---- Sentiment over time (if we have timestamps) ---------------------- #
    if "published_at" in recent and recent["published_at"].notna().any():
        st.subheader("Sentiment over time")
        ts = (
            recent.dropna(subset=["published_at", "sentiment_score"])
            .assign(day=lambda d: d["published_at"].dt.date)
            .groupby("day", as_index=False)["sentiment_score"].mean()
        )
        if len(ts) > 1:
            line = (
                alt.Chart(ts)
                .mark_area(line={"color": INK}, color=alt.Gradient(
                    gradient="linear",
                    stops=[alt.GradientStop(color="#ffffff00", offset=0),
                           alt.GradientStop(color=f"{POSITIVE}55", offset=1)],
                    x1=1, x2=1, y1=1, y2=0,
                ))
                .encode(
                    x=alt.X("day:T", title=None),
                    y=alt.Y("sentiment_score:Q", title="Average sentiment"),
                    tooltip=[alt.Tooltip("day:T", title="Day"),
                             alt.Tooltip("sentiment_score:Q", title="Avg sentiment", format="+.3f")],
                )
                .properties(height=240)
            )
            st.altair_chart(line, use_container_width=True)

    st.caption("Data refreshes every 10 minutes. Built on the financial-news pipeline (scraper → NLP → PostgreSQL).")


if __name__ == "__main__":
    main()