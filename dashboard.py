from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Local timezone offset (UTC+7)
LOCAL_TZ = timezone(timedelta(hours=7))


BASE_DIR = Path(__file__).resolve().parent
LOG_PATH = BASE_DIR / "data" / "logs.jsonl"
LATENCY_SLO_MS = 3000
ERROR_RATE_SLO_PCT = 1.0
QUALITY_SLO_PCT = 95.0


st.set_page_config(page_title="Day 13 Observability Dashboard", layout="wide")


def load_logs(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        # Convert from UTC to local time so chart axes match the user's clock
        df["ts"] = df["ts"].dt.tz_convert(LOCAL_TZ)
    else:
        df["ts"] = pd.NaT
    return df.dropna(subset=["ts"]).sort_values("ts")


def filter_window(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty:
        return df
    end = df["ts"].max()
    start = end - timedelta(minutes=minutes)
    return df[df["ts"] >= start].copy()


def pad_to_now(df: pd.DataFrame, ts_col: str = "ts", fill_value: int | float = 0) -> pd.DataFrame:
    """Extend a minute-bucketed DataFrame to the current local time, filling gaps with 0."""
    if df.empty:
        return df
    now_local = datetime.now(LOCAL_TZ).replace(second=0, microsecond=0)
    last_ts = df[ts_col].max()
    if last_ts >= now_local:
        return df
    # Build missing minute slots from (last_ts + 1min) to now
    extra_range = pd.date_range(start=last_ts + timedelta(minutes=1), end=now_local, freq="1min", tz=LOCAL_TZ)
    if extra_range.empty:
        return df
    fill_cols = {c: fill_value for c in df.columns if c != ts_col}
    fill_cols[ts_col] = extra_range
    pad = pd.DataFrame(fill_cols)
    return pd.concat([df, pad], ignore_index=True)


def request_volume(df: pd.DataFrame) -> pd.DataFrame:
    requests = df[df.get("event").eq("request_received")][["ts"]].copy()
    if requests.empty:
        return pd.DataFrame(columns=["ts", "qps"])
    minute_counts = requests.set_index("ts").resample("1min").size().rename("count")
    out = minute_counts.reset_index()
    out["qps"] = out["count"] / 60.0
    return pad_to_now(out[["ts", "qps"]])


def response_metrics(df: pd.DataFrame) -> pd.DataFrame:
    responses = df[df.get("event").eq("response_sent")].copy()
    if responses.empty:
        return pd.DataFrame(columns=["ts", "latency_ms", "tokens_in", "tokens_out", "cost_usd", "feature"])

    for col in ["latency_ms", "tokens_in", "tokens_out", "cost_usd"]:
        responses[col] = pd.to_numeric(responses.get(col), errors="coerce")

    responses["feature"] = responses.get("feature", "unknown").fillna("unknown")
    return responses.dropna(subset=["latency_ms", "tokens_in", "tokens_out", "cost_usd"])


def error_breakdown(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    per_min = df.set_index("ts").resample("1min").agg(
        total=("event", "size"),
        errors=("level", lambda s: (s == "error").sum()),
    )
    per_min = per_min.reset_index()
    per_min["error_rate_pct"] = (per_min["errors"] / per_min["total"].clip(lower=1)) * 100

    errors = df[df.get("level").eq("error")].copy()
    if errors.empty:
        breakdown = pd.DataFrame({"error_type": ["none"], "count": [0]})
    else:
        error_type = errors.get("error_type", "unknown").fillna("unknown")
        breakdown = error_type.value_counts().rename_axis("error_type").reset_index(name="count")
    return per_min, breakdown


def latency_percentiles(responses: pd.DataFrame) -> pd.DataFrame:
    if responses.empty:
        return pd.DataFrame(columns=["ts", "p50", "p95", "p99"])
    q = (
        responses.set_index("ts")["latency_ms"]
        .resample("1min")
        .quantile([0.50, 0.95, 0.99])
        .unstack()
        .rename(columns={0.50: "p50", 0.95: "p95", 0.99: "p99"})
        .reset_index()
    )
    return pad_to_now(q)


def weekly_quality_proxy(responses: pd.DataFrame) -> pd.DataFrame:
    if responses.empty:
        return pd.DataFrame(columns=["week", "quality_pct"])

    text = responses.get("payload").apply(lambda p: (p or {}).get("answer_preview", "") if isinstance(p, dict) else "")
    marker = text.str.lower().str.contains("hallucination|made up|not sure|unknown", regex=True)
    sampled = pd.DataFrame({"ts": responses["ts"], "is_bad": marker.astype(int)})
    weekly = sampled.set_index("ts").resample("W-MON").agg(samples=("is_bad", "size"), bad=("is_bad", "sum")).reset_index()
    weekly["quality_pct"] = 100 - (weekly["bad"] / weekly["samples"].clip(lower=1) * 100)
    weekly.rename(columns={"ts": "week"}, inplace=True)
    return weekly[["week", "quality_pct"]]


st.title("Layer 2 Observability Dashboard")
st.caption("6-panel service health view from data/logs.jsonl")


with st.sidebar:
    time_range = st.selectbox("Time range", options=["All", 15, 30, 60, 180, 360], index=0)
    auto_refresh = st.toggle("Auto refresh", value=True)
    refresh_seconds = st.slider("Refresh interval (sec)", min_value=2, max_value=60, value=5)
    st.caption(f"Log file: {LOG_PATH}")

# ---------------------------------------------------------------------------
# Real-time chart fragment — only the graphs re-render on each tick,
# the rest of the page (title, sidebar) stays stable.
# ---------------------------------------------------------------------------
_run_every = timedelta(seconds=refresh_seconds) if auto_refresh else None


@st.fragment(run_every=_run_every)
def live_charts():
    """Fragment that reloads data and redraws all 6 panels independently."""
    logs = load_logs(LOG_PATH)
    window = logs.copy() if time_range == "All" else filter_window(logs, int(time_range))

    if window.empty and not logs.empty:
        st.warning("No events in selected window. Showing all available logs instead.")
        window = logs.copy()

    if window.empty:
        st.warning("No log events found. Generate traffic and refresh.")
        st.info(f"Expected log file path: {LOG_PATH}")
        return

    responses = response_metrics(window)
    lat = latency_percentiles(responses)
    traffic = request_volume(window)
    err_series, err_split = error_breakdown(window)
    quality = weekly_quality_proxy(responses)

    if not responses.empty:
        cost_rate = responses.set_index("ts")["cost_usd"].resample("1min").sum().reset_index(name="cost_per_min")
        cost_rate["cumulative_cost"] = cost_rate["cost_per_min"].cumsum()
        avg_cost_per_min = cost_rate["cost_per_min"].mean()
        cost_rate["forecast_1h"] = cost_rate["cumulative_cost"] + avg_cost_per_min * 60
        cost_rate = pad_to_now(cost_rate)
        # Carry forward cumulative & forecast into padded rows
        cost_rate["cumulative_cost"] = cost_rate["cumulative_cost"].ffill()
        cost_rate["forecast_1h"] = cost_rate["forecast_1h"].ffill()

        token_rate = responses.set_index("ts")[["tokens_in", "tokens_out"]].resample("1min").sum().reset_index()
        token_rate = pad_to_now(token_rate)
    else:
        cost_rate = pd.DataFrame(columns=["ts", "cost_per_min", "cumulative_cost", "forecast_1h"])
        token_rate = pd.DataFrame(columns=["ts", "tokens_in", "tokens_out"])

    # Pad error series to now too
    err_series = pad_to_now(err_series)

    # ---- Row 1 ----
    row1 = st.columns(3)
    with row1[0]:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=lat.get("ts"), y=lat.get("p50"), mode="lines", name="P50"))
        fig.add_trace(go.Scatter(x=lat.get("ts"), y=lat.get("p95"), mode="lines", name="P95"))
        fig.add_trace(go.Scatter(x=lat.get("ts"), y=lat.get("p99"), mode="lines", name="P99"))
        fig.add_hline(y=LATENCY_SLO_MS, line_dash="dash", annotation_text="SLO")
        fig.update_layout(title="1) Latency P50/P95/P99 (ms)", margin=dict(l=20, r=10, t=45, b=20), height=320)
        st.plotly_chart(fig, width="stretch")

    with row1[1]:
        fig = px.line(traffic, x="ts", y="qps", title="2) Traffic (QPS)")
        fig.update_layout(margin=dict(l=20, r=10, t=45, b=20), height=320)
        st.plotly_chart(fig, width="stretch")

    with row1[2]:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=err_series.get("ts"), y=err_series.get("error_rate_pct"), mode="lines", name="Error rate %"))
        fig.add_hline(y=ERROR_RATE_SLO_PCT, line_dash="dash", annotation_text="SLO")
        fig.update_layout(title="3) Error Rate %", margin=dict(l=20, r=10, t=45, b=20), height=220)
        st.plotly_chart(fig, width="stretch")

        pie = px.pie(err_split, names="error_type", values="count", title="Error breakdown")
        pie.update_layout(margin=dict(l=20, r=10, t=40, b=10), height=180)
        st.plotly_chart(pie, width="stretch")

    # ---- Row 2 ----
    row2 = st.columns(3)
    with row2[0]:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=cost_rate.get("ts"), y=cost_rate.get("cumulative_cost"), mode="lines", name="Cumulative"))
        fig.add_trace(go.Scatter(x=cost_rate.get("ts"), y=cost_rate.get("forecast_1h"), mode="lines", name="Forecast +1h"))
        fig.update_layout(title="4) Cost USD cumulative + forecast", margin=dict(l=20, r=10, t=45, b=20), height=320)
        st.plotly_chart(fig, width="stretch")

    with row2[1]:
        fig = go.Figure()
        fig.add_trace(go.Bar(x=token_rate.get("ts"), y=token_rate.get("tokens_in"), name="Tokens in"))
        fig.add_trace(go.Bar(x=token_rate.get("ts"), y=token_rate.get("tokens_out"), name="Tokens out"))
        fig.update_layout(title="5) Tokens in/out stacked", barmode="stack", margin=dict(l=20, r=10, t=45, b=20), height=320)
        st.plotly_chart(fig, width="stretch")

    with row2[2]:
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=quality.get("week"), y=quality.get("quality_pct"), mode="lines+markers", name="Quality %"))
        fig.add_hline(y=QUALITY_SLO_PCT, line_dash="dash", annotation_text="SLO")
        fig.update_layout(title="6) Quality proxy % sampled weekly", margin=dict(l=20, r=10, t=45, b=20), height=320)
        st.plotly_chart(fig, width="stretch")
    # ---- Row 3: Waterfall Trace View (full width) ----
    st.markdown("---")
    st.subheader("7) Trace Waterfall View")

    # ── Collect all traces: successful (with spans) + errors ──
    # Successful traces with span data
    has_spans = "spans" in responses.columns
    if has_spans:
        ok_traces = responses[responses.get("spans").apply(lambda s: isinstance(s, dict) and len(s) > 0)].copy()
        ok_traces["_trace_type"] = "ok"
    else:
        ok_traces = pd.DataFrame()

    # Error traces — join request_failed with request_received by correlation_id
    err_events = window[window.get("event").eq("request_failed")].copy() if "event" in window.columns else pd.DataFrame()

    if not err_events.empty:
        # Get request_received timestamps for latency calc
        req_events = window[window.get("event").eq("request_received")][["correlation_id", "ts"]].rename(columns={"ts": "req_ts"})
        err_merged = err_events.merge(req_events, on="correlation_id", how="left")
        err_merged["_err_latency_ms"] = (
            (err_merged["ts"] - err_merged["req_ts"]).dt.total_seconds() * 1000
        ).fillna(0).astype(int)
        err_merged["_trace_type"] = "error"
        err_merged["_error_type"] = err_merged.get("error_type", "unknown").fillna("unknown")
        err_merged["_error_detail"] = err_merged.get("payload").apply(
            lambda p: p.get("detail", "") if isinstance(p, dict) else ""
        )
    else:
        err_merged = pd.DataFrame()

    # Build unified trace list
    trace_items = []  # list of dicts for the dropdown

    if not ok_traces.empty:
        for i, (_, row) in enumerate(ok_traces.iterrows()):
            cid = row.get("correlation_id", "unknown")
            ts_str = row["ts"].strftime("%H:%M:%S") if pd.notna(row["ts"]) else "?"
            total = row["spans"].get("total", row.get("latency_ms", 0))
            trace_items.append({
                "label": f"✅ {cid} @ {ts_str} ({total}ms)",
                "type": "ok",
                "row_idx": i,
            })

    if not err_merged.empty:
        for i, (_, row) in enumerate(err_merged.iterrows()):
            cid = row.get("correlation_id", "unknown")
            ts_str = row["ts"].strftime("%H:%M:%S") if pd.notna(row["ts"]) else "?"
            etype = row.get("_error_type", "Error")
            latency = row.get("_err_latency_ms", 0)
            trace_items.append({
                "label": f"🔴 {cid} @ {ts_str} — {etype} ({latency}ms)",
                "type": "error",
                "row_idx": i,
            })

    if not trace_items:
        st.info("No span data yet. Generate traffic after restarting the API server to see waterfall traces.")
    else:
        # Let user pick which trace to view, default to latest
        selected_idx = st.selectbox(
            "Select trace",
            options=list(range(len(trace_items))),
            format_func=lambda i: trace_items[i]["label"],
            index=len(trace_items) - 1,
            key="waterfall_select",
        )

        selected = trace_items[selected_idx]

        SPAN_DEFS = [
            ("parse",         "Parse",         "#c0392b"),
            ("retrieval",     "Retrieval",     "#e74c3c"),
            ("llm_call",      "LLM Call",      "#7f8c8d"),
            ("quality_check", "Quality Check", "#27ae60"),
            ("post_process",  "Post-process",  "#2c3e50"),
        ]

        if selected["type"] == "ok":
            # ── Successful trace waterfall ──
            selected_row = ok_traces.iloc[selected["row_idx"]]
            spans = selected_row["spans"]
            total_ms = spans.get("total", 1)

            fig = go.Figure()
            fig.add_trace(go.Bar(
                y=["Total Request"], x=[total_ms], orientation="h",
                marker_color="#95a5a6",
                text=[f"Total: {total_ms}ms"], textposition="inside",
                textfont=dict(color="white", size=13),
                hoverinfo="x", showlegend=False,
            ))

            offset = 0
            for key, label, color in SPAN_DEFS:
                dur = spans.get(key, 0)
                if dur <= 0 and key != "parse":
                    continue
                fig.add_trace(go.Bar(
                    y=["Spans"], x=[dur], orientation="h",
                    marker_color=color,
                    text=[f"{label}: {dur}ms"], textposition="inside",
                    textfont=dict(color="white", size=11),
                    hovertemplate=f"{label}: {dur}ms<extra></extra>",
                    showlegend=False, base=offset,
                ))
                offset += dur

            fig.update_layout(
                barmode="overlay",
                xaxis=dict(title="Time (ms)", range=[0, max(total_ms * 1.05, 1)]),
                yaxis=dict(autorange="reversed"),
                height=180, margin=dict(l=120, r=20, t=10, b=40),
            )
            st.plotly_chart(fig, use_container_width=True)

            detail_data = {label: f"{spans.get(key, 0)} ms" for key, label, _ in SPAN_DEFS}
            detail_data["Total"] = f"{total_ms} ms"
            cols = st.columns(len(detail_data))
            for col, (lbl, val) in zip(cols, detail_data.items()):
                col.metric(lbl, val)

        else:
            # ── Error trace waterfall ──
            err_row = err_merged.iloc[selected["row_idx"]]
            err_latency = err_row.get("_err_latency_ms", 0)
            err_type = err_row.get("_error_type", "Unknown")
            err_detail = err_row.get("_error_detail", "")
            err_cid = err_row.get("correlation_id", "?")
            # Infer which span failed from the error detail / error type
            detail_lower = (err_detail + " " + err_type).lower()
            if any(kw in detail_lower for kw in ("vector", "retriev", "rag", "search", "embed")):
                failed_span = "Retrieval"
            elif any(kw in detail_lower for kw in ("llm", "generat", "model", "token", "api", "openai", "claude", "timeout")):
                failed_span = "LLM Call"
            elif any(kw in detail_lower for kw in ("parse", "input", "validation", "pii", "schema")):
                failed_span = "Parse"
            elif any(kw in detail_lower for kw in ("quality", "score", "heuristic")):
                failed_span = "Quality Check"
            else:
                failed_span = err_type  # just show the raw error type

            fig = go.Figure()
            # Total bar in red
            fig.add_trace(go.Bar(
                y=["Total Request"], x=[max(err_latency, 1)], orientation="h",
                marker_color="#e74c3c",
                text=[f"FAILED: {err_latency}ms"], textposition="inside",
                textfont=dict(color="white", size=13),
                hoverinfo="x", showlegend=False,
            ))

            # Show the span that was running when it failed
            fig.add_trace(go.Bar(
                y=["Failed Span"], x=[max(err_latency, 1)], orientation="h",
                marker_color="#c0392b",
                text=[f"💥 {failed_span}: {err_type}"], textposition="inside",
                textfont=dict(color="white", size=11),
                hovertemplate=f"{failed_span}: {err_type}<extra></extra>",
                showlegend=False,
            ))

            fig.update_layout(
                barmode="overlay",
                xaxis=dict(title="Time (ms)", range=[0, max(err_latency * 1.2, 10)]),
                yaxis=dict(autorange="reversed"),
                height=180, margin=dict(l=120, r=20, t=10, b=40),
            )
            st.plotly_chart(fig, use_container_width=True)

            # Error detail box
            st.error(f"**{err_type}** in `{err_cid}`")
            st.code(err_detail or "(no detail)", language="text")
            c1, c2, c3 = st.columns(3)
            c1.metric("Error Type", err_type)
            c2.metric("Latency", f"{err_latency} ms")
            c3.metric("Correlation ID", err_cid)

    # ---- Summary stats inside fragment so they update too ----
    total_requests = int((window.get("event") == "request_received").sum())
    total_errors = int((window.get("level") == "error").sum())
    avg_latency = float(responses["latency_ms"].mean()) if not responses.empty else 0.0
    latest_quality = float(quality["quality_pct"].iloc[-1]) if not quality.empty else 100.0

    st.divider()
    window_label = "all data" if time_range == "All" else f"last {time_range} min"
    st.caption(
        f"Window: {window_label} | requests: {total_requests} | errors: {total_errors} | "
        f"avg latency: {avg_latency:.0f} ms | weekly quality: {latest_quality:.1f}%"
    )


# Invoke the fragment
live_charts()
