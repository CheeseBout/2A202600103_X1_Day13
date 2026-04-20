# Alert Rules and Runbooks

## 1. High latency P95
- Severity: P2
- Trigger: `latency_p95_ms > 5000 for 30m`
- Impact: tail latency breaches SLO
- First checks:
  1. Open top slow traces in the last 1h
  2. Compare RAG span vs LLM span
  3. Check if incident toggle `rag_slow` is enabled
- Mitigation:
  - truncate long queries
  - fallback retrieval source
  - lower prompt size

## 2. High error rate
- Severity: P1
- Trigger: `error_rate_pct > 5 for 5m`
- Impact: users receive failed responses
- First checks:
  1. Group logs by `error_type`
  2. Inspect failed traces
  3. Determine whether failures are LLM, tool, or schema related
- Mitigation:
  - rollback latest change
  - disable failing tool
  - retry with fallback model

## 3. Cost budget spike
- Severity: P2
- Trigger: `hourly_cost_usd > 2x_baseline for 15m`
- Impact: burn rate exceeds budget
- First checks:
  1. Split traces by feature and model
  2. Compare tokens_in/tokens_out
  3. Check if `cost_spike` incident was enabled
- Mitigation:
  - shorten prompts
  - route easy requests to cheaper model
  - apply prompt cache

## 4. Quality drop
- Severity: P2
- Trigger: `quality_avg < 0.8 for 15m`
- Impact: answer quality degrades and user trust decreases
- First checks:
  1. Compare quality by feature and recent deploy window
  2. Check retrieval relevance and prompt size drift
  3. Inspect traces with low-quality outputs and high error spans
- Mitigation:
  - tighten retrieval filters and top-k
  - adjust prompt template and guardrails
  - route complex queries to stronger model tier
