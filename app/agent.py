from __future__ import annotations

import time
from dataclasses import dataclass, field

from . import metrics
from .logging_config import get_logger
from .mock_llm import FakeLLM
from .mock_rag import retrieve
from .pii import hash_user_id, summarize_text
from .tracing import langfuse_context, observe

logger = get_logger()


@dataclass
class AgentResult:
    answer: str
    latency_ms: int
    tokens_in: int
    tokens_out: int
    cost_usd: float
    quality_score: float
    spans: dict = field(default_factory=dict)


class LabAgent:
    def __init__(self, model: str = "claude-sonnet-4-5") -> None:
        self.model = model
        self.llm = FakeLLM(model=model)

    @observe(as_type="generation")
    def run(self, user_id: str, feature: str, session_id: str, message: str) -> AgentResult:
        spans = {}
        t0 = time.perf_counter()

        # ── Span 1: Parse / input prep ──
        prompt = f"Feature={feature}\nDocs=[pending]\nQuestion={message}"
        t1 = time.perf_counter()
        spans["parse"] = int((t1 - t0) * 1000)

        # ── Span 2: RAG Retrieval ──
        docs = []
        try:
            docs = self._do_retrieve(message)
        except RuntimeError as e:
            if "tool_fail" in str(e):
                raise RuntimeError("Vector store timeout") from e
            raise e
        prompt = f"Feature={feature}\nDocs={docs}\nQuestion={message}"
        t2 = time.perf_counter()
        span_retrieval = int((t2 - t1) * 1000) 
        spans["retrieval"] = span_retrieval
        if span_retrieval > 2000:
            logger.error("rag_slow", error_type="rag_slow", duration_ms=span_retrieval)
            langfuse_context.update_current_observation(level="ERROR", status_message="rag_slow")

        # ── Span 3: LLM Call ──
        response = self.llm.generate(prompt)
        t3 = time.perf_counter()
        spans["llm_call"] = int((t3 - t2) * 1000)

        # ── Span 4: Quality check ──
        quality_score = self._heuristic_quality(message, response.text, docs)
        t4 = time.perf_counter()
        spans["quality_check"] = int((t4 - t3) * 1000)

        # ── Span 5: Post-process (cost, result assembly) ──
        cost_usd = self._estimate_cost(response.usage.input_tokens, response.usage.output_tokens)
        if response.usage.output_tokens > 200:
            logger.error("cost_spike", error_type="cost_spike", cost_usd=cost_usd, tokens_out=response.usage.output_tokens)
            langfuse_context.update_current_observation(level="ERROR", status_message="cost_spike")
            
        latency_ms = int((time.perf_counter() - t0) * 1000)
        spans["post_process"] = int((time.perf_counter() - t4) * 1000)
        spans["total"] = latency_ms

        langfuse_context.update_current_trace(
            user_id=hash_user_id(user_id),
            session_id=session_id,
            tags=["lab", feature, self.model],
        )
        langfuse_context.update_current_observation(
            metadata={"doc_count": len(docs), "query_preview": summarize_text(message)},
            usage_details={"input": response.usage.input_tokens, "output": response.usage.output_tokens},
        )

        metrics.record_request(
            latency_ms=latency_ms,
            cost_usd=cost_usd,
            tokens_in=response.usage.input_tokens,
            tokens_out=response.usage.output_tokens,
            quality_score=quality_score,
        )

        return AgentResult(
            answer=response.text,
            latency_ms=latency_ms,
            tokens_in=response.usage.input_tokens,
            tokens_out=response.usage.output_tokens,
            cost_usd=cost_usd,
            quality_score=quality_score,
            spans=spans,
        )

    def _estimate_cost(self, tokens_in: int, tokens_out: int) -> float:
        input_cost = (tokens_in / 1_000_000) * 3
        output_cost = (tokens_out / 1_000_000) * 15
        return round(input_cost + output_cost, 6)

    def _heuristic_quality(self, question: str, answer: str, docs: list[str]) -> float:
        score = 0.5
        if docs:
            score += 0.2
        if len(answer) > 40:
            score += 0.1
        if question.lower().split()[0:1] and any(token in answer.lower() for token in question.lower().split()[:3]):
            score += 0.1
        if "[REDACTED" in answer:
            score -= 0.2
        return round(max(0.0, min(1.0, score)), 2)

    @observe(as_type="span", name="rag_retrieval")
    def _do_retrieve(self, message: str) -> list[str]:
        try:
            return retrieve(message)
        except RuntimeError as e:
            if "Vector store timeout" in str(e):
                raise RuntimeError("tool_fail")
            raise e
