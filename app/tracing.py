from __future__ import annotations

import os
from typing import Any

try:
    from langfuse import observe, get_client
except Exception:  # pragma: no cover
    def observe(*args: Any, **kwargs: Any):
        def decorator(func):
            return func
        return decorator

    class _DummyContext:
        def update_current_trace(self, **kwargs: Any) -> None:
            return None

        def update_current_observation(self, **kwargs: Any) -> None:
            return None

    langfuse_context = _DummyContext()

else:
    class _LangfuseContext:
        def update_current_trace(self, **kwargs: Any) -> None:
            get_client().update_current_trace(**kwargs)

        def update_current_observation(self, **kwargs: Any) -> None:
            client = get_client()
            # Langfuse SDKs differ: some expose observation, others generation/span updates.
            if hasattr(client, "update_current_observation"):
                client.update_current_observation(
                    metadata=kwargs.get("metadata"),
                    usage_details=kwargs.get("usage_details"),
                )
                return
            if hasattr(client, "update_current_generation"):
                client.update_current_generation(
                    metadata=kwargs.get("metadata"),
                    usage_details=kwargs.get("usage_details"),
                )
                return
            if hasattr(client, "update_current_span"):
                client.update_current_span(metadata=kwargs.get("metadata"))

    langfuse_context = _LangfuseContext()

def tracing_enabled() -> bool:
    return bool(os.getenv("LANGFUSE_PUBLIC_KEY") and os.getenv("LANGFUSE_SECRET_KEY"))

