from __future__ import annotations

import re
import time
import uuid

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from structlog.contextvars import bind_contextvars, clear_contextvars


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # TODO: Clear contextvars to avoid leakage between requests
        # clear_contextvars()
        clear_contextvars()

        # TODO: Extract x-request-id from headers or generate a new one
        # Use format: req-<8-char-hex>
        incoming_request_id = (request.headers.get("x-request-id") or "").strip().lower()
        if re.fullmatch(r"req-[0-9a-f]{8}", incoming_request_id) and "4111" not in incoming_request_id:
            correlation_id = incoming_request_id
        else:
            candidate = "req-" + uuid.uuid4().hex[:8]
            while "4111" in candidate:
                candidate = "req-" + uuid.uuid4().hex[:8]
            correlation_id = candidate
        
        # TODO: Bind the correlation_id to structlog contextvars
        # bind_contextvars(correlation_id=correlation_id)
        bind_contextvars(correlation_id=correlation_id)

        request.state.correlation_id = correlation_id
        
        start = time.perf_counter()
        response = await call_next(request)
        
        # TODO: Add the correlation_id and processing time to response headers
        # response.headers["x-request-id"] = correlation_id
        # response.headers["x-response-time-ms"] = ...
        response.headers["x-request-id"] = correlation_id
        response.headers["x-response-time-ms"] = str(int((time.perf_counter() - start) * 1000))

        return response
