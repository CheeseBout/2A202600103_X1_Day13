from __future__ import annotations

import hashlib
import re

PII_PATTERNS: dict[str, str] = {
    "email": r"[\w\.-]+@[\w\.-]+\.\w+",
    "phone_vn": r"(?:\+84|0)[ \.-]?\d{3}[ \.-]?\d{3}[ \.-]?\d{3,4}", # Matches 090 123 4567, 090.123.4567, etc.
    "cccd": r"\b\d{12}\b",
    "credit_card": r"\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b",
    # TODO: Add more patterns (e.g., Passport, Vietnamese address keywords)
    "passport": r"\b[A-Z][0-9]{7}\b",
    "vn_address": r"(?i)\b(?:\d{1,4}[\w/-]*\s+)?(?:duong|d\.\s*|pho|p\.\s*|ngo|hem|to|ap|khu pho|quan|huyen|phuong|xa|tp\.?\s*hcm|ha noi|da nang|can tho|ph\u1ed1|\u0111\u01b0\u1eddng|ng\u00f5|h\u1ebbm|qu\u1eadn|huy\u1ec7n|ph\u01b0\u1eddng|x\u00e3|th\u00e0nh ph\u1ed1)\b",
}


def scrub_text(text: str) -> str:
    safe = text
    for name, pattern in PII_PATTERNS.items():
        safe = re.sub(pattern, f"[REDACTED_{name.upper()}]", safe)
    return safe


def summarize_text(text: str, max_len: int = 80) -> str:
    safe = scrub_text(text).strip().replace("\n", " ")
    return safe[:max_len] + ("..." if len(safe) > max_len else "")


def hash_user_id(user_id: str) -> str:
    return hashlib.sha256(user_id.encode("utf-8")).hexdigest()[:12]
