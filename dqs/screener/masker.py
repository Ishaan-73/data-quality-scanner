"""Value masking — never store raw PII in manifest samples."""

from __future__ import annotations

import re


def mask_value(val: str, category: str) -> str:
    val = str(val).strip()
    if not val:
        return "***"

    if category == "contact" and "@" in val:
        # Email: j***@domain.com
        local, _, domain = val.partition("@")
        return f"{local[0]}***@{domain}" if local else f"***@{domain}"

    if category == "contact" and re.match(r"^\+?\d", val):
        # Phone: preserve first 3 chars, mask rest
        return val[:3] + "*" * max(0, len(val) - 3)

    if category == "identity":
        # Name: J*** S***
        return " ".join(t[0] + "***" if t else "***" for t in val.split())

    if category == "financial":
        # Card: **** **** **** 1234
        digits = re.sub(r"\D", "", val)
        if len(digits) >= 4:
            return "**** **** **** " + digits[-4:]
        return "*" * len(val)

    # Default: preserve first 2 chars
    return val[:2] + "*" * max(0, len(val) - 2)
