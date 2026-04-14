"""PII keyword lists and regex patterns."""

from __future__ import annotations

import re

# Exact column name matches → HIGH
HIGH_KEYWORDS: set[str] = {
    "email", "ssn", "dob", "date_of_birth", "phone", "phone_number",
    "first_name", "last_name", "full_name", "passport", "national_id",
    "ip_address", "credit_card", "iban", "sort_code", "bank_account",
    "tax_id", "medical_record", "diagnosis", "biometric", "device_id",
    "mac_address",
}

# Substring matches → MEDIUM
MEDIUM_KEYWORDS: list[str] = [
    "name", "contact", "address", "birth", "card", "account",
    "identity", "mobile", "location", "geo", "coords",
]

# keyword → (category, pii_type)
KEYWORD_CATEGORY: dict[str, tuple[str, str]] = {
    "email":          ("contact",   "Contact — email"),
    "ssn":            ("identity",  "Identity — SSN"),
    "dob":            ("identity",  "Identity — date of birth"),
    "date_of_birth":  ("identity",  "Identity — date of birth"),
    "phone":          ("contact",   "Contact — phone"),
    "phone_number":   ("contact",   "Contact — phone"),
    "first_name":     ("identity",  "Identity — name"),
    "last_name":      ("identity",  "Identity — name"),
    "full_name":      ("identity",  "Identity — name"),
    "passport":       ("identity",  "Identity — passport"),
    "national_id":    ("identity",  "Identity — national ID"),
    "ip_address":     ("contact",   "Contact — IP address"),
    "credit_card":    ("financial", "Financial — credit card"),
    "iban":           ("financial", "Financial — IBAN"),
    "sort_code":      ("financial", "Financial — sort code"),
    "bank_account":   ("financial", "Financial — bank account"),
    "tax_id":         ("financial", "Financial — tax ID"),
    "medical_record": ("health",    "Health — medical record"),
    "diagnosis":      ("health",    "Health — diagnosis"),
    "biometric":      ("health",    "Health — biometric"),
    "device_id":      ("behavioral","Behavioral — device ID"),
    "mac_address":    ("behavioral","Behavioral — MAC address"),
    # medium
    "name":           ("identity",  "Identity — name (partial)"),
    "contact":        ("contact",   "Contact (partial)"),
    "address":        ("contact",   "Contact — address"),
    "birth":          ("identity",  "Identity — birth date"),
    "card":           ("financial", "Financial — card"),
    "account":        ("financial", "Financial — account"),
    "identity":       ("identity",  "Identity"),
    "mobile":         ("contact",   "Contact — mobile"),
    "location":       ("contact",   "Contact — location"),
    "geo":            ("contact",   "Contact — geolocation"),
    "coords":         ("contact",   "Contact — coordinates"),
}

# pattern_name → (compiled_regex, category, pii_type)
REGEX_PATTERNS: dict[str, tuple[re.Pattern, str, str]] = {
    "email": (
        re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$"),
        "contact", "Contact — email",
    ),
    "phone_e164": (
        re.compile(r"^\+?[1-9]\d{7,14}$"),
        "contact", "Contact — phone",
    ),
    "phone_us": (
        re.compile(r"^(\+1[-.\s]?)?\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}$"),
        "contact", "Contact — phone",
    ),
    "phone_uk": (
        re.compile(r"^(\+44|0)7\d{9}$"),
        "contact", "Contact — phone",
    ),
    "ssn_us": (
        re.compile(r"^\d{3}-\d{2}-\d{4}$"),
        "identity", "Identity — SSN",
    ),
    "ni_uk": (
        re.compile(r"^[A-CEGHJ-PR-TW-Z]{2}\d{6}[A-D]$", re.IGNORECASE),
        "identity", "Identity — NI number",
    ),
    "postcode_uk": (
        re.compile(r"^[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}$", re.IGNORECASE),
        "contact", "Contact — postcode",
    ),
    "zip_us": (
        re.compile(r"^\d{5}(-\d{4})?$"),
        "contact", "Contact — ZIP code",
    ),
    "ipv4": (
        re.compile(r"^(\d{1,3}\.){3}\d{1,3}$"),
        "contact", "Contact — IP address",
    ),
    "ipv6": (
        re.compile(r"^([0-9a-fA-F]{0,4}:){2,7}[0-9a-fA-F]{0,4}$"),
        "contact", "Contact — IP address",
    ),
    "credit_card": (
        re.compile(r"^\d{13,19}$"),
        "financial", "Financial — credit card",
    ),
    "iban": (
        re.compile(r"^[A-Z]{2}\d{2}[A-Z0-9]{4,30}$"),
        "financial", "Financial — IBAN",
    ),
}
