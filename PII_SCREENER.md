# PII_SCREENER.md

## Overview

A prerequisite screener that runs **before** any DQS scan. Inspects a customer's warehouse schema and data to detect columns containing personally identifiable information, produces a per-table PII manifest, and issues a gate decision that controls how (or whether) the DQS scan proceeds.

Integrates directly into the existing `dqs` package — no new config file, no new CLI. Extends `ScanConfig`, hooks into `scanner.py` between schema ingestion and scan dispatch.

---

## Operating Modes

Same two modes as DQS:

| | Mode A — Live | Mode B — Synthetic |
|---|---|---|
| Pass 1 | Runs on schema in memory — no queries | Same |
| Pass 2 | Samples 100 rows per flagged column | Runs on profile data before disconnect |
| Pass 3 | Optional — samples free text columns | Optional — runs on synthetic clone |

---

## Three-Pass Detection

### Pass 1 — Column name heuristics (zero data access)
Pattern-match column names against a curated keyword list. No queries fired.

- **HIGH** — exact match: `email`, `ssn`, `dob`, `date_of_birth`, `phone`, `phone_number`, `first_name`, `last_name`, `full_name`, `passport`, `national_id`, `ip_address`, `credit_card`, `iban`, `sort_code`, `bank_account`, `tax_id`, `medical_record`, `diagnosis`, `biometric`, `device_id`, `mac_address`
- **MEDIUM** — fuzzy match: contains `name`, `contact`, `address`, `birth`, `card`, `account`, `identity`, `mobile`, `location`, `geo`, `coords`
- **CLEAR** — no signal

### Pass 2 — Value sampling (100 rows, read-only)
Runs on HIGH and MEDIUM columns from Pass 1, plus all `VARCHAR`/`TEXT` columns not yet assessed. Pulls `SELECT col FROM table WHERE col IS NOT NULL LIMIT 100`. Applies regex in-memory.

Regex patterns to implement:
- Email: RFC 5322 simplified
- Phone: E.164, UK mobile, US format
- Date of birth: ISO 8601 dates where >80% of sample values are pre-2006
- SSN / NI: US SSN pattern, UK NI pattern
- Postcode: UK, US ZIP
- IP address: IPv4, IPv6
- Credit card: Luhn-checkable 13–19 digit strings
- IBAN: ISO 13616

Confidence upgrade: if regex matches >80% of sample → HIGH. 40–80% → MEDIUM. <40% → CLEAR (or REVIEW if Pass 1 was MEDIUM).

### Pass 3 — Semantic scan (opt-in, free text only)
Only runs on columns that are `VARCHAR`/`TEXT`, flagged REVIEW after Pass 1+2, and when `screener.enable_pass3: true` in config.

Use a local NER model (e.g. `spacy` `en_core_web_sm`) or a prompted LLM call to detect PII entities in sampled text. If entity detected → upgrade to HIGH. If none → CLEAR.

---

## Column Status Values

| Status | Meaning |
|---|---|
| `HIGH` | High-confidence PII. Excluded from DQS value-sampling checks. |
| `MEDIUM` | Suspected PII. Treated as HIGH unless customer confirms otherwise. |
| `REVIEW` | Ambiguous — free text not scanned, or quasi-identifier. Human confirmation required. |
| `CLEAR` | No PII signal detected. DQS checks run normally. |

---

## PII Categories

Screener detects and labels findings across six categories:

- `identity` — name, DOB, national ID, passport, driver's licence
- `contact` — email, phone, address, postcode, IP, geolocation
- `financial` — card number, IBAN, bank account, tax ID, salary
- `health` — medical record, diagnosis, prescription, insurance ID, biometric
- `behavioral` — device ID, MAC, cookie, session ID, activity timestamps linked to person
- `indirect` — postcode alone, quasi-identifiers, FK columns referencing person tables

---

## Gate Decisions

After Pass 1+2 (and optionally Pass 3), the screener evaluates the manifest and emits one of four gate decisions:

| Decision | Condition | DQS behaviour |
|---|---|---|
| `BLOCK` | HIGH PII with no confirmed masking or RBAC | Scan does not proceed. Manifest returned. Customer must remediate. |
| `PROCEED_EXCLUDE` | HIGH PII confirmed present, controls confirmed via Security pillar form | DQS runs. PII-tagged columns skipped in value-sampling checks. |
| `SYNTHETIC_MODE` | Customer preference flag or HIGH PII with no controls | Forces Mode B. Profile pulled, then disconnect. Checks run on clone. |
| `CLEAR` | No HIGH or MEDIUM findings | DQS runs in full. Manifest attached as clean confirmation. |

Gate logic:
1. If any HIGH column AND `screener.on_pii_detected == "block"` → `BLOCK`
2. If any HIGH column AND `screener.on_pii_detected == "synthetic"` → `SYNTHETIC_MODE`
3. If any HIGH column AND `screener.on_pii_detected == "exclude"` (default) → `PROCEED_EXCLUDE`
4. If only MEDIUM/REVIEW/CLEAR → `PROCEED_EXCLUDE` (with those columns flagged)
5. If all CLEAR → `CLEAR`

---

## Manifest Output Schema

```python
class PIIColumnResult(BaseModel):
    column: str
    table: str
    status: Literal["HIGH", "MEDIUM", "REVIEW", "CLEAR"]
    category: Optional[str]           # identity | contact | financial | health | behavioral | indirect
    pii_type: Optional[str]           # e.g. "Contact — email"
    confidence: float                 # 0.0–1.0
    detected_via: str                 # "name_match" | "name_match+regex" | "semantic" | "none"
    sample_count: int                 # how many rows sampled
    match_count: int                  # how many matched
    masked_samples: list[str]         # ≤3 masked value examples, e.g. "j***@company.com"
    notes: Optional[str]              # e.g. quasi-identifier risk explanation

class PIIManifest(BaseModel):
    scan_id: str
    table: str
    row_count: int
    screened_at: datetime
    passes_run: list[int]             # e.g. [1, 2]
    gate_decision: Literal["BLOCK", "PROCEED_EXCLUDE", "SYNTHETIC_MODE", "CLEAR"]
    gate_reason: str
    columns: list[PIIColumnResult]
    high_count: int
    medium_count: int
    review_count: int
    clear_count: int
```

---

## Architecture

```
dqs/
├── screener/
│   ├── __init__.py           # run_screener(config, connector) → PIIManifest
│   ├── pass1.py              # column_name_scan(schema_profile) → list[PIIColumnResult]
│   ├── pass2.py              # value_sample_scan(connector, flagged_columns) → list[PIIColumnResult]
│   ├── pass3.py              # semantic_scan(connector, review_columns) → list[PIIColumnResult]
│   ├── patterns.py           # PII_KEYWORDS dict, REGEX_PATTERNS dict
│   ├── masker.py             # mask_value(val, category) → str  (for masked_samples)
│   └── gate.py               # evaluate_gate(manifest, screener_config) → gate_decision
├── config/
│   └── models.py             # extend: ScreenerConfig, PIIColumnResult, PIIManifest
```

`run_screener()` is called inside `scanner.py` after `get_schema_profile()` and before dispatching to `live_scan.py` or `synthetic_clone.py`. Returns a `PIIManifest`. If gate decision is `BLOCK`, raise `PIIBlockError` with the manifest attached.

---

## Config Extension

Add a `screener` block to the existing scan YAML:

```yaml
screener:
  enabled: true                  # default true
  enable_pass3: false            # opt-in NER/semantic scan
  sample_size: 100               # rows to sample in Pass 2
  on_pii_detected: exclude       # block | exclude | synthetic
  categories:                    # omit to scan all
    - identity
    - contact
    - financial
    - health
    - behavioral
    - indirect
```

Extend `ScanConfig` in `dqs/config/models.py` with an optional `screener: Optional[ScreenerConfig] = None` field. If absent, screener runs with defaults.

---

## Integration Points

1. **`scanner.py` → `scan_from_file()`** — after loading config and instantiating connector, before dispatching to mode:
   ```python
   if config.screener and config.screener.enabled:
       manifest = run_screener(config, connector)
       if manifest.gate_decision == "BLOCK":
           raise PIIBlockError(manifest)
       if manifest.gate_decision == "SYNTHETIC_MODE":
           config.mode = "synthetic"
       scan_report.pii_manifest = manifest
   ```

2. **`dqs/checks/base.py` → `BaseCheck.run()`** — before executing any check, if `column` is in the manifest's HIGH/MEDIUM columns, skip value-sampling logic and return a `CheckResult` with `skipped=True, skip_reason="pii_excluded"`.

3. **`scorer.py`** — skipped checks do not penalise the score. Exclude them from the denominator when aggregating.

4. **`reporter.py`** — add a PII manifest section to console output (Rich table) and to JSON/CSV output. Console output prints gate decision banner first, before any dimension results.

---

## CLI

No new commands needed. Screener runs automatically if `screener.enabled: true` in config.

```bash
# Screener runs as part of normal scan
python -m dqs.cli scan --config my_scan.yaml --mode live

# To skip screener explicitly
python -m dqs.cli scan --config my_scan.yaml --mode live --skip-screener

# To run screener only (no DQS checks)
python -m dqs.cli scan --config my_scan.yaml --screener-only
```

Add `--skip-screener` and `--screener-only` flags to `cli.py`.

---

## Masking Rules for `masked_samples`

Masked samples are stored in the manifest and shown in the portal. Never store raw values.

| Category | Masking rule |
|---|---|
| Email | `j***@domain.com` — preserve domain, mask local part after first char |
| Phone | `+44 7*** ******` — preserve country code, mask digits |
| Name | `J*** S***` — preserve first char of each token |
| Date | `19**-**-**` — preserve century, mask rest |
| Card | `**** **** **** 1234` — preserve last 4 only |
| Other | Replace all chars after position 2 with `*` |

---

## Error Types

```python
class PIIBlockError(Exception):
    def __init__(self, manifest: PIIManifest): ...

class PIIScreenerError(Exception):
    """Raised when screener fails to run (connection issue, schema empty, etc.)"""
```

---

## Key Constraints

- Pass 1 fires **zero queries** — works entirely on the schema profile already in memory from `get_schema_profile()`
- Pass 2 fires **one query per flagged column** — never a `SELECT *`
- Raw values **never stored** — only masked samples written to manifest
- Screener must complete before **any** DQS check executes
- `PIIManifest` is a top-level field on `ScanReport` — always present in output if screener ran
- All regex runs **in-memory** after sampling — no values sent to external services unless Pass 3 uses an external LLM (opt-in, documented in config)
