"""
column_semantics_test.py

Test how well an LLM can produce semantic column descriptions from schema + sample rows.
The LLM is given domain context but NOT the dataset name.

Usage:
  export GOOGLE_API_KEY=...
  python3 column_semantics_test.py \
    --db test/tpcds_dirty.duckdb \
    --domain "Retail and e-commerce operations including transactions, customers, products, stores, and returns" \
    --tables customer item store_sales \
    --model gemini-2.0-flash \
    --sample-pct 0.20 \
    --max-rows 50 \
    --out column_semantics_result.json
"""

import argparse
import json
import os
import sys

from google import genai
from google.genai import types
import duckdb

# ── Constants ────────────────────────────────────────────────────────────────

DEFAULT_TABLES = ["customer", "item", "store_sales", "catalog_sales", "date_dim", "customer_address"]
DEFAULT_MODEL  = "gemini-2.0-flash"
DEFAULT_OUT    = "column_semantics_result.json"

SYSTEM = (
    "You are a senior data analyst. You will be given a database table's schema and a sample "
    "of rows. Your task is to produce a concise one-sentence semantic description for EACH column "
    "based solely on the data patterns you observe, plus a confidence score (0–100) reflecting "
    "how certain you are about your interpretation given the sample data.\n"
    "Confidence rubric:\n"
    "  0–40  : column name is opaque and sample values give little signal\n"
    "  41–70 : some evidence from name or values, but still uncertain\n"
    "  71–100: clear pattern visible from both column name and sample values\n"
    "Do NOT reference any well-known benchmark or toy dataset names.\n"
    "Respond ONLY with a valid JSON object mapping each column name to an object with two keys:\n"
    '  "description" (string) and "confidence" (integer 0-100).\n'
    'Example: {"col_name": {"description": "...", "confidence": 85}, ...}'
)

# ── Data helpers ─────────────────────────────────────────────────────────────

def get_schema(con, table):
    return con.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        f"WHERE table_name='{table}' ORDER BY ordinal_position"
    ).fetchall()


def get_sample(con, table, sample_pct=0.20, max_rows=50):
    total = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    n = max(1, min(max_rows, int(total * sample_pct)))
    result = con.execute(f"SELECT * FROM {table} ORDER BY random() LIMIT {n}")
    cols = [d[0] for d in result.description]
    rows = result.fetchall()
    return cols, rows, total


# ── Prompt builder ────────────────────────────────────────────────────────────

def build_prompt(domain, table, schema, cols, rows):
    schema_str = "\n".join(f"  {name} ({dtype})" for name, dtype in schema)
    header = ", ".join(cols)
    row_lines = "\n".join(", ".join(str(v) for v in row) for row in rows)
    return (
        f"Domain context: {domain}\n\n"
        f"Table: {table}\n\n"
        f"Schema:\n{schema_str}\n\n"
        f"Sample rows ({len(rows)} rows shown):\n"
        f"{header}\n{row_lines}"
    )


# ── API call ──────────────────────────────────────────────────────────────────

def describe_columns(client, model, domain, table, schema, cols, rows):
    prompt = build_prompt(domain, table, schema, cols, rows)
    response = client.models.generate_content(
        model=model,
        contents=prompt,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM,
            max_output_tokens=8192,
        ),
    )
    raw = response.text.strip()

    # Strip markdown code fences if present
    if raw.startswith("```"):
        lines = raw.split("\n")
        raw = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])

    try:
        result = json.loads(raw)
    except json.JSONDecodeError:
        result = {"_raw": raw, "_error": "json_parse_failed"}

    usage = response.usage_metadata
    return result, usage


# ── Display ───────────────────────────────────────────────────────────────────

def color_confidence(score):
    if score >= 75:
        return f"\033[92m{score}%\033[0m"   # green
    elif score >= 45:
        return f"\033[93m{score}%\033[0m"   # yellow
    else:
        return f"\033[91m{score}%\033[0m"   # red


def print_table_results(table, total, sampled, schema, result, usage):
    print(f"\n## {table}  ({total:,} total rows, {sampled} sampled)")

    if "_error" in result:
        print(f"  [ERROR] Could not parse LLM response:\n  {result.get('_raw', '')[:300]}")
        return

    col_width  = max(len(c) for c, _ in schema) + 2
    type_width = max(len(t) for _, t in schema) + 2
    desc_width = 68

    print(f"{'Column':<{col_width}}  {'Type':<{type_width}}  {'Conf':>5}  Description")
    print("-" * (col_width + type_width + desc_width + 14))

    for col_name, dtype in schema:
        entry = result.get(col_name, {})
        if isinstance(entry, str):
            desc, conf = entry, None
        else:
            desc = entry.get("description", "—")
            conf = entry.get("confidence")

        if len(desc) > desc_width:
            desc = desc[:desc_width - 3] + "..."

        conf_display = color_confidence(conf) if isinstance(conf, int) else "—"
        conf_plain   = f"{conf}%" if isinstance(conf, int) else "—"
        # Print with color but keep alignment using plain width
        print(f"{col_name:<{col_width}}  {dtype:<{type_width}}  {conf_plain:>5}  {desc}")

    in_tok  = getattr(usage, "prompt_token_count", "?")
    out_tok = getattr(usage, "candidates_token_count", "?")
    print(f"\n  → Tokens: {in_tok} in / {out_tok} out")


# ── HTML report ──────────────────────────────────────────────────────────────

def build_html(output: dict) -> str:
    domain = output.get("domain", "")
    model  = output.get("model", "")

    table_blocks = ""
    for tname, tdata in output.get("tables", {}).items():
        if "_error" in tdata:
            table_blocks += f'<h2>{tname}</h2><p class="err">Error: {tdata["_error"]}</p>'
            continue

        rows_html = ""
        for col, entry in tdata.get("columns", {}).items():
            if "_error" in tdata["columns"]:
                break
            if isinstance(entry, str):
                desc, conf, dtype = entry, None, ""
            else:
                desc  = entry.get("description", "—")
                conf  = entry.get("confidence")
                dtype = entry.get("type", "")

            if isinstance(conf, int):
                if conf >= 80:
                    badge = f'<span class="badge green">{conf}%</span>'
                else:
                    badge = f'<span class="badge red">{conf}%</span>'
            else:
                badge = f'<span class="badge grey">—</span>'

            rows_html += f"<tr><td>{col}</td><td class='mono'>{dtype}</td><td>{badge}</td><td>{desc}</td></tr>"

        total   = tdata.get("total_rows", "?")
        sampled = tdata.get("sampled_rows", "?")
        table_blocks += f"""
        <div class="table-card">
          <div class="table-header">
            <span class="table-name">{tname}</span>
            <span class="meta">{total:,} rows &nbsp;·&nbsp; {sampled} sampled</span>
          </div>
          <table>
            <thead><tr><th>Column</th><th>Type</th><th>Confidence</th><th>Description</th></tr></thead>
            <tbody>{rows_html}</tbody>
          </table>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Column Semantics Review</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
          background: #f4f6f9; margin: 0; padding: 24px; color: #1a2433; }}
  h1   {{ font-size: 20px; font-weight: 700; margin-bottom: 4px; }}
  .sub {{ font-size: 13px; color: #6b7a99; margin-bottom: 28px; }}
  .table-card  {{ background: #fff; border-radius: 10px; margin-bottom: 20px;
                  box-shadow: 0 1px 4px rgba(0,0,0,.08); overflow: hidden; }}
  .table-header {{ display: flex; align-items: baseline; gap: 12px;
                   padding: 14px 18px; border-bottom: 1px solid #eef2f7; background: #f8fafc; }}
  .table-name  {{ font-size: 15px; font-weight: 700; color: #1B3D6F; }}
  .meta        {{ font-size: 12px; color: #9ba8c0; }}
  table  {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th     {{ padding: 9px 14px; text-align: left; font-size: 11px; font-weight: 600;
            color: #9ba8c0; text-transform: uppercase; letter-spacing: .05em;
            border-bottom: 1px solid #eef2f7; }}
  td     {{ padding: 9px 14px; border-bottom: 1px solid #f5f6f8; vertical-align: top; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: #f8fafc; }}
  .mono  {{ font-family: monospace; font-size: 12px; color: #6b7a99; }}
  .badge {{ display: inline-block; padding: 2px 10px; border-radius: 20px;
            font-size: 12px; font-weight: 700; }}
  .green {{ background: #e6f4ea; color: #2e7d32; }}
  .red   {{ background: #fdecea; color: #c62828; }}
  .grey  {{ background: #f0f0f0; color: #9e9e9e; }}
  .err   {{ color: #c62828; font-size: 13px; padding: 12px 18px; }}
</style>
</head>
<body>
<h1>Column Semantics Review</h1>
<div class="sub">Model: <strong>{model}</strong> &nbsp;·&nbsp; Domain: {domain}</div>
{table_blocks}
</body>
</html>"""


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Test LLM column semantic description quality")
    parser.add_argument("--db",         required=True,             help="Path to DuckDB file")
    parser.add_argument("--domain",     required=True,             help="Domain context (no dataset name)")
    parser.add_argument("--tables",     nargs="+", default=None,   help="Tables to analyse (default: curated subset)")
    parser.add_argument("--model",      default=DEFAULT_MODEL,     help=f"Gemini model ID (default: {DEFAULT_MODEL})")
    parser.add_argument("--sample-pct", type=float, default=0.20,  help="Fraction of rows to sample (default: 0.20)")
    parser.add_argument("--max-rows",   type=int,   default=20,    help="Max sample rows per table (default: 20)")
    parser.add_argument("--out",        default=DEFAULT_OUT,       help=f"JSON output path (default: {DEFAULT_OUT})")
    parser.add_argument("--merge",       action="store_true",      help="Skip tables already successfully parsed in --out")
    parser.add_argument("--render-only", action="store_true",      help="Re-render HTML from existing JSON + DB types (no API calls)")
    args = parser.parse_args()

    # ── render-only mode: enrich existing JSON with types then write HTML ──
    if args.render_only:
        if not os.path.exists(args.out):
            print(f"Error: {args.out} not found.", file=sys.stderr)
            sys.exit(1)
        with open(args.out) as f:
            output = json.load(f)
        con = duckdb.connect(args.db, read_only=True)
        for tname, tdata in output.get("tables", {}).items():
            cols_entry = tdata.get("columns", {})
            if "_error" in cols_entry:
                continue
            schema = get_schema(con, tname)
            for col_name, dtype in schema:
                if col_name in cols_entry and isinstance(cols_entry[col_name], dict):
                    cols_entry[col_name]["type"] = dtype
        con.close()
        with open(args.out, "w") as f:
            json.dump(output, f, indent=2, default=str)
        html_out = args.out.replace(".json", ".html")
        with open(html_out, "w") as f:
            f.write(build_html(output))
        print(f"HTML re-rendered: {html_out}")
        return

    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        print("Error: GOOGLE_API_KEY environment variable not set.", file=sys.stderr)
        print("Get your key at: https://aistudio.google.com/apikey", file=sys.stderr)
        sys.exit(1)

    client = genai.Client(api_key=api_key)

    tables = args.tables or DEFAULT_TABLES
    con    = duckdb.connect(args.db, read_only=True)

    # Load existing results if merging
    output = {"domain": args.domain, "model": args.model, "tables": {}}
    if args.merge and os.path.exists(args.out):
        with open(args.out) as f:
            existing = json.load(f)
        output["tables"] = existing.get("tables", {})
        # Skip tables already successfully parsed (no _error key in columns)
        skip = {t for t, d in output["tables"].items() if "_error" not in d.get("columns", {"_error": True})}
        tables = [t for t in tables if t not in skip]
        if skip:
            print(f"Skipping (already done): {', '.join(sorted(skip))}")
    total_in = total_out = 0

    for table in tables:
        print(f"\nProcessing {table} ...", end=" ", flush=True)
        try:
            schema            = get_schema(con, table)
            cols, rows, total = get_sample(con, table, args.sample_pct, args.max_rows)
            result, usage     = describe_columns(client, args.model, args.domain, table, schema, cols, rows)

            in_tok  = getattr(usage, "prompt_token_count", 0) or 0
            out_tok = getattr(usage, "candidates_token_count", 0) or 0
            total_in  += in_tok
            total_out += out_tok

            print("done")
            print_table_results(table, total, len(rows), schema, result, usage)

            # Embed data type into each column entry
            for col_name, dtype in schema:
                if col_name in result and isinstance(result[col_name], dict):
                    result[col_name]["type"] = dtype

            output["tables"][table] = {
                "total_rows":   total,
                "sampled_rows": len(rows),
                "columns":      result,
                "tokens_in":    in_tok,
                "tokens_out":   out_tok,
            }
        except Exception as exc:
            print(f"ERROR: {exc}")
            output["tables"][table] = {"_error": str(exc)}

    con.close()

    print(f"\n{'='*60}")
    print(f"Total tokens used: {total_in:,} in / {total_out:,} out")

    with open(args.out, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"Results saved to: {args.out}")

    html_out = args.out.replace(".json", ".html")
    with open(html_out, "w") as f:
        f.write(build_html(output))
    print(f"HTML report:      {html_out}")


if __name__ == "__main__":
    main()
