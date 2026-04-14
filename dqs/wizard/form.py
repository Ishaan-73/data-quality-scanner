"""
Questionnaire dict → self-contained HTML form.

The generated HTML lets a non-technical client:
  - View auto-discovered fields (pre-filled, editable)
  - Fill in TODO fields (valid values, SLAs, rules, etc.)
  - Download the completed questionnaire as YAML

No server required — pure browser JS (js-yaml via CDN).
"""

from __future__ import annotations

import json
from typing import Any


def build_form(questionnaire: dict) -> str:
    """Return a self-contained HTML string for the questionnaire."""
    q_json = json.dumps(questionnaire, indent=2, default=str)
    meta   = questionnaire.get("meta", {})
    tables = questionnaire.get("tables", [])
    scan_name = meta.get("scan_name", "DQS Scan")

    sections_html = "\n".join(_table_section(t) for t in tables)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>DQS Questionnaire — {scan_name}</title>
<script src="https://cdn.jsdelivr.net/npm/js-yaml@4.1.0/dist/js-yaml.min.js"></script>
<style>
  :root{{
    --blue:#1B3D6F; --green:#47A848; --light:#F0F4F8;
    --border:#D1DCE8; --text:#1a2433; --muted:#64748b;
    --red:#dc2626; --yellow:#d97706;
  }}
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'Segoe UI',system-ui,sans-serif;background:var(--light);color:var(--text);font-size:14px}}
  header{{background:var(--blue);color:#fff;padding:18px 32px;display:flex;align-items:center;gap:16px}}
  header h1{{font-size:20px;font-weight:700}}
  header span{{font-size:13px;opacity:.7}}
  .container{{max-width:1100px;margin:0 auto;padding:24px 16px}}
  .intro{{background:#fff;border:1px solid var(--border);border-radius:10px;padding:20px 24px;margin-bottom:24px}}
  .intro h2{{font-size:15px;color:var(--blue);margin-bottom:8px}}
  .intro p{{color:var(--muted);line-height:1.6}}
  .pill{{display:inline-block;padding:2px 10px;border-radius:99px;font-size:11px;font-weight:600}}
  .pill-todo{{background:#FEF3C7;color:#92400E}}
  .pill-auto{{background:#D1FAE5;color:#065F46}}
  .pill-optional{{background:#E0E7FF;color:#3730A3}}

  /* Table section card */
  .tbl-card{{background:#fff;border:1px solid var(--border);border-radius:10px;margin-bottom:20px;overflow:hidden}}
  .tbl-header{{background:var(--blue);color:#fff;padding:14px 20px;display:flex;align-items:center;justify-content:space-between;cursor:pointer;user-select:none}}
  .tbl-header h2{{font-size:15px;font-weight:600}}
  .tbl-header .chevron{{transition:transform .2s}}
  .tbl-header.open .chevron{{transform:rotate(180deg)}}
  .tbl-body{{padding:20px;display:none}}
  .tbl-body.open{{display:block}}

  /* Sub-section */
  .sub{{margin-bottom:20px}}
  .sub-title{{font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.05em;color:var(--muted);margin-bottom:10px;display:flex;align-items:center;gap:8px}}
  .sub-title::after{{content:'';flex:1;height:1px;background:var(--border)}}

  /* Grid of fields */
  .field-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:12px}}
  .field{{background:var(--light);border:1px solid var(--border);border-radius:8px;padding:12px}}
  .field label{{font-size:12px;font-weight:600;color:var(--muted);display:block;margin-bottom:6px}}
  .field input,.field select,.field textarea{{
    width:100%;border:1px solid var(--border);border-radius:6px;
    padding:7px 10px;font-size:13px;background:#fff;color:var(--text);
    font-family:inherit;resize:vertical
  }}
  .field input:focus,.field select:focus,.field textarea:focus{{
    outline:2px solid var(--green);border-color:var(--green)
  }}
  .field .hint{{font-size:11px;color:var(--muted);margin-top:4px}}

  /* Repeater rows */
  .rep-row{{display:flex;gap:8px;align-items:flex-start;margin-bottom:8px}}
  .rep-row input{{flex:1;border:1px solid var(--border);border-radius:6px;padding:7px 10px;font-size:13px}}
  .rep-row button{{flex-shrink:0;background:none;border:1px solid var(--border);border-radius:6px;
    padding:6px 10px;cursor:pointer;color:var(--muted);font-size:14px}}
  .rep-row button:hover{{background:#fee2e2;color:var(--red);border-color:var(--red)}}
  .add-btn{{background:none;border:1px dashed var(--green);border-radius:6px;
    padding:6px 14px;color:var(--green);font-size:12px;cursor:pointer;font-weight:600;margin-top:6px}}
  .add-btn:hover{{background:#f0fdf4}}

  /* Action bar */
  .action-bar{{position:sticky;bottom:0;background:#fff;border-top:1px solid var(--border);
    padding:16px 24px;display:flex;align-items:center;gap:12px;z-index:100}}
  .btn-primary{{background:var(--green);color:#fff;border:none;border-radius:8px;
    padding:10px 24px;font-size:14px;font-weight:600;cursor:pointer}}
  .btn-primary:hover{{background:#3a9139}}
  .btn-secondary{{background:#fff;color:var(--blue);border:2px solid var(--blue);border-radius:8px;
    padding:10px 24px;font-size:14px;font-weight:600;cursor:pointer}}
  .btn-secondary:hover{{background:var(--light)}}
  .status-msg{{font-size:13px;color:var(--muted);margin-left:auto}}

  /* AI role badge colours */
  select[data-field="ai_role"] option[value="training_data"]{{background:#D1FAE5}}
  select[data-field="ai_role"] option[value="target_variable"]{{background:#FEE2E2}}
  select[data-field="ai_role"] option[value="feature_store"]{{background:#E0E7FF}}
</style>
</head>
<body>

<header>
  <div>
    <h1>GovernIQ &nbsp;|&nbsp; DQS Client Questionnaire</h1>
    <span>Scan: {scan_name} &nbsp;·&nbsp; Fill in the highlighted fields, then click "Download YAML"</span>
  </div>
</header>

<div class="container">

  <div class="intro">
    <h2>How to fill this in</h2>
    <p>
      Fields marked <span class="pill pill-auto">Auto-discovered</span> were pre-filled from your schema — please verify them.<br>
      Fields marked <span class="pill pill-todo">TODO</span> require your business knowledge — they unlock the most impactful AI-readiness checks.<br>
      Fields marked <span class="pill pill-optional">Optional</span> add value but can be left blank.<br><br>
      When done, click <strong>Download YAML</strong> at the bottom, then hand the file to your DQS engineer who will run:<br>
      <code style="background:#F0F4F8;padding:2px 8px;border-radius:4px;font-size:12px">
        dqs generate-config --questionnaire questionnaire_filled.yaml --out scan_config.yaml
      </code>
    </p>
  </div>

  <div id="tables">
{sections_html}
  </div>

</div>

<div class="action-bar">
  <button class="btn-primary" onclick="downloadYAML()">⬇ Download Completed YAML</button>
  <button class="btn-secondary" onclick="expandAll()">Expand All</button>
  <button class="btn-secondary" onclick="collapseAll()">Collapse All</button>
  <span class="status-msg" id="statusMsg"></span>
</div>

<script>
// ── Embedded questionnaire seed data ──────────────────────────────────────
const SEED = {q_json};

// ── Toggle sections ───────────────────────────────────────────────────────
document.querySelectorAll('.tbl-header').forEach(h => {{
  h.addEventListener('click', () => {{
    h.classList.toggle('open');
    h.nextElementSibling.classList.toggle('open');
  }});
}});
function expandAll()  {{ document.querySelectorAll('.tbl-body').forEach(b => b.classList.add('open'));
                          document.querySelectorAll('.tbl-header').forEach(h => h.classList.add('open')); }}
function collapseAll(){{ document.querySelectorAll('.tbl-body').forEach(b => b.classList.remove('open'));
                          document.querySelectorAll('.tbl-header').forEach(h => h.classList.remove('open')); }}

// ── Repeater helpers ──────────────────────────────────────────────────────
function addRow(containerId) {{
  const c = document.getElementById(containerId);
  const row = document.createElement('div');
  row.className = 'rep-row';
  row.innerHTML = `<input type="text" placeholder="add value..."><button onclick="this.parentElement.remove()" title="Remove">✕</button>`;
  c.appendChild(row);
}}

// ── Read form state back to a questionnaire dict ──────────────────────────
function readForm() {{
  const q = JSON.parse(JSON.stringify(SEED));   // deep clone seed

  document.querySelectorAll('[data-table]').forEach(el => {{
    const tblIdx  = parseInt(el.dataset.table);
    const field   = el.dataset.field;
    const colIdx  = el.dataset.col !== undefined ? parseInt(el.dataset.col) : null;
    const subField= el.dataset.sub;
    const tbl     = q.tables[tblIdx];
    if (!tbl) return;

    const val = el.type === 'checkbox' ? el.checked : el.value.trim();

    if (colIdx !== null && subField) {{
      // column-level sub-field
      if (tbl[field] && tbl[field][colIdx]) {{
        let v = val;
        // coerce numbers
        if (['min','max','freshness_sla_hours','late_arrival_hours','must_be_positive'].includes(subField)) {{
          if (el.type === 'checkbox') {{ v = el.checked; }}
          else {{ v = val === '' ? null : (isNaN(Number(val)) ? val : Number(val)); }}
        }}
        if (subField === 'valid_values') {{
          // read repeater inputs
          const cont = document.getElementById(`rep_${{field}}_${{tblIdx}}_${{colIdx}}`);
          if (cont) {{
            v = Array.from(cont.querySelectorAll('input')).map(i=>i.value.trim()).filter(Boolean);
          }}
        }}
        tbl[field][colIdx][subField] = v;
      }}
    }} else if (field) {{
      // table-level field
      if (field === 'business_rules' || field === 'foreign_keys' || field === 'conditional_completeness') {{
        // handled via repeaters
      }} else if (field === 'volume_change_pct') {{
        tbl.volume.max_daily_change_pct = val === '' ? 30 : Number(val);
      }} else if (field === 'prior_table') {{
        tbl.volume.prior_table = val || null;
      }} else {{
        tbl[field] = val || null;
      }}
    }}
  }});

  // Read repeater lists for business_rules, foreign_keys, conditional_completeness
  document.querySelectorAll('[data-rep-table]').forEach(cont => {{
    const tblIdx  = parseInt(cont.dataset.repTable);
    const repField= cont.dataset.repField;
    const tbl = q.tables[tblIdx];
    if (!tbl) return;

    if (repField === 'business_rules') {{
      tbl.business_rules = Array.from(cont.querySelectorAll('input.rule-input'))
        .map(i => i.value.trim()).filter(Boolean);
    }} else if (repField === 'foreign_keys') {{
      const rows = cont.querySelectorAll('.fk-row');
      tbl.foreign_keys = Array.from(rows).map(row => {{
        const inputs = row.querySelectorAll('input');
        return {{
          column:             inputs[0]?.value.trim() || '',
          references_table:  inputs[1]?.value.trim() || '',
          references_column: inputs[2]?.value.trim() || '',
        }};
      }}).filter(r => r.column);
    }} else if (repField === 'conditional_completeness') {{
      const rows = cont.querySelectorAll('.cc-row');
      tbl.conditional_completeness = Array.from(rows).map(row => {{
        const inputs = row.querySelectorAll('input');
        return {{
          condition:               inputs[0]?.value.trim() || '',
          column_must_not_be_null: inputs[1]?.value.trim() || '',
        }};
      }}).filter(r => r.condition);
    }}
  }});

  return q;
}}

// ── Download ──────────────────────────────────────────────────────────────
function downloadYAML() {{
  const q   = readForm();
  const yml = jsyaml.dump(q, {{ lineWidth: 120, noRefs: true }});
  const blob= new Blob([yml], {{type:'text/yaml'}});
  const a   = document.createElement('a');
  a.href    = URL.createObjectURL(blob);
  a.download= 'questionnaire_filled.yaml';
  a.click();
  document.getElementById('statusMsg').textContent = '✓ Downloaded questionnaire_filled.yaml';
  setTimeout(() => document.getElementById('statusMsg').textContent = '', 4000);
}}
</script>
</body>
</html>"""


# ── Per-table HTML section ──────────────────────────────────────────────────

def _table_section(tbl: dict) -> str:
    idx  = tbl.get("_idx", 0)
    name = tbl["name"]

    ai_role_html         = _ai_role_select(idx, tbl.get("ai_role","unknown"))
    categorical_html     = _categorical_section(idx, tbl.get("categorical_columns") or [])
    numeric_html         = _numeric_section(idx, tbl.get("numeric_columns") or [])
    timestamp_html       = _timestamp_section(idx, tbl.get("timestamp_columns") or [])
    id_html              = _id_section(idx, tbl.get("id_columns") or [])
    monetary_html        = _monetary_section(idx, tbl.get("monetary_columns") or [])
    fk_html              = _fk_section(idx, tbl.get("foreign_keys") or [])
    rule_html            = _rules_section(idx, tbl.get("business_rules") or [])
    cc_html              = _cond_completeness_section(idx, tbl.get("conditional_completeness") or [])
    vol_html             = _volume_section(idx, tbl.get("volume") or {})

    row_summary = (
        f"<span style='font-size:12px;opacity:.7;margin-left:8px'>"
        f"{len(tbl.get('categorical_columns') or [])} categorical · "
        f"{len(tbl.get('numeric_columns') or [])} numeric · "
        f"{len(tbl.get('timestamp_columns') or [])} timestamp"
        f"</span>"
    )

    return f"""
  <div class="tbl-card">
    <div class="tbl-header" id="hdr_{idx}">
      <h2>{name}{row_summary}</h2>
      <span class="chevron">▼</span>
    </div>
    <div class="tbl-body" id="body_{idx}">

      <div class="sub">
        <div class="sub-title">Table Role <span class="pill pill-todo">TODO</span></div>
        {ai_role_html}
      </div>

      {id_html}
      {monetary_html}
      {categorical_html}
      {numeric_html}
      {timestamp_html}
      {fk_html}
      {rule_html}
      {cc_html}
      {vol_html}

    </div>
  </div>"""


def _ai_role_select(idx: int, current: str) -> str:
    opts = ["unknown","training_data","feature_store","target_variable","reference","operational"]
    options = "\n".join(
        f'<option value="{o}" {"selected" if o == current else ""}>{o}</option>'
        for o in opts
    )
    return f"""
      <div class="field" style="max-width:340px">
        <label>AI / ML Role</label>
        <select data-table="{idx}" data-field="ai_role" data-field="ai_role">
          {options}
        </select>
        <div class="hint">What does this table feed into?</div>
      </div>"""


def _id_section(idx: int, id_cols: list) -> str:
    if not id_cols:
        return ""
    rows = ""
    for ci, c in enumerate(id_cols):
        kind = "Primary Key" if c.get("is_primary_key") else "Business Key"
        rows += f"""
        <div class="field">
          <label>{c['column']} <span class="pill pill-auto">Auto-discovered</span></label>
          <input type="text" value="{kind}" readonly style="background:#f8fafc;color:var(--muted)"/>
          <div class="hint">Will run uniqueness checks (IDs 5, 6, 7)</div>
        </div>"""
    return f"""
      <div class="sub">
        <div class="sub-title">ID / Key Columns <span class="pill pill-auto">Auto-discovered</span></div>
        <div class="field-grid">{rows}</div>
      </div>"""


def _monetary_section(idx: int, monetary_cols: list) -> str:
    if not monetary_cols:
        return ""
    rows = ""
    for ci, c in enumerate(monetary_cols):
        ccy_col = c.get("currency_column") or ""
        ccy_val = ", ".join(c.get("valid_currencies") or [])
        rows += f"""
        <div class="field">
          <label>{c['column']} <span class="pill pill-auto">Auto-discovered</span></label>
          <input type="text" value="Monetary column" readonly style="background:#f8fafc;color:var(--muted)"/>
          <div class="hint">No-negative check (ID 12) runs automatically</div>
        </div>
        <div class="field">
          <label>Currency column (for {c['column']}) <span class="pill pill-optional">Optional</span></label>
          <input type="text" placeholder="e.g. currency_code"
            value="{ccy_col}"
            data-table="{idx}" data-field="monetary_columns" data-col="{ci}" data-sub="currency_column"/>
          <div class="hint">Sibling column holding the currency code</div>
        </div>
        <div class="field">
          <label>Valid currencies <span class="pill pill-optional">Optional</span></label>
          <input type="text" placeholder="USD, EUR, GBP"
            value="{ccy_val}"
            data-table="{idx}" data-field="monetary_columns" data-col="{ci}" data-sub="valid_currencies_csv"/>
          <div class="hint">Enables check ID 15 — comma-separated</div>
        </div>"""
    return f"""
      <div class="sub">
        <div class="sub-title">Monetary Columns <span class="pill pill-auto">Auto-discovered</span></div>
        <div class="field-grid">{rows}</div>
      </div>"""


def _categorical_section(idx: int, cat_cols: list) -> str:
    if not cat_cols:
        return ""
    fields = ""
    for ci, c in enumerate(cat_cols):
        rep_id = f"rep_categorical_columns_{idx}_{ci}"
        existing_rows = "".join(
            f'<div class="rep-row"><input type="text" value="{_esc(v)}"/>'
            f'<button onclick="this.parentElement.remove()" title="Remove">✕</button></div>'
            for v in (c.get("valid_values") or [])
        )
        pill = "pill-auto" if c.get("valid_values") else "pill-todo"
        pill_lbl = "Auto-discovered" if c.get("valid_values") else "TODO"
        std_checked = "checked" if c.get("standardize") else ""
        fields += f"""
        <div class="field" style="grid-column:span 1">
          <label>{c['column']} — valid values <span class="pill {pill}">{pill_lbl}</span></label>
          <div id="{rep_id}">{existing_rows}</div>
          <button class="add-btn" onclick="addRow('{rep_id}')">+ add value</button>
          <div class="hint">Enables allowed-value check (ID 9). Verify auto-filled values.</div>
          <label style="margin-top:8px;display:flex;align-items:center;gap:6px;font-size:12px">
            <input type="checkbox" {std_checked}
              data-table="{idx}" data-field="categorical_columns" data-col="{ci}" data-sub="standardize"/>
            Also check case/trim standardization (ID 16)
          </label>
        </div>"""
    return f"""
      <div class="sub">
        <div class="sub-title">Categorical Columns — Allowed Values</div>
        <div class="field-grid">{fields}</div>
      </div>"""


def _numeric_section(idx: int, num_cols: list) -> str:
    if not num_cols:
        return ""
    fields = ""
    for ci, c in enumerate(num_cols):
        mn = "" if c.get("min") is None else c["min"]
        mx = "" if c.get("max") is None else c["max"]
        pill = "pill-auto" if (c.get("min") is not None or c.get("max") is not None) else "pill-todo"
        pill_lbl = "Auto-discovered" if pill == "pill-auto" else "TODO"
        pos_checked = "checked" if c.get("must_be_positive") else ""
        fields += f"""
        <div class="field">
          <label>{c['column']} <span class="pill {pill}">{pill_lbl}</span></label>
          <div style="display:flex;gap:8px;margin-bottom:4px">
            <input type="number" step="any" placeholder="min"
              value="{mn}"
              data-table="{idx}" data-field="numeric_columns" data-col="{ci}" data-sub="min"
              style="flex:1"/>
            <input type="number" step="any" placeholder="max"
              value="{mx}"
              data-table="{idx}" data-field="numeric_columns" data-col="{ci}" data-sub="max"
              style="flex:1"/>
          </div>
          <label style="display:flex;align-items:center;gap:6px;font-size:12px">
            <input type="checkbox" {pos_checked}
              data-table="{idx}" data-field="numeric_columns" data-col="{ci}" data-sub="must_be_positive"/>
            Must be ≥ 0 (no negatives check — ID 12)
          </label>
          <div class="hint">Enables range check (ID 11). Leave blank if unknown.</div>
        </div>"""
    return f"""
      <div class="sub">
        <div class="sub-title">Numeric Columns — Ranges</div>
        <div class="field-grid">{fields}</div>
      </div>"""


def _timestamp_section(idx: int, ts_cols: list) -> str:
    if not ts_cols:
        return ""
    fields = ""
    for ci, c in enumerate(ts_cols):
        sla = "" if c.get("freshness_sla_hours") is None else c["freshness_sla_hours"]
        late = "" if c.get("late_arrival_hours") is None else c["late_arrival_hours"]
        after = c.get("must_be_after") or ""
        evt_checked = "checked" if c.get("is_event_time") else ""
        fields += f"""
        <div class="field">
          <label>{c['column']} <span class="pill pill-todo">TODO</span></label>
          <div style="margin-bottom:6px">
            <span style="font-size:11px;color:var(--muted)">Freshness SLA (hours)</span>
            <input type="number" step="1" min="1" placeholder="e.g. 24"
              value="{sla}"
              data-table="{idx}" data-field="timestamp_columns" data-col="{ci}" data-sub="freshness_sla_hours"/>
            <div class="hint">Enables freshness (ID 20) and stale table (ID 21) checks</div>
          </div>
          <label style="display:flex;align-items:center;gap:6px;font-size:12px;margin-bottom:6px">
            <input type="checkbox" {evt_checked}
              data-table="{idx}" data-field="timestamp_columns" data-col="{ci}" data-sub="is_event_time"/>
            This is an event timestamp (when the event actually happened)
          </label>
          <div style="margin-bottom:6px">
            <span style="font-size:11px;color:var(--muted)">Max late-arrival lag (hours) — if event timestamp</span>
            <input type="number" step="1" min="1" placeholder="e.g. 6"
              value="{late}"
              data-table="{idx}" data-field="timestamp_columns" data-col="{ci}" data-sub="late_arrival_hours"/>
            <div class="hint">Enables late-arriving data check (ID 22)</div>
          </div>
          <div>
            <span style="font-size:11px;color:var(--muted)">Must be after column (chronological order)</span>
            <input type="text" placeholder="e.g. created_at"
              value="{after}"
              data-table="{idx}" data-field="timestamp_columns" data-col="{ci}" data-sub="must_be_after"/>
            <div class="hint">Enables cross-field consistency check (ID 13)</div>
          </div>
        </div>"""
    return f"""
      <div class="sub">
        <div class="sub-title">Timestamp Columns — Freshness &amp; Ordering</div>
        <div class="field-grid">{fields}</div>
      </div>"""


def _fk_section(idx: int, fks: list) -> str:
    existing = ""
    for fk in fks:
        existing += f"""
        <div class="rep-row fk-row">
          <input type="text" placeholder="column" value="{_esc(fk.get('column',''))}"/>
          <input type="text" placeholder="references_table" value="{_esc(fk.get('references_table',''))}"/>
          <input type="text" placeholder="references_column" value="{_esc(fk.get('references_column',''))}"/>
          <button onclick="this.parentElement.remove()" title="Remove">✕</button>
        </div>"""
    rep_id = f"rep_fk_{idx}"
    return f"""
      <div class="sub">
        <div class="sub-title">Foreign Keys <span class="pill pill-todo">TODO</span></div>
        <div style="font-size:12px;color:var(--muted);margin-bottom:8px">
          Enables FK violation (ID 17), orphan (ID 18), and join coverage (ID 19) checks.<br>
          Format: <em>column → references_table.references_column</em>
        </div>
        <div id="{rep_id}" data-rep-table="{idx}" data-rep-field="foreign_keys">{existing}</div>
        <button class="add-btn" onclick="addFKRow('{rep_id}')">+ add foreign key</button>
      </div>"""


def _rules_section(idx: int, rules: list) -> str:
    existing = "".join(
        f'<div class="rep-row"><input class="rule-input" type="text" placeholder="SQL condition" value="{_esc(r)}"/>'
        f'<button onclick="this.parentElement.remove()" title="Remove">✕</button></div>'
        for r in rules
    )
    rep_id = f"rep_rules_{idx}"
    return f"""
      <div class="sub">
        <div class="sub-title">Business Rules <span class="pill pill-todo">TODO</span></div>
        <div style="font-size:12px;color:var(--muted);margin-bottom:8px">
          SQL expressions that must always be TRUE. Enables check ID 26.<br>
          Examples: <em>net_paid = sales_price * quantity</em> &nbsp;·&nbsp; <em>discount_pct BETWEEN 0 AND 100</em>
        </div>
        <div id="{rep_id}" data-rep-table="{idx}" data-rep-field="business_rules">{existing}</div>
        <button class="add-btn" onclick="addRuleRow('{rep_id}')">+ add rule</button>
      </div>"""


def _cond_completeness_section(idx: int, items: list) -> str:
    existing = ""
    for cc in items:
        existing += f"""
        <div class="rep-row cc-row">
          <input type="text" placeholder="condition  e.g. status = 'shipped'" value="{_esc(cc.get('condition',''))}"/>
          <input type="text" placeholder="column must not be null" value="{_esc(cc.get('column_must_not_be_null',''))}"/>
          <button onclick="this.parentElement.remove()" title="Remove">✕</button>
        </div>"""
    rep_id = f"rep_cc_{idx}"
    return f"""
      <div class="sub">
        <div class="sub-title">Conditional Completeness <span class="pill pill-todo">TODO</span></div>
        <div style="font-size:12px;color:var(--muted);margin-bottom:8px">
          "When [condition] is true, [column] must not be null." Enables check ID 4.
        </div>
        <div id="{rep_id}" data-rep-table="{idx}" data-rep-field="conditional_completeness">{existing}</div>
        <button class="add-btn" onclick="addCCRow('{rep_id}')">+ add condition</button>
      </div>"""


def _volume_section(idx: int, volume: dict) -> str:
    pct   = volume.get("max_daily_change_pct", 30)
    prior = volume.get("prior_table") or ""
    return f"""
      <div class="sub">
        <div class="sub-title">Volume Monitoring <span class="pill pill-optional">Optional</span></div>
        <div class="field-grid">
          <div class="field">
            <label>Max daily row-count change (%)</label>
            <input type="number" step="1" min="1" max="100" value="{pct}"
              data-table="{idx}" data-field="volume_change_pct"/>
            <div class="hint">Alert if row count grows/shrinks by more than this. Enables check ID 23.</div>
          </div>
          <div class="field">
            <label>Yesterday's snapshot table <span class="pill pill-optional">Optional</span></label>
            <input type="text" placeholder="schema.table_yesterday"
              value="{prior}"
              data-table="{idx}" data-field="prior_table"/>
            <div class="hint">Required for volume change check (ID 23). Leave blank if unavailable.</div>
          </div>
        </div>
      </div>"""


def _esc(s: Any) -> str:
    return str(s).replace('"', "&quot;").replace("'", "&#39;") if s else ""


# ── Inject table index + add JS helpers into full HTML ──────────────────────

def build_form(questionnaire: dict) -> str:
    # Tag each table with its index so the JS can address it
    for i, tbl in enumerate(questionnaire.get("tables", [])):
        tbl["_idx"] = i

    html = _build_form_inner(questionnaire)

    # Inject extra JS helpers at end of <script> block
    extra_js = """
<script>
function addFKRow(containerId) {
  const c = document.getElementById(containerId);
  const row = document.createElement('div');
  row.className = 'rep-row fk-row';
  row.innerHTML = `
    <input type="text" placeholder="column"/>
    <input type="text" placeholder="references_table"/>
    <input type="text" placeholder="references_column"/>
    <button onclick="this.parentElement.remove()" title="Remove">✕</button>`;
  c.appendChild(row);
}
function addRuleRow(containerId) {
  const c = document.getElementById(containerId);
  const row = document.createElement('div');
  row.className = 'rep-row';
  row.innerHTML = `<input class="rule-input" type="text" placeholder="SQL condition e.g. amount >= 0"/>
    <button onclick="this.parentElement.remove()" title="Remove">✕</button>`;
  c.appendChild(row);
}
function addCCRow(containerId) {
  const c = document.getElementById(containerId);
  const row = document.createElement('div');
  row.className = 'rep-row cc-row';
  row.innerHTML = `
    <input type="text" placeholder="condition  e.g. status = 'shipped'"/>
    <input type="text" placeholder="column must not be null  e.g. shipped_at"/>
    <button onclick="this.parentElement.remove()" title="Remove">✕</button>`;
  c.appendChild(row);
}
</script>"""
    return html.replace("</body>", extra_js + "\n</body>")


# rename inner function so build_form wrapper can call it
_build_form_inner = lambda q: _generate_html(q)


def _generate_html(questionnaire: dict) -> str:
    q_json = json.dumps(questionnaire, indent=2, default=str)
    meta   = questionnaire.get("meta", {})
    tables = questionnaire.get("tables", [])
    scan_name = meta.get("scan_name", "DQS Scan")
    sections_html = "\n".join(_table_section(t) for t in tables)

    # Return the full HTML template
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>DQS Questionnaire — {scan_name}</title>
<script src="https://cdn.jsdelivr.net/npm/js-yaml@4.1.0/dist/js-yaml.min.js"></script>
<style>
  :root{{--blue:#1B3D6F;--green:#47A848;--light:#F0F4F8;--border:#D1DCE8;--text:#1a2433;--muted:#64748b;--red:#dc2626}}
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'Segoe UI',system-ui,sans-serif;background:var(--light);color:var(--text);font-size:14px}}
  header{{background:var(--blue);color:#fff;padding:18px 32px}}
  header h1{{font-size:20px;font-weight:700;margin-bottom:4px}}
  header p{{font-size:13px;opacity:.7}}
  .container{{max-width:1100px;margin:0 auto;padding:24px 16px 100px}}
  .intro{{background:#fff;border:1px solid var(--border);border-radius:10px;padding:20px 24px;margin-bottom:24px;line-height:1.7}}
  .intro h2{{font-size:15px;color:var(--blue);margin-bottom:8px}}
  .pill{{display:inline-block;padding:2px 9px;border-radius:99px;font-size:11px;font-weight:600}}
  .pill-todo{{background:#FEF3C7;color:#92400E}}
  .pill-auto{{background:#D1FAE5;color:#065F46}}
  .pill-optional{{background:#E0E7FF;color:#3730A3}}
  .tbl-card{{background:#fff;border:1px solid var(--border);border-radius:10px;margin-bottom:20px;overflow:hidden}}
  .tbl-header{{background:var(--blue);color:#fff;padding:14px 20px;display:flex;align-items:center;justify-content:space-between;cursor:pointer;user-select:none}}
  .tbl-header h2{{font-size:15px;font-weight:600}}
  .chevron{{transition:transform .2s}}
  .tbl-header.open .chevron{{transform:rotate(180deg)}}
  .tbl-body{{padding:20px;display:none}}
  .tbl-body.open{{display:block}}
  .sub{{margin-bottom:22px}}
  .sub-title{{font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.05em;color:var(--muted);margin-bottom:10px;display:flex;align-items:center;gap:8px}}
  .sub-title::after{{content:'';flex:1;height:1px;background:var(--border)}}
  .field-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:12px}}
  .field{{background:var(--light);border:1px solid var(--border);border-radius:8px;padding:12px}}
  .field label{{font-size:12px;font-weight:600;color:var(--muted);display:block;margin-bottom:6px}}
  .field input,.field select,.field textarea{{width:100%;border:1px solid var(--border);border-radius:6px;padding:7px 10px;font-size:13px;background:#fff;color:var(--text);font-family:inherit}}
  .field input:focus,.field select:focus{{outline:2px solid var(--green);border-color:var(--green)}}
  .field .hint{{font-size:11px;color:var(--muted);margin-top:4px}}
  .rep-row{{display:flex;gap:8px;align-items:center;margin-bottom:8px}}
  .rep-row input{{flex:1;border:1px solid var(--border);border-radius:6px;padding:7px 10px;font-size:13px}}
  .rep-row button{{flex-shrink:0;background:none;border:1px solid var(--border);border-radius:6px;padding:6px 10px;cursor:pointer;color:var(--muted)}}
  .rep-row button:hover{{background:#fee2e2;color:var(--red);border-color:var(--red)}}
  .add-btn{{background:none;border:1px dashed var(--green);border-radius:6px;padding:6px 14px;color:var(--green);font-size:12px;cursor:pointer;font-weight:600;margin-top:4px}}
  .add-btn:hover{{background:#f0fdf4}}
  .action-bar{{position:fixed;bottom:0;left:0;right:0;background:#fff;border-top:1px solid var(--border);padding:14px 32px;display:flex;align-items:center;gap:12px;z-index:100}}
  .btn-primary{{background:var(--green);color:#fff;border:none;border-radius:8px;padding:10px 24px;font-size:14px;font-weight:600;cursor:pointer}}
  .btn-primary:hover{{background:#3a9139}}
  .btn-secondary{{background:#fff;color:var(--blue);border:2px solid var(--blue);border-radius:8px;padding:9px 20px;font-size:13px;font-weight:600;cursor:pointer}}
  .btn-secondary:hover{{background:var(--light)}}
  #statusMsg{{font-size:13px;color:var(--muted);margin-left:auto}}
</style>
</head>
<body>
<header>
  <h1>GovernIQ &nbsp;·&nbsp; DQS Client Questionnaire</h1>
  <p>Scan: {scan_name} &nbsp;·&nbsp; Fill in highlighted fields, then click Download YAML</p>
</header>

<div class="container">
  <div class="intro">
    <h2>Instructions</h2>
    Fields marked <span class="pill pill-auto">Auto-discovered</span> were pre-filled from your schema — please verify.<br>
    Fields marked <span class="pill pill-todo">TODO</span> need your business knowledge — these unlock the highest-impact AI-readiness checks.<br>
    Fields marked <span class="pill pill-optional">Optional</span> add value but can be skipped.<br><br>
    When done: click <strong>Download YAML</strong>, then share the file with your DQS engineer who will run:<br>
    <code style="background:#F0F4F8;padding:3px 10px;border-radius:4px;font-size:12px;display:inline-block;margin-top:6px">
      dqs generate-config --questionnaire questionnaire_filled.yaml --out scan_config.yaml
    </code>
  </div>

  <div id="tables">
{sections_html}
  </div>
</div>

<div class="action-bar">
  <button class="btn-primary" onclick="downloadYAML()">⬇ Download Completed YAML</button>
  <button class="btn-secondary" onclick="expandAll()">Expand All</button>
  <button class="btn-secondary" onclick="collapseAll()">Collapse All</button>
  <span id="statusMsg"></span>
</div>

<script>
const SEED = {q_json};

document.querySelectorAll('.tbl-header').forEach(h => {{
  h.addEventListener('click', () => {{
    h.classList.toggle('open');
    h.nextElementSibling.classList.toggle('open');
  }});
}});
function expandAll()   {{ document.querySelectorAll('.tbl-body,.tbl-header').forEach(e => e.classList.add('open')); }}
function collapseAll() {{ document.querySelectorAll('.tbl-body,.tbl-header').forEach(e => e.classList.remove('open')); }}
function addRow(id) {{
  const c = document.getElementById(id);
  const r = document.createElement('div'); r.className='rep-row';
  r.innerHTML=`<input type="text" placeholder="value"/><button onclick="this.parentElement.remove()">✕</button>`;
  c.appendChild(r);
}}

function readForm() {{
  const q = JSON.parse(JSON.stringify(SEED));
  document.querySelectorAll('[data-table]').forEach(el => {{
    const ti = parseInt(el.dataset.table), field = el.dataset.field;
    const ci = el.dataset.col !== undefined ? parseInt(el.dataset.col) : null;
    const sub = el.dataset.sub;
    const tbl = q.tables[ti]; if (!tbl) return;
    let val = el.type==='checkbox' ? el.checked : el.value.trim();
    if (ci !== null && sub && tbl[field] && tbl[field][ci]) {{
      if (sub==='valid_values') {{
        const cont = document.getElementById(`rep_categorical_columns_${{ti}}_${{ci}}`);
        val = cont ? Array.from(cont.querySelectorAll('input')).map(i=>i.value.trim()).filter(Boolean) : [];
      }} else if (['min','max','freshness_sla_hours','late_arrival_hours'].includes(sub)) {{
        val = val==='' ? null : (isNaN(Number(val)) ? val : Number(val));
      }}
      tbl[field][ci][sub] = val;
    }} else if (field==='volume_change_pct') {{
      tbl.volume.max_daily_change_pct = val==='' ? 30 : Number(val);
    }} else if (field==='prior_table') {{
      tbl.volume.prior_table = val||null;
    }} else if (!['business_rules','foreign_keys','conditional_completeness'].includes(field)) {{
      tbl[field] = val||null;
    }}
  }});
  document.querySelectorAll('[data-rep-table]').forEach(cont => {{
    const ti = parseInt(cont.dataset.repTable), rf = cont.dataset.repField;
    const tbl = q.tables[ti]; if (!tbl) return;
    if (rf==='business_rules')
      tbl.business_rules = Array.from(cont.querySelectorAll('.rule-input')).map(i=>i.value.trim()).filter(Boolean);
    else if (rf==='foreign_keys')
      tbl.foreign_keys = Array.from(cont.querySelectorAll('.fk-row')).map(r=>{{
        const ins=r.querySelectorAll('input');
        return {{column:ins[0]?.value.trim(),references_table:ins[1]?.value.trim(),references_column:ins[2]?.value.trim()}};
      }}).filter(r=>r.column);
    else if (rf==='conditional_completeness')
      tbl.conditional_completeness = Array.from(cont.querySelectorAll('.cc-row')).map(r=>{{
        const ins=r.querySelectorAll('input');
        return {{condition:ins[0]?.value.trim(),column_must_not_be_null:ins[1]?.value.trim()}};
      }}).filter(r=>r.condition);
  }});
  return q;
}}

function downloadYAML() {{
  const q=readForm(), yml=jsyaml.dump(q,{{lineWidth:120,noRefs:true}});
  const a=document.createElement('a');
  a.href=URL.createObjectURL(new Blob([yml],{{type:'text/yaml'}}));
  a.download='questionnaire_filled.yaml'; a.click();
  const m=document.getElementById('statusMsg');
  m.textContent='✓ Downloaded questionnaire_filled.yaml';
  setTimeout(()=>m.textContent='',4000);
}}
</script>
</body>
</html>"""
