import duckdb, yaml

con = duckdb.connect("test/tpcds_dirty.duckdb")

tables = con.execute("""
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
""").fetchall()

constraints = con.execute("""
    SELECT tc.table_name, tc.constraint_type, kcu.column_name, NULL, NULL
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
    WHERE tc.table_schema = 'main'
""").fetchall()

columns = con.execute("""
    SELECT table_name, column_name, data_type
    FROM information_schema.columns WHERE table_schema = 'main'
""").fetchall()

con.close()

pks = {t: [r[2] for r in constraints if r[0] == t and r[1] == "PRIMARY KEY"] for (t,) in tables}
fks = {t: [r for r in constraints if r[0] == t and r[1] == "FOREIGN KEY"] for (t,) in tables}
cols = {}
for t, c, dt in columns:
    cols.setdefault(t, []).append((c, dt))

checks = []

for (table,) in tables:
    # Check 1: null ratio on first non-SK numeric/varchar column
    non_sk = [(c, dt) for c, dt in cols.get(table, []) if not c.endswith("_sk")]
    if non_sk:
        checks.append({"check_id": 1, "table": table, "column": non_sk[0][0]})

    # Check 5: PK duplicate ratio
    for pk_col in pks.get(table, []):
        checks.append({"check_id": 5, "table": table, "pk_column": pk_col})

    # Check 6: business key (natural ID string columns)
    id_cols = [c for c, dt in cols.get(table, []) if c.endswith("_id") and dt == "VARCHAR"]
    if id_cols:
        checks.append({"check_id": 6, "table": table, "business_key_columns": id_cols[:1]})

    # Check 12: negative values on price/cost/amount columns
    money_cols = [c for c, dt in cols.get(table, []) if any(k in c for k in ("price", "cost", "amt", "amount", "paid", "quantity"))]
    for mc in money_cols[:2]:
        checks.append({"check_id": 12, "table": table, "column": mc})

    # Check 17: FK violations
    for _, _, child_col, ref_table, ref_col in fks.get(table, []):
        if ref_table and ref_col:
            checks.append({
                "check_id": 17,
                "child_table": table, "child_column": child_col,
                "parent_table": ref_table, "parent_column": ref_col,
            })

config = {
    "scan_name": "tpcds_dirty_scan",
    "mode": "live",
    "connector": {"dialect": "duckdb", "duckdb_path": "test/tpcds_dirty.duckdb"},
    "checks": checks,
}

out = "test/tpcds_dirty_scan.yaml"
with open(out, "w") as f:
    yaml.dump(config, f, default_flow_style=False, sort_keys=False)

print(f"Generated {len(checks)} checks for {len(tables)} tables → {out}")
