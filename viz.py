import json, sys

file = sys.argv[1] if len(sys.argv) > 1 else "tpcds_result.json"
r = json.load(open(file))

print(f"\n{'='*50}")
print(f"  {r['scan_name']}  —  Score: {r['overall_score']:.1f}/100")
print(f"  {r['checks_passed']} passed  {r['checks_failed']} failed  {r['checks_skipped']} skipped")
print(f"{'='*50}")

for dim in r["dimensions"]:
    s = dim["dimension_score"]
    bar = "█" * int(s / 5) + "░" * (20 - int(s / 5))
    flag = "✗" if dim["checks_failed"] else " "
    print(f" {flag} {dim['dimension']:22s} {bar} {s:5.1f}")

failed = [c for d in r["dimensions"] for c in d["checks"] if not c["passed"] and not c["skipped"]]
if failed:
    print(f"\n  Failed checks:")
    for c in failed:
        loc = c.get("table") or ""
        col = f".{c['column']}" if c.get("column") else ""
        val = f"{c['metric_value']:.4f}" if c["metric_value"] is not None else "N/A"
        print(f"    [{c['check_id']:2d}] {c['check_name']:35s} {loc}{col}  val={val}")
print()
