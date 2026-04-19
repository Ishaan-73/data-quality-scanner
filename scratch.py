from dqs.scanner import scan_from_file
import json
res = scan_from_file('tpcds_full_scan.yaml')
print(json.dumps(res, indent=2))
