from dqs.scanner import scan_from_file
import json
import os

print(f"CWD: {os.getcwd()}")
res = scan_from_file('../tpcds_full_scan.yaml')
print(f"Success! Found tables.")
