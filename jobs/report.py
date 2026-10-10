
"""Print a Markdown summary of all daily runs. Paste the output into README.md."""
import json
from pathlib import Path

SUMMARY_DIR = Path(__file__).resolve().parent.parent / "data" / "summary"

days = [json.loads(p.read_text()) for p in sorted(SUMMARY_DIR.glob("*.json"))]
if not days:
    raise SystemExit("No summaries found. Run the DAG first.")

keys = [k for k in days[0] if k != "date"]
total = {k: sum(d[k] for d in days) for k in keys}

print(f"Days processed: {len(days)} ({days[0]['date']} to {days[-1]['date']})\n")
print("| Metric | Count |")
print("| --- | ---: |")
for k in keys:
    print(f"| {k} | {total[k]:,} |")

if total["flagged"] and total["known_fraud"]:
    print(f"\nShare of all transactions flagged: {total['flagged'] / total['transactions']:.2%}")
    print(f"Known fraud caught (recall): {total['known_fraud_flagged'] / total['known_fraud']:.1%}")
    print(f"Flags that are known fraud (precision): {total['known_fraud_flagged'] / total['flagged']:.1%}")