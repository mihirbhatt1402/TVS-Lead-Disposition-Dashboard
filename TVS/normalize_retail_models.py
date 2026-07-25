"""
One-time script: normalize purchasedModel column in the TVS retail Google Sheet.
Steps:
  1. Fetch all retail rows via getCurrentRetails Apps Script
  2. For each row where normalized name differs from raw name, record (row_index, clean_name)
  3. POST batch update to Apps Script batchUpdateRetailModels endpoint

Run once, then the sheet stays clean going forward.
"""

import json, time, requests
from pathlib import Path
import sys

# Add parent dir to import PURCHASED_MODEL_MAP and normalize_purchased_model
sys.path.insert(0, str(Path(__file__).parent))
from push_tvs_data import (
    APPS_SCRIPT_URL, SECRET, normalize_purchased_model, proxy_get
)

def fetch_all_retails():
    page, all_rows, headers = 0, [], None
    while True:
        for attempt in range(3):
            try:
                data = proxy_get('getCurrentRetails', {'page': page, 'pageSize': 25000}, timeout=300)
                break
            except Exception as e:
                if attempt < 2:
                    print(f"  retry {attempt+1}: {e}")
                    time.sleep(15)
                else:
                    raise
        if headers is None:
            headers = data['headers']
        rows = data.get('rows', [])
        all_rows.extend(rows)
        print(f"  Page {page}: +{len(rows):,} rows (total {len(all_rows):,}/{data.get('total','?')})")
        if data.get('done', True):
            break
        page += 1
    return headers, all_rows

print("Fetching retail master…")
headers, rows = fetch_all_retails()
print(f"Total rows: {len(rows):,}")

try:
    pm_col = next(i for i, h in enumerate(headers) if h.lower() == 'purchasedmodel')
except StopIteration:
    print("ERROR: purchasedModel column not found in response headers:", headers)
    sys.exit(1)

updates = []
for i, row in enumerate(rows):
    raw = str(row[pm_col] or '').strip()
    if not raw:
        continue
    normalized = normalize_purchased_model(raw)
    if normalized != raw and normalized != 'Unknown':
        updates.append({'rowIndex': i, 'model': normalized})

print(f"\nRows needing normalization: {len(updates):,}")
if not updates:
    print("Nothing to update — sheet is already clean.")
    sys.exit(0)

# Show sample
for u in updates[:20]:
    raw_val = str(rows[u['rowIndex']][pm_col] or '')
    print(f"  row {u['rowIndex']:5d}: '{raw_val}' → '{u['model']}'")
if len(updates) > 20:
    print(f"  … and {len(updates)-20} more")

confirm = input(f"\nWrite {len(updates):,} updates to the sheet? [y/N] ").strip().lower()
if confirm != 'y':
    print("Aborted.")
    sys.exit(0)

# POST in batches of 500
BATCH = 500
for start in range(0, len(updates), BATCH):
    batch = updates[start:start+BATCH]
    payload = json.dumps({'secret': SECRET, 'updates': batch})
    resp = requests.post(
        APPS_SCRIPT_URL,
        data=payload,
        params={'action': 'batchUpdateRetailModels'},
        headers={'Content-Type': 'application/json'},
        timeout=300
    )
    resp.raise_for_status()
    result = resp.json()
    print(f"  Batch {start}–{start+len(batch)-1}: {result}")
    time.sleep(2)

print("Done.")
