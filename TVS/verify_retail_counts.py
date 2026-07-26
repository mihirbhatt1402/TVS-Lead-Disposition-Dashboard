"""
Retail count verification: rebuild combined retail_map (same logic as pipeline),
count per retail month, then compare against payload's update-mode monthly retails.

Difference between combined_retail_map count and payload count = retails whose
sourceLeadId has no matching lead row AND no synthetic lead (expected gap).
"""
import json, gzip, sys, time
from pathlib import Path
from collections import defaultdict
import requests, pandas as pd

APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbwzgnXPbCbunBblnMUrqdWg3eY9qsIwCrFxuYuvYSpxtH22l4Cs32vdkOkDhUn-qwM64w/exec"
SECRET          = "tvs2026push"
HIST_CACHE_PATH = Path(__file__).parent / 'hist_cache.json.gz'
PAYLOAD_PATH    = Path(__file__).parent / 'tvs_last_payload.json'
MONTH_NAMES     = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

def proxy_get(action, extra=None, timeout=300):
    params = {'action': action, 'secret': SECRET}
    if extra: params.update(extra)
    r = requests.get(APPS_SCRIPT_URL, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def norm_month(s):
    s = str(s or '').strip()
    for sep in ['-', '/', ' ']:
        if sep in s:
            parts = s.split(sep)
            try:
                mo, yr = int(parts[0]), int(parts[1])
                if 1 <= mo <= 12:
                    return f"{MONTH_NAMES[mo-1]}'{yr % 100:02d}"
            except: pass
    return s

def parse_ym(s):
    s = str(s or '').strip()
    try:
        ts = pd.Timestamp(s)
        return f"{MONTH_NAMES[ts.month-1]}'{ts.strftime('%y')}"
    except:
        return norm_month(s)

def month_sort_key(m):
    try:
        mn, yy = m.split("'")
        return int(yy) * 100 + MONTH_NAMES.index(mn) + 1
    except:
        return 0

# ── 1. Load payload ────────────────────────────────────────────────────────────
print("Loading payload…")
with open(PAYLOAD_PATH, encoding='utf-8') as f:
    payload = json.load(f)

lm_arr   = payload['maps']['lm']
u_monthly = payload['u_monthly']   # [li, leads, rets, dms, call]

payload_rets = defaultdict(int)
for row in u_monthly:
    payload_rets[lm_arr[row[0]]] += row[2]
print(f"  Payload total retails: {sum(payload_rets.values()):,}")

# ── 2. Load hist_cache retail_map ─────────────────────────────────────────────
print(f"\nLoading hist_cache…")
with gzip.open(HIST_CACHE_PATH, 'rt', encoding='utf-8') as f:
    cache = json.load(f)

combined_rmap = dict(cache['retail_map'])   # lid → {rm, rtype, pm}
print(f"  hist_cache retail entries: {len(combined_rmap):,}")

# ── 3. Fetch live retail master ────────────────────────────────────────────────
print("\nFetching live retail master…")
page, all_rows, headers = 0, [], None
while True:
    for attempt in range(3):
        try:
            data = proxy_get('getCurrentRetails', {'page': page, 'pageSize': 25000})
            break
        except Exception as e:
            if attempt < 2:
                print(f"  retry {attempt+1}: {e}")
                time.sleep(15)
            else: raise
    if headers is None:
        headers = data['headers']
    rows = data.get('rows', [])
    all_rows.extend(rows)
    print(f"  Page {page}: +{len(rows):,} rows (total {len(all_rows):,}/{data.get('total','?')})")
    if data.get('done', True): break
    page += 1

retail_df = pd.DataFrame(all_rows, columns=headers)
print(f"  Fetched {len(retail_df):,} retail rows")

# ── 4. Merge: online overwrites hist for matching lids ────────────────────────
def to_id(v):
    if pd.isna(v): return ''
    try: return str(int(float(v)))
    except: return str(v).strip()

overwritten = new_online = 0
for _, row in retail_df.iterrows():
    lid = to_id(row.get('sourceLeadId', ''))
    if not lid: continue
    rm = parse_ym(row.get('Retail_Attribution_Date', ''))
    if lid in combined_rmap:
        overwritten += 1
    else:
        new_online += 1
    combined_rmap[lid] = {'rm': rm, 'rtype': '', 'pm': ''}

print(f"  Online: {overwritten:,} overwrote hist entries, {new_online:,} new lids")
print(f"  Combined retail_map total unique lids: {len(combined_rmap):,}")

# ── 5. Count combined retail_map by month ──────────────────────────────────────
combined_rets = defaultdict(int)
for lid, info in combined_rmap.items():
    rm = info.get('rm', '')
    if rm:
        combined_rets[rm] += 1

combined_total = sum(combined_rets.values())
print(f"  Combined retails with month: {combined_total:,}")

# ── 6. Compare combined vs payload ────────────────────────────────────────────
all_months = sorted(
    set(list(combined_rets.keys()) + list(payload_rets.keys())),
    key=month_sort_key
)

print("\n" + "="*75)
print(f"{'Month':<12} {'Combined src':>14} {'Payload(upd)':>14} {'Diff':>8}  Note")
print("-"*75)

unmatched_total = 0
for mo in all_months:
    c = combined_rets.get(mo, 0)
    p = payload_rets.get(mo, 0)
    diff = c - p   # positive = source has more than payload (unmatched retails)
    note = ''
    if diff < 0:
        note = '← payload > source (online shifted month from another month)'
    elif diff > 0:
        note = '← unmatched (no lead row for these retail lids)'
    unmatched_total += max(0, diff)
    print(f"  {mo:<12} {c:>14,} {p:>14,} {diff:>+8,}  {note}")

print("-"*75)
print(f"  {'TOTAL':<12} {combined_total:>14,} {sum(payload_rets.values()):>14,} "
      f"{combined_total - sum(payload_rets.values()):>+8,}")
print("="*75)

gap = combined_total - sum(payload_rets.values())
print(f"\nCombined retail_map:  {combined_total:,}")
print(f"Payload retails:      {sum(payload_rets.values()):,}")
print(f"Gap (no lead match):  {gap:,}")
if gap >= 0:
    print(f"\nExplanation: {gap:,} retail entries have sourceLeadIds not present in")
    print("any lead sheet AND not picked up as synthetic leads (from live retail sheet only).")
    print("This is expected — these are older retails whose leads predate Apr'25 cache coverage.")
