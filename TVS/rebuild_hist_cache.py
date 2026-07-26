"""
HIST CACHE REBUILD — from new Lead Master files (FY25-26 + FY26-27 combined).

Source files (all in HIST_DIR):
  - Leads Data Master_Leads_FY_25_26 Part 1.xlsb  → sheet 'Raw Data'
  - Leads Data Master_Leads_FY_25_26 Part 2.xlsx   → sheet 'Sheet1'
  - Leads Data Master_Leads_FY_25_26 & FY_26_27 Part 3.xlsx → sheet 'Sheet1'

These files contain BOTH lead data AND retail reconciliation in each row:
  - Lead data:   opty_id, Lead Month, ModelName, Source, Lead Type, Dealer_Name,
                 City, State, Zone, Enquiry ID
  - Retail data: DMS/Call Out (retail type), Retail Month, purchasedModel
                 → rows with blank DMS/Call Out = not yet retailed

Output: TVS/hist_cache.json.gz  (replaces previous cache completely)

Run locally:  python TVS/rebuild_hist_cache.py
"""
import json, gzip, re, sys, time, os
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone

HIST_DIR        = os.environ.get('TVS_HIST_DIR', r'C:\Users\mihir.bhatt\Desktop\New folder (2)')
HIST_CACHE_PATH = Path(__file__).parent / 'hist_cache.json.gz'
MONTH_NAMES     = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
LEAD_MASTER_START       = "Apr'25"
LEAD_MASTER_START_ORDER = 2504   # Apr'25 → 25*100+4

# ─── Source file specs ────────────────────────────────────────────────────────
NEW_LEAD_FILES = [
    {'path': 'Leads Data Master_Leads_FY_25_26 Part 1.xlsb',            'engine': 'pyxlsb',  'sheet': 'Raw Data'},
    {'path': 'Leads Data Master_Leads_FY_25_26 Part 2.xlsx',             'engine': 'openpyxl','sheet': 'Sheet1'},
    {'path': 'Leads Data Master_Leads_FY_25_26 & FY_26_27 Part 3.xlsx', 'engine': 'openpyxl','sheet': 'Sheet1'},
]

# ─── Helpers (mirror push_tvs_data.py exactly) ────────────────────────────────

def norm_month(s):
    s = str(s or '').strip()
    if not s: return s
    m   = re.search(r'([A-Za-z]{3})', s)
    yr4 = re.search(r'(\d{4})', s)
    yr2 = re.search(r"['\-\s](\d{2})\b", s)
    if m:
        mn = m.group(1)[0].upper() + m.group(1)[1:].lower()
        if yr4: return f"{mn}'{yr4.group(1)[2:]}"
        if yr2: return f"{mn}'{yr2.group(1)}"
    return s

def parse_ym(s):
    s = str(s or '').strip()
    if not s: return ''
    try:
        ts = pd.Timestamp(s)
        return f"{MONTH_NAMES[ts.month-1]}'{ts.strftime('%y')}"
    except Exception:
        return norm_month(s)

def to_id(v):
    if pd.isna(v): return ''
    try:    return str(int(float(v)))
    except: return str(v).strip()

def month_order(lm):
    try:
        s = norm_month(str(lm or '').strip())
        mn, yy = s.split("'")
        mi = MONTH_NAMES.index(mn) + 1
        return int(yy) * 100 + mi
    except Exception:
        return 0

def is_valid_month(s):
    """True if s matches Mon'YY pattern."""
    return bool(re.fullmatch(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'\d{2}", str(s or '').strip()))

def _excel_serial_to_month(val):
    try:
        n = int(float(val))
        if 30000 < n < 100000:
            dt = datetime(1899, 12, 30) + timedelta(days=n)
            return f"{MONTH_NAMES[dt.month-1]}'{dt.strftime('%y')}"
    except Exception:
        pass
    return parse_ym(val)

# ─── Read and process each file ───────────────────────────────────────────────

all_leads_dfs = []
retail_map    = {}     # {to_id(opty_id) → {rm, rtype, pm}}

print("=" * 65)
print("HIST CACHE REBUILD")
print("=" * 65)

for spec in NEW_LEAD_FILES:
    path = os.path.join(HIST_DIR, spec['path'])
    if not os.path.exists(path):
        print(f"\nERROR: File not found: {path}")
        sys.exit(1)

    t0 = time.time()
    print(f"\nReading: {spec['path']}", flush=True)
    df = pd.read_excel(path, sheet_name=spec['sheet'], engine=spec['engine'])
    print(f"  {len(df):,} rows, {len(df.columns)} cols  ({time.time()-t0:.0f}s)", flush=True)

    # ── Column validation ──────────────────────────────────────────────────
    missing = [c for c in ['opty_id','Lead Month','ModelName','Source','Lead Type',
                           'Dealer_Name','City','State','Zone','DMS/Call Out','Retail Month']
               if c not in df.columns]
    if missing:
        print(f"  WARNING: missing columns: {missing}", flush=True)

    # ── Normalize lead ID ──────────────────────────────────────────────────
    df['_lid'] = df['opty_id'].apply(to_id)
    blank_ids  = (df['_lid'] == '').sum()
    if blank_ids:
        print(f"  Skipping {blank_ids:,} rows with blank opty_id", flush=True)
    df = df[df['_lid'] != ''].copy()

    # ── Normalize Lead Month ───────────────────────────────────────────────
    df['_lm'] = df['Lead Month'].apply(lambda v: norm_month(str(v or '').strip()))
    invalid_lm = df[df['_lm'].apply(lambda s: not is_valid_month(s))]
    if len(invalid_lm):
        print(f"  Fallback-parse for {len(invalid_lm):,} rows with non-standard Lead Month", flush=True)
        df.loc[invalid_lm.index, '_lm'] = df.loc[invalid_lm.index, 'Lead Created Date'].apply(parse_ym)

    # ── Filter: Lead Month < Apr'25 excluded ──────────────────────────────
    before = len(df)
    df = df[df['_lm'].apply(month_order) >= LEAD_MASTER_START_ORDER].copy()
    removed = before - len(df)
    if removed:
        print(f"  Removed {removed:,} rows with Lead Month < {LEAD_MASTER_START}", flush=True)

    # ── Build leads DataFrame ─────────────────────────────────────────────
    leads_df = pd.DataFrame({
        'SorceLeadId': df['_lid'],
        'LeadMonth':   df['_lm'],
        'ModelName':   df['ModelName'].astype(str).str.strip().fillna('Unknown'),
        'Source':      df['Source'].astype(str).str.strip().fillna('Unknown')        if 'Source'      in df.columns else 'Unknown',
        'LeadType':    df['Lead Type'].astype(str).str.strip().fillna('Unknown')     if 'Lead Type'   in df.columns else 'Unknown',
        'State':       df['State'].astype(str).str.strip().str.title().fillna('')    if 'State'       in df.columns else '',
        'Zone':        df['Zone'].astype(str).str.strip().fillna('Unknown')          if 'Zone'        in df.columns else 'Unknown',
        'BuyingDays':  '0',
        'CityName':    df['City'].astype(str).str.strip().fillna('')                 if 'City'        in df.columns else '',
        'DealerName':  df['Dealer_Name'].astype(str).str.strip().fillna('')          if 'Dealer_Name' in df.columns else '',
    })
    all_leads_dfs.append(leads_df)
    print(f"  {len(leads_df):,} lead rows added to buffer", flush=True)

    # ── Build retail_map from DMS/Call Out column ─────────────────────────
    retailed = df[df['DMS/Call Out'].fillna('').astype(str).str.strip().isin(['DMS','Call Out'])].copy()
    print(f"  {len(retailed):,} retailed rows (DMS/Call Out in DMS|Call Out)", flush=True)

    # Retail month — prefer 'Retail Month' column, fallback to Retail_Attribution_Date
    if 'Retail Month' in df.columns:
        retailed['_rm'] = retailed['Retail Month'].apply(lambda v: norm_month(str(v or '').strip()))
    elif 'Retail_Attribution_Date' in df.columns:
        retailed['_rm'] = retailed['Retail_Attribution_Date'].apply(_excel_serial_to_month)
    else:
        retailed['_rm'] = ''

    # Purchased model
    pm_col = next((c for c in df.columns if c.lower() in ('purchasedmodel','purchased model')), None)
    retailed['_pm'] = retailed[pm_col].apply(lambda v: str(v or '').strip()) if pm_col else ''

    added = skipped_bad_rm = dup = 0
    for _, row in retailed.iterrows():
        lid   = row['_lid']
        rm    = row['_rm']
        rtype = str(row['DMS/Call Out'] or '').strip()
        pm    = row['_pm']

        if not is_valid_month(rm):
            skipped_bad_rm += 1
            continue
        if month_order(rm) < LEAD_MASTER_START_ORDER:
            continue   # Jan-Mar'25 retails excluded
        if lid not in retail_map:
            retail_map[lid] = {'rm': rm, 'rtype': rtype, 'pm': pm}
            added += 1
        else:
            dup += 1

    print(f"  retail_map: +{added:,} new  |  {dup:,} duplicates skipped  |  {skipped_bad_rm:,} bad retail month", flush=True)

# ─── Merge all leads & deduplicate ────────────────────────────────────────────
print("\nMerging and deduplicating leads…", flush=True)
hist_leads = pd.concat(all_leads_dfs, ignore_index=True)
before_dedup = len(hist_leads)
hist_leads = hist_leads.drop_duplicates(subset=['SorceLeadId'], keep='last')
print(f"  Combined: {before_dedup:,}  → after dedup: {len(hist_leads):,}  (removed {before_dedup-len(hist_leads):,})", flush=True)

# ─── Validation summary ────────────────────────────────────────────────────────
print("\n" + "="*65)
print("VALIDATION")
print("="*65)

# Month distribution
month_dist = hist_leads['LeadMonth'].value_counts()
months_sorted = sorted(month_dist.index, key=month_order)
print(f"\nLead month distribution:")
for mo in months_sorted:
    print(f"  {mo:8s}: {month_dist[mo]:>10,}")

# Retail map month distribution
from collections import Counter
rm_dist = Counter(v['rm'] for v in retail_map.values())
print(f"\nRetail map month distribution:")
for mo in sorted(rm_dist.keys(), key=month_order):
    rtype_dms = sum(1 for v in retail_map.values() if v['rm']==mo and v['rtype']=='DMS')
    rtype_co  = sum(1 for v in retail_map.values() if v['rm']==mo and v['rtype']=='Call Out')
    print(f"  {mo:8s}: {rm_dist[mo]:>8,}  (DMS={rtype_dms:,}  CO={rtype_co:,})")

print(f"\nTotals:")
print(f"  hist_leads  : {len(hist_leads):,}")
print(f"  retail_map  : {len(retail_map):,}")
dms_total = sum(1 for v in retail_map.values() if v['rtype']=='DMS')
co_total  = sum(1 for v in retail_map.values() if v['rtype']=='Call Out')
print(f"  DMS entries : {dms_total:,}")
print(f"  CO entries  : {co_total:,}")

blank_rt = sum(1 for v in retail_map.values() if not v['rtype'])
print(f"  Blank rtype : {blank_rt:,}  (must be 0)")

# ─── Write hist_cache.json.gz ─────────────────────────────────────────────────
print(f"\nWriting {HIST_CACHE_PATH}…", flush=True)
cache_data = {
    'generated':  datetime.now(timezone.utc).isoformat(),
    'retail_map': retail_map,
    'leads':      hist_leads.to_dict('records'),
}
t0 = time.time()
with gzip.open(HIST_CACHE_PATH, 'wt', encoding='utf-8') as cf:
    json.dump(cache_data, cf, separators=(',', ':'))
size_kb = HIST_CACHE_PATH.stat().st_size // 1024
print(f"  Saved: {size_kb:,} KB  ({time.time()-t0:.0f}s)", flush=True)

print("\n" + "="*65)
print("REBUILD COMPLETE")
print("="*65)
print(f"  hist_cache.json.gz: {size_kb:,} KB")
print(f"  Leads:              {len(hist_leads):,}")
print(f"  Retail map:         {len(retail_map):,}  (DMS={dms_total:,} + CO={co_total:,})")
print(f"\nNext: commit TVS/hist_cache.json.gz and push.")
