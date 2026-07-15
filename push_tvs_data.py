"""
TVS Lead Disposition — Daily Data Push
Runs via GitHub Actions at 11 AM IST every day.

DATA SOURCES:
  Historical (Apr-Jun): XLSX files on public Google Drive  → fetched via direct download
  Current month (Jul+): Private Google Sheets             → fetched via Apps Script proxy
"""

import json, sys, io, re, urllib.request
import pandas as pd
import requests

MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

def norm_month(s):
    """Normalize any month string to Mon'YY format (e.g. Jul'26)."""
    s = str(s or "").strip()
    if not s:
        return s
    m = re.search(r'([A-Za-z]{3})', s)
    yr4 = re.search(r'(\d{4})', s)
    yr2 = re.search(r"['\-\s](\d{2})\b", s)
    if m:
        mn = m.group(1)[0].upper() + m.group(1)[1:].lower()
        if yr4:
            return f"{mn}'{yr4.group(1)[2:]}"
        if yr2:
            return f"{mn}'{yr2.group(1)}"
    return s

APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbwzgnXPbCbunBblnMUrqdWg3eY9qsIwCrFxuYuvYSpxtH22l4Cs32vdkOkDhUn-qwM64w/exec"
SECRET = "tvs2026push"

# ─── Helpers ─────────────────────────────────────────────────────────────────

def to_id(v):
    if pd.isna(v): return ""
    try:    return str(int(float(v)))
    except: return str(v).strip()

def read_drive_xlsx(file_id, label=""):
    print(f"Downloading {label} from Google Drive…", flush=True)
    session = requests.Session()
    resp = session.get("https://drive.google.com/uc?export=download",
                       params={"id": file_id}, timeout=60)
    if "text/html" in resp.headers.get("Content-Type", ""):
        action = re.search(r'<form[^>]*action="([^"]+)"', resp.text)
        inputs = dict(re.findall(r'<input[^>]*name="([^"]+)"[^>]*value="([^"]+)"', resp.text))
        if not action or not inputs:
            raise RuntimeError(f"Cannot parse Drive confirmation page for {label}")
        resp = session.get(action.group(1), params=inputs, stream=True, timeout=120)
    resp.raise_for_status()
    buf = io.BytesIO()
    for chunk in resp.iter_content(chunk_size=1024*1024):
        buf.write(chunk)
    buf.seek(0)
    print(f"  {label}: {len(buf.getvalue())/1024:.0f} KB", flush=True)
    return buf

def proxy_get(action, extra_params=None):
    params = {"action": action, "secret": SECRET}
    if extra_params:
        params.update(extra_params)
    resp = requests.get(APPS_SCRIPT_URL, params=params, timeout=120)
    resp.raise_for_status()
    return resp.json()

# ─── Fetch current month leads (paginated) ───────────────────────────────────

def fetch_current_leads():
    print("Fetching current-month leads from Apps Script (paginated)…", flush=True)
    page, all_rows, headers = 0, [], None
    while True:
        data = proxy_get("getCurrentLeads", {"page": page, "pageSize": 25000})
        if "error" in data:
            raise RuntimeError(f"getCurrentLeads error: {data['error']}")
        if headers is None:
            headers = data["headers"]
        all_rows.extend(data["rows"])
        total = data.get("total", "?")
        print(f"  Page {page}: +{len(data['rows'])} rows  (fetched {len(all_rows):,}/{total})", flush=True)
        if data.get("done", True):
            break
        page += 1
    print(f"  Current leads total: {len(all_rows):,}", flush=True)
    return pd.DataFrame(all_rows, columns=headers)

# ─── Historical XLSX processing ───────────────────────────────────────────────

def read_hist_leads(file_id, label):
    buf = read_drive_xlsx(file_id, label)
    df = pd.read_excel(buf, dtype=str, engine="openpyxl")
    df.columns = [c.strip() for c in df.columns]
    return df

def read_hist_retails(file_id, label):
    buf = read_drive_xlsx(file_id, label)
    df = pd.read_excel(buf, dtype=str, engine="openpyxl")
    df.columns = [c.strip() for c in df.columns]
    return df

def build_retail_map_from_hist(ret_df):
    """Build {lead_id: {rm, rtype}} from historical retails XLSX."""
    id_col  = next((c for c in ret_df.columns if c.lower().replace(" ","") in ("sorceleadid","sourceleadid")), None)
    mth_col = next((c for c in ret_df.columns if c.lower() in ("retail month","retailmonth")), None)
    rt_col  = next((c for c in ret_df.columns if "dms" in c.lower() or ("call" in c.lower() and "out" in c.lower())), None)
    if not id_col:
        raise ValueError(f"Cannot find SorceLeadId in historical retails. Columns: {list(ret_df.columns)}")
    print(f"  Hist retail cols: id={id_col}, month={mth_col}, type={rt_col}", flush=True)
    retail_map = {}
    for _, row in ret_df.iterrows():
        rid = to_id(row.get(id_col, ""))
        if rid:
            retail_map[rid] = {
                "rm":    norm_month(str(row.get(mth_col, "") or "")),
                "rtype": str(row.get(rt_col,  "") or "").strip() if rt_col else "",
            }
    return retail_map

def standardize_hist_leads(df):
    """Normalize historical XLSX columns to canonical names."""
    def col(candidates):
        for c in candidates:
            m = next((x for x in df.columns
                      if x.lower().replace(" ","").replace("_","") ==
                         c.lower().replace(" ","").replace("_","")), None)
            if m: return m
        return None

    mapping = {
        col(["SorceLeadId", "SourceLeadId"]):          "SorceLeadId",
        col(["LeadMonth", "Lead Month"]):               "LeadMonth",
        col(["Source"]):                                "Source",
        col(["LeadType", "Lead Type"]):                 "LeadType",
        col(["ModelName", "Model Name"]):               "ModelName",
        col(["State"]):                                 "State",
        col(["Zone"]):                                  "Zone",
        col(["BuyingDays", "Buying Days"]):             "BuyingDays",
        col(["CityName", "City Name", "City"]):         "CityName",
        col(["DealerName", "Dealer Name", "OutletName", "Outlet Name", "Dealer"]): "DealerName",
    }
    mapping = {k: v for k, v in mapping.items() if k}
    out = df.rename(columns=mapping)
    # Normalize month format to Mon'YY
    if "LeadMonth" in out.columns:
        out["LeadMonth"] = out["LeadMonth"].apply(norm_month)
    # Keep only the canonical columns that exist
    keep = [c for c in ["SorceLeadId","LeadMonth","Source","LeadType","ModelName",
                         "State","Zone","BuyingDays","CityName","DealerName"] if c in out.columns]
    return out[keep].copy()

# ─── Current month leads processing ──────────────────────────────────────────

# Maps current-month sheet column names → canonical names
CURR_COL_MAP = {
    "opty_id":     "SorceLeadId",
    "Lead_Month":  "LeadMonth",
    "Medium":      "Source",
    "lead_type":   "LeadType",
    "model":       "ModelName",
    "State":       "State",
    "City":        "CityName",
    "Dealer_Name": "DealerName",
}

def build_retail_map_from_curr(curr_df):
    """
    Extract retail data embedded in current-month leads sheet.
    Rows with non-empty 'Retail Date' are retailed.
    Uses DMS_Retail_Month as retail month and 'Retail By' as retail type.
    """
    retail_map = {}
    for _, row in curr_df.iterrows():
        retail_date = str(row.get("Retail Date", "") or "").strip()
        if not retail_date:
            continue
        lid = to_id(row.get("opty_id", ""))
        if not lid:
            continue
        retail_map[lid] = {
            "rm":    norm_month(str(row.get("DMS_Retail_Month", "") or "").strip()),
            "rtype": str(row.get("Retail By",        "") or "").strip(),
        }
    return retail_map

def fetch_current_retails():
    """Fetch current-month retails from the separate retails sheet via Apps Script."""
    print("Fetching current-month retails from Apps Script…", flush=True)
    data = proxy_get("getCurrentRetails")
    if "error" in data:
        print(f"  WARNING: getCurrentRetails error: {data['error']}", flush=True)
        return {}
    rows = data.get("rows", [])
    print(f"  Retails sheet rows: {len(rows):,}", flush=True)
    retail_map = {}
    for row in rows:
        lid = to_id(row[0]) if len(row) > 0 else ""
        rm  = norm_month(str(row[1]).strip()) if len(row) > 1 else ""
        if lid:
            retail_map[lid] = {"rm": rm, "rtype": ""}
    return retail_map

def standardize_curr_leads(curr_df, state_to_zone):
    """Rename columns, derive Zone from historical state lookup, add BuyingDays=0."""
    out = curr_df.rename(columns=CURR_COL_MAP).copy()
    out["State"] = out["State"].astype(str).str.strip().str.title()
    # Add Zone from state lookup
    out["Zone"] = out["State"].map(state_to_zone).fillna("Unknown")
    out["BuyingDays"] = "0"
    # Normalize month format to Mon'YY
    if "LeadMonth" in out.columns:
        out["LeadMonth"] = out["LeadMonth"].apply(norm_month)
    # Drop raw retail columns (already extracted into retail_map)
    for c in ["DMS_Retail_Month", "Retail Date", "Retail By"]:
        if c in out.columns:
            out = out.drop(columns=[c])
    # Keep only canonical columns
    keep = [c for c in ["SorceLeadId","LeadMonth","Source","LeadType","ModelName",
                         "State","Zone","BuyingDays","CityName","DealerName"] if c in out.columns]
    return out[keep].copy()

# ─── Core aggregation (unchanged logic, now works on combined DataFrame) ──────

def build_payload(all_leads, retail_map):
    dl_col = "DealerName" if "DealerName" in all_leads.columns else None

    lm_idx,  src_idx, lt_idx, mdl_idx, st_idx, zone_idx, city_idx = {},{},{},{},{},{},{}
    lm_arr,  src_arr, lt_arr, mdl_arr, st_arr, zone_arr, city_arr  = [],[],[],[],[],[],[]
    u_lm_idx = {}
    u_lm_arr = []
    dl_idx, dl_arr = {}, []
    city_to_state = {}

    def ix(d, arr, v):
        if v not in d:
            d[v] = len(arr)
            arr.append(v)
        return d[v]

    monthly, sm, ltm, mm, stm, zm, bdm, cm = {},{},{},{},{},{},{},{}
    u_monthly, u_sm, u_ltm, u_mm, u_stm, u_zm, u_bdm = {},{},{},{},{},{},{}
    cdm  = {}
    csm  = {}
    cdsm = {}

    def bump(d, k, is_ret, rtype=""):
        if k not in d:
            d[k] = [0, 0, 0, 0]
        d[k][0] += 1
        if is_ret:
            d[k][1] += 1
            rt_u = rtype.upper()
            if "DMS" in rt_u:
                d[k][2] += 1
            elif "CALL" in rt_u:
                d[k][3] += 1

    total = len(all_leads)
    print(f"Aggregating {total:,} leads…", flush=True)

    for i, (_, row) in enumerate(all_leads.iterrows()):
        if i % 100000 == 0 and i > 0:
            print(f"  {i:,}/{total:,} ({100*i//total}%)", flush=True)

        lid  = to_id(row.get("SorceLeadId", ""))
        lm   = str(row.get("LeadMonth",    "") or "").strip()
        src  = str(row.get("Source",       "") or "").strip() or "Unknown"
        lt   = str(row.get("LeadType",     "") or "").strip() or "Unknown"
        mdl  = str(row.get("ModelName",    "") or "").strip() or "Unknown"
        st   = str(row.get("State",        "") or "").strip().title() or "Unknown"
        zone = str(row.get("Zone",         "") or "").strip() or "Unknown"
        bd   = str(row.get("BuyingDays",   "") or "0").strip() or "0"
        city = str(row.get("CityName",     "") or "").strip() or "Unknown"

        if not lm or not lid:
            continue

        is_ret = lid in retail_map
        li   = ix(lm_idx,   lm_arr,   lm)
        si   = ix(src_idx,  src_arr,  src)
        tti  = ix(lt_idx,   lt_arr,   lt)
        mi   = ix(mdl_idx,  mdl_arr,  mdl)
        sti  = ix(st_idx,   st_arr,   st)
        zi   = ix(zone_idx, zone_arr, zone)
        cti  = ix(city_idx, city_arr, city)

        city_to_state[cti] = sti
        rtype = retail_map[lid]["rtype"] if is_ret else ""

        bump(monthly, li,                  is_ret, rtype)
        bump(sm,      f"{si}|{li}",        is_ret, rtype)
        bump(ltm,     f"{tti}|{si}|{li}", is_ret, rtype)
        bump(mm,      f"{mi}|{si}|{li}",  is_ret, rtype)
        bump(stm,     f"{sti}|{si}|{li}", is_ret, rtype)
        bump(zm,      f"{zi}|{li}",        is_ret, rtype)
        bump(bdm,     f"{bd}|{si}|{li}",  is_ret, rtype)
        bump(cm,      f"{cti}|{li}",       is_ret, rtype)
        bump(csm,     f"{cti}|{si}|{li}", is_ret, rtype)

        if dl_col:
            dl  = str(row.get(dl_col, "") or "").strip() or "Unknown"
            dli = ix(dl_idx, dl_arr, dl)
            bump(cdm,  f"{cti}|{dli}|{li}",      is_ret, rtype)
            bump(cdsm, f"{cti}|{dli}|{si}|{li}", is_ret, rtype)

        rm  = retail_map[lid].get("rm", "") if is_ret else ""
        um  = rm if rm else lm
        uli = ix(u_lm_idx, u_lm_arr, um)
        bump(u_monthly, uli,                  is_ret, rtype)
        bump(u_sm,      f"{si}|{uli}",        is_ret, rtype)
        bump(u_ltm,     f"{tti}|{si}|{uli}", is_ret, rtype)
        bump(u_mm,      f"{mi}|{si}|{uli}",  is_ret, rtype)
        bump(u_stm,     f"{sti}|{si}|{uli}", is_ret, rtype)
        bump(u_zm,      f"{zi}|{uli}",        is_ret, rtype)
        bump(u_bdm,     f"{bd}|{si}|{uli}",  is_ret, rtype)

    def to_rows(d, key_fn):
        return [[*key_fn(k), v[0], v[1], v[2], v[3]] for k, v in d.items()]

    city_state_arr = [city_to_state.get(i) for i in range(len(city_arr))]

    maps_payload = {
        "lm":         lm_arr,  "src": src_arr, "lt": lt_arr, "mdl": mdl_arr,
        "st":         st_arr,  "zone": zone_arr, "city": city_arr,
        "city_state": city_state_arr,
        "u_lm":       u_lm_arr,
    }
    if dl_col and dl_arr:
        maps_payload["dl"] = dl_arr
        print(f"Dealers: {len(dl_arr):,}  City×Dealer×Month rows: {len(cdm):,}", flush=True)

    payload = {
        "t":       pd.Timestamp.now().isoformat(),
        "rt_cols": 1,
        "maps":    maps_payload,
        "monthly": to_rows(monthly, lambda k: [int(k)]),
        "sm":      to_rows(sm,  lambda k: list(map(int, k.split("|")))),
        "ltm":     to_rows(ltm, lambda k: list(map(int, k.split("|")))),
        "mm":      to_rows(mm,  lambda k: list(map(int, k.split("|")))),
        "stm":     to_rows(stm, lambda k: list(map(int, k.split("|")))),
        "zm":      to_rows(zm,  lambda k: list(map(int, k.split("|")))),
        "bdm":     to_rows(bdm, lambda k: [int(k.split("|")[0])] + list(map(int, k.split("|")[1:]))),
        "cm":      to_rows(cm,  lambda k: list(map(int, k.split("|")))),
        "csm":     to_rows(csm, lambda k: list(map(int, k.split("|")))),
        **({"cdm":  to_rows(cdm,  lambda k: list(map(int, k.split("|")))),
            "cdsm": to_rows(cdsm, lambda k: list(map(int, k.split("|"))))} if dl_col and dl_arr else {}),
        "u_monthly": to_rows(u_monthly, lambda k: [int(k)]),
        "u_sm":      to_rows(u_sm,  lambda k: list(map(int, k.split("|")))),
        "u_ltm":     to_rows(u_ltm, lambda k: list(map(int, k.split("|")))),
        "u_mm":      to_rows(u_mm,  lambda k: list(map(int, k.split("|")))),
        "u_stm":     to_rows(u_stm, lambda k: list(map(int, k.split("|")))),
        "u_zm":      to_rows(u_zm,  lambda k: list(map(int, k.split("|")))),
        "u_bdm":     to_rows(u_bdm, lambda k: [int(k.split("|")[0])] + list(map(int, k.split("|")[1:]))),
    }
    print(f"Done — {total:,} leads  {len(retail_map):,} retails  {len(u_lm_arr)} update-months", flush=True)
    return payload

# ─── Main ─────────────────────────────────────────────────────────────────────

print("=" * 60, flush=True)
print("TVS Lead Disposition — Daily Data Push", flush=True)
print("=" * 60, flush=True)

# 1. Get historical file IDs from Apps Script CONFIG
print("\n[1/5] Fetching config from Apps Script…", flush=True)
config = proxy_get("getConfig")
hist_lead_ids   = config["histLeadFileIds"]
hist_retail_ids = config["histRetailFileIds"]
print(f"  Historical lead files:   {hist_lead_ids}", flush=True)
print(f"  Historical retail files: {hist_retail_ids}", flush=True)

# 2. Download and combine all historical XLSX files
print("\n[2/5] Loading historical data…", flush=True)
hist_lead_dfs   = [read_hist_leads(fid,   f"Leads-{i+1}")   for i, fid in enumerate(hist_lead_ids)]
hist_retail_dfs = [read_hist_retails(fid, f"Retails-{i+1}") for i, fid in enumerate(hist_retail_ids)]

hist_leads_raw  = pd.concat(hist_lead_dfs,   ignore_index=True)
hist_retails_raw = pd.concat(hist_retail_dfs, ignore_index=True)
print(f"  Historical leads:   {len(hist_leads_raw):,} rows", flush=True)
print(f"  Historical retails: {len(hist_retails_raw):,} rows", flush=True)

# 3. Build state→zone lookup from historical data (which has Zone column)
state_to_zone = {}
for _, row in hist_leads_raw.iterrows():
    s = str(row.get("State", "") or "").strip().title()
    z = str(row.get("Zone",  "") or "").strip()
    if s and z:
        state_to_zone[s] = z
print(f"  State→Zone mappings: {len(state_to_zone)}", flush=True)

# 4. Fetch current month from Apps Script proxy
print("\n[3/5] Fetching current-month data from Apps Script…", flush=True)
curr_leads_raw = fetch_current_leads()

# 5. Build retail maps and merge
print("\n[4/5] Building retail maps…", flush=True)
hist_retail_map  = build_retail_map_from_hist(hist_retails_raw)
curr_embed_map   = build_retail_map_from_curr(curr_leads_raw)       # from embedded Retail Date col
curr_sheet_map   = fetch_current_retails()                           # from separate retails sheet
print(f"  Historical retail map:     {len(hist_retail_map):,}", flush=True)
print(f"  Current (embedded) map:    {len(curr_embed_map):,}", flush=True)
print(f"  Current (retails sheet):   {len(curr_sheet_map):,}", flush=True)

# Merge: historical base → retails sheet → embedded (most specific wins)
retail_map = {**hist_retail_map, **curr_sheet_map, **curr_embed_map}
print(f"  Combined retail map:       {len(retail_map):,}", flush=True)

# 6. Standardize and concat leads
print("\n[5/5] Processing all leads…", flush=True)
hist_leads_std = standardize_hist_leads(hist_leads_raw)
curr_leads_std = standardize_curr_leads(curr_leads_raw, state_to_zone)

print(f"  Hist leads (standardized):  {len(hist_leads_std):,}", flush=True)
print(f"  Curr leads (standardized):  {len(curr_leads_std):,}", flush=True)

all_leads = pd.concat([hist_leads_std, curr_leads_std], ignore_index=True)
print(f"  Combined total:             {len(all_leads):,}", flush=True)

payload  = build_payload(all_leads, retail_map)
json_str = json.dumps(payload, separators=(",", ":"))
print(f"\nPayload size: {len(json_str)/1024:.1f} KB", flush=True)

print("POSTing to Apps Script…", flush=True)
url  = APPS_SCRIPT_URL + "?secret=" + SECRET
data = json_str.encode("utf-8")
req  = urllib.request.Request(url, data=data, method="POST",
       headers={"Content-Type": "application/json"})
with urllib.request.urlopen(req, timeout=60) as resp:
    body = resp.read().decode()
print(f"Response: {body}", flush=True)

if '"ok":true' not in body:
    print("ERROR: Apps Script did not confirm success!", file=sys.stderr)
    sys.exit(1)

print("=" * 60, flush=True)
print("Done.", flush=True)
