"""
TVS Lead Disposition — Daily Data Push
Runs via GitHub Actions at 11 AM IST every day.

DATA SOURCES:
  Historical leads (Apr-Jun): Google Sheet 1jPYG0LGFFd_ljWpfPr2NPfIU0fK1i7px → fetched via XLSX export
  Current leads   (Jul+):     Private Google Sheet via Apps Script proxy
  Retails (all months):       Google Sheet 1ZWBlzxX-g2R5iCcrsGUWrqSvxIHcchFHtajDDPcFJgE → XLSX export
"""

import json, sys, io, re, time, urllib.request
import pandas as pd
import requests

MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbwzgnXPbCbunBblnMUrqdWg3eY9qsIwCrFxuYuvYSpxtH22l4Cs32vdkOkDhUn-qwM64w/exec"
SECRET = "tvs2026push"

# ─── Helpers ─────────────────────────────────────────────────────────────────

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

def parse_ym(s):
    """Parse any date/month string to Mon'YY — handles ISO datetime and Mon-YYYY formats."""
    s = str(s or '').strip()
    if not s:
        return ''
    try:
        ts = pd.Timestamp(s)
        return f"{MONTH_NAMES[ts.month - 1]}'{ts.strftime('%y')}"
    except Exception:
        return norm_month(s)

def lid_to_month(lid):
    """Decode lead creation month from 18-digit sequential CRM ID (YYMMDD prefix)."""
    try:
        yy = int(lid[0:2])
        mm = int(lid[2:4])
        if 1 <= mm <= 12:
            return f"{MONTH_NAMES[mm - 1]}'{yy:02d}"
    except Exception:
        pass
    return ''

def to_id(v):
    if pd.isna(v): return ""
    try:    return str(int(float(v)))
    except: return str(v).strip()

def read_drive_xlsx(file_id, label=""):
    print(f"Downloading {label} from Google Drive…", flush=True)
    session = requests.Session()
    resp = session.get("https://drive.google.com/uc",
                       params={"export": "download", "id": file_id}, timeout=60)
    ct = resp.headers.get("Content-Type", "")
    if resp.status_code == 404 or (resp.status_code == 200 and "text/html" in ct and "404" in resp.text[:500]):
        # Google Sheet (not a Drive file) — use Sheets export endpoint
        print(f"  {label} Drive 404, trying Sheets export…", flush=True)
        resp = session.get(
            f"https://docs.google.com/spreadsheets/d/{file_id}/export",
            params={"format": "xlsx"}, stream=True, timeout=120)
    elif "text/html" in ct:
        html = resp.text
        # Method 1: modern Drive confirmation page — extract full link including uuid
        direct = re.search(r'href="(https://drive\.usercontent\.google\.com/download[^"]*confirm[^"]+)"', html)
        if direct:
            resp = session.get(direct.group(1).replace("&amp;", "&"), stream=True, timeout=120)
        else:
            # Method 2: old-style confirmation form
            action = re.search(r'<form[^>]*action="([^"]+)"', html)
            inputs = dict(re.findall(r'<input[^>]*name="([^"]+)"[^>]*value="([^"]+)"', html))
            if action and inputs:
                resp = session.get(action.group(1).replace("&amp;", "&"),
                                   params=inputs, stream=True, timeout=120)
            else:
                # Method 3: last resort
                resp = session.get("https://drive.google.com/uc",
                                   params={"export": "download", "id": file_id, "confirm": "t"},
                                   stream=True, timeout=120)
    resp.raise_for_status()
    buf = io.BytesIO()
    for chunk in resp.iter_content(chunk_size=1024*1024):
        buf.write(chunk)
    buf.seek(0)
    size_kb = len(buf.getvalue()) / 1024
    if size_kb < 10:
        raise RuntimeError(f"{label} download too small ({size_kb:.0f} KB) — possible Drive auth error")
    print(f"  {label}: {size_kb:.0f} KB", flush=True)
    return buf

def proxy_get(action, extra_params=None, timeout=120):
    params = {"action": action, "secret": SECRET}
    if extra_params:
        params.update(extra_params)
    resp = requests.get(APPS_SCRIPT_URL, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

RETAILS_FILE_ID = '1ZWBlzxX-g2R5iCcrsGUWrqSvxIHcchFHtajDDPcFJgE'
RETAILS_TAB     = 'Raw'

# ─── Retails: load via Apps Script proxy with XLSX fallback ───────────────────

def _fetch_retails_via_proxy():
    """Inner: fetch retail master via Apps Script (paginated, 3 retries per page)."""
    page, all_rows, headers = 0, [], None
    while True:
        for attempt in range(3):
            try:
                data = proxy_get("getCurrentRetails", {"page": page, "pageSize": 25000}, timeout=300)
                break
            except Exception as e:
                if attempt < 2:
                    print(f"  WARNING: page {page} attempt {attempt+1} failed ({e}); retrying in 30s…", flush=True)
                    time.sleep(30)
                else:
                    raise RuntimeError(f"getCurrentRetails page {page} failed after 3 attempts: {e}")
        if "error" in data:
            raise RuntimeError(f"getCurrentRetails error: {data['error']}")
        if headers is None:
            headers = data["headers"]
        rows  = data.get("rows", [])
        total = data.get("total", "?")
        all_rows.extend(rows)
        print(f"  Page {page}: +{len(rows)} rows  (fetched {len(all_rows):,}/{total})", flush=True)
        if data.get("done", True):
            break
        page += 1
    return pd.DataFrame(all_rows, columns=headers)

def _fetch_retails_via_xlsx():
    """Inner: download retail master XLSX directly (fallback when proxy is unavailable)."""
    buf = read_drive_xlsx(RETAILS_FILE_ID, "RetailMaster")
    df = pd.read_excel(buf, sheet_name=RETAILS_TAB, dtype=str, engine='openpyxl')
    df.columns = [c.strip() for c in df.columns]
    proc_col = next((c for c in df.columns if c.lower() == 'process'), None)
    if proc_col:
        df = df[df[proc_col].str.strip().str.upper() == 'TVS'].copy()
    return df

def fetch_retails():
    """
    Fetch all TVS retails from the retail master.
    Primary path: Apps Script proxy (authenticated, works in GitHub Actions).
    Fallback path: direct XLSX download (works when logged in locally).
    Returns a DataFrame with at least: sourceLeadId and one of
    Retail_Attribution_Date (old) / performanceMonth (new) for retail month.
    Also purchasedModel and createTime if the new Apps Script is deployed.
    """
    print("Fetching retail master…", flush=True)
    try:
        df = _fetch_retails_via_proxy()
        print(f"  Retails master (proxy): {len(df):,} TVS rows", flush=True)
        return df
    except Exception as proxy_err:
        print(f"  Proxy failed: {proxy_err}", flush=True)
        print("  Falling back to direct XLSX download…", flush=True)
        try:
            df = _fetch_retails_via_xlsx()
            print(f"  Retails master (XLSX): {len(df):,} TVS rows", flush=True)
            return df
        except Exception as xlsx_err:
            raise RuntimeError(f"Both retail fetch paths failed. Proxy: {proxy_err} | XLSX: {xlsx_err}")

def build_retail_map_from_proxy(retail_df):
    """Build retail_map {normalized_sourceLeadId → {rm, rtype}} from the proxy DataFrame.
    Handles both old 2-column (sourceLeadId, Retail_Attribution_Date) and
    new 4-column (sourceLeadId, performanceMonth, purchasedModel, createTime) responses.
    """
    retail_map = {}
    for _, row in retail_df.iterrows():
        lid = to_id(row.get('sourceLeadId', ''))
        if not lid:
            continue
        # performanceMonth is available in new Apps Script; fall back to Retail_Attribution_Date
        rm = parse_ym(row.get('performanceMonth', '') or row.get('Retail_Attribution_Date', ''))
        retail_map[lid] = {'rm': rm, 'rtype': ''}
    return retail_map

def make_synthetic_leads(retail_df, matched_lid_set):
    """
    Create synthetic lead rows for retails whose sourceLeadId is absent from hist/curr data.
    These are leads that were retailed and removed from the active sheet before the pipeline ran.
    Model: from purchasedModel if Apps Script returns it; else 'Unknown'.
    Lead month: from createTime, or decoded from the 18-digit sourceLeadId (YYMMDD prefix),
                or falls back to retail month.
    State/city/dealer/source are Unknown since that data is unavailable.
    """
    rows = []
    for _, row in retail_df.iterrows():
        lid = to_id(row.get('sourceLeadId', ''))
        if not lid or lid in matched_lid_set:
            continue
        # Lead month: try createTime, then decode from sourceLeadId, then retail month
        lm = (parse_ym(row.get('createTime', ''))
              or lid_to_month(lid)
              or parse_ym(row.get('performanceMonth', '') or row.get('Retail_Attribution_Date', '')))
        model = str(row.get('purchasedModel', '') or '').strip() or 'Unknown'
        rows.append({
            'SorceLeadId': lid,
            'LeadMonth':   lm,
            'ModelName':   model,
            'Source':      'Unknown',
            'LeadType':    'Unknown',
            'State':       'Unknown',
            'Zone':        'Unknown',
            'BuyingDays':  '0',
            'CityName':    'Unknown',
            'DealerName':  'Unknown',
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=[
        'SorceLeadId', 'LeadMonth', 'ModelName', 'Source', 'LeadType',
        'State', 'Zone', 'BuyingDays', 'CityName', 'DealerName'])

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
        col(["SorceLeadId", "SourceLeadId"]): "SorceLeadId",
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
    if "LeadMonth" in out.columns:
        out["LeadMonth"] = out["LeadMonth"].apply(norm_month)
    keep = [c for c in ["SorceLeadId","LeadMonth","Source","LeadType","ModelName",
                         "State","Zone","BuyingDays","CityName","DealerName"] if c in out.columns]
    return out[keep].copy()

# ─── Current month leads processing ──────────────────────────────────────────

# Maps current-month sheet column names → canonical names
CURR_COL_MAP = {
    "opty_id":     "SorceLeadId",   # opportunity ID — matches retail sourceLeadId
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
    Extract retail type and month from retailed leads embedded in the current leads sheet.
    Rows with non-empty 'Retail Date' are retailed; this provides DMS_Retail_Month and Retail By.
    Used to enhance rtype (DMS vs Call) over retail master which has no type info.
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

def standardize_curr_leads(curr_df, state_to_zone):
    """Rename columns, derive Zone from historical state lookup, add BuyingDays=0."""
    out = curr_df.rename(columns=CURR_COL_MAP).copy()
    out["State"] = out["State"].astype(str).str.strip().str.title()
    out["Zone"] = out["State"].map(state_to_zone).fillna("Unknown")
    out["BuyingDays"] = "0"
    if "LeadMonth" in out.columns:
        out["LeadMonth"] = out["LeadMonth"].apply(norm_month)
    # Drop raw retail columns (already extracted into retail_map)
    for c in ["DMS_Retail_Month", "Retail Date", "Retail By"]:
        if c in out.columns:
            out = out.drop(columns=[c])
    keep = [c for c in ["SorceLeadId","LeadMonth","Source","LeadType","ModelName",
                         "State","Zone","BuyingDays","CityName","DealerName"] if c in out.columns]
    return out[keep].copy()

# ─── Core aggregation ─────────────────────────────────────────────────────────

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

# 1. Get historical lead file IDs from Apps Script CONFIG
print("\n[1/6] Fetching config from Apps Script…", flush=True)
config = proxy_get("getConfig")
hist_lead_ids = config["histLeadFileIds"]
print(f"  Historical lead files: {hist_lead_ids}", flush=True)

# 2. Fetch retails master (proxy primary, XLSX fallback)
print("\n[2/6] Loading retail master…", flush=True)
retail_df = fetch_retails()

# 3. Download historical leads XLSX
print("\n[3/6] Loading historical lead data…", flush=True)
hist_lead_dfs  = [read_hist_leads(fid, f"Leads-{i+1}") for i, fid in enumerate(hist_lead_ids)]
hist_leads_raw = pd.concat(hist_lead_dfs, ignore_index=True)
print(f"  Historical leads: {len(hist_leads_raw):,} rows", flush=True)

# Build state→zone lookup from historical leads (which has Zone column)
state_to_zone = {}
for _, row in hist_leads_raw.iterrows():
    s = str(row.get("State", "") or "").strip().title()
    z = str(row.get("Zone",  "") or "").strip()
    if s and z:
        state_to_zone[s] = z
print(f"  State→Zone mappings: {len(state_to_zone)}", flush=True)

# 4. Fetch current month leads from Apps Script proxy
print("\n[4/6] Fetching current-month leads from Apps Script…", flush=True)
curr_leads_raw = fetch_current_leads()

# 5. Build retail map: base from retail master, enhanced by embedded retail cols in leads sheet
print("\n[5/6] Building retail maps…", flush=True)
retail_map     = build_retail_map_from_proxy(retail_df)       # all retails, retail month from master
curr_embed_map = build_retail_map_from_curr(curr_leads_raw)   # provides rtype (DMS/Call) + DMS month
retail_map.update(curr_embed_map)                              # embed overrides for rtype accuracy
print(f"  Retail master:          {len(retail_df):,}", flush=True)
print(f"  Current (embedded):     {len(curr_embed_map):,}", flush=True)
print(f"  Combined retail map:    {len(retail_map):,}", flush=True)

# 6. Standardize and concat leads; inject synthetic rows for unmatched retails
print("\n[6/6] Processing all leads…", flush=True)
hist_leads_std = standardize_hist_leads(hist_leads_raw)
curr_leads_std = standardize_curr_leads(curr_leads_raw, state_to_zone)
print(f"  Hist leads (standardized):          {len(hist_leads_std):,}", flush=True)
print(f"  Curr leads (standardized):          {len(curr_leads_std):,}", flush=True)

all_leads = pd.concat([hist_leads_std, curr_leads_std], ignore_index=True)

# Find retails with no matching lead row (e.g. retailed and removed from the active sheet mid-month)
# Inject synthetic lead rows for them using retail master model/month data so they are counted.
matched_lid_set = {to_id(v) for v in all_leads["SorceLeadId"].dropna() if to_id(v)}
synthetic_leads = make_synthetic_leads(retail_df, matched_lid_set)
if len(synthetic_leads):
    all_leads = pd.concat([all_leads, synthetic_leads], ignore_index=True)
    print(f"  Synthetic retail leads (gap fill):  {len(synthetic_leads):,}", flush=True)

print(f"  Combined total:                     {len(all_leads):,}", flush=True)

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
