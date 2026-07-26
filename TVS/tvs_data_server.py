"""
TVS Lead Disposition — Local Data Server
Reads Leads + Retails XLSX, joins them, serves aggregated JSON to the dashboard.
Runs on http://localhost:5050

Usage:  python tvs_data_server.py
Then open: http://localhost:5050  in your browser
"""

import json, math, http.server, socketserver, threading, os, sys
from pathlib import Path

# ── Paths to your XLSX files ──────────────────────────────────────
LEADS_PATH   = r"C:\Users\mihir.bhatt\Downloads\Leads Data Master_Leads_FY_26_27.xlsx"
RETAILS_PATH = r"C:\Users\mihir.bhatt\Downloads\Retail Data Master_Retails_FY_26_27.xlsx"
PORT = 5050

# ── Dashboard HTML path (served at /) ─────────────────────────────
DASHBOARD_PATH = Path(__file__).parent / "TVS_LDR_Dashboard_v2.0.html"

print("Loading pandas…", flush=True)
import pandas as pd

def to_id(v):
    """Normalise a SorceLeadId value to string, handling int64 precision loss."""
    if pd.isna(v):
        return ""
    try:
        return str(int(float(v)))
    except Exception:
        return str(v).strip()

def build_payload():
    print("Reading Retails…", flush=True)
    ret = pd.read_excel(RETAILS_PATH, dtype=str)
    ret.columns = [c.strip() for c in ret.columns]

    # Normalise join key
    ret_id_col = next((c for c in ret.columns if c.lower().replace(' ','') in ('sorceleadid','sourceleadid')), None)
    ret_mth_col = next((c for c in ret.columns if c.lower() == 'retail month'), None)
    if not ret_id_col:
        raise ValueError(f"Cannot find SorceLeadId column in Retails. Columns: {list(ret.columns)}")

    retail_map = {}
    for _, row in ret.iterrows():
        rid = to_id(row.get(ret_id_col, ''))
        if rid:
            retail_map[rid] = {'rm': str(row.get(ret_mth_col, '') or '')}

    print(f"Retail records: {len(retail_map):,}", flush=True)

    print("Reading Leads…", flush=True)
    leads = pd.read_excel(LEADS_PATH, dtype=str)
    leads.columns = [c.strip() for c in leads.columns]

    # Column name helpers
    def col(candidates):
        for c in candidates:
            match = next((x for x in leads.columns if x.lower().replace(' ','').replace('_','') == c.lower().replace(' ','').replace('_','')), None)
            if match:
                return match
        return None

    id_col    = col(['SorceLeadId', 'SourceLeadId'])
    lm_col    = col(['LeadMonth', 'Lead Month'])
    src_col   = col(['Source'])
    lt_col    = col(['LeadType', 'Lead Type'])
    mdl_col   = col(['ModelName', 'Model Name'])
    st_col    = col(['State'])
    zone_col  = col(['Zone'])
    bd_col    = col(['BuyingDays', 'Buying Days'])

    if not id_col:
        raise ValueError(f"Cannot find SorceLeadId in Leads. Columns: {list(leads.columns)}")

    # Index maps
    lm_idx, src_idx, lt_idx, mdl_idx, st_idx, zone_idx = {},{},{},{},{},{}
    lm_arr, src_arr, lt_arr, mdl_arr, st_arr, zone_arr = [],[],[],[],[],[]

    def ix(d, arr, v):
        if v not in d:
            d[v] = len(arr)
            arr.append(v)
        return d[v]

    # Aggregation dicts: key → [leads, retails]
    monthly, sm, ltm, mm, stm, zm, bdm = {},{},{},{},{},{},{}

    def bump(d, k, is_ret):
        if k not in d:
            d[k] = [0, 0]
        d[k][0] += 1
        if is_ret:
            d[k][1] += 1

    print(f"Processing {len(leads):,} lead rows…", flush=True)
    for _, row in leads.iterrows():
        lid = to_id(row.get(id_col, ''))
        lm  = str(row.get(lm_col,   '') or '').strip()
        src = str(row.get(src_col,  '') or '').strip() or 'Unknown'
        lt  = str(row.get(lt_col,   '') or '').strip() or 'Unknown'
        mdl = str(row.get(mdl_col,  '') or '').strip() or 'Unknown'
        st  = str(row.get(st_col,   '') or '').strip() or 'Unknown'
        zone= str(row.get(zone_col, '') or '').strip() or 'Unknown'
        bd  = str(row.get(bd_col,   '') or '0').strip() or '0'

        if not lm or not lid:
            continue

        is_ret = lid in retail_map

        li   = ix(lm_idx,   lm_arr,   lm)
        si   = ix(src_idx,  src_arr,  src)
        tti  = ix(lt_idx,   lt_arr,   lt)
        mi   = ix(mdl_idx,  mdl_arr,  mdl)
        sti  = ix(st_idx,   st_arr,   st)
        zi   = ix(zone_idx, zone_arr, zone)

        bump(monthly, li,                     is_ret)
        bump(sm,      f"{si}|{li}",           is_ret)
        bump(ltm,     f"{tti}|{si}|{li}",    is_ret)
        bump(mm,      f"{mi}|{si}|{li}",     is_ret)
        bump(stm,     f"{sti}|{si}|{li}",    is_ret)
        bump(zm,      f"{zi}|{li}",           is_ret)
        bump(bdm,     f"{bd}|{si}|{li}",     is_ret)

    def to_rows(d, key_fn):
        out = []
        for k, v in d.items():
            out.append([*key_fn(k), v[0], v[1]])
        return out

    payload = {
        "t": pd.Timestamp.now().isoformat(),
        "maps": {
            "lm":   lm_arr,
            "src":  src_arr,
            "lt":   lt_arr,
            "mdl":  mdl_arr,
            "st":   st_arr,
            "zone": zone_arr,
        },
        "monthly": to_rows(monthly, lambda k: [int(k)]),
        "sm":      to_rows(sm,  lambda k: list(map(int, k.split("|")))),
        "ltm":     to_rows(ltm, lambda k: list(map(int, k.split("|")))),
        "mm":      to_rows(mm,  lambda k: list(map(int, k.split("|")))),
        "stm":     to_rows(stm, lambda k: list(map(int, k.split("|")))),
        "zm":      to_rows(zm,  lambda k: list(map(int, k.split("|")))),
        "bdm":     to_rows(bdm, lambda k: [int(k.split("|")[0])] + list(map(int, k.split("|")[1:]))),
    }

    print(f"Done — {len(leads):,} leads · {len(retail_map):,} retails matched · JSON keys: {list(payload.keys())}", flush=True)
    return payload


# ── Build payload once on startup ─────────────────────────────────
print("=" * 60)
try:
    PAYLOAD_JSON = json.dumps(build_payload())
    print(f"Payload size: {len(PAYLOAD_JSON)/1024:.1f} KB", flush=True)
except Exception as e:
    print(f"ERROR building payload: {e}", flush=True)
    sys.exit(1)

DASHBOARD_HTML = DASHBOARD_PATH.read_text(encoding="utf-8")

# ── HTTP server ────────────────────────────────────────────────────
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # suppress per-request noise

    def do_GET(self):
        if self.path in ('/', '/index.html', '/dashboard'):
            body = DASHBOARD_HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", len(body))
            self.end_headers()
            self.wfile.write(body)

        elif self.path in ('/data', '/data.json', '/tvs-data'):
            body = PAYLOAD_JSON.encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", len(body))
            self.end_headers()
            self.wfile.write(body)

        else:
            self.send_response(404)
            self.end_headers()

print("=" * 60)
print(f"TVS Dashboard server starting on http://localhost:{PORT}")
print(f"Open: http://localhost:{PORT}")
print("=" * 60, flush=True)

socketserver.TCPServer.allow_reuse_address = True
with socketserver.TCPServer(("", PORT), Handler) as httpd:
    httpd.serve_forever()
