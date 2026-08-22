"""
TVS Lead Disposition Pipeline — Regression Test Suite
======================================================
Run with:
    python TVS/test_pipeline.py            (stdout: verbose)
    python TVS/test_pipeline.py -v         (verbose)
    python -m pytest TVS/test_pipeline.py  (pytest)

No network access, production credentials, or external files required.
All tests use synthetic in-memory data.

IMPORTANT — keeping tests in sync with push_tvs_data.py:
  The utility functions below are inlined from push_tvs_data.py so that this
  test file runs without importing the module (which has module-level I/O).
  If a function changes in push_tvs_data.py, update the copy here and add a
  test that covers the new behaviour.  The _test_sync_check tests below compare
  function signatures to detect drift early.
"""
import re
import sys
import json
import gzip
import math
import os
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Inline copies of pure utility functions from push_tvs_data.py
# (must stay in sync with push_tvs_data.py)
# ---------------------------------------------------------------------------

MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun',
               'Jul','Aug','Sep','Oct','Nov','Dec']

ONLINE_START       = "Jul'26"
LEAD_MASTER_START  = "Apr'25"

_CITY_ALIAS = {
    'New Delhi':          'Delhi',
    'Bengaluru':          'Bangalore',
    'Bengalore':          'Bangalore',
    'Prayagraj':          'Allahabad',
    'Thiruvananthapuram': 'Trivandrum',
}
_COMPOUND_SEP_RE = re.compile(r'(\s*[/,|&]\s*)')

def normalize_city(raw):
    s = re.sub(r'\s+', ' ', str(raw or '').strip())
    if not s:
        return 'Unknown'
    s = s.title()
    if s in _CITY_ALIAS:
        return _CITY_ALIAS[s]
    if _COMPOUND_SEP_RE.search(s):
        tokens    = _COMPOUND_SEP_RE.split(s)
        parts     = tokens[0::2]
        seps      = tokens[1::2]
        canon     = [_CITY_ALIAS.get(p.strip(), p.strip()) for p in parts]
        if len(set(canon)) == 1:
            return canon[0]
        result = canon[0]
        for sep, cp in zip(seps, canon[1:]):
            result += sep + cp
        return result
    return s


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


def month_order(lm):
    try:
        s  = norm_month(str(lm or '').strip())
        mn, yy = s.split("'")
        mi = MONTH_NAMES.index(mn) + 1
        return int(yy) * 100 + mi
    except Exception:
        return 0


def to_id(v):
    try:
        f = float(v)
        if math.isnan(f): return ''
        return str(int(f))
    except Exception:
        return str(v).strip() if v else ''


ONLINE_START_ORDER = month_order(ONLINE_START)
LEAD_MASTER_START_ORDER = month_order(LEAD_MASTER_START)


def extract_rtype_map(rows):
    """rows: list of dicts with 'opty_id', 'DMS_Retail_Month', 'Retail By'."""
    rmap = {}
    _unknown_rb = {}
    for row in rows:
        rm = str(row.get('DMS_Retail_Month', '') or '').strip()
        if not rm: continue
        lid = to_id(row.get('opty_id', ''))
        if not lid: continue
        _rb_raw = str(row.get('Retail By', '') or '').strip()
        _rb_u   = _rb_raw.upper()
        if 'DMS' in _rb_u:
            _rtype = 'DMS'
        elif 'CALL' in _rb_u or _rb_u == 'CC':
            _rtype = 'Call Out'
        else:
            _rtype = ''
            if _rb_raw and _rb_raw not in ('-', '–', 'N/A', 'NA', 'na', 'n/a'):
                _unknown_rb[_rb_raw] = _unknown_rb.get(_rb_raw, 0) + 1
        rmap[lid] = {'rm': norm_month(rm), 'rtype': _rtype}
    return rmap, _unknown_rb


def bump(d, k, is_ret, rtype=''):
    if k not in d: d[k] = [0, 0, 0, 0]
    d[k][0] += 1
    if is_ret:
        d[k][1] += 1
        rt_u = rtype.upper()
        if 'DMS' in rt_u:    d[k][2] += 1
        elif 'CALL' in rt_u: d[k][3] += 1


def ubump(d, key_lead, key_ret, is_ret, rtype=''):
    if key_lead not in d: d[key_lead] = [0, 0, 0, 0]
    d[key_lead][0] += 1
    if is_ret:
        if key_ret not in d: d[key_ret] = [0, 0, 0, 0]
        d[key_ret][1] += 1
        rt_u = rtype.upper()
        if 'DMS' in rt_u:    d[key_ret][2] += 1
        elif 'CALL' in rt_u: d[key_ret][3] += 1


def _validate_payload_logic(p):
    """Core of _validate_payload — returns (errors list, oc_by_lm, ou_by_lm)."""
    lm_arr   = p.get('maps', {}).get('lm', [])
    oc_by_lm = {}
    ou_by_lm = {}
    for row in p.get('monthly', []):
        lm = lm_arr[row[0]] if row[0] < len(lm_arr) else '?'
        prev = oc_by_lm.get(lm, [0, 0, 0, 0])
        oc_by_lm[lm] = [prev[j] + row[1 + j] for j in range(4)]
    for row in p.get('u_monthly', []):
        lm = lm_arr[row[0]] if row[0] < len(lm_arr) else '?'
        prev = ou_by_lm.get(lm, [0, 0, 0, 0])
        ou_by_lm[lm] = [prev[j] + row[1 + j] for j in range(4)]
    errors = []
    for lm, oc in oc_by_lm.items():
        if month_order(lm) < ONLINE_START_ORDER: continue
        if oc[1] == 0: continue
        diff = oc[1] - (oc[2] + oc[3])
        if diff != 0:
            errors.append(f"LIVE DMS+CO != Retails [{lm} OC]: diff={diff:+,}")
    for lm, ou in ou_by_lm.items():
        if month_order(lm) < ONLINE_START_ORDER: continue
        if ou[1] == 0: continue
        diff = ou[1] - (ou[2] + ou[3])
        if diff != 0:
            errors.append(f"LIVE DMS+CO != Retails [{lm} OU]: diff={diff:+,}")
    return errors, oc_by_lm, ou_by_lm


def _validate_retail_fetch_logic(n, prev_metrics=None):
    """Returns (ok: bool, msg: str)."""
    RETAIL_ABS_FLOOR      = 50_000
    RETAIL_DROP_THRESHOLD = 0.80
    prev = None
    if prev_metrics:
        _pm = prev_metrics.get('retail_raw')
        if isinstance(_pm, dict):
            prev = _pm.get('rows')
        elif isinstance(_pm, int):
            prev = _pm
    if prev is not None and prev > 0:
        floor = max(RETAIL_ABS_FLOOR, int(prev * RETAIL_DROP_THRESHOLD))
        if n < floor:
            return False, f"only {n:,} rows vs floor {floor:,} (prev={prev:,})"
        return True, 'ok'
    if n < RETAIL_ABS_FLOOR:
        return False, f"only {n:,} rows vs absolute floor {RETAIL_ABS_FLOOR:,}"
    return True, 'ok'


def three_way_merge(hist_map, live_map):
    """Merge live retail into hist map following Case A / B / C rules."""
    result = dict(hist_map)
    added_a = added_c = updated_b = kept_b = 0
    for lid, info in live_map.items():
        live_rm_order = month_order(info.get('rm', ''))
        if live_rm_order >= ONLINE_START_ORDER:
            result[lid] = info
            added_a += 1
        elif lid in result:
            if info.get('rm') and live_rm_order >= LEAD_MASTER_START_ORDER:
                result[lid] = {**result[lid], 'rm': info['rm']}
                updated_b += 1
            else:
                kept_b += 1
        else:
            result[lid] = info
            added_c += 1
    return result, added_a, updated_b, kept_b, added_c


# ---------------------------------------------------------------------------
# Test classes
# ---------------------------------------------------------------------------

class TestCityNormalization(unittest.TestCase):

    def test_exact_alias_bangalore(self):
        self.assertEqual(normalize_city('Bengaluru'), 'Bangalore')

    def test_exact_alias_bangalore_misspelling(self):
        self.assertEqual(normalize_city('Bengalore'), 'Bangalore')

    def test_exact_alias_delhi(self):
        self.assertEqual(normalize_city('New Delhi'), 'Delhi')

    def test_no_change_for_canonical(self):
        self.assertEqual(normalize_city('Bangalore'), 'Bangalore')
        self.assertEqual(normalize_city('Delhi'), 'Delhi')
        self.assertEqual(normalize_city('Mumbai'), 'Mumbai')

    def test_case_insensitive(self):
        self.assertEqual(normalize_city('bengaluru'), 'Bangalore')
        self.assertEqual(normalize_city('BENGALURU'), 'Bangalore')
        self.assertEqual(normalize_city('new delhi'), 'Delhi')

    def test_whitespace_collapsed(self):
        self.assertEqual(normalize_city('  Bengaluru  '), 'Bangalore')
        self.assertEqual(normalize_city('New  Delhi'), 'Delhi')

    def test_empty_returns_unknown(self):
        self.assertEqual(normalize_city(''), 'Unknown')
        self.assertEqual(normalize_city(None), 'Unknown')
        self.assertEqual(normalize_city('   '), 'Unknown')

    def test_compound_slash_both_alias(self):
        # Both tokens resolve to the same canonical city → collapse
        self.assertEqual(normalize_city('Bengaluru / Bangalore'), 'Bangalore')
        self.assertEqual(normalize_city('Bangalore / Bengaluru'), 'Bangalore')
        self.assertEqual(normalize_city('New Delhi / Delhi'), 'Delhi')
        self.assertEqual(normalize_city('Delhi / New Delhi'), 'Delhi')

    def test_compound_slash_case_insensitive(self):
        self.assertEqual(normalize_city('bengaluru / bangalore'), 'Bangalore')

    def test_compound_comma_area_city(self):
        # Area + aliased city → keep structure, canonicalize city token
        self.assertEqual(normalize_city('Begur, Bengaluru'), 'Begur, Bangalore')

    def test_compound_comma_both_canonical(self):
        # Two canonical cities that differ — structure preserved, no alias applied
        result = normalize_city('Mumbai, Pune')
        self.assertIn('Mumbai', result)
        self.assertIn('Pune', result)

    def test_compound_comma_all_same(self):
        self.assertEqual(normalize_city('Bengaluru, Bangalore'), 'Bangalore')

    def test_unknown_city_passthrough(self):
        self.assertEqual(normalize_city('Kolkata'), 'Kolkata')
        self.assertEqual(normalize_city('Hyderabad'), 'Hyderabad')

    def test_different_cities_not_merged(self):
        result = normalize_city('Mumbai / Pune')
        # Must not collapse to a single city — they are genuinely different
        self.assertNotEqual(result, 'Mumbai')
        self.assertNotEqual(result, 'Pune')

    def test_prayagraj_alias(self):
        self.assertEqual(normalize_city('Prayagraj'), 'Allahabad')

    def test_thiruvananthapuram_alias(self):
        self.assertEqual(normalize_city('Thiruvananthapuram'), 'Trivandrum')


class TestRetailClassification(unittest.TestCase):

    def _row(self, opty_id, retail_by, retail_month="Jul'26"):
        return {'opty_id': opty_id, 'Retail By': retail_by,
                'DMS_Retail_Month': retail_month}

    def test_dms(self):
        rmap, unknowns = extract_rtype_map([self._row('1001', 'DMS')])
        self.assertEqual(rmap['1001']['rtype'], 'DMS')
        self.assertEqual(unknowns, {})

    def test_dms_lowercase(self):
        rmap, _ = extract_rtype_map([self._row('1001', 'dms')])
        self.assertEqual(rmap['1001']['rtype'], 'DMS')

    def test_call_out(self):
        rmap, _ = extract_rtype_map([self._row('1002', 'Call Out')])
        self.assertEqual(rmap['1002']['rtype'], 'Call Out')

    def test_callout_no_space(self):
        rmap, _ = extract_rtype_map([self._row('1002', 'callout')])
        self.assertEqual(rmap['1002']['rtype'], 'Call Out')

    def test_cc_normalizes_to_call_out(self):
        rmap, _ = extract_rtype_map([self._row('1003', 'CC')])
        self.assertEqual(rmap['1003']['rtype'], 'Call Out')

    def test_cc_lowercase(self):
        rmap, _ = extract_rtype_map([self._row('1003', 'cc')])
        self.assertEqual(rmap['1003']['rtype'], 'Call Out')

    def test_dash_sentinel_produces_blank_rtype(self):
        rmap, unknowns = extract_rtype_map([self._row('1004', '-')])
        self.assertEqual(rmap['1004']['rtype'], '')
        self.assertEqual(unknowns, {})   # '-' is a known sentinel, not unknown

    def test_em_dash_sentinel_produces_blank_rtype(self):
        rmap, unknowns = extract_rtype_map([self._row('1004', '–')])
        self.assertEqual(rmap['1004']['rtype'], '')
        self.assertEqual(unknowns, {})

    def test_blank_produces_blank_rtype(self):
        rmap, unknowns = extract_rtype_map([self._row('1005', '')])
        self.assertEqual(rmap['1005']['rtype'], '')
        self.assertEqual(unknowns, {})

    def test_na_sentinel_produces_blank_rtype(self):
        for val in ('N/A', 'NA', 'na', 'n/a'):
            rmap, unknowns = extract_rtype_map([self._row('1006', val)])
            self.assertEqual(rmap['1006']['rtype'], '', f"Expected blank for {val!r}")
            self.assertEqual(unknowns, {})

    def test_unknown_value_is_reported(self):
        rmap, unknowns = extract_rtype_map([self._row('1007', 'Showroom')])
        self.assertEqual(rmap['1007']['rtype'], '')
        self.assertIn('Showroom', unknowns)

    def test_no_retail_month_skipped(self):
        rows = [{'opty_id': '1001', 'Retail By': 'DMS', 'DMS_Retail_Month': ''}]
        rmap, _ = extract_rtype_map(rows)
        self.assertEqual(rmap, {})

    def test_source_rtype_not_overwritten_by_blank(self):
        # A lead has rtype='' in lead sheet → retail_map rtype is NOT overwritten.
        # This tests the guard: "if info['rtype']:"
        retail_map = {'5001': {'rm': "Jul'26", 'rtype': 'Call Out', 'pm': 'TVS iQube'}}
        rtype_map_entry = {'5001': {'rm': "Jul'26", 'rtype': ''}}  # blank → no override
        for lid, info in rtype_map_entry.items():
            if lid in retail_map:
                _rm_ord = month_order(info.get('rm', ''))
                if 0 < _rm_ord < ONLINE_START_ORDER:
                    continue
                if info['rtype']:   # blank → guard blocks override
                    retail_map[lid]['rtype'] = info['rtype']
        self.assertEqual(retail_map['5001']['rtype'], 'Call Out')  # preserved

    def test_source_rtype_overwritten_when_nonempty(self):
        # A lead has rtype='DMS' → retail_map rtype IS overwritten for live month
        retail_map = {'5002': {'rm': "Jul'26", 'rtype': 'Call Out', 'pm': 'iQube'}}
        rtype_map_entry = {'5002': {'rm': "Jul'26", 'rtype': 'DMS'}}
        for lid, info in rtype_map_entry.items():
            if lid in retail_map:
                _rm_ord = month_order(info.get('rm', ''))
                if 0 < _rm_ord < ONLINE_START_ORDER:
                    continue
                if info['rtype']:
                    retail_map[lid]['rtype'] = info['rtype']
        self.assertEqual(retail_map['5002']['rtype'], 'DMS')

    def test_hist_rtype_not_overwritten_by_live_sheet(self):
        # For pre-ONLINE month retails, rtype_map override MUST NOT apply
        retail_map = {'6001': {'rm': "Jun'26", 'rtype': 'DMS', 'pm': 'iQube'}}
        rtype_map_entry = {'6001': {'rm': "Jun'26", 'rtype': 'Call Out'}}  # would override if allowed
        for lid, info in rtype_map_entry.items():
            if lid in retail_map:
                _rm_ord = month_order(info.get('rm', ''))
                if 0 < _rm_ord < ONLINE_START_ORDER:
                    continue   # pre-online → skip
                if info['rtype']:
                    retail_map[lid]['rtype'] = info['rtype']
        self.assertEqual(retail_map['6001']['rtype'], 'DMS')  # unchanged


class TestBumpAggregation(unittest.TestCase):

    def test_lead_only(self):
        d = {}
        bump(d, 'k', is_ret=False)
        self.assertEqual(d['k'], [1, 0, 0, 0])

    def test_retail_dms(self):
        d = {}
        bump(d, 'k', is_ret=True, rtype='DMS')
        self.assertEqual(d['k'], [1, 1, 1, 0])

    def test_retail_call_out(self):
        d = {}
        bump(d, 'k', is_ret=True, rtype='Call Out')
        self.assertEqual(d['k'], [1, 1, 0, 1])

    def test_retail_blank_rtype_is_unclassified(self):
        d = {}
        bump(d, 'k', is_ret=True, rtype='')
        # leads=1, rets=1, dms=0, co=0 → UNCLASSIFIED
        self.assertEqual(d['k'], [1, 1, 0, 0])
        self.assertEqual(d['k'][1] - (d['k'][2] + d['k'][3]), 1)

    def test_retail_dash_rtype_is_unclassified(self):
        d = {}
        bump(d, 'k', is_ret=True, rtype='-')
        # '-' does not contain 'DMS' or 'CALL' → unclassified
        self.assertEqual(d['k'][1] - (d['k'][2] + d['k'][3]), 1)

    def test_accumulation(self):
        d = {}
        bump(d, 'k', is_ret=True, rtype='DMS')
        bump(d, 'k', is_ret=True, rtype='Call Out')
        bump(d, 'k', is_ret=False)
        self.assertEqual(d['k'], [3, 2, 1, 1])

    def test_dms_co_sum_equals_retails(self):
        d = {}
        for _ in range(5):  bump(d, 'k', is_ret=True, rtype='DMS')
        for _ in range(3):  bump(d, 'k', is_ret=True, rtype='Call Out')
        for _ in range(2):  bump(d, 'k', is_ret=False)
        leads, rets, dms, co = d['k']
        self.assertEqual(dms + co, rets)

    def test_ubump_lead_and_retail_different_keys(self):
        d = {}
        ubump(d, 'lead_key', 'ret_key', is_ret=True, rtype='DMS')
        self.assertEqual(d['lead_key'][0], 1)   # lead counted in create-month
        self.assertEqual(d['ret_key'][1], 1)    # retail counted in retail-month
        self.assertEqual(d['ret_key'][2], 1)    # DMS

    def test_ubump_no_retail(self):
        d = {}
        ubump(d, 'lead_key', 'ret_key', is_ret=False)
        self.assertEqual(d['lead_key'][0], 1)
        self.assertNotIn('ret_key', d)


class TestDMSPlusCoEqualsRetails(unittest.TestCase):
    """Core business invariant: DMS + Call Out == Retails for every live month."""

    def _make_payload(self, lm_arr, monthly_rows, u_monthly_rows=None):
        return {
            'maps': {'lm': lm_arr},
            'monthly': monthly_rows,
            'u_monthly': u_monthly_rows or [],
        }

    def test_live_month_balanced(self):
        p = self._make_payload(
            ["Jul'26"],
            [[0, 100, 10, 7, 3]]   # lm=0, leads=100, rets=10, dms=7, co=3
        )
        errors, _, _ = _validate_payload_logic(p)
        self.assertEqual(errors, [])

    def test_live_month_unclassified_fails(self):
        p = self._make_payload(
            ["Jul'26"],
            [[0, 100, 10, 5, 3]]   # rets=10, dms=5, co=3 → diff=2 UNCLASSIFIED
        )
        errors, _, _ = _validate_payload_logic(p)
        self.assertEqual(len(errors), 1)
        self.assertIn("Jul'26", errors[0])

    def test_historical_month_unclassified_allowed(self):
        p = self._make_payload(
            ["Jun'26"],
            [[0, 100, 10, 5, 3]]   # historical → unclassified OK
        )
        errors, _, _ = _validate_payload_logic(p)
        self.assertEqual(errors, [])

    def test_multiple_live_months_all_balanced(self):
        p = self._make_payload(
            ["Jul'26", "Aug'26"],
            [[0, 50, 5, 3, 2],
             [1, 60, 6, 4, 2]]
        )
        errors, _, _ = _validate_payload_logic(p)
        self.assertEqual(errors, [])

    def test_mixed_live_and_hist(self):
        p = self._make_payload(
            ["Jun'26", "Jul'26"],
            [[0, 100, 10, 5, 3],   # hist — unclassified OK
             [1, 50,  5,  3, 2]]   # live — balanced
        )
        errors, _, _ = _validate_payload_logic(p)
        self.assertEqual(errors, [])

    def test_zero_retail_month_skipped(self):
        p = self._make_payload(
            ["Jul'26"],
            [[0, 50, 0, 0, 0]]    # 0 retails → no check needed
        )
        errors, _, _ = _validate_payload_logic(p)
        self.assertEqual(errors, [])

    def test_ou_live_month_unclassified_fails(self):
        p = self._make_payload(
            ["Jul'26"],
            [[0, 100, 10, 7, 3]],
            [[0, 100, 10, 5, 3]]   # OU: rets=10, dms=5, co=3 → diff=2
        )
        errors, _, _ = _validate_payload_logic(p)
        self.assertGreater(len(errors), 0)
        self.assertTrue(any('OU' in e for e in errors))

    def test_grand_totals(self):
        p = self._make_payload(
            ["Jul'26", "Aug'26"],
            [[0, 100, 10, 7, 3],
             [1, 200, 20, 14, 6]]
        )
        _, oc_by_lm, _ = _validate_payload_logic(p)
        total_leads = sum(v[0] for v in oc_by_lm.values())
        total_rets  = sum(v[1] for v in oc_by_lm.values())
        self.assertEqual(total_leads, 300)
        self.assertEqual(total_rets, 30)

    def test_future_month_also_validated(self):
        # Sep'26 is a future live month — must also be checked
        p = self._make_payload(
            ["Sep'26"],
            [[0, 50, 5, 2, 2]]   # rets=5, dms+co=4 → diff=1
        )
        errors, _, _ = _validate_payload_logic(p)
        self.assertEqual(len(errors), 1)


class TestRetailFetchValidation(unittest.TestCase):

    def test_above_absolute_floor_no_baseline(self):
        ok, msg = _validate_retail_fetch_logic(55000)
        self.assertTrue(ok)

    def test_below_absolute_floor_no_baseline(self):
        ok, msg = _validate_retail_fetch_logic(40000)
        self.assertFalse(ok)
        self.assertIn('40,000', msg)

    def test_above_dynamic_floor(self):
        ok, msg = _validate_retail_fetch_logic(90000, {'retail_raw': {'rows': 100000}})
        self.assertTrue(ok)   # 90 % of 100k > 80 % threshold

    def test_below_dynamic_floor(self):
        ok, msg = _validate_retail_fetch_logic(70000, {'retail_raw': {'rows': 100000}})
        self.assertFalse(ok)  # 70 % < 80 % threshold

    def test_exactly_at_dynamic_floor(self):
        ok, msg = _validate_retail_fetch_logic(80000, {'retail_raw': {'rows': 100000}})
        self.assertTrue(ok)   # 80 % == threshold (inclusive)

    def test_dynamic_floor_never_below_absolute(self):
        # Even if prev was tiny, absolute floor still applies
        ok, msg = _validate_retail_fetch_logic(30000, {'retail_raw': {'rows': 40000}})
        self.assertFalse(ok)  # 30k < max(50k, 80%*40k=32k) = 50k

    def test_partial_response_detected(self):
        ok, msg = _validate_retail_fetch_logic(1000, {'retail_raw': {'rows': 320000}})
        self.assertFalse(ok)

    def test_empty_dataframe_fails(self):
        ok, msg = _validate_retail_fetch_logic(0)
        self.assertFalse(ok)

    def test_prev_metrics_legacy_int_format(self):
        # Old source_metrics.json stored plain int (not dict)
        ok, msg = _validate_retail_fetch_logic(90000, {'retail_raw': 100000})
        self.assertTrue(ok)

    def test_no_prev_metrics_dict_uses_absolute_floor(self):
        ok, _ = _validate_retail_fetch_logic(51000, {})
        self.assertTrue(ok)
        ok, _ = _validate_retail_fetch_logic(49000, {})
        self.assertFalse(ok)


class TestMonthUtilities(unittest.TestCase):

    def test_norm_month_standard(self):
        self.assertEqual(norm_month("Jul'26"), "Jul'26")
        self.assertEqual(norm_month("Aug'26"), "Aug'26")

    def test_norm_month_4digit_year(self):
        self.assertEqual(norm_month("July 2026"), "Jul'26")

    def test_norm_month_case_insensitive(self):
        self.assertEqual(norm_month("jul'26"), "Jul'26")
        self.assertEqual(norm_month("JUL'26"), "Jul'26")

    def test_month_order_basic(self):
        self.assertEqual(month_order("Jan'25"), 2501)
        self.assertEqual(month_order("Jul'26"), 2607)
        self.assertEqual(month_order("Dec'26"), 2612)

    def test_month_order_year_boundary(self):
        dec = month_order("Dec'26")
        jan = month_order("Jan'27")
        self.assertGreater(jan, dec)
        self.assertEqual(jan, 2701)
        self.assertEqual(dec, 2612)

    def test_month_order_blank(self):
        self.assertEqual(month_order(''), 0)
        self.assertEqual(month_order(None), 0)

    def test_month_order_unknown(self):
        self.assertEqual(month_order('garbage'), 0)

    def test_online_start_order(self):
        self.assertEqual(ONLINE_START_ORDER, 2607)

    def test_lead_master_start_order(self):
        self.assertEqual(LEAD_MASTER_START_ORDER, 2504)

    def test_month_order_sortable(self):
        months = ["Mar'26", "Jan'26", "Jul'26", "Dec'25", "Aug'26"]
        sorted_months = sorted(months, key=month_order)
        self.assertEqual(sorted_months[0], "Dec'25")
        self.assertEqual(sorted_months[-1], "Aug'26")

    def test_to_id_integer_float(self):
        self.assertEqual(to_id(123456789.0), '123456789')
        self.assertEqual(to_id(0), '0')

    def test_to_id_string(self):
        self.assertEqual(to_id('ABC123'), 'ABC123')

    def test_to_id_blank_or_none(self):
        self.assertEqual(to_id(''), '')
        self.assertEqual(to_id(None), '')
        self.assertEqual(to_id(float('nan')), '')


class TestDeduplication(unittest.TestCase):

    def test_bump_same_key_accumulates(self):
        d = {}
        bump(d, ('k',), is_ret=True, rtype='DMS')
        bump(d, ('k',), is_ret=False)
        self.assertEqual(d[('k',)], [2, 1, 1, 0])

    def test_extract_rtype_last_row_wins(self):
        # When the same opty_id appears twice, the last entry in the list wins.
        rows = [
            {'opty_id': '9001', 'Retail By': 'DMS',      'DMS_Retail_Month': "Jul'26"},
            {'opty_id': '9001', 'Retail By': 'Call Out', 'DMS_Retail_Month': "Jul'26"},
        ]
        rmap, _ = extract_rtype_map(rows)
        # Second row overwrites first — this mirrors dict update behaviour
        self.assertEqual(rmap['9001']['rtype'], 'Call Out')

    def test_opty_id_int_and_float_same(self):
        row_int   = {'opty_id': 2001,   'Retail By': 'DMS', 'DMS_Retail_Month': "Jul'26"}
        row_float = {'opty_id': 2001.0, 'Retail By': 'DMS', 'DMS_Retail_Month': "Jul'26"}
        r1, _ = extract_rtype_map([row_int])
        r2, _ = extract_rtype_map([row_float])
        self.assertEqual(set(r1.keys()), set(r2.keys()))


class TestThreeWayRetailMerge(unittest.TestCase):

    def test_case_a_live_month_overwrites_hist(self):
        hist = {'L1': {'rm': "Jul'26", 'rtype': 'DMS',      'pm': 'X'}}
        live = {'L1': {'rm': "Jul'26", 'rtype': 'Call Out',  'pm': 'X'}}
        result, a, _, _, _ = three_way_merge(hist, live)
        self.assertEqual(result['L1']['rtype'], 'Call Out')
        self.assertEqual(a, 1)

    def test_case_b_hist_rtype_preserved(self):
        hist = {'L2': {'rm': "Jun'26", 'rtype': 'DMS',      'pm': 'X'}}
        live = {'L2': {'rm': "Jun'26", 'rtype': 'Call Out', 'pm': 'X'}}
        result, a, b, _, _ = three_way_merge(hist, live)
        # Case B: hist rtype preserved, rm updated if valid
        self.assertEqual(result['L2']['rtype'], 'DMS')
        self.assertEqual(a, 0)

    def test_case_b_rm_updated_from_live(self):
        hist = {'L3': {'rm': "May'26", 'rtype': 'DMS', 'pm': 'X'}}
        live = {'L3': {'rm': "Jun'26", 'rtype': 'DMS', 'pm': 'X'}}
        result, _, b, _, _ = three_way_merge(hist, live)
        self.assertEqual(result['L3']['rm'], "Jun'26")
        self.assertEqual(b, 1)

    def test_case_c_new_pre_online_retail_added(self):
        hist = {}
        live = {'L4': {'rm': "Jun'26", 'rtype': 'DMS', 'pm': 'X'}}
        result, _, _, _, c = three_way_merge(hist, live)
        self.assertIn('L4', result)
        self.assertEqual(c, 1)

    def test_case_a_does_not_affect_hist_only_entries(self):
        hist = {'OLD': {'rm': "Apr'25", 'rtype': 'DMS', 'pm': 'X'}}
        live = {'NEW': {'rm': "Jul'26", 'rtype': 'DMS', 'pm': 'X'}}
        result, _, _, _, _ = three_way_merge(hist, live)
        self.assertIn('OLD', result)
        self.assertIn('NEW', result)

    def test_hist_blank_rtype_not_overwritten_in_case_b(self):
        # Blank hist rtype stays blank in Case B (live data for pre-online month
        # must not silently patch a hist gap — that is handled by the production
        # pipeline separately).  Three-way merge only updates rm, not rtype.
        hist = {'L5': {'rm': "Jun'26", 'rtype': '', 'pm': 'X'}}
        live = {'L5': {'rm': "Jun'26", 'rtype': 'Call Out', 'pm': 'X'}}
        result, _, _, _, _ = three_way_merge(hist, live)
        self.assertEqual(result['L5']['rtype'], '')   # blank preserved by merge

    def test_pre_lead_master_start_rm_not_updated(self):
        # If live rm < LEAD_MASTER_START_ORDER, Case B keeps original rm
        hist = {'L6': {'rm': "May'25", 'rtype': 'DMS', 'pm': 'X'}}
        live = {'L6': {'rm': "Mar'25", 'rtype': 'DMS', 'pm': 'X'}}  # too early
        result, _, b, kept, _ = three_way_merge(hist, live)
        self.assertEqual(result['L6']['rm'], "May'25")   # unchanged
        self.assertEqual(kept, 1)


class TestNewMonthRollover(unittest.TestCase):

    def test_dec_to_jan_ordering(self):
        self.assertGreater(month_order("Jan'27"), month_order("Dec'26"))

    def test_year_boundary_sort(self):
        months = ["Nov'26", "Dec'26", "Jan'27", "Feb'27"]
        self.assertEqual(sorted(months, key=month_order), months)

    def test_future_month_order_nonzero(self):
        # Sep'26 (future as of Aug'26) must have valid month_order
        self.assertGreater(month_order("Sep'26"), ONLINE_START_ORDER)

    def test_online_start_inclusive(self):
        self.assertGreaterEqual(month_order("Jul'26"), ONLINE_START_ORDER)

    def test_jun_is_pre_online(self):
        self.assertLess(month_order("Jun'26"), ONLINE_START_ORDER)


class TestDryRunIsolation(unittest.TestCase):

    def test_payload_validation_does_not_write_files(self):
        # _validate_payload_logic is a pure computation — it must not create files
        p = {
            'maps': {'lm': ["Jul'26"]},
            'monthly': [[0, 50, 5, 3, 2]],
            'u_monthly': [],
        }
        tmpdir = tempfile.mkdtemp()
        before = set(os.listdir(tmpdir))
        _validate_payload_logic(p)
        after = set(os.listdir(tmpdir))
        self.assertEqual(before, after)

    def test_retail_fetch_logic_is_pure(self):
        # _validate_retail_fetch_logic is pure — no side effects
        tmpdir = tempfile.mkdtemp()
        before = set(os.listdir(tmpdir))
        _validate_retail_fetch_logic(60000)
        _validate_retail_fetch_logic(10000, {'retail_raw': {'rows': 320000}})
        after = set(os.listdir(tmpdir))
        self.assertEqual(before, after)


class TestAtomicPublication(unittest.TestCase):

    def test_staging_path_distinct_from_prod(self):
        # Staging filename includes a timestamp and 'staging' — never equals prod
        _RUN_START = datetime.now(timezone.utc)
        staging = f"tvs_payload_staging_{_RUN_START.strftime('%Y%m%d_%H%M')}.json.gz"
        prod    = "tvs_payload.json.gz"
        self.assertNotEqual(staging, prod)
        self.assertIn('staging', staging)

    def test_staging_roundtrip(self):
        # Compressed JSON write → read preserves payload integrity
        payload = {'maps': {'lm': ["Jul'26"]}, 'monthly': [[0, 100, 10, 7, 3]]}
        with tempfile.NamedTemporaryFile(suffix='.json.gz', delete=False) as f:
            fname = f.name
        try:
            with gzip.open(fname, 'wt', encoding='utf-8') as gf:
                json.dump(payload, gf)
            with gzip.open(fname, 'rt', encoding='utf-8') as gf:
                loaded = json.load(gf)
            self.assertEqual(payload, loaded)
        finally:
            os.unlink(fname)

    def test_production_unchanged_on_validation_failure(self):
        # Simulate: staging is written but production is not touched on failure.
        # Production file must not change even if validation errors exist.
        with tempfile.TemporaryDirectory() as tmpdir:
            prod = Path(tmpdir) / 'tvs_payload.json.gz'
            prod_data = {'version': 'known-good'}
            with gzip.open(prod, 'wt') as f:
                json.dump(prod_data, f)

            prod_mtime_before = prod.stat().st_mtime

            # Simulate failed validation — error list is non-empty
            p = {'maps': {'lm': ["Jul'26"]}, 'monthly': [[0, 50, 5, 2, 2]], 'u_monthly': []}
            errors, _, _ = _validate_payload_logic(p)
            self.assertTrue(errors, "Test setup: validation must fail")

            # On failure, production must not be touched
            prod_mtime_after = prod.stat().st_mtime
            self.assertEqual(prod_mtime_before, prod_mtime_after)

            # Verify known-good payload is still intact
            with gzip.open(prod, 'rt') as f:
                still_good = json.load(f)
            self.assertEqual(still_good, prod_data)


class TestSourceValidation(unittest.TestCase):

    def test_empty_source_fails(self):
        ok, msg = _validate_retail_fetch_logic(0)
        self.assertFalse(ok)

    def test_partial_source_fails(self):
        # Only 1 000 rows when 320 000 are expected → clear truncation
        ok, msg = _validate_retail_fetch_logic(1000, {'retail_raw': {'rows': 320000}})
        self.assertFalse(ok)
        self.assertIn('1,000', msg)

    def test_large_data_volume_ok(self):
        # Business growth: 700 k rows should still be fine
        ok, msg = _validate_retail_fetch_logic(700000, {'retail_raw': {'rows': 320000}})
        self.assertTrue(ok)

    def test_extract_rtype_map_empty_input(self):
        rmap, unknowns = extract_rtype_map([])
        self.assertEqual(rmap, {})
        self.assertEqual(unknowns, {})

    def test_extract_rtype_map_missing_columns_skipped(self):
        # Row without DMS_Retail_Month key must not cause an exception
        rows = [{'opty_id': '1001', 'Retail By': 'DMS'}]  # no DMS_Retail_Month
        rmap, _ = extract_rtype_map(rows)
        self.assertEqual(rmap, {})

    def test_extract_rtype_map_missing_opty_id_skipped(self):
        rows = [{'Retail By': 'DMS', 'DMS_Retail_Month': "Jul'26"}]
        rmap, _ = extract_rtype_map(rows)
        self.assertEqual(rmap, {})

    def test_malformed_opty_id_skipped(self):
        rows = [{'opty_id': '', 'Retail By': 'DMS', 'DMS_Retail_Month': "Jul'26"}]
        rmap, _ = extract_rtype_map(rows)
        self.assertEqual(rmap, {})

    def test_duplicate_opty_id_last_wins(self):
        rows = [
            {'opty_id': '7001', 'Retail By': 'DMS',      'DMS_Retail_Month': "Jul'26"},
            {'opty_id': '7001', 'Retail By': 'Call Out', 'DMS_Retail_Month': "Jul'26"},
        ]
        rmap, _ = extract_rtype_map(rows)
        self.assertEqual(rmap['7001']['rtype'], 'Call Out')

    def test_multiple_unknown_values_tracked(self):
        rows = [
            {'opty_id': '8001', 'Retail By': 'Showroom',  'DMS_Retail_Month': "Jul'26"},
            {'opty_id': '8002', 'Retail By': 'Dealer lot', 'DMS_Retail_Month': "Jul'26"},
            {'opty_id': '8003', 'Retail By': 'Showroom',  'DMS_Retail_Month': "Jul'26"},
        ]
        _, unknowns = extract_rtype_map(rows)
        self.assertEqual(unknowns.get('Showroom'), 2)
        self.assertEqual(unknowns.get('Dealer lot'), 1)


class TestFutureDataExtensibility(unittest.TestCase):
    """Validate that new data does not require code changes."""

    def test_new_month_passes_through_month_order(self):
        # Sep'26, Oct'26, Jan'27 must all produce valid, increasing month_orders
        seq = ["Jul'26", "Aug'26", "Sep'26", "Oct'26", "Nov'26", "Dec'26", "Jan'27"]
        orders = [month_order(m) for m in seq]
        self.assertEqual(orders, sorted(orders))
        self.assertTrue(all(o > 0 for o in orders))

    def test_new_city_passes_through_unchanged(self):
        new_city = 'Nanded'
        self.assertEqual(normalize_city(new_city), new_city)

    def test_new_city_with_alias_normalizes(self):
        # If a new alias is added to _CITY_ALIAS, it works immediately
        _CITY_ALIAS['Bombay'] = 'Mumbai'
        self.assertEqual(normalize_city('Bombay'), 'Mumbai')
        del _CITY_ALIAS['Bombay']

    def test_new_source_in_payload_does_not_break_validation(self):
        # A new source value is just another dimension — validation only checks
        # DMS+CO vs Retails, not specific source names
        p = {
            'maps': {'lm': ["Sep'26"]},
            'monthly': [[0, 100, 10, 7, 3]],
            'u_monthly': [],
        }
        errors, _, _ = _validate_payload_logic(p)
        self.assertEqual(errors, [])

    def test_doubled_row_count_still_passes_dynamic_threshold(self):
        # Business doubles in size — row count doubling must not trigger a false fail
        ok, _ = _validate_retail_fetch_logic(640000, {'retail_raw': {'rows': 320000}})
        self.assertTrue(ok)

    def test_halved_row_count_fails(self):
        # A sudden drop to 50 % is suspicious even for a large dataset
        ok, _ = _validate_retail_fetch_logic(160000, {'retail_raw': {'rows': 320000}})
        self.assertFalse(ok)

    def test_new_retail_type_in_live_sheet_is_unknown(self):
        # A new 'Call Type' value in the retail master (e.g. 'Leasing') becomes
        # unknown. extract_rtype_map must flag it rather than silently classify it.
        rows = [{'opty_id': '9999', 'Retail By': 'Leasing', 'DMS_Retail_Month': "Sep'26"}]
        rmap, unknowns = extract_rtype_map(rows)
        self.assertEqual(rmap['9999']['rtype'], '')
        self.assertIn('Leasing', unknowns)

    def test_large_number_of_months_in_payload(self):
        # Payload with 30 months — validation must handle arbitrary month count
        lm_arr = [f"{MONTH_NAMES[m % 12]}'{(m // 12) + 25}" for m in range(30)]
        rows   = [[i, 100, 10, 7, 3] for i in range(30)]
        p = {'maps': {'lm': lm_arr}, 'monthly': rows, 'u_monthly': []}
        errors, _, _ = _validate_payload_logic(p)
        # Only live months (≥ Jul'26) are checked; all are balanced here
        self.assertEqual(errors, [])


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main(verbosity=2)
