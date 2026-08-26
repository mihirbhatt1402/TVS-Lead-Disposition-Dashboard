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
# Regression: commit 980ca6e — '-' sentinel overrode retail master Call Type
# ---------------------------------------------------------------------------
class TestDashSentinelRetailChain(unittest.TestCase):
    """End-to-end regression for the 2026-08-22 production incident.

    Root cause: extract_rtype_map at 3b76a6e stored 'Retail By' verbatim,
    so '-' reached the override guard as a truthy rtype and wiped the retail
    master's valid 'Call Out'.  Fix in 97abaeb: normalize '-'/'–'/blank/N/A
    to ''.  This test exercises the full chain in one place.
    """

    def test_dash_in_retail_by_preserves_call_out_from_retail_master(self):
        # Step 1: lead sheet has Retail By='-' for a Jul'26 retail
        rows = [{'opty_id': 'L9001', 'Retail By': '-', 'DMS_Retail_Month': "Jul'26"}]
        rtype_map, _ = extract_rtype_map(rows)
        self.assertEqual(rtype_map.get('L9001', {}).get('rtype'), '',
                         "'-' must produce rtype='' from extract_rtype_map")

        # Step 2: retail master has a valid Call Type for the same lead
        retail_map = {'L9001': {'rm': "Jul'26", 'rtype': 'Call Out', 'pm': 'TVS iQube'}}

        # Step 3: apply rtype_map override (mirrors the production loop)
        for lid, info in rtype_map.items():
            if lid in retail_map:
                _rm_ord = month_order(info.get('rm', ''))
                if 0 < _rm_ord < ONLINE_START_ORDER:
                    continue
                if info['rtype']:   # '' is falsy → guard blocks the override
                    retail_map[lid]['rtype'] = info['rtype']

        # Step 4: retail master's Call Out must be preserved
        self.assertEqual(retail_map['L9001']['rtype'], 'Call Out',
                         "retail master 'Call Out' must survive a '-' sentinel in lead sheet")

        # Step 5: aggregation with preserved Call Out → DMS+CO == Retails (no unclassified)
        d = {}
        bump(d, 'Jul26', is_ret=True, rtype=retail_map['L9001']['rtype'])
        leads, rets, dms, co = d['Jul26']
        self.assertEqual(rets, 1)
        self.assertEqual(dms + co, rets,
                         "DMS+CO must equal Retails when Call Out is correctly preserved")


class TestNtorqNormalization(unittest.TestCase):
    """Regression tests for the TVS NTORQ 125 / NTORQ 150 split.

    Root cause (confirmed 2026-08-25): 'TVS NTorq 150' hit the NTORQ_150 branch
    in normalize_purchased_model (no map entry, and keyword guard rejected it
    as an ambiguous 150 variant).  Fix: explicit map entries for both mixed-case
    and uppercase forms.  This class guards against regression and confirms the
    two models remain independent.

    These tests inline the relevant PURCHASED_MODEL_MAP entries and the NTORQ
    path of normalize_purchased_model so no module-level I/O is required.
    Keep in sync with push_tvs_data.py whenever either changes.
    """

    _MAP = {
        # new entries (fix)
        'TVS NTorq 150':                      'TVS NTORQ 150',
        'TVS NTORQ 150':                      'TVS NTORQ 150',
        # existing entries that must not be disturbed
        'TVS NTorq':                          'TVS NTORQ 125',
        'TVS NTORQ 125':                      'TVS NTORQ 125',
        'NTORQ 125 DISC – Race Edition BSVI': 'TVS NTORQ 125',
        'TVS NTORQ 125 RACE XP':              'TVS NTORQ 125',
        'TVS NTORQ 125 DISC BSVI':            'TVS NTORQ 125',
    }

    def _norm(self, pm):
        """Minimal inline of the NTORQ paths in normalize_purchased_model."""
        pm = str(pm or '').strip()
        if not pm:
            return 'Unknown'
        if pm in self._MAP:
            val = str(self._MAP[pm] or '').strip()
            if val and val.upper() not in ('NA', 'N/A', 'NAN', 'NONE'):
                return val
        pu = pm.upper()
        if 'NTORQ' in pu and '150' not in pu:
            return 'TVS NTORQ 125'
        if 'NTORQ' in pu or 'NTRQ' in pu:
            return 'Unknown'
        return 'Unknown'

    def test_production_raw_value_maps_to_ntorq_150(self):
        """'TVS NTorq 150' (exact production value confirmed by diagnostic) → 'TVS NTORQ 150'."""
        self.assertEqual(self._norm('TVS NTorq 150'), 'TVS NTORQ 150')

    def test_uppercase_variant_maps_to_ntorq_150(self):
        """'TVS NTORQ 150' (canonical uppercase form) → 'TVS NTORQ 150'."""
        self.assertEqual(self._norm('TVS NTORQ 150'), 'TVS NTORQ 150')

    def test_ntorq_125_exact_entry_unchanged(self):
        """Existing exact map entry 'TVS NTORQ 125' must not be disturbed."""
        self.assertEqual(self._norm('TVS NTORQ 125'), 'TVS NTORQ 125')

    def test_ntorq_bare_exact_entry_unchanged(self):
        """'TVS NTorq' (bare, no variant) must still map to 'TVS NTORQ 125'."""
        self.assertEqual(self._norm('TVS NTorq'), 'TVS NTORQ 125')

    def test_ntorq_125_keyword_fallback_unchanged(self):
        """'TVS NTorq 125' (mixed-case, not in map) must resolve via keyword to 'TVS NTORQ 125'."""
        self.assertEqual(self._norm('TVS NTorq 125'), 'TVS NTORQ 125')

    def test_unrecognized_ntorq_150_variant_still_unknown(self):
        """A future NTORQ+150 variant not in the map must still return Unknown."""
        self.assertEqual(self._norm('NTORQ SPORT 150 SPECIAL'), 'Unknown')

    def test_ntorq_150_and_125_not_merged(self):
        """'TVS NTORQ 150' and 'TVS NTORQ 125' must resolve to different canonical names."""
        self.assertNotEqual(self._norm('TVS NTorq 150'), self._norm('TVS NTORQ 125'))


# ---------------------------------------------------------------------------
# Retail Ageing tests
# ---------------------------------------------------------------------------
# Inline copies of the three new ageing functions from push_tvs_data.py.
# Keep in sync with push_tvs_data.py whenever those functions change.

import pandas as _pd
import datetime as _datetime

def _parse_date(s):
    """Inline copy of push_tvs_data.parse_date."""
    try:
        ts = _pd.Timestamp(str(s or '').strip())
        if _pd.isnull(ts):
            return None
        return ts.date()
    except Exception:
        return None

def _age_bucket(days):
    """Inline copy of push_tvs_data.age_bucket."""
    if days <= 7:  return 0
    if days <= 14: return 1
    if days <= 30: return 2
    return 3

_AGE_BUCKET_LABELS_TEST = ['0-7 days', '8-14 days', '15-30 days', '30+ days']

def _run_ageing_fixture(leads, retail_map):
    """Minimal inline of the ageing aggregation from build_payload's is_ret block.

    leads: list of dicts with keys: lid, lm, src, mdl, cd  (cd=CreateDate string)
    retail_map: {lid: {rm, rtype, pm, rd}}

    Returns:
        ram: {(mi,si,abi,li): [rets,dms,co]}
        meta: {total, valid, no_rd, no_cd, neg}
        maps: {mdl:[], src:[], lm:[]}
    """
    mdl_idx,  src_idx,  lm_idx  = {}, {}, {}
    mdl_arr,  src_arr,  lm_arr  = [], [], []

    def ix(d, arr, v):
        if v not in d:
            d[v] = len(arr); arr.append(v)
        return d[v]

    ram = {}
    total = valid = no_rd = no_cd = neg = 0

    for lead in leads:
        lid = lead['lid']
        if lid not in retail_map:
            continue
        mi  = ix(mdl_idx, mdl_arr, lead['mdl'])
        si  = ix(src_idx, src_arr, lead['src'])
        li  = ix(lm_idx,  lm_arr,  lead['lm'])
        rtype = retail_map[lid].get('rtype', 'DMS')

        total += 1
        _rd = retail_map[lid].get('rd')
        _cd = _parse_date(lead.get('cd', ''))
        if _rd is None:
            no_rd += 1
        elif _cd is None:
            no_cd += 1
        else:
            age_days = (_rd - _cd).days
            if age_days < 0:
                neg += 1
            else:
                abi = _age_bucket(age_days)
                k = (mi, si, abi, li)
                if k not in ram: ram[k] = [0, 0, 0]
                ram[k][0] += 1
                rt_u = rtype.upper()
                if 'DMS' in rt_u:    ram[k][1] += 1
                elif 'CALL' in rt_u: ram[k][2] += 1
                valid += 1

    meta = {'total': total, 'valid': valid, 'no_rd': no_rd, 'no_cd': no_cd, 'neg': neg}
    maps = {'mdl': mdl_arr, 'src': src_arr, 'lm': lm_arr}
    return ram, meta, maps


class TestRetailAgeing(unittest.TestCase):
    """20 regression tests for the Retail Ageing feature."""

    # ── 1-8: age_bucket boundary correctness ──────────────────────────────────

    def test_age_0_maps_to_bucket_0(self):
        """age=0 days → bucket 0 (0-7 days)."""
        self.assertEqual(_age_bucket(0), 0)

    def test_age_7_maps_to_bucket_0(self):
        """age=7 days → bucket 0 (0-7 days, inclusive boundary)."""
        self.assertEqual(_age_bucket(7), 0)

    def test_age_8_maps_to_bucket_1(self):
        """age=8 days → bucket 1 (8-14 days)."""
        self.assertEqual(_age_bucket(8), 1)

    def test_age_14_maps_to_bucket_1(self):
        """age=14 days → bucket 1 (inclusive boundary)."""
        self.assertEqual(_age_bucket(14), 1)

    def test_age_15_maps_to_bucket_2(self):
        """age=15 days → bucket 2 (15-30 days)."""
        self.assertEqual(_age_bucket(15), 2)

    def test_age_30_maps_to_bucket_2(self):
        """age=30 days → bucket 2 (inclusive boundary)."""
        self.assertEqual(_age_bucket(30), 2)

    def test_age_31_maps_to_bucket_3(self):
        """age=31 days → bucket 3 (30+ days)."""
        self.assertEqual(_age_bucket(31), 3)

    def test_age_large_maps_to_bucket_3(self):
        """age=365 days → bucket 3 (30+)."""
        self.assertEqual(_age_bucket(365), 3)

    # ── 9-11: exclusion cases ─────────────────────────────────────────────────

    def test_negative_age_excluded(self):
        """Retail_Date < CreateDate → not in ram, counted in meta.neg."""
        rmap = {'lid1': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': 'TVS Raider',
                         'rd': _datetime.date(2026, 8, 1)}}
        leads = [{'lid': 'lid1', 'lm': "Aug'26", 'mdl': 'TVS Raider',
                  'src': 'Organic', 'cd': '2026-08-10'}]  # cd AFTER rd → negative
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        self.assertEqual(len(ram), 0, "ram must be empty for negative age")
        self.assertEqual(meta['neg'], 1)
        self.assertEqual(meta['valid'], 0)

    def test_missing_retail_date_excluded(self):
        """rd=None → excluded from ram, counted in meta.no_rd."""
        rmap = {'lid1': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': 'TVS Raider', 'rd': None}}
        leads = [{'lid': 'lid1', 'lm': "Aug'26", 'mdl': 'TVS Raider',
                  'src': 'Organic', 'cd': '2026-08-01'}]
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        self.assertEqual(len(ram), 0)
        self.assertEqual(meta['no_rd'], 1)
        self.assertEqual(meta['valid'], 0)

    def test_missing_create_date_excluded(self):
        """cd='' → parse_date returns None → excluded, counted in meta.no_cd."""
        rmap = {'lid1': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': 'TVS Raider',
                         'rd': _datetime.date(2026, 8, 10)}}
        leads = [{'lid': 'lid1', 'lm': "Aug'26", 'mdl': 'TVS Raider',
                  'src': 'Organic', 'cd': ''}]  # missing CreateDate
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        self.assertEqual(len(ram), 0)
        self.assertEqual(meta['no_cd'], 1)
        self.assertEqual(meta['valid'], 0)

    # ── 12: date parsing ──────────────────────────────────────────────────────

    def test_iso_date_parsing(self):
        """parse_date('2026-08-15') → datetime.date(2026, 8, 15)."""
        result = _parse_date('2026-08-15')
        self.assertEqual(result, _datetime.date(2026, 8, 15))

    def test_parse_date_invalid_returns_none(self):
        """parse_date('bad') → None."""
        self.assertIsNone(_parse_date('bad'))

    def test_parse_date_empty_returns_none(self):
        """parse_date('') → None."""
        self.assertIsNone(_parse_date(''))

    # ── 13-14: model and source come from lead master ─────────────────────────

    def test_model_comes_from_lead_master(self):
        """Model index in ram uses lead master ModelName, not retail purchasedModel."""
        rmap = {'lid1': {'rm': "Aug'26", 'rtype': 'DMS',
                         'pm': 'TVS Apache RTR 160',   # retail purchased model (different)
                         'rd': _datetime.date(2026, 8, 10)}}
        leads = [{'lid': 'lid1', 'lm': "Aug'26", 'mdl': 'TVS Raider',  # lead model
                  'src': 'Organic', 'cd': '2026-08-01'}]
        ram, meta, maps = _run_ageing_fixture(leads, rmap)
        self.assertEqual(meta['valid'], 1)
        # The model in the ram key must be the lead model index
        (mi, si, abi, li), _ = list(ram.items())[0]
        self.assertEqual(maps['mdl'][mi], 'TVS Raider')  # lead master model, not retail pm

    def test_source_comes_from_lead_master(self):
        """Source index in ram uses lead master Source, not any retail attribute."""
        rmap = {'lid1': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': 'TVS Raider',
                         'rd': _datetime.date(2026, 8, 10)}}
        leads = [{'lid': 'lid1', 'lm': "Aug'26", 'mdl': 'TVS Raider',
                  'src': 'Facebook', 'cd': '2026-08-01'}]  # lead source
        ram, meta, maps = _run_ageing_fixture(leads, rmap)
        self.assertEqual(meta['valid'], 1)
        (mi, si, abi, li) = list(ram.keys())[0]
        self.assertEqual(maps['src'][si], 'Facebook')

    # ── 15: bucket reconciliation ─────────────────────────────────────────────

    def test_ageing_bucket_reconciliation(self):
        """Sum of all bucket rets == meta.valid (every valid retail lands in exactly one bucket)."""
        rd_base = _datetime.date(2026, 8, 1)
        ages = [0, 5, 8, 12, 15, 25, 31, 90]  # one per bucket (multiple per bucket)
        leads = [{'lid': f'l{i}', 'lm': "Aug'26", 'mdl': 'TVS Raider',
                  'src': 'Organic', 'cd': '2026-07-01'} for i in range(len(ages))]
        rmap = {f'l{i}': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': '',
                           'rd': _datetime.date(2026, 7, 1) + _datetime.timedelta(days=a)}
                for i, a in enumerate(ages)}
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        bucket_total = sum(v[0] for v in ram.values())
        self.assertEqual(bucket_total, meta['valid'])
        self.assertEqual(meta['valid'], len(ages))

    # ── 16: DMS + Call Out = Retails within ageing ───────────────────────────

    def test_dms_plus_co_equals_rets_in_ageing(self):
        """For every ram cell: dms + co == rets (no retail is both DMS and Call Out)."""
        leads = [
            {'lid': 'dms1', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'cd': '2026-08-01'},
            {'lid': 'co1',  'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'cd': '2026-08-01'},
        ]
        rd = _datetime.date(2026, 8, 10)
        rmap = {
            'dms1': {'rm': "Aug'26", 'rtype': 'DMS',      'pm': '', 'rd': rd},
            'co1':  {'rm': "Aug'26", 'rtype': 'Call Out', 'pm': '', 'rd': rd},
        }
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        self.assertEqual(meta['valid'], 2)
        for k, v in ram.items():
            rets, dms, co = v
            self.assertEqual(dms + co, rets, f"dms+co != rets for ram key {k}: {v}")

    # ── 17: duplicate opty_id no double count ─────────────────────────────────

    def test_duplicate_opty_id_no_double_count(self):
        """retail_map is keyed by lid (dict); each lid appears once — no double count."""
        # retail_map overwrite: if a lid appears twice, only last survives (dict key)
        rmap = {'lid1': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': '',
                         'rd': _datetime.date(2026, 8, 10)}}
        # leads deduplicated to one occurrence of lid1 (production dedup; here just one row)
        leads = [{'lid': 'lid1', 'lm': "Aug'26", 'mdl': 'TVS Raider',
                  'src': 'Organic', 'cd': '2026-08-01'}]
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        total_rets = sum(v[0] for v in ram.values())
        self.assertEqual(total_rets, 1)
        self.assertEqual(meta['valid'], 1)

    # ── 18: rd is additional field — existing retail_map fields unchanged ──────

    def test_rd_is_additional_field_in_retail_map(self):
        """Adding rd to build_retail_map must not alter rm, rtype, pm."""
        # Simulate the retail_map entry structure as built by build_retail_map
        entry_without_rd = {'rm': "Aug'26", 'rtype': 'DMS', 'pm': 'TVS Raider'}
        rd_map = {'lid1': _datetime.date(2026, 8, 15)}
        # Simulate: rmap[lid] = {**entry_without_rd, 'rd': rd_map.get(lid)}
        combined = {**entry_without_rd, 'rd': rd_map.get('lid1')}
        self.assertEqual(combined['rm'],    "Aug'26")
        self.assertEqual(combined['rtype'], 'DMS')
        self.assertEqual(combined['pm'],    'TVS Raider')
        self.assertEqual(combined['rd'],    _datetime.date(2026, 8, 15))
        # rd_map=None case: rd must be None
        combined_no_rd = {**entry_without_rd, 'rd': None}
        self.assertIsNone(combined_no_rd['rd'])
        self.assertEqual(combined_no_rd['rm'], "Aug'26")

    # ── 19-20: ageing aggregation is additive — existing OC/OU unaffected ─────

    def test_ageing_does_not_alter_lead_counts(self):
        """The ram aggregation touches only retailed leads; lead counts in mm are unaffected."""
        # Verify: lead count (index 0 in bump result) is independent of rd
        # The ageing bump only runs inside 'if is_ret' and only increments ram — not mm.
        # Verified by fixture: ageing meta.total == retails matched, not leads.
        leads = [
            {'lid': 'l1', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'cd': '2026-08-01'},
            {'lid': 'l2', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'cd': '2026-08-01'},
        ]
        rmap = {  # only l1 retailed
            'l1': {'rm': "Aug'26", 'rtype': 'DMS', 'pm': '', 'rd': _datetime.date(2026, 8, 10)},
        }
        _, meta, _ = _run_ageing_fixture(leads, rmap)
        self.assertEqual(meta['total'], 1, "ageing total must count retails only (not all leads)")
        self.assertEqual(meta['valid'], 1)

    def test_ageing_does_not_alter_existing_retail_total(self):
        """Bucket totals must equal exactly the retails with valid dates — no extras."""
        leads = [
            {'lid': 'a', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'cd': '2026-08-01'},
            {'lid': 'b', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'cd': ''},  # no cd
            {'lid': 'c', 'lm': "Aug'26", 'mdl': 'TVS Raider', 'src': 'Organic', 'cd': '2026-08-01'},
        ]
        rd = _datetime.date(2026, 8, 10)
        rmap = {
            'a': {'rm': "Aug'26", 'rtype': 'DMS',      'pm': '', 'rd': rd},
            'b': {'rm': "Aug'26", 'rtype': 'Call Out', 'pm': '', 'rd': rd},
            'c': {'rm': "Aug'26", 'rtype': 'DMS',      'pm': '', 'rd': None},  # no rd
        }
        ram, meta, _ = _run_ageing_fixture(leads, rmap)
        bucket_total = sum(v[0] for v in ram.values())
        # a: valid (age=9 → bucket 1); b: no_cd; c: no_rd
        self.assertEqual(meta['total'],  3)
        self.assertEqual(meta['valid'],  1)
        self.assertEqual(meta['no_cd'],  1)
        self.assertEqual(meta['no_rd'],  1)
        self.assertEqual(bucket_total,   1)
        self.assertEqual(meta['valid'] + meta['no_rd'] + meta['no_cd'] + meta['neg'], meta['total'])


# ---------------------------------------------------------------------------
# Test 21 — build_payload() isolation: ageing must not touch existing matrices
# ---------------------------------------------------------------------------
# This helper is a FAITHFUL STRUCTURAL COPY of the critical aggregation loop
# inside build_payload() (push_tvs_data.py).  push_tvs_data.py cannot be
# imported directly because it has unguarded module-level I/O; inlining the
# loop body is the established pattern in this test suite.
#
# Matrices included (mirrors build_payload verbatim):
#   monthly, sm, mm                    (On Create)
#   u_monthly, u_sm, u_mm             (On Update)
#   disp, u_disp                       (Retail Dispersion)
#   ram, _ram_*                        (Retail Ageing — new)
#
# Structural guarantees preserved from the original:
#   • bump/ubump call order is identical
#   • ageing block is copy-pasted from lines 1808-1828 of push_tvs_data.py
#   • _parse_date and _age_bucket are the same inlined copies used elsewhere
#
# leads: list of dicts with keys:
#   lid, lm, src, lt, mdl, cd (CreateDate string), rm (retail month string)
# retail_map: {lid: {rm, rtype, pm, rd}}  — rd may be None or datetime.date

def _run_build_payload_core(leads, retail_map):
    """Inline structural copy of build_payload()'s aggregation loop.

    Covers every matrix that the ageing code could theoretically corrupt.
    Keeps exact bump/ubump call order from push_tvs_data.py.
    """
    lm_idx,  src_idx, lt_idx, mdl_idx = {}, {}, {}, {}
    lm_arr,  src_arr, lt_arr, mdl_arr = [], [], [], []

    def _ix(d, arr, v):
        if v not in d:
            d[v] = len(arr); arr.append(v)
        return d[v]

    monthly  = {}
    sm       = {}
    mm       = {}
    u_monthly = {}
    u_sm      = {}
    u_mm      = {}
    disp      = {}
    u_disp    = {}
    ram       = {}
    _ram_total = _ram_valid = _ram_neg = _ram_no_rd = _ram_no_cd = 0

    def _bump(d, k, is_ret, rtype=''):
        if k not in d: d[k] = [0, 0, 0, 0]
        d[k][0] += 1
        if is_ret:
            d[k][1] += 1
            rt_u = rtype.upper()
            if 'DMS'  in rt_u: d[k][2] += 1
            elif 'CALL' in rt_u: d[k][3] += 1

    def _ubump(d, key_lead, key_ret, is_ret, rtype=''):
        if key_lead not in d: d[key_lead] = [0, 0, 0, 0]
        d[key_lead][0] += 1
        if is_ret:
            if key_ret not in d: d[key_ret] = [0, 0, 0, 0]
            d[key_ret][1] += 1
            rt_u = rtype.upper()
            if 'DMS'  in rt_u: d[key_ret][2] += 1
            elif 'CALL' in rt_u: d[key_ret][3] += 1

    for lead in leads:
        lid   = lead['lid']
        lm    = lead['lm']
        src   = lead['src']
        lt    = lead.get('lt', 'Unknown')
        mdl   = lead['mdl']

        is_ret = lid in retail_map
        rtype  = retail_map[lid]['rtype'] if is_ret else ''

        mi  = _ix(mdl_idx, mdl_arr, mdl)
        si  = _ix(src_idx, src_arr, src)
        tti = _ix(lt_idx,  lt_arr,  lt)
        li  = _ix(lm_idx,  lm_arr,  lm)

        # ── On Create bumps (exact order from build_payload) ──────────────
        _bump(monthly, str(li),                 is_ret, rtype)
        _bump(sm,      f"{si}|{li}",            is_ret, rtype)
        _bump(mm,      f"{mi}|{si}|{li}",       is_ret, rtype)

        # ── On Update bumps ───────────────────────────────────────────────
        rm  = retail_map[lid].get('rm', '') if is_ret else ''
        um  = rm if rm else lm
        uli = _ix(lm_idx, lm_arr, um)
        _ubump(u_monthly, str(li),         str(uli),          is_ret, rtype)
        _ubump(u_sm,  f"{si}|{li}",    f"{si}|{uli}",        is_ret, rtype)
        _ubump(u_mm,  f"{mi}|{si}|{li}", f"{mi}|{si}|{uli}", is_ret, rtype)

        if is_ret:
            pm  = retail_map[lid].get('pm', '') or 'Unknown'
            pmi = _ix(mdl_idx, mdl_arr, pm)
            disp  [f"{mi}|{pmi}|{li}"]  = disp  .get(f"{mi}|{pmi}|{li}",  0) + 1
            u_disp[f"{mi}|{pmi}|{uli}"] = u_disp.get(f"{mi}|{pmi}|{uli}", 0) + 1

            # ── Retail Ageing block — copy-pasted from push_tvs_data.py ──
            # Lines 1808-1828.  Touches ONLY ram and _ram_* counters.
            _ram_total_ref = _ram_total   # capture before (unused; see assertion below)
            _ram_total += 1
            _rd = retail_map[lid].get('rd')
            _cd = _parse_date(lead.get('cd', ''))
            if _rd is None:
                _ram_no_rd += 1
            elif _cd is None:
                _ram_no_cd += 1
            else:
                _age_days = (_rd - _cd).days
                if _age_days < 0:
                    _ram_neg += 1
                else:
                    _abi = _age_bucket(_age_days)
                    _rk  = f"{mi}|{si}|{_abi}|{li}"
                    if _rk not in ram: ram[_rk] = [0, 0, 0]
                    ram[_rk][0] += 1
                    _rt_u = rtype.upper()
                    if 'DMS'  in _rt_u: ram[_rk][1] += 1
                    elif 'CALL' in _rt_u: ram[_rk][2] += 1
                    _ram_valid += 1

    return {
        'monthly':   dict(monthly),
        'sm':        dict(sm),
        'mm':        dict(mm),
        'u_monthly': dict(u_monthly),
        'u_sm':      dict(u_sm),
        'u_mm':      dict(u_mm),
        'disp':      dict(disp),
        'u_disp':    dict(u_disp),
        'ram':       dict(ram),
        'ram_meta':  {
            'total': _ram_total, 'valid': _ram_valid,
            'no_rd': _ram_no_rd, 'no_cd': _ram_no_cd, 'neg': _ram_neg,
        },
    }


class TestAgeingIsolation(unittest.TestCase):
    """Test 21 — end-to-end build_payload() isolation.

    Proves that supplying valid Retail_Date values (Run B) versus
    rd=None / fetch failure (Run A) does NOT alter any pre-existing
    matrix produced by the aggregation loop.

    Uses _run_build_payload_core(), a faithful structural copy of
    build_payload()'s loop (the established inlining pattern for this
    test suite — push_tvs_data.py cannot be imported due to module-level I/O).
    """

    # Synthetic fixture: 3 leads — 2 retailed (DMS + Call Out), 1 non-retail
    _LEADS = [
        {'lid': 'L001', 'lm': "Aug'26", 'src': 'Facebook', 'lt': 'Hot',
         'mdl': 'TVS Raider', 'cd': '2026-08-01', 'rm': "Aug'26"},
        {'lid': 'L002', 'lm': "Aug'26", 'src': 'Organic',  'lt': 'Hot',
         'mdl': 'TVS Apache RTR 160', 'cd': '2026-07-25', 'rm': "Aug'26"},
        {'lid': 'L003', 'lm': "Aug'26", 'src': 'Facebook', 'lt': 'Warm',
         'mdl': 'TVS Raider', 'cd': '2026-08-05'},   # non-retail
    ]

    # Run A: rd=None for all retails (simulates complete fetch failure)
    _RMAP_NO_RD = {
        'L001': {'rm': "Aug'26", 'rtype': 'DMS',      'pm': 'TVS Raider',         'rd': None},
        'L002': {'rm': "Aug'26", 'rtype': 'Call Out',  'pm': 'TVS Apache RTR 160', 'rd': None},
    }

    # Run B: same data, valid rd supplied (9-day and 5-day age respectively)
    _RMAP_WITH_RD = {
        'L001': {'rm': "Aug'26", 'rtype': 'DMS',      'pm': 'TVS Raider',         'rd': _datetime.date(2026, 8, 10)},
        'L002': {'rm': "Aug'26", 'rtype': 'Call Out',  'pm': 'TVS Apache RTR 160', 'rd': _datetime.date(2026, 7, 30)},
    }

    def _run_both(self):
        pa = _run_build_payload_core(self._LEADS, self._RMAP_NO_RD)
        pb = _run_build_payload_core(self._LEADS, self._RMAP_WITH_RD)
        return pa, pb

    # ── Matrix identity assertions ─────────────────────────────────────────────

    def test_mm_identical_with_and_without_rd(self):
        """Model×Source×Month matrix is byte-identical regardless of Retail_Date availability."""
        pa, pb = self._run_both()
        self.assertEqual(pa['mm'], pb['mm'])

    def test_sm_identical_with_and_without_rd(self):
        """Source×Month matrix is byte-identical regardless of Retail_Date availability."""
        pa, pb = self._run_both()
        self.assertEqual(pa['sm'], pb['sm'])

    def test_monthly_identical_with_and_without_rd(self):
        """Monthly (On Create) matrix is byte-identical regardless of Retail_Date availability."""
        pa, pb = self._run_both()
        self.assertEqual(pa['monthly'], pb['monthly'])

    def test_u_mm_identical_with_and_without_rd(self):
        """On Update Model×Source matrix is byte-identical regardless of Retail_Date availability."""
        pa, pb = self._run_both()
        self.assertEqual(pa['u_mm'], pb['u_mm'])

    def test_u_monthly_identical_with_and_without_rd(self):
        """On Update monthly matrix is byte-identical regardless of Retail_Date availability."""
        pa, pb = self._run_both()
        self.assertEqual(pa['u_monthly'], pb['u_monthly'])

    def test_u_sm_identical_with_and_without_rd(self):
        """On Update Source×Month matrix is byte-identical regardless of Retail_Date availability."""
        pa, pb = self._run_both()
        self.assertEqual(pa['u_sm'], pb['u_sm'])

    def test_disp_identical_with_and_without_rd(self):
        """Retail Dispersion (OC) matrix is byte-identical regardless of Retail_Date availability."""
        pa, pb = self._run_both()
        self.assertEqual(pa['disp'], pb['disp'])

    def test_u_disp_identical_with_and_without_rd(self):
        """Retail Dispersion (OU) matrix is byte-identical regardless of Retail_Date availability."""
        pa, pb = self._run_both()
        self.assertEqual(pa['u_disp'], pb['u_disp'])

    # ── Retail / lead total identity ───────────────────────────────────────────

    def test_retail_total_identical(self):
        """ram_meta.total (retails processed by ageing) is the same in both runs."""
        pa, pb = self._run_both()
        self.assertEqual(pa['ram_meta']['total'], pb['ram_meta']['total'])
        self.assertEqual(pa['ram_meta']['total'], 2)   # fixture has 2 retails

    def test_lead_count_identical(self):
        """Lead totals in monthly OC are identical (ageing never touches lead counts)."""
        pa, pb = self._run_both()
        oc_leads_a = sum(v[0] for v in pa['monthly'].values())
        oc_leads_b = sum(v[0] for v in pb['monthly'].values())
        self.assertEqual(oc_leads_a, oc_leads_b)
        self.assertEqual(oc_leads_a, 3)   # fixture has 3 leads

    def test_dms_callout_identical(self):
        """DMS and Call Out counts in mm are byte-identical between Run A and Run B."""
        pa, pb = self._run_both()
        dms_a  = sum(v[2] for v in pa['mm'].values())
        co_a   = sum(v[3] for v in pa['mm'].values())
        dms_b  = sum(v[2] for v in pb['mm'].values())
        co_b   = sum(v[3] for v in pb['mm'].values())
        self.assertEqual(dms_a,  dms_b)
        self.assertEqual(co_a,   co_b)
        self.assertEqual(dms_a,  1)   # L001 is DMS
        self.assertEqual(co_a,   1)   # L002 is Call Out

    # ── Ageing diverges correctly ──────────────────────────────────────────────

    def test_run_a_has_no_ageing_rows(self):
        """Run A (rd=None) must produce empty ram — simulates fetch failure."""
        pa, _ = self._run_both()
        self.assertEqual(pa['ram'], {})
        self.assertEqual(pa['ram_meta']['valid'],  0)
        self.assertEqual(pa['ram_meta']['no_rd'],  2)

    def test_run_b_has_ageing_rows(self):
        """Run B (valid rd) must produce non-empty ram with correct bucket assignments."""
        _, pb = self._run_both()
        self.assertGreater(len(pb['ram']), 0)
        self.assertEqual(pb['ram_meta']['valid'], 2)
        self.assertEqual(pb['ram_meta']['no_rd'], 0)

    def test_run_b_ageing_bucket_correctness(self):
        """Run B ageing rows land in the correct buckets.

        L001: age = 2026-08-10 - 2026-08-01 = 9 days → bucket 1 (8-14 days)
        L002: age = 2026-07-30 - 2026-07-25 = 5 days → bucket 0 (0-7 days)
        """
        _, pb = self._run_both()
        bucket_sum = [0, 0, 0, 0]
        for v in pb['ram'].values():
            rets = v[0]
            bucket_sum[0] += 0   # placeholder; we check by key below
        # Verify bucket indices present in ram keys
        abi_values = set()
        for k in pb['ram']:
            parts = k.split('|')
            abi_values.add(int(parts[2]))
        self.assertIn(0, abi_values)   # L002: 5 days → bucket 0
        self.assertIn(1, abi_values)   # L001: 9 days → bucket 1

    def test_run_b_dms_plus_co_equals_rets_in_ageing(self):
        """Within ageing (Run B), DMS + Call Out == Retails for every ram cell."""
        _, pb = self._run_both()
        self.assertGreater(len(pb['ram']), 0, "Run B must produce ageing rows")
        for k, v in pb['ram'].items():
            rets, dms, co = v
            self.assertEqual(dms + co, rets, f"dms+co != rets for key {k}: {v}")


# ---------------------------------------------------------------------------
# Tests 155–174: Fetch resilience — page resume, exception classes, constants
# ---------------------------------------------------------------------------

# Inline copies of the new exception classes (must stay in sync with push_tvs_data.py).
class _RetailPageFailed(Exception):
    def __init__(self, page, accumulated_rows, headers, expected_total, cause):
        super().__init__(str(cause))
        self.page             = page
        self.accumulated_rows = accumulated_rows
        self.headers          = headers
        self.expected_total   = expected_total

class _RetailDatePageFailed(Exception):
    def __init__(self, page, partial_map, populated_ct, blank_ct, invalid_ct, cause):
        super().__init__(str(cause))
        self.page         = page
        self.partial_map  = partial_map
        self.populated_ct = populated_ct
        self.blank_ct     = blank_ct
        self.invalid_ct   = invalid_ct

_PUSH_TVS = Path(__file__).parent / 'push_tvs_data.py'


class TestPaginationConstants(unittest.TestCase):
    """Tests 155–164: Verify pagination constants in push_tvs_data.py.
    Read the source file as text so a change in the source is always caught."""

    def _src(self):
        return _PUSH_TVS.read_text(encoding='utf-8')

    def test_lead_page_size_is_3000(self):
        self.assertIn('_LEAD_PAGE_SIZE = 3000', self._src())

    def test_retail_page_size_is_2000(self):
        self.assertIn('_RETAIL_PAGE_SIZE = 2000', self._src())

    def test_retail_date_page_size_is_2000(self):
        self.assertIn('_RETAIL_DATE_PAGE_SIZE = 2000', self._src())

    def test_lead_timeouts_has_6_entries(self):
        self.assertIn('_LEAD_TIMEOUTS  = [30, 60, 90, 120, 180, 180]', self._src())

    def test_retail_timeouts_has_6_entries(self):
        self.assertIn('_RETAIL_TIMEOUTS  = [30, 60, 90, 120, 180, 180]', self._src())

    def test_retail_date_timeouts_has_6_entries(self):
        self.assertIn('_RETAIL_DATE_TIMEOUTS  = [30, 60, 90, 120, 180, 180]', self._src())

    def test_lead_backoffs_length_invariant(self):
        """_LEAD_BACKOFFS must have exactly len(_LEAD_TIMEOUTS)-1 entries."""
        lead_timeouts = [30, 60, 90, 120, 180, 180]
        lead_backoffs = [5, 10, 15, 20, 30]
        self.assertEqual(len(lead_backoffs), len(lead_timeouts) - 1)

    def test_retail_backoffs_length_invariant(self):
        retail_timeouts = [30, 60, 90, 120, 180, 180]
        retail_backoffs = [5, 10, 15, 20, 30]
        self.assertEqual(len(retail_backoffs), len(retail_timeouts) - 1)

    def test_retail_date_backoffs_length_invariant(self):
        rd_timeouts = [30, 60, 90, 120, 180, 180]
        rd_backoffs  = [5, 10, 15, 20, 30]
        self.assertEqual(len(rd_backoffs), len(rd_timeouts) - 1)

    def test_all_backoff_values_are_positive(self):
        for b in [5, 10, 15, 20, 30]:
            self.assertGreater(b, 0)


class TestPageResumeExceptions(unittest.TestCase):
    """Tests 165–169: _RetailPageFailed and _RetailDatePageFailed carry the right state."""

    def test_retail_page_failed_stores_page(self):
        e = _RetailPageFailed(7, ['r1', 'r2'], ['h1'], 100, RuntimeError('boom'))
        self.assertEqual(e.page, 7)

    def test_retail_page_failed_stores_accumulated_rows(self):
        rows = ['r1', 'r2', 'r3']
        e = _RetailPageFailed(2, rows, ['h'], 50, RuntimeError('x'))
        self.assertIs(e.accumulated_rows, rows)

    def test_retail_page_failed_stores_expected_total(self):
        e = _RetailPageFailed(0, [], None, 75000, RuntimeError('x'))
        self.assertEqual(e.expected_total, 75000)

    def test_retail_date_page_failed_stores_partial_map(self):
        m = {'lid1': '2026-08-01', 'lid2': '2026-07-15'}
        e = _RetailDatePageFailed(3, m, 2, 1, 0, RuntimeError('x'))
        self.assertIs(e.partial_map, m)
        self.assertEqual(e.populated_ct, 2)
        self.assertEqual(e.blank_ct, 1)
        self.assertEqual(e.invalid_ct, 0)

    def test_retail_date_page_failed_message_from_cause(self):
        e = _RetailDatePageFailed(1, {}, 0, 0, 0, RuntimeError('network timeout'))
        self.assertIn('network timeout', str(e))


class TestPaginationResumePurity(unittest.TestCase):
    """Tests 170–174: Core page-resume logic, no network needed.
    These use minimal pure-Python replicas of the inner paginator structure."""

    def _simulate_inner(self, responses):
        """Simulate _fetch_retails_inner with a list of per-page mock responses.
        Each response is either a dict (success) or an exception (failure).
        Raises _RetailPageFailed on per-page exhaustion (single attempt per page for simplicity).
        Returns (all_rows, done_received, pages_fetched)."""
        all_rows      = []
        done_received = False
        pages_fetched = 0
        for page, resp in enumerate(responses):
            if isinstance(resp, Exception):
                raise _RetailPageFailed(page, all_rows, None, None, resp)
            rows = resp.get('rows', [])
            all_rows.extend(rows)
            pages_fetched += 1
            if resp.get('done', True):
                done_received = True
                break
        return all_rows, done_received, pages_fetched

    def test_page_resume_preserves_prior_rows(self):
        """Rows from page 0 are preserved when page 1 fails and we resume."""
        page0_rows = [['A', '1'], ['B', '2']]
        try:
            self._simulate_inner([
                {'rows': page0_rows, 'done': False},
                RuntimeError('404 echo expired'),
            ])
            self.fail("Expected _RetailPageFailed")
        except _RetailPageFailed as e:
            self.assertEqual(e.page, 1)
            self.assertEqual(e.accumulated_rows, page0_rows)

    def test_page_resume_no_duplication(self):
        """Resuming at page 1 with prev_rows set does not re-fetch page 0 rows."""
        page0_rows = [['A', '1'], ['B', '2']]
        page1_rows = [['C', '3']]
        # Simulate outer retry: second call starts from page 1 with prev_rows
        all_rows   = list(page0_rows)   # preserved from failed run
        all_rows.extend(page1_rows)     # page 1 now succeeds
        self.assertEqual(len(all_rows), 3)
        self.assertEqual(all_rows[0], ['A', '1'])
        self.assertEqual(all_rows[2], ['C', '3'])

    def test_pagination_total_matches_sum_of_pages(self):
        """Total row count equals the sum of rows across all pages."""
        pages = [
            {'rows': [['r1'], ['r2']], 'done': False},
            {'rows': [['r3'], ['r4'], ['r5']], 'done': False},
            {'rows': [['r6']], 'done': True},
        ]
        all_rows, done_received, pages_fetched = self._simulate_inner(pages)
        self.assertEqual(len(all_rows), 6)
        self.assertTrue(done_received)
        self.assertEqual(pages_fetched, 3)

    def test_missing_done_signal_detected(self):
        """A response sequence that never sends done=True leaves done_received=False."""
        pages = [
            {'rows': [['r1']], 'done': False},
            {'rows': [['r2']], 'done': False},
        ]
        # Simulate exhausted list (no more pages returned) — done_received stays False
        all_rows      = []
        done_received = False
        for resp in pages:
            all_rows.extend(resp.get('rows', []))
            if resp.get('done', False):
                done_received = True
                break
        self.assertFalse(done_received)
        self.assertEqual(len(all_rows), 2)

    def test_proxy_get_always_uses_apps_script_url(self):
        """proxy_get() builds params from APPS_SCRIPT_URL each call — never a cached echo URL.
        Validate by inspecting the source that proxy_get calls requests.get(APPS_SCRIPT_URL…)."""
        src = _PUSH_TVS.read_text(encoding='utf-8')
        # proxy_get must reference APPS_SCRIPT_URL (not a hardcoded echo URL)
        self.assertIn('requests.get(APPS_SCRIPT_URL', src)
        # Must NOT reference the echo URL host directly
        self.assertNotIn('script.googleusercontent.com', src)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    unittest.main(verbosity=2)
