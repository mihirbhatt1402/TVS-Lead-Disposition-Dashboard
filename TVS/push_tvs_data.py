"""
TVS Lead Disposition — Daily Data Push
Runs via GitHub Actions at 11:30 AM IST every day.

PRODUCTION EXECUTION FLOW
  GitHub Actions → Load hist_cache.json.gz → Download Live Leads (GSheets)
  → Download Live Retail (GSheet) → Merge Historical + Live
  → Lead-Retail Reconciliation → Generate Payload → Compress → Push to Apps Script
  → Update Firebase → Dashboard Refreshed

DATA SOURCES
  Leads (historical) : hist_cache.json.gz — permanent production source (committed to git)
                       Coverage: Apr'25 – Jun'26  (2,634,996 rows, permanently frozen)
                       [Bootstrap only] Excel files on local disk were a one-time source used
                       to generate the cache. They are never accessed during normal production.
  Leads (live)       : 7 Google Sheets via Apps Script proxy (Jul'26 onwards)
  Retails (historical): hist_cache.json.gz — permanent production source (committed to git)
                        Coverage: Jan'25 – Jun'26  (384,217 entries after null-enquiryId
                        exclusion; excludes 30,427 bulk-import rows with no CRM match;
                        permanently frozen)
                        [Bootstrap only] Excel files on local disk were a one-time source.
  Retails (live)     : Google Sheet via Apps Script proxy (all dates, overwrites hist per lid)

JOIN: Lead.opty_id = Retail.sourceLeadId  (primary-key join, date/source never matter)
RETAIL MONTH: performanceMonth is authoritative for On Update (u_monthly); LeadMonth for On Create (monthly)
MERGE: hist leads + live leads deduped by SorceLeadId (keep='last'; live wins on overlap)
       hist retail map + live retail map merged by sourceLeadId (live overwrites hist per key)
"""

import json, sys, re, time, os, gzip, base64, traceback, shutil, argparse
import threading, concurrent.futures
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timedelta, timezone

MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbwdTKif3l3gYJKMwZBO6PjmYgNbWulkQ9TMEIsN-6xMdG2efbndnSoHE4tC63Oe6AKmlQ/exec"
SECRET = "tvs2026push"

RETAILS_FILE_ID = '1ZWBlzxX-g2R5iCcrsGUWrqSvxIHcchFHtajDDPcFJgE'
RETAILS_TAB     = 'Raw'

# Live lead masters — each entry is authoritative for its month range only.
#
# CLOSED months (frozen sheets — do NOT edit id/tab for closed entries):
#   Jul'26  — dedicated July GSheet, rows hard-capped to Jul'26.
#   Aug'26  — FROZEN snapshot committed at month-close (2026-09-01).
#             This sheet must receive no new lead data; it is the permanent
#             source for August leads from this point forward.
#
# OPEN months:
#   Sep'26  — current/live Lead Master (activated 2026-09-03).
#             When October closes, set max_mo=2609, add frozen=True, and
#             add a new Oct'26 entry.
#
# min_mo / max_mo: month_order integers (YY*100+MM). None = no upper bound.
# 'frozen': True documents closed months; has no runtime effect.
LEAD_SHEETS = [
    {
        'id':     '1gaRoPLebv7jaBgWEET-XSQuhqE_XgQlGru39TA-FoSo',
        'tab':    'TVS',
        'label':  "Jul'26-LeadMaster",
        'min_mo': 2607,
        'max_mo': 2607,
        'frozen': True,
    },
    # Aug'26 — FROZEN snapshot sheet (month closed 2026-09-01).
    # Retail continues to update; lead rows are permanently fixed.
    {
        'id':     '1Wp26qCv3d6oEq1h2wGamlHmCb9YuYrNDa8x8i653W3M',
        'tab':    'TVS',
        'label':  "Aug'26-LeadMaster-FROZEN",
        'min_mo': 2608,
        'max_mo': 2608,
        'frozen': True,
    },
    # Sep'26 — current/open Lead Master (activated 2026-09-03).
    # max_mo=None so it automatically covers Sep'26 and any later months
    # until this entry is capped and a new month is added at close.
    {
        'id':     '1iSw5zXF67q5Wkoz2mSPFqql9OPAcqmd0um5BEHUGf4o',
        'tab':    'TVS',
        'label':  "Sep'26-LeadMaster",
        'min_mo': 2609,
        'max_mo': None,   # open month — no upper bound until month-close
    },
]

# Months whose Lead Master GSheet has not yet been provided.
# The pipeline treats these as covered (no hard-fail for current-month) but
# fetches 0 lead rows and prints a clear warning.
# When a month's sheet is ready: add it to LEAD_SHEETS above and remove it here.
PENDING_LEAD_MONTHS: set = set()  # Sep'26 activated 2026-09-03

# Bootstrap/DR only: local path to historical Excel files, used only when rebuilding
# hist_cache.json.gz from scratch. Never accessed during normal production runs
# (the committed cache is always present on GitHub Actions).
HIST_DIR        = os.environ.get('TVS_HIST_DIR', r'C:\Users\mihir.bhatt\Desktop\New folder (2)')
HIST_CACHE_PATH = Path(__file__).parent / 'hist_cache.json.gz'

# Online sheets are only used for months from ONLINE_START onwards; historical Excel (Apr'25–Jun'26)
# is the authoritative source for everything before this.
ONLINE_START = "Jul'26"

# Earliest month for which CRM lead data exists. Gap-fill rows are never created for months
# before this — retails in Jan'25–Mar'25 have no lead counterpart by design (lead master
# starts Apr'25) and must not generate synthetic lead rows in the dashboard.
LEAD_MASTER_START = "Apr'25"

# Lead master column map: sheet column → canonical name
# purchasedModel (raw from retail sheet) → canonical lead-model name
PURCHASED_MODEL_MAP = {
    # TVS Apache RTR 200 4V
    'Apache 200 4V 1ch-R Mode': 'TVS Apache RTR 200 4V',
    'Apache 200 4V 2ch-R Mode': 'TVS Apache RTR 200 4V',

    # TVS Apache RR 310
    'APACHE RR 310 BSVI': 'TVS Apache RR 310',

    # TVS Apache RTR 160
    'APACHE RTR 160 2V BSVI DISC': 'TVS Apache RTR 160',
    'APACHE RTR 160 2V BSVI DRUM': 'TVS Apache RTR 160',

    # TVS Apache RTR 160 4V
    'APACHE RTR 160 4V BSVI DRUM': 'TVS Apache RTR 160 4V',

    # TVS Apache RTR 180
    'APACHE RTR 180 BSVI': 'TVS Apache RTR 180',

    # TVS Apache RR 310
    'TVS Apache RR 310': 'TVS Apache RR 310',

    # TVS Apache RTR 160
    'TVS Apache RTR 160': 'TVS Apache RTR 160',

    # TVS Apache RTR 160 4V
    'TVS Apache RTR 160 4V': 'TVS Apache RTR 160 4V',
    'TVS APACHE RTR 160 4V - Disc HP': 'TVS Apache RTR 160 4V',
    'TVS APACHE RTR 160 4V - Drum HP': 'TVS Apache RTR 160 4V',

    # TVS Apache RTR 180
    'TVS Apache RTR 180': 'TVS Apache RTR 180',

    # TVS Apache RTR 200 4V
    'TVS Apache RTR 200 4V': 'TVS Apache RTR 200 4V',

    # TVS Apache RTR 160 4V
    'TVS APACHE RTR 160 4V - RM DISC': 'TVS Apache RTR 160 4V',
    'TVS APACHE RTR 160 4V - RM DRUM': 'TVS Apache RTR 160 4V',
    'TVS APACHE RTR 160 4V - RM SPL ED': 'TVS Apache RTR 160 4V',
    'APACHE RTR 160 4V BSVI DISC': 'TVS Apache RTR 160 4V',

    # TVS Apache RTR 200 4V
    'APACHE RTR 200 BSVI': 'TVS Apache RTR 200 4V',

    # APACHE RTR 165
    'APACHE RTR 165 RP': 'APACHE RTR 165',

    # TVS Apache RTR 160 4V
    'Apache RTR 160 4V Disc BT': 'TVS Apache RTR 160 4V',

    # TVS Apache RTR 160
    'APACHE RTR 160 2V RM DISC': 'TVS Apache RTR 160',
    'APACHE RTR 160 2V RM DRUM': 'TVS Apache RTR 160',

    # TVS Jupiter
    'JUPITER BSVI': 'TVS Jupiter',
    'JUPITER BSVI - SMW': 'TVS Jupiter',
    'JUPITER BSVI-AOL': 'TVS Jupiter',
    'JUPITER CLASSIC BSVI': 'TVS Jupiter',
    'JUPITER ZX BSVI - AOL': 'TVS Jupiter',
    'JUPITER ZX DISC BSVI-ISS': 'TVS Jupiter',
    'TVS Jupiter': 'TVS Jupiter',
    'JUPITER CLASSIC – HBS': 'TVS Jupiter',
    'JUPITER ZX BSVI': 'TVS Jupiter',
    'JUPITER ZX DISC BSVI': 'TVS Jupiter',
    'Jupiter ZX Disc Ref (BSIV)': 'TVS Jupiter',
    'JUPITER ZX DISC SXC': 'TVS Jupiter',
    'TVS JUPITER CLASSIC DISC': 'TVS Jupiter',

    # TVS Jupiter 125
    'TVS Jupiter 125': 'TVS Jupiter 125',
    'JUPITER 125 BSVI': 'TVS Jupiter 125',
    'JUPITER 125 DRUM BSVI': 'TVS Jupiter 125',
    'JUPITER 125 SMW BSVI': 'TVS Jupiter 125',

    # TVS NTORQ 125
    'NTORQ 125 DISC – Race Edition BSVI': 'TVS NTORQ 125',
    'NTORQ 125 DISC – Super Squad Edition': 'TVS NTORQ 125',
    'NTORQ 125 DRUM NC BSVI': 'TVS NTORQ 125',
    'TVS NTORQ 125': 'TVS NTORQ 125',
    'TVS NTORQ 125 DISC BSVI': 'TVS NTORQ 125',
    'TVS NTORQ 125 RACE XP': 'TVS NTORQ 125',
    'TVS NTORQ 125 DRUM BSVI': 'TVS NTORQ 125',
    'TVS NTORQ 125 DISC': 'TVS NTORQ 125',
    'NTORQ 125 DISC – HBS': 'TVS NTORQ 125',
    'NTORQ 125 DISC – SSE': 'TVS NTORQ 125',
    'NTORQ 125 XT': 'TVS NTORQ 125',
    'NTORQ 125 DISC â€“ Race Edition BSVI': 'TVS NTORQ 125',
    'NTORQ 125 DISC â€“ SSE': 'TVS NTORQ 125',

    # TVS Scooty Pep Plus
    'Scooty Pep+ - BSVI': 'TVS Scooty Pep Plus',
    'Scooty Pep+ Matte series-BSVI': 'TVS Scooty Pep Plus',
    'TVS Scooty Pep Plus': 'TVS Scooty Pep Plus',
    'Scooty Pep+ Spl Edition': 'TVS Scooty Pep Plus',
    'Scooty PEP+': 'TVS Scooty Pep Plus',
    'Scooty Pep+ -BSVI Tamil Ed': 'TVS Scooty Pep Plus',

    # TVS Radeon
    'TVS Radeon': 'TVS Radeon',
    'TVS RADEON - DISC BSVI': 'TVS Radeon',
    'TVS RADEON 110 DUAL TONE': 'TVS Radeon',
    'TVS RADEON 110 ES MAG BSVI': 'TVS Radeon',
    'TVS RADEON 110 ES MAG REF BSVI': 'TVS Radeon',
    'TVS RADEON BSVI Disc Dual Tone': 'TVS Radeon',
    'TVS RADEON 110 ES MAG DRUM': 'TVS Radeon',
    'TVS RADEON BSVI DIGI Drum Dual Tone': 'TVS Radeon',
    'TVS RADEON BSVI DIGI Disc Dual Tone': 'TVS Radeon',

    # TVS Raider
    'TVS Raider': 'TVS Raider',
    'TVS RAIDER DISC': 'TVS Raider',

    # TVS Ronin
    'TVS Ronin': 'TVS Ronin',
    'TVS RONIN 2CH MID': 'TVS Ronin',
    'TVS RONIN 1CH BASE+': 'TVS Ronin',
    'TVS RONIN 1CH BASE': 'TVS Ronin',
    'TVS RONIN 2CH MID SPL': 'TVS Ronin',

    # TVS Sport
    'TVS Sport': 'TVS Sport',
    'TVS SPORT DURALIFE KS SWL BSVI': 'TVS Sport',
    'TVS SPORT ELS BSVI': 'TVS Sport',
    'TVS SPORT KLS BSVI': 'TVS Sport',
    'TVS SPORT ES-U559': 'TVS Sport',

    # TVS Star City Plus
    'STARCITY + ES DISC BSVI': 'TVS Star City Plus',
    'StarCity + ES DT BSVI': 'TVS Star City Plus',
    'TVS Star City Plus': 'TVS Star City Plus',
    'StarCity + BSIV  110 ES MAG WHL': 'TVS Star City Plus',
    'StarCity + ES BSVI': 'TVS Star City Plus',

    # TVS XL100
    'TVS XL 100 COM BSVI': 'TVS XL100',
    'TVS XL 100 COM iTs-BSVI': 'TVS XL100',
    'TVS XL 100 HD BSIV – SBS': 'TVS XL100',
    'TVS XL 100 HD BSVI': 'TVS XL100',
    'TVS XL 100 HD iTs BSVI': 'TVS XL100',
    'TVS XL 100 HD iTs Spl. Edition-BSVI': 'TVS XL100',
    'TVS XL 100 HD iTs Winner Edition': 'TVS XL100',
    'TVS XL100': 'TVS XL100',

    # TVS Scooty Zest
    'Scooty Zest Matte series – BSVI': 'TVS Scooty Zest',
    'TVS Scooty Zest': 'TVS Scooty Zest',
    'Scooty Zest – BSVI': 'TVS Scooty Zest',
    'Scooty Zest Matte series â€“ BSVI': 'TVS Scooty Zest',

    # TVS Apache RTR 160
    'APACHE RTR 160 2V RM DISC BT': 'TVS Apache RTR 160',

    # TVS Apache RTR 200 4V
    'TVS Apache RTR 200 Fi E100': 'TVS Apache RTR 200 4V',

    # TVS Raider
    'TVS RAIDER DISC CONNECTED': 'TVS Raider',

    # TVS Apache RTR 180
    'APACHE RTR 180 RM': 'TVS Apache RTR 180',

    # TVS Apache RR 310
    'APACHE RR 310 BTO-DYNAMIC': 'TVS Apache RR 310',
    'APACHE RR 310 BTO-RACE+DYN': 'TVS Apache RR 310',

    # TVS NTORQ 125
    'NTORQ 125 DISC ? SSE': 'TVS NTORQ 125',

    # TVS Raider
    'TVS RAIDER DISC - SS': 'TVS Raider',

    # TVS NTORQ 125
    'NTORQ 125 DISC ? Race Edition BSVI': 'TVS NTORQ 125',

    # TVS Scooty Zest
    'Scooty Zest Matte series ? BSVI': 'TVS Scooty Zest',
    'Scooty Zest ? BSVI': 'TVS Scooty Zest',

    # TVS Apache RR 310
    'APACHE RR 310 BTO-RC REP+RC DYN+RD AL': 'TVS Apache RR 310',

    # TVS Jupiter
    'JUPITER ZX DRUM SXC': 'TVS Jupiter',

    # TVS Raider
    'TVS RAIDER DISC - SSE': 'TVS Raider',

    # TVS Apache RTR 310
    'TVS Apache RTR 310': 'TVS Apache RTR 310',

    # TVS Apache RTR 160
    'TVS Apache RTR160': 'TVS Apache RTR 160',
    'TVS Apache 160': 'TVS Apache RTR 160',

    # TVS Raider
    'Raider': 'TVS Raider',

    # TVS NTORQ 125
    'ntorq 125': 'TVS NTORQ 125',

    # TVS XL100
    'TVS XL 100': 'TVS XL100',

    # TVS Apache RR 310
    'RR 310': 'TVS Apache RR 310',

    # TVS Jupiter
    'Jupiter': 'TVS Jupiter',

    # TVS XL100
    'XL 100': 'TVS XL100',

    # TVS NTORQ 125
    'Ntorq': 'TVS NTORQ 125',

    # TVS Apache RTR 160
    'RTR 160': 'TVS Apache RTR 160',

    # TVS Jupiter 125
    'JUPITER 125 DISC SX': 'TVS Jupiter 125',

    # TVS iQube
    'TVS iQube': 'TVS iQube',
    'TVS IQube UG-New': 'TVS iQube',

    # TVS Ronin
    'TVS RONIN 2CH MID SPECIAL EDITION': 'TVS Ronin',

    # TVS Apache RTR 310
    'APACHE RTR 310 ? BASE YEL': 'TVS Apache RTR 310',

    # TVS iQube
    'TVS IQube S-New': 'TVS iQube',

    # TVS Apache RTR 160 4V
    'TVS APACHE RTR 160 4V ? U626': 'TVS Apache RTR 160 4V',
    'TVS APACHE RTR 160 4V - 2CH ABS BT': 'TVS Apache RTR 160 4V',

    # TVS Apache RTR 160
    'Apache RTR': 'TVS Apache RTR 160',

    # TVS Radeon
    'Radeon': 'TVS Radeon',

    # TVS Apache RTR 310
    'APACHE RTR 310 ? BASE BLK': 'TVS Apache RTR 310',

    # TVS Sport
    'Sport': 'TVS Sport',

    # TVS XL100
    'XL100': 'TVS XL100',

    # TVS Apache RTR 310
    'APACHE RTR 310 ? DYN YEL': 'TVS Apache RTR 310',

    # TVS Apache RR 310
    'APACHE RR 310 BTO - RACE': 'TVS Apache RR 310',

    # TVS Jupiter 125
    'Jupiter 125': 'TVS Jupiter 125',

    # TVS Ronin
    'Ronin': 'TVS Ronin',

    # TVS Jupiter
    'JUPITERBSVI SMW INS- OBDIIA': 'TVS Jupiter',

    # TVS Scooty Zest
    'Zest': 'TVS Scooty Zest',

    # TVS Star City Plus
    'StaR city+': 'TVS Star City Plus',

    # TVS iQube
    'TVS IQUBE ELECTRIC 9': 'TVS iQube',

    # TVS Apache RTR 160 4V
    'TVS APACHE RTR 1604V? RM OBDIIA DRUM B.E': 'TVS Apache RTR 160 4V',

    # TVS Apache RTR 160
    'TVS APACHE RTR 160-2V RM OBDIIA DRUM B.E': 'TVS Apache RTR 160',

    # TVS Apache RTR 310
    'APACHE RTR 310 ? DYN BLK': 'TVS Apache RTR 310',

    # TVS iQube
    'TVS iQUBE ELECTRIC SMARTXONNECT 9 W BRWN': 'TVS iQube',
    'TVS iQUBE ELECTRIC SMARTXONNECT T GREY': 'TVS iQube',
    'TVS iQUBE ELECTRIC S -C BRONZE GLOSSY': 'TVS iQube',
    'TVS iQUBE ELECTRIC SMARTXONNECT T.GREY': 'TVS iQube',
    'TVS iQUBE ELECTRIC SMARTXONNECT P.WHITE': 'TVS iQube',
    'TVS iQUBE ELECTRIC S -MINT BLUE â€“ GLOSSY': 'TVS iQube',
    'TVS iQUBE ELECTRIC SMARTXONNECT SHINIG.R': 'TVS iQube',
    'TVS IQUBE ELECTRIC S- MINT BLUE â€“ GLOSSY': 'TVS iQube',
    'TVS iQUBE ELECTRIC SMARTXONNECT 9P.WHITE': 'TVS iQube',
    'TVS IQUBE ELECTRIC S- C BRONZE GLOSSY': 'TVS iQube',
    'TVS iQUBE ELECTRIC ST12 M52V S BLUE': 'TVS iQube',
    'TVS IQUBE ELECTRIC S-MERCURY GREYâ€“GLOSSY': 'TVS iQube',
    'TVS iQUBE ELECTRIC S MERCURY GREY': 'TVS iQube',
    'TVS iQube ELECTRIC ST 12 S BLUE': 'TVS iQube',
    'TVS iQUBE ELECTRIC SMARTXONNECT PEARL W': 'TVS iQube',
    'TVS iQUBE ELECTRIC ST12 M52V TG MTTE': 'TVS iQube',
    'TVS iQube ELECTRIC ST 17 S BLUE': 'TVS iQube',

    # TVS Apache RTR 160
    'TVS Apache RTR': 'TVS Apache RTR 160',
    'TVS APACHE RTR 160 2V DISC BT RACING EDI': 'TVS Apache RTR 160',

    # TVS NTORQ 125
    'TVS NTorq': 'TVS NTORQ 125',

    # TVS NTORQ 150  (production raw value confirmed 2026-08-25)
    'TVS NTorq 150':   'TVS NTORQ 150',
    'TVS NTORQ 150':   'TVS NTORQ 150',

    # TVS Scooty Zest
    'TVS Zest': 'TVS Scooty Zest',

    # TVS iQube
    'TVS iQube ST': 'TVS iQube',
    'TVS iQube S': 'TVS iQube',

    # TVS Jupiter
    'TVS JUPITER110 DISC ALLOY SXC': 'TVS Jupiter',
    'TVS JUPITER110 DRUM ALLOY SXC': 'TVS Jupiter',

    # TVS Star City Plus
    'TVS StaR city+': 'TVS Star City Plus',

    # TVS Jupiter
    'TVS JUPITER110 DRUM': 'TVS Jupiter',

    # TVS Ronin
    'TVS RONIN 1CH BASE-LNG Black': 'TVS Ronin',

    # TVS Jupiter
    'TVS JUPITER110 DRUM ALLOY': 'TVS Jupiter',

    # TVS Ronin
    'TVS RONIN 1CH BASE-FL RED': 'TVS Ronin',

    # TVS iQube
    'IQUBE ST 12': 'TVS iQube',

    # TVS Raider
    'TVS RAIDER DRUM': 'TVS Raider',

    # TVS Apache RR 310
    'APACHE RR310-OBDIIA-M23?BASE-RAR': 'TVS Apache RR 310',

    # TVS Radeon
    'TVS RADEON BSVI Drum ? Black Edn': 'TVS Radeon',

    # TVS Apache RTR 310
    'APACHE RTR 310 ? DYN+DYN PRO YEL': 'TVS Apache RTR 310',

    # TVS Apache RR 310
    'APACHE RR310-OBDIIA-M23?DYN PRO-RCR TR': 'TVS Apache RR 310',
    'APACHE RR310-OBDIIA-M23?BASE-SMG': 'TVS Apache RR 310',
    'APACHE RR310-OBDIIA-M23?BASE W/O QS-RAR': 'TVS Apache RR 310',

    # TVS Raider
    'TVS RAIDER DISC - LCD SX': 'TVS Raider',

    # TVS Apache RTR 310
    'APACHE RTR 310 ? DYN-PRO YEL': 'TVS Apache RTR 310',

    # TVS iQube
    'TVS IQUBE ST 17': 'TVS iQube',
    'TVS iQube 2.2 kWh': 'TVS iQube',
    'TVS iQube 3.4 kWh': 'TVS iQube',
    'TVS iQube S 3.4 kWh': 'TVS iQube',
    'TVS iQube ST 3.4 kWh': 'TVS iQube',

    # TVS Apache RR 310
    'APACHE RR310-OBDIIA-M23?DYN-RAR': 'TVS Apache RR 310',

    # TVS iQube
    'TVS iQube ST 5.1 kWh': 'TVS iQube',

    # TVS Apache RTR 160 4V
    'TVS APACHE RTR 160 4V USD ? 2CH': 'TVS Apache RTR 160 4V',

    # TVS Jupiter
    'TVS JUPITER SMW - INSW': 'TVS Jupiter',

    # TVS Apache RR 310
    'APACHE RR310-OBDIIA-M23?DYN PRO-SEP': 'TVS Apache RR 310',

    # TVS Jupiter 125
    'TVS JUPITER 125 DISC OBDIIB': 'TVS Jupiter 125',

    # TVS Jupiter
    'TVS JUPITER110 DISC ALLOY SXC OBDIIB': 'TVS Jupiter',

    # TVS Ronin
    'TVS RONIN MID 2CH ? CHARCOAL EMBER': 'TVS Ronin',

    # TVS Jupiter 125
    'TVS JUPITER 125 DRUM OBDIIB': 'TVS Jupiter 125',

    # TVS Jupiter
    'TVS JUPITER110 DRUM ALLOY SXC OBDIIB': 'TVS Jupiter',

    # TVS Apache RR 310
    'APACHE RR310-OBDIIA-M23?DYN+DYN PRO-SMG': 'TVS Apache RR 310',

    # TVS Jupiter
    'TVS JUPITER110 DRUM ALLOY OBDIIB': 'TVS Jupiter',

    # TVS Ronin
    'TVS RONIN MID 2CH ? GLACIER SILVER': 'TVS Ronin',

    # TVS Jupiter
    'TVS JUPITER110 DRUM OBDIIB': 'TVS Jupiter',

    # TVS Jupiter 125
    'TVS JUPITER 125 DISC SXC OBDIIB': 'TVS Jupiter 125',

    # TVS Apache RTR 160
    'TVS APACHE RTR160-OBDIIB 2V DRUM': 'TVS Apache RTR 160',
    'TVS APACHE RTR160-OBDIIB 2V DISC': 'TVS Apache RTR 160',
    'TVS APACHE RTR160-OBDIIB 2V DISC BT': 'TVS Apache RTR 160',

    # TVS XL100
    'TVS XL 100 HD iTs Winner Edition OBDIIB': 'TVS XL100',

    # TVS NTORQ 125
    'TVS NTORQ 125 RACE XP BSVI OBDIIB': 'TVS NTORQ 125',

    # TVS XL100
    'TVS XL 100 HD iTs OBDIIB': 'TVS XL100',

    # TVS Radeon
    'TVS RADEON 110 ES MAG BSVI-OBD IIA': 'TVS Radeon',

    # TVS Apache RTR 180
    'APACHE RTR 180 RM-OBIIA': 'TVS Apache RTR 180',

    # TVS Sport
    'TVS SPORT ELS BSVI-OBD IIA': 'TVS Sport',

    # TVS Radeon
    'TVS RADEON BSVI DIGIDrum DT OBDIIA': 'TVS Radeon',

    # TVS Raider
    'RAIDER IGO I-ECU RD WH OBDIIB': 'TVS Raider',

    # TVS Apache RTR 160 4V
    'TVSAPACHERTR1604V?OBDIIB 2CH USD': 'TVS Apache RTR 160 4V',

    # TVS Apache RTR 180
    'APACHE RTR 180 RM-OBD IIA': 'TVS Apache RTR 180',

    # TVS Sport
    'TVS SPORT ELS BSVI-OBIIA': 'TVS Sport',

    # TVS Ronin
    'TVS RONIN 1CH BASE-LNG Black - OBDIIB': 'TVS Ronin',

    # TVS Apache RTR 160
    'TVS APACHE RTR160-OBDIIB 2V RAC ED': 'TVS Apache RTR 160',

    # TVS Apache RR 310
    'TVS Apache RR': 'TVS Apache RR 310',

    # TVS Ronin
    'TVS RONIN 1CH BASE-FL RED - OBDIIB': 'TVS Ronin',

    # TVS Apache RTR 310
    'APACHE RTR 310 ? BASE BLK-OBD IIA': 'TVS Apache RTR 310',

    # TVS Ronin
    'TVS RONIN MID 2CH ? CHARCOAL EMBR OBDIIB': 'TVS Ronin',

    # TVS Scooty Zest
    'Scooty Zest Matte series ? OBDIIB': 'TVS Scooty Zest',
    'Scooty Zest ? OBDIIB': 'TVS Scooty Zest',

    # TVS Apache RR 310
    'APACHE RR310-O2B-M24?BASE-RAR': 'TVS Apache RR 310',

    # TVS Ronin
    'TVS RONIN MID 2CH ? GLACIER SILVR OBDIIB': 'TVS Ronin',

    # TVS NTORQ 125
    'TVS NTORQ 125 SUPER SQUAD BSVI OBDIIB': 'TVS NTORQ 125',

    # TVS XL100
    'TVS XL 100 COM iTs- OBDIIB': 'TVS XL100',

    # TVS Raider
    'RAIDER SS DISC OBDIIB': 'TVS Raider',

    # TVS Apache RTR 160 4V
    'TVSAPACHERTR1604V–OBDIIB 2CH USD': 'TVS Apache RTR 160 4V',
    'TVS APACHE RTR 160 4V USD – 2CH': 'TVS Apache RTR 160 4V',

    # TVS Ronin
    'TVS RONIN MID 2CH – CHARCOAL EMBR OBDIIB': 'TVS Ronin',

    # TVS Radeon
    'TVS RADEON BSVI Drum – Black Edn': 'TVS Radeon',

    # TVS Scooty Zest
    'Scooty Zest – OBDIIB': 'TVS Scooty Zest',

    # TVS Ronin
    'TVS RONIN MID 2CH – CHARCOAL EMBER': 'TVS Ronin',

    # TVS Scooty Zest
    'Scooty Zest Matte series – OBDIIB': 'TVS Scooty Zest',

    # TVS NTORQ 125
    'TVS NTORQ 125 DISC BSVI OBDIIB': 'TVS NTORQ 125',

    # TVS Apache RTR 160
    'TVS APACHE RTR160-OBDIIB 2V DRUM BLK.EDI': 'TVS Apache RTR 160',

    # TVS Apache RR 310
    'APACHE RR310-OBDIIA-M23–BASE-SMG': 'TVS Apache RR 310',

    # TVS Sport
    'SPORT ES OBDIIB': 'TVS Sport',

    # TVS NTORQ 125
    'TVS NTORQ 125 RACE EDT\xa0 BSVI OBDIIB': 'TVS NTORQ 125',

    # TVS Radeon
    'RADEON DRUM BLACK EDITION OBDIIB': 'TVS Radeon',

    # TVS Ronin
    'TVS RONIN 2CH MID SPECIAL EDI OBDIIB': 'TVS Ronin',

    # TVS Apache RTR 160 4V
    'TVS APACHE RTR1604V–OBDIIB SPL ED': 'TVS Apache RTR 160 4V',

    # TVS Ronin
    'TVS RONIN MID 2CH – GLACIER SILVER': 'TVS Ronin',

    # TVS Apache RTR 160 4V
    'TVS APACHE RTR 1604V– RM OBDIIA DRUM B.E': 'TVS Apache RTR 160 4V',

    # TVS Raider
    'RAIDER DISC OBDIIB': 'TVS Raider',

    # TVS Apache RR 310
    'APACHE RR310-O2B-M24–BASE-RAR': 'TVS Apache RR 310',
    'APACHE RR310-OBDIIA-M23–DYN PRO-RCR TR': 'TVS Apache RR 310',

    # TVS Sport
    'SPORT ES+ OBDIIB': 'TVS Sport',

    # TVS Raider
    'RAIDER DRUM OBDIIB': 'TVS Raider',

    # TVS Ronin
    'TVS RONIN MID 2CH – GLACIER SILVR OBDIIB': 'TVS Ronin',

    # TVS Radeon
    'RADEON DRUM OBDIIB': 'TVS Radeon',

    # TVS Apache RR 310
    'APACHE RR310-O2B-M24–BASE W/O QS-RAR': 'TVS Apache RR 310',

    # TVS Raider
    'RAIDER SQD EDN I-ECU OBDIIB': 'TVS Raider',

    # TVS Apache RR 310
    'APACHE RR310-O2B-M24-BASE-GRY': 'TVS Apache RR 310',
    'APACHE RR310-OBDIIA-M23–BASE-RAR': 'TVS Apache RR 310',

    # TVS Radeon
    'RADEON DISC DIGI OBDIIB': 'TVS Radeon',

    # TVS NTORQ 125
    'TVS NTORQ 125 XT BSVI OBDIIB': 'TVS NTORQ 125',

    # TVS Raider
    'RAIDER DISC IGO I-ECU OBDIIB': 'TVS Raider',

    # TVS Radeon
    'RADEON DRUM DIGI OBDIIB': 'TVS Radeon',

    # TVS XL100
    'TVS XL 100 HD OBDIIB': 'TVS XL100',

    # TVS Ronin
    'TVS RONIN 2CH MID SPECIAL EDITION - OBDI': 'TVS Ronin',
    'TVS RONIN MID 2CH – CHARCOAL EMBER - OBD': 'TVS Ronin',

    # TVS iQube
    'TVS IQube UG-Beige': 'TVS iQube',

    # TVS Apache RTR 310
    'APACHE RTR 310 – BASE BLK-OBD IIA': 'TVS Apache RTR 310',

    # TVS Star City Plus
    'CITY+ DRUM OBDIIB': 'TVS Star City Plus',

    # TVS Jupiter 125
    'TVS JUPITER 125 DISC DT SXC OBDIIB': 'TVS Jupiter 125',

    # TVS Apache RR 310
    'APACHE RR310-OBDIIA-M23–BASE W/O QS-RAR': 'TVS Apache RR 310',

    # TVS Apache RTR 310
    'APACHE RTR 310 – BASE YEL': 'TVS Apache RTR 310',

    # TVS Apache RTR 180
    'TVS APACHE RTR180-OBDIIB DISC': 'TVS Apache RTR 180',

    # TVS Ronin
    'TVS RONIN MID 2CH – GLACIER SILVER- OBD': 'TVS Ronin',

    # TVS Apache RTR 200 4V
    'TVS APACHE RTR 200 4V–OBDIIB 2CH': 'TVS Apache RTR 200 4V',

    # TVS Apache RTR 160
    '2024 TVS Apache RTR 160': 'TVS Apache RTR 160',

    # TVS iQube
    'TVS IQUBE ST 17-Beige': 'TVS iQube',

    # TVS Apache RTR 200 4V
    '2025 TVS Apache RTR 200 4V': 'TVS Apache RTR 200 4V',

    # TVS Apache RTR 160
    'TVS APACHE RTR 160 2V DC ABS': 'TVS Apache RTR 160',

    # TVS iQube
    'U759 iQUBE 11 Black': 'TVS iQube',

    # TVS Star City Plus
    'CITY+ DISC OBDIIB': 'TVS Star City Plus',

    # TVS Apache RTR 310
    '2024 TVS Apache RTR 310': 'TVS Apache RTR 310',

    # TVS Raider
    'RAIDER SX I-ECU OBDIIB': 'TVS Raider',

    # TVS iQube
    'TVS iQube 11 Fr. Disc black': 'TVS iQube',

    # TVS Apache RTR 310
    'APACHE RTR 310-O2B-M24-BASE-RC-RED': 'TVS Apache RTR 310',

    # TVS iQube
    'TVS IQube S-Beige': 'TVS iQube',
    'iQube': 'TVS iQube',

    # TVS Apache RTR 200 4V
    '2024 TVS Apache RTR 200 4V': 'TVS Apache RTR 200 4V',

    # TVS Apache RTR 310
    'Apache RTR 310': 'TVS Apache RTR 310',

    # TVS Apache RR 310
    'Apache RR': 'TVS Apache RR 310',

    # TVS Scooty Zest
    'TVS Zest 110': 'TVS Scooty Zest',

    # TVS Apache RTR 200 4V
    'APACHE 200-4V PL TFT USD 2CH A.EDI': 'TVS Apache RTR 200 4V',

    # TVS NTORQ 125
    'NTORQ 125 RACE XP OBDIIB TORQUE ASSIST': 'TVS NTORQ 125',

    # TVS XL100
    'TVS XL 100 HEAVY DUTY ES': 'TVS XL100',

    # TVS Raider
    'RAIDER - OBDIIB 1CH ABS': 'TVS Raider',

    # TVS Sport
    'SPORT ELS REFRESH OBDIIB': 'TVS Sport',

    # TVS Apache RTR 310
    'APACHE RTR 310-O2B-M24- BASE-GL BLK': 'TVS Apache RTR 310',

    # TVS Apache RTR 200 4V
    'APACHE  200 4V – PL 2CH USD+TFT OBDIIB': 'TVS Apache RTR 200 4V',

    # TVS XL100
    'TVS XL 100 HD iTs – SBS Spl. Edition': 'TVS XL100',

    # TVS Radeon
    'TVS RADEON - DIGI DISC': 'TVS Radeon',

    # TVS Scooty Zest
    'TVS ZEST - OBDIIB SXC BLACK': 'TVS Scooty Zest',
    'TVS ZEST - OBDIIB SXC NARDO GREY': 'TVS Scooty Zest',

    # TVS Apache RTR 160 4V
    'APACHE 160-4V PL TFT USD 2CH A.EDI': 'TVS Apache RTR 160 4V',

    # TVS Apache RR 310
    'APACHE RR310-O2B-M24–BASE-SMG': 'TVS Apache RR 310',

    # TVS Apache RTR 160
    'APACHE 160-2V Disc 2CH A -EDI OBDIIB': 'TVS Apache RTR 160',

    # TVS Apache RTR 160 4V
    'APACHE  160 4V â€“ PL 2CH USD+TFT OBDIIB': 'TVS Apache RTR 160 4V',

    # TVS Radeon
    'TVS RADEON - DIGI DRUM': 'TVS Radeon',

    # TVS iQube
    'U759 iQUBE': 'TVS iQube',

    # TVS Ronin
    'TVS Ronin TD': 'TVS Ronin',

    # TVS Apache RTR 310
    'APACHE RTR 310-O2B-M24-DYN-RC-RED': 'TVS Apache RTR 310',

    # TVS Ronin
    'TVS RONIN BASE OBIIB 1CH – MATTE WHITE': 'TVS Ronin',

    # TVS Apache RTR 200 4V
    'APACHE  200 4V â€“ PL 2CH USD+TFT OBDIIB': 'TVS Apache RTR 200 4V',

    # TVS Apache RTR 310
    'APACHE RTR 310-O2B-M24-DYN-PRO+ SP BLU': 'TVS Apache RTR 310',
    'APACHE RTR 310 – BASE BLK': 'TVS Apache RTR 310',
    'APACHE RR310-O2B-M24-DYN-SEP-BLU': 'TVS Apache RTR 310',
    'APACHE RTR 310-O2B-M24-BASE-BLK YEL': 'TVS Apache RTR 310',
    'APACHE RTR 310-O2B-M25-DYN+DYPR-GBLK GLD': 'TVS Apache RTR 310',

    # TVS Apache RTR 200 4V
    'TVS Apache RTR 200': 'TVS Apache RTR 200 4V',

    # TVS Apache RTR 160
    'TVS Apache 2V': 'TVS Apache RTR 160',
    'TVS Apache': 'TVS Apache RTR 160',

    # TVS Jupiter
    'Jupiter Disc SXC OBDIIB – SPL': 'TVS Jupiter',

    # TVS Apache RTR 160 4V
    'TVS APACHE RTR 1604V-OBDIIB DISC BLK.EDI': 'TVS Apache RTR 160 4V',

    # TVS Apache RTR 160
    'TVS APACHE RTR 160-OBDIIB 2V DC ABS': 'TVS Apache RTR 160',

    # TVS Apache RR 310
    'APACHE RR 310-O2B-M25-DYN+DYPR-GBLK GLD': 'TVS Apache RR 310',
    'APACHE RR310-O2B-M24-DYN PRO-SEP-BLU': 'TVS Apache RR 310',

    # TVS Jupiter
    'TVS JUPITER110 DRUM SMW OBDIIB': 'TVS Jupiter',

    # TVS Apache RTR 160 4V
    'APACHE  160 4V – PL 2CH USD OBDIIB': 'TVS Apache RTR 160 4V',

    # TVS Apache RTR 310
    'APACHE RTR 310-O2B-M24-DYN+DYPR-RC-RED': 'TVS Apache RTR 310',

    # TVS NTORQ 125
    'NTORQ 125 SSE R.LCD OBD2B': 'TVS NTORQ 125',
    'NTORQ 125 DISC R.LCD OBD2B': 'TVS NTORQ 125',
    'NTORQ 125 RE R.LCD OBD2B': 'TVS NTORQ 125',

    # TVS Apache RTR 160 4V
    'APACHE  160 4V – PL DISC B.T OBDIIB': 'TVS Apache RTR 160 4V',

    # TVS Raider
    'Raider LCD OBDIIB 1CH ABS': 'TVS Raider',

    # TVS iQube
    'TVS iQUBE  S15 BLACK Fr Disc': 'TVS iQube',

    # TVS Apache RTR 180
    'APACHE 180-2V Disc 1CH A -EDI OBDIIB': 'TVS Apache RTR 180',

    # TVS Jupiter
    'TVS Jupiter 110 Special Edition': 'TVS Jupiter',

    # TVS iQube
    'TVS iQUBE  S15 BEIGE  Fr Disc': 'TVS iQube',

    # TVS Apache RTR 160 4V
    'APACHE  160 4V – PL DISC SPL ED OBDIIB': 'TVS Apache RTR 160 4V',

    # TVS Jupiter
    'TVS Jupiter 110cc': 'TVS Jupiter',
    'Jupiter 110': 'TVS Jupiter',
    'Jupiter X': 'TVS Jupiter',

    # TVS iQube
    'TVS iQube 11 Fr. Disc Beige': 'TVS iQube',

    # TVS Apache RTR 310
    'APACHE RTR 310-O2B-M24-DYN PRO-RC-RED TR': 'TVS Apache RTR 310',
}
# Lead model map uses the same 345-entry lookup as the retail map
LEAD_MODEL_MAP = PURCHASED_MODEL_MAP

# Diagnostic: tracks raw model values that normalize to 'Unknown'.
# Populated by normalize_purchased_model (reason) and the main aggregation loop (context).
# Cleared implicitly on each fresh process start; never written to production payload.
_unk_mdl_reasons: dict = {}   # {stripped_raw: reason_str}
_unk_mdl_detail:  dict = {}   # {stripped_raw: {raw_repr, leads, rets, by_month, by_src}}

def normalize_lead_model(mdl):
    """Map raw lead ModelName to canonical model name using lookup table, with keyword fallback."""
    mdl = str(mdl or '').strip()
    if not mdl: return 'Unknown'
    mapped = LEAD_MODEL_MAP.get(mdl)
    if mapped is not None and str(mapped).strip().upper() not in ('', 'NA', 'N/A', 'NAN', 'NONE'):
        return mapped
    # Fallback: keyword matching (catches long variant names not in the lookup table)
    return normalize_purchased_model(mdl)

def normalize_purchased_model(pm):
    """Map raw purchasedModel string to canonical lead-model name."""
    pm = str(pm or '').strip()
    if not pm:
        _unk_mdl_reasons[''] = 'EMPTY'
        return 'Unknown'
    # Try exact match (handles both proper unicode and corrupted encodings via keyword fallback)
    if pm in PURCHASED_MODEL_MAP:
        val = str(PURCHASED_MODEL_MAP[pm] or '').strip()
        if val and val.upper() not in ('NA', 'N/A', 'NAN', 'NONE'):
            return val
    pu = pm.upper()
    # Keyword-based fallback for variants not in the explicit map
    if 'RR 310' in pu or 'RR310' in pu:                          return 'TVS Apache RR 310'
    if 'RTR 310' in pu or 'RTR310' in pu:                        return 'TVS Apache RTR 310'
    if '200' in pu and ('4V' in pu or 'RTR' in pu):              return 'TVS Apache RTR 200 4V'
    if '180' in pu and 'APACHE' in pu:                            return 'TVS Apache RTR 180'
    if '160' in pu and '4V' in pu and ('APACHE' in pu or 'RTR' in pu): return 'TVS Apache RTR 160 4V'
    if '160' in pu and ('APACHE' in pu or 'RTR' in pu):          return 'TVS Apache RTR 160'
    if 'RAIDER' in pu:                                            return 'TVS Raider'
    if 'JUPITER 125' in pu:                                       return 'TVS Jupiter 125'
    if ('JUPITER' in pu or 'JUPTR' in pu) and '125' not in pu:   return 'TVS Jupiter'
    if 'NTORQ' in pu and '150' not in pu:                        return 'TVS NTORQ 125'
    if 'NTORQ' in pu or 'NTRQ' in pu:
        _unk_mdl_reasons[pm] = 'NTORQ_150'
        return 'Unknown'
    if 'IQUBE' in pu or 'IQUE' in pu:                            return 'TVS iQube'
    if 'RONIN' in pu:                                             return 'TVS Ronin'
    if 'RADEON' in pu:                                            return 'TVS Radeon'
    if 'ORBITER' in pu:
        _unk_mdl_reasons[pm] = 'ORBITER'
        return 'Unknown'
    if 'SPORT' in pu and 'TVS' not in pu.replace('TVS SPORT',''):return 'TVS Sport'
    if 'SPORT' in pu:                                             return 'TVS Sport'
    if 'XL 100' in pu or 'XL100' in pu:                          return 'TVS XL100'
    if 'ZEST' in pu:                                              return 'TVS Scooty Zest'
    if 'STAR CITY' in pu or 'STARCITY' in pu or 'CITY+' in pu:  return 'TVS Star City Plus'
    _unk_mdl_reasons[pm] = 'CATCH_ALL'
    return 'Unknown'

LEAD_COL_MAP = {
    'opty_id':     'SorceLeadId',
    'Lead_Month':  'LeadMonth',
    'Date':        'CreateDate',
    'model':       'ModelName',
    'City':        'CityName',
    'State':       'State',
    'Dealer_Name': 'DealerName',
    'lead_type':   'LeadType',
    'Medium':      'Source',
    # optional — for DMS/CC retail-type split if columns exist
    'Retail By':        '_RetailBy',
    'DMS_Retail_Month': '_RetailMonth',
    'Status_Name':      'StatusName',
}

# ─── Helpers ──────────────────────────────────────────────────────────────────

# Explicit city aliases (title-cased key → canonical name).
# Only strong, confirmed aliases belong here — do not add guesses.
# Keys must be in Title Case (how str.title() would produce them).
_CITY_ALIAS = {
    'New Delhi':          'Delhi',
    'Bengaluru':          'Bangalore',
    'Bengalore':          'Bangalore',   # misspelling seen in source data
    'Prayagraj':          'Allahabad',
    'Thiruvananthapuram': 'Trivandrum',
}

# Separators that indicate a compound city string such as
# "Bengaluru / Bangalore" or "Begur, Bengaluru".
_COMPOUND_SEP_RE = re.compile(r'(\s*[/,|&]\s*)')

def normalize_city(raw):
    """Canonical city name: strip, collapse whitespace, title-case, apply alias.

    Also handles compound strings produced by source data entry:
      'Bengaluru / Bangalore'  -> 'Bangalore'   (both tokens same canon)
      'Begur, Bengaluru'       -> 'Begur, Bangalore'
      'New Delhi / Delhi'      -> 'Delhi'
    Separators in the original string are preserved when parts differ.
    """
    s = re.sub(r'\s+', ' ', str(raw or '').strip())
    if not s:
        return 'Unknown'
    # title() capitalises after any non-alpha char: handles spaces, hyphens,
    # parentheses, apostrophes — matches JS canonicalCity() exactly.
    s = s.title()
    # Fast path: exact alias match on the whole string
    if s in _CITY_ALIAS:
        return _CITY_ALIAS[s]
    # Compound string: split, canonicalize each token, then rejoin.
    # re.split with a capturing group keeps the separators in the result list.
    if _COMPOUND_SEP_RE.search(s):
        tokens     = _COMPOUND_SEP_RE.split(s)   # [part, sep, part, sep, ...]
        parts      = tokens[0::2]                 # city tokens
        seps       = tokens[1::2]                 # separators between them
        canon      = [_CITY_ALIAS.get(p.strip(), p.strip()) for p in parts]
        # Collapse when every token resolves to the same canonical city
        if len(set(canon)) == 1:
            return canon[0]
        # Otherwise reassemble, preserving original separators
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

def parse_ym(s):
    s = str(s or '').strip()
    if not s: return ''
    try:
        ts = pd.Timestamp(s)
        return f"{MONTH_NAMES[ts.month-1]}'{ts.strftime('%y')}"
    except Exception:
        return norm_month(s)

def parse_date(s):
    """Parse ISO date string YYYY-MM-DD → datetime.date object, or None on failure."""
    try:
        ts = pd.Timestamp(str(s or '').strip())
        if pd.isnull(ts):
            return None
        return ts.date()
    except Exception:
        return None

_AGE_BUCKET_LABELS = ['0-7 days', '8-14 days', '15-30 days', '30+ days']

def age_bucket(days):
    """Map ageing days → bucket index: 0=0-7, 1=8-14, 2=15-30, 3=30+."""
    if days <= 7:  return 0
    if days <= 14: return 1
    if days <= 30: return 2
    return 3

def lid_to_month(lid):
    """Decode month from 18-digit CRM ID YYMMDD prefix."""
    try:
        yy, mm = int(lid[0:2]), int(lid[2:4])
        if 1 <= mm <= 12:
            return f"{MONTH_NAMES[mm-1]}'{yy:02d}"
    except Exception:
        pass
    return ''

def to_id(v):
    if pd.isna(v): return ''
    try:    return str(int(float(v)))
    except: return str(v).strip()

def month_order(lm):
    """Convert 'Apr'25' → sortable YYMM integer. Unknown → 0."""
    try:
        s = norm_month(str(lm or '').strip())
        mn, yy = s.split("'")
        mi = MONTH_NAMES.index(mn) + 1
        return int(yy) * 100 + mi
    except Exception:
        return 0

ONLINE_START_ORDER      = month_order(ONLINE_START)
LEAD_MASTER_START_ORDER = month_order(LEAD_MASTER_START)

# ── CLI arguments & run tracking ──────────────────────────────────────────────
_ap = argparse.ArgumentParser(description='TVS Lead Disposition daily data push', add_help=False)
_ap.add_argument('--dry-run', action='store_true',
                 help='Fetch, process and validate but skip Firebase POST and GitHub Pages write')
DRY_RUN    = _ap.parse_known_args()[0].dry_run
_RUN_START = datetime.now(timezone.utc)
if DRY_RUN:
    print("*** DRY RUN MODE — production payload will NOT be updated ***", flush=True)

# ── Per-run telemetry ──────────────────────────────────────────────────────────
_as_calls_total  = 0               # total Apps Script proxy_get() calls this run
_as_calls_lock   = threading.Lock()
_fetch_perf      = {}              # {label: {duration_s, rows}} — populated by fetch workers
_fetch_perf_lock = threading.Lock()


def proxy_get(action, extra_params=None, timeout=120):
    global _as_calls_total
    with _as_calls_lock:
        _as_calls_total += 1
    params = {'action': action, 'secret': SECRET}
    if extra_params:
        params.update(extra_params)
    resp = requests.get(APPS_SCRIPT_URL, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

# ─── Sheet reader (paginated via Apps Script getSheetData) ────────────────────

# Only these columns are needed from each lead sheet — reduces payload ~70%
LEAD_COLS = 'opty_id,Lead_Month,Date,model,City,State,Dealer_Name,lead_type,Medium,Retail By,DMS_Retail_Month,Status_Name'

# Page size: 3000 rows/page keeps each Apps Script execution under ~30 s,
# narrowing the transient echo-URL window (302 bounce-back race condition).
# 6 per-page attempts with escalating ceilings; 5 backoff gaps.
# Short sleeps: 404s are transient redirect-URL expiry, not rate-limit cooldowns.
_LEAD_PAGE_SIZE = 3000
_LEAD_TIMEOUTS  = [30, 60, 90, 120, 180, 180]
_LEAD_BACKOFFS  = [5, 10, 15, 20, 30]       # len == len(_LEAD_TIMEOUTS) - 1

class _PageFetchFailed(Exception):
    """Raised when a page exhausts all retry attempts.
    Carries accumulated rows/headers so the sheet-level caller can resume
    from this page rather than restarting the whole fetch from page 0."""
    def __init__(self, page, accumulated_rows, headers, cause):
        super().__init__(str(cause))
        self.page             = page
        self.accumulated_rows = accumulated_rows
        self.headers          = headers

class _RetailPageFailed(Exception):
    """Raised by _fetch_retails_inner when a page exhausts all per-page retry attempts.
    The outer fetch_retails() catches this, resumes from e.page preserving
    e.accumulated_rows, then waits before the next sheet-level attempt.
    Every attempt calls proxy_get() fresh — stale echo URLs are never reused."""
    def __init__(self, page, accumulated_rows, headers, expected_total, cause):
        super().__init__(str(cause))
        self.page             = page
        self.accumulated_rows = accumulated_rows
        self.headers          = headers
        self.expected_total   = expected_total

class _RetailDatePageFailed(Exception):
    """Raised by _fetch_retail_date_inner when a page exhausts all per-page retry attempts."""
    def __init__(self, page, partial_map, populated_ct, blank_ct, invalid_ct, cause):
        super().__init__(str(cause))
        self.page         = page
        self.partial_map  = partial_map
        self.populated_ct = populated_ct
        self.blank_ct     = blank_ct
        self.invalid_ct   = invalid_ct

def fetch_sheet_via_proxy(file_id, label, tab_name=None,
                          _start_page=0, _prev_rows=None, _prev_headers=None):
    """Read any Google Sheet via Apps Script proxy. Returns raw DataFrame.

    _start_page / _prev_rows / _prev_headers let the sheet-level retry loop
    resume from a failed page instead of restarting from page 0.

    Every attempt issues a FRESH request to APPS_SCRIPT_URL via proxy_get()
    — stale echo URLs (302 bounce-back race condition) are never retried.
    """
    page      = _start_page
    all_rows  = list(_prev_rows) if _prev_rows else []
    headers   = _prev_headers
    extra = {'fileId': file_id, 'pageSize': _LEAD_PAGE_SIZE, 'cols': LEAD_COLS}
    if tab_name:
        extra['tabName'] = tab_name
    while True:
        extra['page'] = page
        data       = None
        last_exc   = None
        http_codes = []
        for attempt, _timeout in enumerate(_LEAD_TIMEOUTS):
            t0 = time.monotonic()
            try:
                data = proxy_get('getSheetData', extra, timeout=_timeout)
                elapsed = time.monotonic() - t0
                http_codes.append(200)
                if 'error' in data:
                    raise RuntimeError(f"Apps Script error: {data['error']}")
                rows_ct = len(data.get('rows', []))
                print(
                    f"[FETCH] Dataset:{label} Page:{page} "
                    f"Offset:{page*_LEAD_PAGE_SIZE:,} "
                    f"Attempt:{attempt+1}/{len(_LEAD_TIMEOUTS)} "
                    f"Rows:{rows_ct:,} Cumulative:{len(all_rows)+rows_ct:,} "
                    f"Expected:{data.get('total','?')} "
                    f"Elapsed:{elapsed:.1f}s HTTP:200 Fresh:YES", flush=True)
                last_exc = None
                break
            except Exception as e:
                elapsed = time.monotonic() - t0
                http_codes.append('ERR')
                last_exc = e
                if attempt < len(_LEAD_TIMEOUTS) - 1:
                    _sleep = _LEAD_BACKOFFS[attempt]
                    print(
                        f"[FETCH] Dataset:{label} Page:{page} "
                        f"Attempt:{attempt+1}/{len(_LEAD_TIMEOUTS)} "
                        f"FAILED ({type(e).__name__}: {e}) "
                        f"Elapsed:{elapsed:.1f}s Retry:{_sleep}s Fresh:YES", flush=True)
                    time.sleep(_sleep)
                else:
                    print(
                        f"[FAILED PAGE] Dataset:{label} Page:{page} "
                        f"Offset:{page*_LEAD_PAGE_SIZE:,} "
                        f"Attempts:{len(_LEAD_TIMEOUTS)} "
                        f"HTTP_statuses:{http_codes} Final_error:{e}", flush=True)
                    traceback.print_exc()
                    raise _PageFetchFailed(
                        page, all_rows, headers,
                        RuntimeError(f"getSheetData {label} page {page} failed after "
                                     f"{len(_LEAD_TIMEOUTS)} attempts: {e}"))
        if data is None or last_exc is not None:
            raise _PageFetchFailed(page, all_rows, headers,
                RuntimeError(f"getSheetData {label} page {page}: no data returned"))
        if headers is None:
            headers = data['headers']
        rows = data.get('rows', [])
        all_rows.extend(rows)
        if data.get('done', True):
            break
        page += 1
    return pd.DataFrame(all_rows, columns=headers)

# ─── Lead sheet processing ─────────────────────────────────────────────────────

def extract_rtype_map(raw_df):
    """Extract {opty_id → {rm, rtype}} from embedded retail columns if present.

    Normalizes 'Retail By' values: 'DMS' → 'DMS'; 'CC'/'Call Out'/any variant
    containing 'CALL' → 'Call Out'. Unknown non-empty values are preserved verbatim
    and reported so the dashboard's bump() will count them as unclassified — making
    the validation catch them rather than silently discarding them.
    """
    rmap = {}
    if 'DMS_Retail_Month' not in raw_df.columns:
        return rmap
    _unknown_rb: dict = {}
    for _, row in raw_df.iterrows():
        rm = str(row.get('DMS_Retail_Month', '') or '').strip()
        if not rm: continue
        lid = to_id(row.get('opty_id', ''))
        if not lid: continue
        _rb_raw = str(row.get('Retail By', '') or '').strip()
        _rb_u   = _rb_raw.upper()
        if 'DMS' in _rb_u:
            _rtype = 'DMS'
        elif 'CALL' in _rb_u or _rb_u == 'CC':
            # 'CC' = Call Center abbreviation used in lead sheets; treat as Call Out
            _rtype = 'Call Out'
        else:
            # '-', blank, 'N/A', or any unrecognized sentinel → no override.
            # Only DMS / Call Out variants provide positive classification evidence.
            # Storing '' ensures the override guard (if info['rtype']) skips it,
            # preserving the live retail sheet's correctly-classified Call Type.
            _rtype = ''
            if _rb_raw and _rb_raw not in ('-', '–', 'N/A', 'NA', 'na', 'n/a'):
                _unknown_rb[_rb_raw] = _unknown_rb.get(_rb_raw, 0) + 1
        rmap[lid] = {'rm': norm_month(rm), 'rtype': _rtype}
    if _unknown_rb:
        print(f"  NOTE: extract_rtype_map — unrecognized 'Retail By' values "
              f"(will be unclassified in aggregation): "
              f"{dict(sorted(_unknown_rb.items()))}", flush=True)
    return rmap

def standardize_leads(raw_df):
    """Rename to canonical columns; derive LeadMonth from Date if blank."""
    df = raw_df.rename(columns=LEAD_COL_MAP).copy()
    if 'State' in df.columns:
        df['State'] = df['State'].astype(str).str.strip().str.title()
    df['Zone']       = 'Unknown'
    df['BuyingDays'] = '0'
    if 'LeadMonth' in df.columns:
        df['LeadMonth'] = df['LeadMonth'].apply(parse_ym)
    if 'CreateDate' in df.columns:
        empty_lm = df.get('LeadMonth', pd.Series(dtype=str)).str.strip() == ''
        if empty_lm.any():
            df.loc[empty_lm, 'LeadMonth'] = df.loc[empty_lm, 'CreateDate'].apply(parse_ym)
    if 'SorceLeadId' in df.columns:
        still_empty = df.get('LeadMonth', pd.Series(dtype=str)).str.strip() == ''
        if still_empty.any():
            df.loc[still_empty, 'LeadMonth'] = df.loc[still_empty, 'SorceLeadId'].apply(
                lambda v: lid_to_month(to_id(v)))
    keep = ['SorceLeadId','LeadMonth','CreateDate','ModelName','Source','LeadType',
            'State','Zone','BuyingDays','CityName','DealerName','StatusName']
    return df[[c for c in keep if c in df.columns]].copy()

# ─── Status classification (Geo & Dealer tab) ─────────────────────────────────
# Exact canonical mapping: Status_Name (normalised) → 'O', 'B', or 'L'.
# Source of truth: TVS CRM Lead Master → Status_Name column.
# Do NOT use broad keyword matching — only explicit values map to B or L.
# Any Status_Name absent from this dict is logged and tagged 'U' (unclassified)
# so it neither inflates nor deflates O/B/L counts.
#
# To add a new status: append to the appropriate section below.
_STATUS_TAG_MAP = {
    # ── Booking ────────────────────────────────────────────────────────────────
    'Booked':                                            'B',
    'Booked (Callback Scheduled)':                       'B',

    # ── Open ───────────────────────────────────────────────────────────────────
    'Booking Request':                                   'O',
    'Booking Requested (Callback Scheduled)':            'O',
    'Booking Requested (Customer Not Responded)':        'O',
    'Booking Requested (Dealer Visit Scheduled)':        'O',
    'Booking Requested (Home Visit Scheduled)':          'O',
    'Call for verification':                             'O',
    'Call for verification (Callback Scheduled)':        'O',
    'Call for verification (Customer Not Responded)':    'O',
    'Call for verification (Dealer Visit Scheduled)':    'O',
    'Customer Not Responded':                            'O',
    'Enquiry Re Opened (Callback Scheduled)':            'O',
    'Enquiry Re Opened (Customer Not Responded)':        'O',
    'Enquiry Re Opened (Dealer Visit Scheduled)':        'O',
    'Enquiry Re Opened (Home Visit Scheduled)':          'O',
    'L1 Verified (Callback Scheduled)':                  'O',
    'L1 Verified (Customer Not Responded)':              'O',
    'L1 Verified (Dealer Visit Scheduled)':              'O',
    'Pending Retail':                                    'O',
    'Price Quote':                                       'O',
    'Price Quote (Callback Scheduled)':                  'O',
    'Price Quote (Customer Not Responded)':              'O',
    'Price Quote (Dealer Visit Scheduled)':              'O',
    'Price Quote (No Dealer Connect)':                   'O',
    'Test Ride Completed (Callback Scheduled)':          'O',
    'Test Ride Requested':                               'O',
    'Test Ride Requested (Callback Scheduled)':          'O',
    'Test Ride Requested (Customer Not Responded)':      'O',
    'Test Ride Requested (Dealer Visit Scheduled)':      'O',
    'Test Ride Requested (Home Visit Scheduled)':        'O',

    # ── Lost ───────────────────────────────────────────────────────────────────
    'Lost Not Contactable':                              'L',
    'Lost Not Purchased':                                'L',
    'Lost Purchased':                                    'L',
    'Lost To Co-Dealer':                                 'L',
}

# Track Status_Name values NOT in the map so they can be reviewed.
_STATUS_UNKNOWN_COUNTS: dict = {}

def _norm_sn(sn: str) -> str:
    """Normalise a Status_Name string: strip edges, collapse internal whitespace."""
    return ' '.join(sn.strip().split())

def classify_status(sn: str) -> str:
    """
    Map a raw Status_Name string to 'O' (open), 'B' (booking), 'L' (lost),
    or 'U' (unclassified / unknown).

    Only values present in _STATUS_TAG_MAP return B or L.
    Unknown values NEVER silently become B or L — they return 'U' and
    are counted in _STATUS_UNKNOWN_COUNTS for post-run review.
    """
    norm = _norm_sn(sn) if sn else ''
    tag  = _STATUS_TAG_MAP.get(norm)
    if tag is not None:
        return tag
    # Unknown — log it; return 'U' so counts stay honest.
    _STATUS_UNKNOWN_COUNTS[norm or '(blank)'] = _STATUS_UNKNOWN_COUNTS.get(norm or '(blank)', 0) + 1
    return 'U'

# ─── Retail master ─────────────────────────────────────────────────────────────
# Page size: 2000 rows/page for a narrow echo-URL window (same rationale as leads).
# 6 per-page attempts; 5 backoff gaps. Short sleeps: 404s are transient, not rate-limits.
_RETAIL_PAGE_SIZE = 2000
_RETAIL_TIMEOUTS  = [30, 60, 90, 120, 180, 180]
_RETAIL_BACKOFFS  = [5, 10, 15, 20, 30]       # len == len(_RETAIL_TIMEOUTS) - 1

def _fetch_retails_inner(start_page, prev_rows, prev_headers, prev_expected_total):
    """Paginate through the retail master from start_page.
    Raises _RetailPageFailed with accumulated state on page-level retry exhaustion.
    Every attempt issues a FRESH request to APPS_SCRIPT_URL via proxy_get() —
    stale echo URLs (302 bounce-back race condition) are never retried."""
    page           = start_page
    all_rows       = list(prev_rows)  if prev_rows       else []
    headers        = prev_headers
    expected_total = prev_expected_total
    done_received  = False
    pages_fetched  = 0
    while True:
        last_exc   = None
        http_codes = []
        for attempt, _timeout in enumerate(_RETAIL_TIMEOUTS):
            t0 = time.monotonic()
            try:
                data = proxy_get('getCurrentRetails',
                                 {'page': page, 'pageSize': _RETAIL_PAGE_SIZE},
                                 timeout=_timeout)
                elapsed = time.monotonic() - t0
                http_codes.append(200)
                if 'error' in data:
                    raise RuntimeError(f"Apps Script error: {data['error']}")
                rows_ct = len(data.get('rows', []))
                print(
                    f"[FETCH] Dataset:Retail Page:{page} "
                    f"Offset:{page*_RETAIL_PAGE_SIZE:,} "
                    f"Attempt:{attempt+1}/{len(_RETAIL_TIMEOUTS)} "
                    f"Rows:{rows_ct:,} Cumulative:{len(all_rows)+rows_ct:,} "
                    f"Expected:{data.get('total','?')} "
                    f"Elapsed:{elapsed:.1f}s HTTP:200 Fresh:YES", flush=True)
                last_exc = None
                break
            except Exception as e:
                elapsed = time.monotonic() - t0
                http_codes.append('ERR')
                last_exc = e
                if attempt < len(_RETAIL_TIMEOUTS) - 1:
                    _sleep = _RETAIL_BACKOFFS[attempt]
                    print(
                        f"[FETCH] Dataset:Retail Page:{page} "
                        f"Attempt:{attempt+1}/{len(_RETAIL_TIMEOUTS)} "
                        f"FAILED ({type(e).__name__}: {e}) "
                        f"Elapsed:{elapsed:.1f}s Retry:{_sleep}s Fresh:YES", flush=True)
                    time.sleep(_sleep)
                else:
                    print(
                        f"[FAILED PAGE] Dataset:Retail Page:{page} "
                        f"Offset:{page*_RETAIL_PAGE_SIZE:,} "
                        f"Attempts:{len(_RETAIL_TIMEOUTS)} "
                        f"HTTP_statuses:{http_codes} Final_error:{e}", flush=True)
        if last_exc is not None:
            raise _RetailPageFailed(page, all_rows, headers, expected_total, last_exc)
        if headers is None:
            headers = data['headers']
        if expected_total is None and isinstance(data.get('total'), int):
            expected_total = data['total']
        rows = data.get('rows', [])
        all_rows.extend(rows)
        pages_fetched += 1
        if data.get('done', True):
            done_received = True
            break
        page += 1
    return all_rows, headers, expected_total, done_received, pages_fetched


def fetch_retails():
    """Fetch TVS retail master with page-resume on sustained page failures.

    Architecture: every retry — at both the page level and the sheet level —
    issues a FRESH request to APPS_SCRIPT_URL via proxy_get().  Stale echo URLs
    are never reused.  Page-resume preserves all rows fetched before the failed
    page so no rows are lost or duplicated across a resume.

    'done: true' is the authoritative end-of-pagination signal from Apps Script.
    The 'total' field reflects raw sheet row count (getLastRow) and may include
    blank/filtered rows the paginator skips — fetched != total is normal when
    'done' was received.

    Never returns partial data — raises RuntimeError on unrecoverable failure so
    the caller's _fail_exit() guard prevents a partial payload reaching Firebase.
    """
    t_start = time.monotonic()
    print("Fetching retail master via Apps Script…", flush=True)
    resume = dict(start_page=0, prev_rows=[], prev_headers=None, prev_expected_total=None)
    for sheet_attempt in range(3):
        try:
            all_rows, headers, expected_total, done_received, pages_fetched = \
                _fetch_retails_inner(**resume)
            elapsed = time.monotonic() - t_start
            status  = 'OK' if done_received else 'WARN:no-done-signal'
            print(
                f"[COMPLETE] Dataset:Retail "
                f"Expected:{expected_total if expected_total is not None else '?'} "
                f"Raw_rows:{len(all_rows):,} Pages:{pages_fetched} "
                f"Sheet_attempts:{sheet_attempt+1} Duration:{elapsed:.1f}s Status:{status}",
                flush=True)
            if not done_received:
                raise RuntimeError(
                    f"Retail pagination ended without 'done' signal after {pages_fetched} pages "
                    f"({len(all_rows):,} rows fetched). Possible truncation — aborting.")
            if expected_total is not None and len(all_rows) != expected_total:
                print(f"  NOTE: Apps Script total={expected_total:,}, fetched={len(all_rows):,} — "
                      f"gap likely due to blank/non-TVS rows in sheet (normal). "
                      f"'done' received; proceeding.", flush=True)
            df = pd.DataFrame(all_rows, columns=headers)
            print(f"  Retail master: {len(df):,} TVS rows  (pages={pages_fetched})", flush=True)
            return df
        except _RetailPageFailed as e:
            resume = dict(
                start_page=e.page,
                prev_rows=e.accumulated_rows,
                prev_headers=e.headers,
                prev_expected_total=e.expected_total)
            if sheet_attempt < 2:
                print(
                    f"  WARNING: Retail page {e.page} exhausted all {len(_RETAIL_TIMEOUTS)} "
                    f"per-page retries (sheet attempt {sheet_attempt+1}/3). "
                    f"Resuming from page {e.page} in 30s "
                    f"({len(e.accumulated_rows):,} rows preserved, NOT discarded)…",
                    flush=True)
                time.sleep(30)
            else:
                raise RuntimeError(
                    f"Retail fetch: page {e.page} exhausted all retries across "
                    f"3 sheet-level attempts: {e}") from e


# 2000 rows/page keeps echo-URL windows narrow (same rationale as retail).
_RETAIL_DATE_PAGE_SIZE = 2000
_RETAIL_DATE_TIMEOUTS  = [30, 60, 90, 120, 180, 180]
_RETAIL_DATE_BACKOFFS  = [5, 10, 15, 20, 30]  # transient 404s resolve in seconds

def _fetch_retail_date_inner(start_page, prev_map, prev_populated, prev_blank, prev_invalid):
    """Paginate through Retail_Date from start_page.
    Raises _RetailDatePageFailed with accumulated map on page-level retry exhaustion.
    Every attempt issues a FRESH request to APPS_SCRIPT_URL via proxy_get()."""
    page         = start_page
    rd_map       = dict(prev_map)  if prev_map       else {}
    populated_ct = prev_populated  if prev_populated  else 0
    blank_ct     = prev_blank      if prev_blank      else 0
    invalid_ct   = prev_invalid    if prev_invalid     else 0
    done_received = False
    pages_fetched = 0
    while True:
        last_exc   = None
        http_codes = []
        for attempt, _timeout in enumerate(_RETAIL_DATE_TIMEOUTS):
            t0 = time.monotonic()
            try:
                data = proxy_get('getSheetData', {
                    'fileId':   RETAILS_FILE_ID,
                    'tabName':  RETAILS_TAB,
                    'cols':     'sourceLeadId,Retail_Date',
                    'pageSize': _RETAIL_DATE_PAGE_SIZE,
                    'page':     page,
                }, timeout=_timeout)
                elapsed = time.monotonic() - t0
                http_codes.append(200)
                last_exc = None
                rows_ct  = len(data.get('rows', []))
                print(
                    f"[FETCH] Dataset:Retail_Date Page:{page} "
                    f"Offset:{page*_RETAIL_DATE_PAGE_SIZE:,} "
                    f"Attempt:{attempt+1}/{len(_RETAIL_DATE_TIMEOUTS)} "
                    f"Rows:{rows_ct:,} Map:{len(rd_map):,} "
                    f"Elapsed:{elapsed:.1f}s HTTP:200 Fresh:YES", flush=True)
                break
            except Exception as e:
                elapsed = time.monotonic() - t0
                http_codes.append('ERR')
                last_exc = e
                if attempt < len(_RETAIL_DATE_TIMEOUTS) - 1:
                    _sleep = _RETAIL_DATE_BACKOFFS[attempt]
                    print(
                        f"[FETCH] Dataset:Retail_Date Page:{page} "
                        f"Attempt:{attempt+1}/{len(_RETAIL_DATE_TIMEOUTS)} "
                        f"FAILED ({type(e).__name__}: {e}) "
                        f"Elapsed:{elapsed:.1f}s Retry:{_sleep}s Fresh:YES", flush=True)
                    time.sleep(_sleep)
                else:
                    print(
                        f"[FAILED PAGE] Dataset:Retail_Date Page:{page} "
                        f"Offset:{page*_RETAIL_DATE_PAGE_SIZE:,} "
                        f"Attempts:{len(_RETAIL_DATE_TIMEOUTS)} "
                        f"HTTP_statuses:{http_codes} Final_error:{e}", flush=True)
        if last_exc is not None:
            raise _RetailDatePageFailed(
                page, rd_map, populated_ct, blank_ct, invalid_ct, last_exc)
        if 'error' in data:
            raise RuntimeError(f"fetch_retail_date_map: Apps Script error: {data['error']}")
        headers = data.get('headers', [])
        rows    = data.get('rows',    [])
        done    = data.get('done',    True)
        lid_col = next((i for i, h in enumerate(headers)
                        if h.lower() in ('sourceleadid', 'enquiryid', 'opty_id')), 0)
        rd_col  = next((i for i, h in enumerate(headers)
                        if h.lower() == 'retail_date'), None)
        if rd_col is None:
            raise RuntimeError(
                f"fetch_retail_date_map: 'Retail_Date' column not found in headers {headers}")
        for row in rows:
            lid = to_id(row[lid_col]) if lid_col < len(row) else ''
            if not lid:
                continue
            rd_raw = row[rd_col] if rd_col < len(row) else None
            if not rd_raw or str(rd_raw).strip() == '':
                blank_ct += 1
                continue
            rd = parse_date(rd_raw)
            if rd is None:
                invalid_ct += 1
                continue
            rd_map[lid] = rd
            populated_ct += 1
        pages_fetched += 1
        print(
            f"  Retail_Date page {page}: +{len(rows):,} rows "
            f"(map={len(rd_map):,}  blank={blank_ct}  invalid={invalid_ct})", flush=True)
        if done:
            done_received = True
            break
        page += 1
    return rd_map, populated_ct, blank_ct, invalid_ct, done_received, pages_fetched


def fetch_retail_date_map():
    """Fetch {lid: datetime.date} from OEM CPS Retail Raw via getSheetData.

    Retail_Date is a separate column not returned by getCurrentRetails.
    Uses page-resume: if a page fails all per-page retries, resumes from that
    page after 30s, preserving accumulated map entries.
    Every attempt issues a FRESH request to APPS_SCRIPT_URL via proxy_get() —
    stale echo URLs are never retried.
    Returns an empty dict on total failure so the pipeline continues without
    ageing data rather than aborting the production run.
    """
    t_start = time.monotonic()
    print("Fetching Retail_Date map from OEM CPS Retail Raw…", flush=True)
    resume = dict(start_page=0, prev_map={}, prev_populated=0, prev_blank=0, prev_invalid=0)
    for sheet_attempt in range(3):
        try:
            rd_map, populated_ct, blank_ct, invalid_ct, done_received, pages_fetched = \
                _fetch_retail_date_inner(**resume)
            elapsed = time.monotonic() - t_start
            if not done_received:
                print(
                    f"  WARNING: Retail_Date pagination ended without 'done' signal "
                    f"after {pages_fetched} pages. Retail Ageing may be incomplete.",
                    flush=True)
            total_fetched = populated_ct + blank_ct + invalid_ct
            pct = f"{100*populated_ct/total_fetched:.1f}%" if total_fetched else "0%"
            print(
                f"[COMPLETE] Dataset:Retail_Date "
                f"Map_entries:{len(rd_map):,} "
                f"populated={populated_ct:,}({pct}) "
                f"blank={blank_ct:,} invalid={invalid_ct:,} "
                f"Pages:{pages_fetched} Duration:{elapsed:.1f}s",
                flush=True)
            return rd_map
        except _RetailDatePageFailed as e:
            resume = dict(
                start_page=e.page,
                prev_map=e.partial_map,
                prev_populated=e.populated_ct,
                prev_blank=e.blank_ct,
                prev_invalid=e.invalid_ct)
            if sheet_attempt < 2:
                print(
                    f"  WARNING: Retail_Date page {e.page} exhausted all per-page retries "
                    f"(sheet attempt {sheet_attempt+1}/3). "
                    f"Resuming from page {e.page} in 30s "
                    f"({len(e.partial_map):,} map entries preserved, NOT discarded)…",
                    flush=True)
                time.sleep(30)
            else:
                print(
                    f"  ERROR: Retail_Date fetch failed after 3 sheet-level attempts. "
                    f"Continuing without Retail_Date — Retail Ageing will be empty.",
                    flush=True)
                return {}
        except RuntimeError as e:
            print(
                f"  ERROR: Retail_Date fetch error: {e}. "
                f"Continuing without Retail_Date — Retail Ageing will be empty.",
                flush=True)
            return {}


def _validate_retail_fetch(df, prev_metrics=None):
    """Print a sanity report for the freshly fetched retail DataFrame.

    Fails via _fail_exit() if the row count is suspiciously low.
    Floor is dynamic when a previous-run baseline exists:
      floor = max(RETAIL_ABS_FLOOR, int(prev_count * RETAIL_DROP_THRESHOLD))
    This prevents false rejections as the business grows while still catching
    truncated fetches.  prev_metrics should be the _prev_metrics dict from the
    global pipeline state (pass explicitly so the function is unit-testable).
    """
    print("\n-- Retail fetch validation --------------------------------------------------", flush=True)
    n = len(df)
    print(f"  Total rows: {n:,}", flush=True)

    RETAIL_ABS_FLOOR       = 50_000   # absolute minimum regardless of history
    RETAIL_DROP_THRESHOLD  = 0.80     # must be ≥ 80 % of previous run

    prev = None
    if prev_metrics:
        _pm = prev_metrics.get('retail_raw')
        if isinstance(_pm, dict):
            prev = _pm.get('rows')
        elif isinstance(_pm, int):
            prev = _pm

    if prev is not None and prev > 0:
        floor = max(RETAIL_ABS_FLOOR, int(prev * RETAIL_DROP_THRESHOLD))
        pct   = f'{n / prev:.1%}'
        print(f"  Previous run count: {prev:,}  Current: {n:,}  Ratio: {pct}", flush=True)
        if n < floor:
            _fail_exit(
                'Retail fetch size validation',
                f'Retail fetch produced only {n:,} rows — less than '
                f'{RETAIL_DROP_THRESHOLD:.0%} of previous run ({prev:,}). '
                f'Minimum expected: {floor:,}. '
                f'This almost certainly indicates an incomplete fetch or sheet truncation. '
                f'If this is a genuine data reduction, delete source_metrics.json to reset the baseline.'
            )
        print(f"  Retail row count OK ({pct} of previous run).", flush=True)
    else:
        # No baseline yet (first run or reset). Use absolute floor only.
        if n < RETAIL_ABS_FLOOR:
            _fail_exit(
                'Retail fetch size validation',
                f'Retail fetch produced only {n:,} rows — expected ≥ {RETAIL_ABS_FLOOR:,}. '
                f'This is almost certainly an incomplete fetch. '
                f'(No historical baseline available; using absolute floor.)'
            )
        print(f"  Retail row count OK ({n:,} rows; no previous baseline).", flush=True)
    if 'performanceMonth' in df.columns:
        # The live retail sheet stores performanceMonth as raw date strings (YYYY-MM-DD),
        # not as month labels like "Jul'26". Count rows using parse_ym() so we match
        # the same logic used in build_retail_map.
        _pm_parsed = df['performanceMonth'].fillna('').apply(lambda v: parse_ym(str(v)))
        _pm_dist   = _pm_parsed.value_counts().sort_index()
        print("  performanceMonth distribution (parsed to month keys):", flush=True)
        for pm, cnt in _pm_dist.items():
            print(f"    {pm!r:20s}: {cnt:,}", flush=True)
        jul26 = int((_pm_parsed == "Jul'26").sum())
        aug26 = int((_pm_parsed == "Aug'26").sum())
        print(f"  Jul'26 retail rows (rm=Jul'26): {jul26:,}", flush=True)
        print(f"  Aug'26 retail rows (rm=Aug'26): {aug26:,}", flush=True)
    else:
        print("  WARNING: performanceMonth column not present in retail sheet!", flush=True)
    print("── End retail fetch validation ──────────────────────", flush=True)

def build_retail_map(retail_df, rd_map=None):
    """Build {sourceLeadId -> {rm, rtype, pm, rd}} for LIVE GSheet records.
    rm    : from 'performanceMonth' column (not Retail_Attribution_Date).
    rtype : from 'Call Type' column — 'DMS' or 'Call Out' (case-insensitive, trimmed).
            Unexpected values are logged and collected in the returned warnings list.
    rd    : datetime.date from rd_map (Retail_Date column), or None if unavailable.
            Used exclusively for Retail Ageing — never replaces rm or any existing field.
    Returns (rmap, unexpected_call_types) where unexpected_call_types is a list of
    {'lid': ..., 'call_type': ...} dicts for the validation report.
    """
    rmap = {}
    unexpected_ct = []
    has_ct = 'Call Type' in retail_df.columns
    has_pm = 'performanceMonth' in retail_df.columns
    for _, row in retail_df.iterrows():
        lid = to_id(row.get('sourceLeadId', ''))
        if not lid: continue
        rm = parse_ym(row.get('performanceMonth', '') if has_pm else row.get('Retail_Attribution_Date', ''))
        pm = normalize_purchased_model(row.get('purchasedModel', ''))
        if has_ct:
            _ct = str(row.get('Call Type', '') or '').strip()
            _ct_lower = _ct.lower()
            if _ct_lower == 'dms':
                rtype = 'DMS'
            elif _ct_lower in ('call out', 'callout'):
                rtype = 'Call Out'
            else:
                print(f"  WARNING: Unexpected Call Type {_ct!r} for lid={lid} — defaulting to DMS", flush=True)
                unexpected_ct.append({'lid': lid, 'call_type': _ct})
                rtype = 'DMS'
        else:
            rtype = 'DMS'
        rmap[lid] = {'rm': rm, 'rtype': rtype, 'pm': pm,
                     'rd': rd_map.get(lid) if rd_map else None}
    return rmap, unexpected_ct

def make_synthetic_leads(retail_df, matched_lids):
    """Create lead rows for retailed IDs absent from all lead sheets.
    Skips retail months before LEAD_MASTER_START — no CRM lead data exists for those months.
    """
    rows = []
    for _, row in retail_df.iterrows():
        lid = to_id(row.get('sourceLeadId', ''))
        if not lid or lid in matched_lids: continue
        rm    = parse_ym(row.get('Retail_Attribution_Date', ''))
        lm    = rm or lid_to_month(lid)
        if month_order(lm) < LEAD_MASTER_START_ORDER: continue   # no leads exist before Apr'25
        model = str(row.get('purchasedModel', '') or '').strip() or 'Unknown'
        rows.append({
            'SorceLeadId': lid, 'LeadMonth': lm, 'ModelName': model,
            'Source': 'Unknown', 'LeadType': 'Unknown', 'State': 'Unknown',
            'Zone': 'Unknown', 'BuyingDays': '0', 'CityName': 'Unknown', 'DealerName': 'Unknown',
        })
    cols = ['SorceLeadId','LeadMonth','ModelName','Source','LeadType',
            'State','Zone','BuyingDays','CityName','DealerName']
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=cols)

# ─── Historical file specs ────────────────────────────────────────────────────

# Historical Lead Master — bootstrap/DR only.
# These files are NEVER read during normal production runs. hist_cache.json.gz is the
# permanent production source. Only needed if the cache must be rebuilt from scratch.
# Each file contains BOTH lead data AND retail reconciliation (DMS/Call Out + Retail Month
# columns); retail_map is built from these same files — no separate retail master needed.
# Use rebuild_hist_cache.py (not this bootstrap path) to regenerate the cache from scratch.
HIST_LEAD_FILES = [
    {'path': 'Leads Data Master_Leads_FY_25_26 Part 1.xlsb',             'engine': 'pyxlsb',  'sheet': 'Raw Data'},
    {'path': 'Leads Data Master_Leads_FY_25_26 Part 2.xlsx',              'engine': 'openpyxl','sheet': 'Sheet1'},
    {'path': 'Leads Data Master_Leads_FY_25_26 & FY_26_27 Part 3.xlsx',  'engine': 'openpyxl','sheet': 'Sheet1'},
]

# Historical Retail Master — no longer a separate source.
# Retail data (DMS/Call Out + Retail Month) is now embedded in HIST_LEAD_FILES above.
# Kept empty to avoid loading the old retail master during any accidental bootstrap run.
HIST_RETAIL_FILES = []

# Column rename map for historical lead Excel files
_FILE_LEAD_RENAME = {
    'opty_id':            'SorceLeadId',   # primary key
    'Lead Month':         'LeadMonth',
    'Lead Created Date':  'CreateDate',
    'Lead Type':          'LeadType',
    'Dealer_Name':        'DealerName',
    'City':               'CityName',
    # 'Source' and 'Zone' and 'ModelName' and 'State' are already correct
}

def _excel_serial_to_month(val):
    """Convert pyxlsb integer date serial → 'Mon'YY string (e.g. 45717 → 'Mar'25)."""
    try:
        n = int(float(val))
        if 30000 < n < 100000:   # plausible Excel date range (1982–2173)
            dt = datetime(1899, 12, 30) + timedelta(days=n)
            return f"{MONTH_NAMES[dt.month-1]}'{dt.strftime('%y')}"
    except Exception:
        pass
    return parse_ym(val)

def standardize_file_leads(df):
    """Rename Excel columns to canonical pipeline names; derive LeadMonth when blank."""
    df = df.rename(columns={k: v for k, v in _FILE_LEAD_RENAME.items() if k in df.columns}).copy()
    if 'SorceLeadId' not in df.columns:
        raise KeyError('SorceLeadId column not found after rename — check _FILE_LEAD_RENAME')
    df['SorceLeadId'] = df['SorceLeadId'].apply(to_id)
    df['LeadMonth']   = df.get('LeadMonth', pd.Series(dtype=str)).apply(
                            lambda v: norm_month(str(v or '').strip()))
    if 'State' in df.columns:
        df['State'] = df['State'].astype(str).str.strip().str.title()
    if 'ModelName' in df.columns:
        df['ModelName'] = df['ModelName'].apply(normalize_lead_model)
    if 'CreateDate' in df.columns:
        empty_lm = df.get('LeadMonth', pd.Series(dtype=str)).str.strip() == ''
        if empty_lm.any():
            df.loc[empty_lm, 'LeadMonth'] = df.loc[empty_lm, 'CreateDate'].apply(parse_ym)
    if 'BuyingDays' not in df.columns: df['BuyingDays'] = '0'
    if 'Zone'       not in df.columns: df['Zone']       = 'Unknown'
    if 'Source'     not in df.columns: df['Source']     = 'Unknown'
    keep = ['SorceLeadId','LeadMonth','ModelName','Source','LeadType',
            'State','Zone','BuyingDays','CityName','DealerName']
    out = df[[c for c in keep if c in df.columns]].copy()
    out = out[out['SorceLeadId'].astype(str).str.len() > 0]
    out = out[out['LeadMonth'].astype(str).str.len()   > 0]
    return out

def load_hist_leads():
    """Read all historical lead Excel files; return combined standardized DataFrame."""
    dfs = []
    for spec in HIST_LEAD_FILES:
        path = os.path.join(HIST_DIR, spec['path'])
        if not os.path.exists(path):
            print(f"  SKIP (not found): {spec['path']}", flush=True)
            continue
        try:
            print(f"  Reading {spec['path']}…", flush=True)
            df = pd.read_excel(path, sheet_name=spec['sheet'], engine=spec['engine'])
            df = standardize_file_leads(df)
            months = sorted(df['LeadMonth'].dropna().unique())
            print(f"    {len(df):,} leads, months: {months[:4]}…{months[-2:] if len(months)>4 else ''}", flush=True)
            dfs.append(df)
        except Exception as e:
            print(f"  WARNING: Could not load {spec['path']}: {e}", flush=True)
    if not dfs:
        return pd.DataFrame()
    combined = pd.concat(dfs, ignore_index=True)
    # Deduplicate within historical files: keep last (later files are more authoritative)
    before = len(combined)
    combined = combined.drop_duplicates(subset=['SorceLeadId'], keep='last')
    if before > len(combined):
        print(f"  Deduplicated {before - len(combined):,} duplicates within hist lead files", flush=True)
    return combined

def load_hist_retail_map():
    """Build {sourceLeadId -> {rm, rtype, pm}} from historical retail Excel files.
    Rows with null enquiryId are excluded (bulk-import artifacts; match zero leads).
    Retail type is read directly from the DMS/Call Out column in the workbook.
    """
    rmap = {}
    for spec in HIST_RETAIL_FILES:
        path = os.path.join(HIST_DIR, spec['path'])
        if not os.path.exists(path):
            print(f"  SKIP (not found): {spec['path']}", flush=True)
            continue
        try:
            print(f"  Reading {spec['path']}…", flush=True)
            df = pd.read_excel(path, sheet_name=spec.get('sheet', 0), engine=spec['engine'])

            # Exclude rows with null enquiryId — these are bulk-import artifacts
            # (concentrated in Jan-Mar'25, 0 lead matches) excluded from the
            # business-approved retail count (388,094).
            if 'enquiryId' in df.columns:
                raw_count = len(df)
                df = df[df['enquiryId'].notna()].copy()
                excluded = raw_count - len(df)
                if excluded:
                    print(f"    Excluded {excluded:,} rows with null enquiryId (raw={raw_count:,})", flush=True)

            # --- primary key: accept either casing ---
            id_col = next((c for c in df.columns if c.lower() in ('sorceLeadId', 'sourceleadid')), None)
            if id_col is None:
                print(f"  WARNING: no sourceLeadId column in {spec['path']}; skipping", flush=True)
                continue
            df['xlid'] = df[id_col].apply(to_id)

            # --- retail month ---
            if 'Retail Month' in df.columns:
                df['xrm'] = df['Retail Month'].astype(str).str.strip().apply(norm_month)
            elif 'Retail_Attribution_Date' in df.columns:
                df['xrm'] = df['Retail_Attribution_Date'].apply(_excel_serial_to_month)
            else:
                print(f"  WARNING: no retail-month column in {spec['path']}; skipping", flush=True)
                continue

            # --- purchased model ---
            pm_col = next((c for c in df.columns if c.lower() in ('purchasedmodel','purchased model 2','purchased model')), None)
            df['xpm'] = df[pm_col].apply(normalize_purchased_model) if pm_col else 'Unknown'

            # --- retail type ---
            rt_col = next((c for c in df.columns if 'dms' in c.lower() or 'call' in c.lower()), None)
            df['xrt'] = df[rt_col].fillna('').astype(str).str.strip() if rt_col else ''

            valid   = df[df['xlid'].str.len() > 0].copy()
            deduped = valid.drop_duplicates(subset=['xlid'], keep='first')
            added   = 0
            for r in deduped[['xlid','xrm','xpm','xrt']].to_dict('records'):
                lid = r['xlid']
                if lid not in rmap:
                    rmap[lid] = {'rm': r['xrm'], 'rtype': r['xrt'], 'pm': r['xpm']}
                    added += 1
            print(f"    {added:,} entries added  (file has {len(valid):,} valid rows)", flush=True)
        except Exception as e:
            print(f"  WARNING: Could not load {spec['path']}: {e}", flush=True)
    return rmap

# ─── Core aggregation ─────────────────────────────────────────────────────────

def build_payload(all_leads, retail_map):
    dl_col = 'DealerName' if 'DealerName' in all_leads.columns else None

    lm_idx,  src_idx, lt_idx, mdl_idx, st_idx, zone_idx, city_idx = {},{},{},{},{},{},{}
    lm_arr,  src_arr, lt_arr, mdl_arr, st_arr, zone_arr, city_arr  = [],[],[],[],[],[],[]
    dl_idx,  dl_arr  = {}, []
    city_to_state = {}

    def ix(d, arr, v):
        if v not in d:
            d[v] = len(arr); arr.append(v)
        return d[v]

    monthly, sm, ltm, mm, stm, zm, bdm, cm = {},{},{},{},{},{},{},{}
    u_monthly, u_sm, u_ltm, u_mm, u_stm, u_zm, u_bdm = {},{},{},{},{},{},{}
    mxst, u_mxst = {}, {}
    mlt,  u_mlt  = {}, {}   # model × lead-type × month
    stlt, u_stlt = {}, {}   # state × lead-type × month
    stcm, u_stcm = {}, {}   # state × city × month
    univ, u_univ = {}, {}   # mdl × src × st × lt × month (universal — covers any non-city/dealer combo)
    stdm, u_stdm = {}, {}   # state × dealer × month
    mxdl, u_mxdl = {}, {}  # model × dealer × month
    ltdl, u_ltdl = {}, {}  # lead-type × dealer × month
    disp, u_disp = {}, {}   # enquired_model × purchased_model × month (retails only)
    cdm, csm, cdsm = {},{},{}
    dl_sn = {}  # city × dealer × lead-month → [L_open, L_booking, L_lost]
    cxm, u_cxm = {}, {}           # city × model × month
    cxsm, u_cxsm = {}, {}         # city × src × model × month (source-filterable)
    u_cm, u_csm = {}, {}          # city × month (retail-month attribution)
    u_cdm, u_cdsm = {}, {}        # city × dealer × month (retail-month attribution)
    ram = {}                       # Retail Ageing: model × src × age_bucket × lead_month → [rets, dms, co]
    _ram_total = _ram_valid = _ram_neg = _ram_no_rd = _ram_no_cd = 0

    def bump(d, k, is_ret, rtype=''):
        if k not in d: d[k] = [0,0,0,0]
        d[k][0] += 1
        if is_ret:
            d[k][1] += 1
            rt_u = rtype.upper()
            if 'DMS' in rt_u:   d[k][2] += 1
            elif 'CALL' in rt_u: d[k][3] += 1

    def ubump(d, key_lead, key_ret, is_ret, rtype=''):
        # Lead always counted in create-month row; retail in retail-month row.
        if key_lead not in d: d[key_lead] = [0,0,0,0]
        d[key_lead][0] += 1
        if is_ret:
            if key_ret not in d: d[key_ret] = [0,0,0,0]
            d[key_ret][1] += 1
            rt_u = rtype.upper()
            if 'DMS' in rt_u:   d[key_ret][2] += 1
            elif 'CALL' in rt_u: d[key_ret][3] += 1

    total = len(all_leads)
    print(f"Aggregating {total:,} leads…", flush=True)

    import gc as _gc

    # Extract columns to numpy arrays — avoids pandas Series overhead per row
    _c = all_leads.columns.tolist()
    def _col(name, default=''):
        return all_leads[name].fillna(default).astype(str).values if name in _c else [default] * total

    _lids   = all_leads['SorceLeadId'].fillna('').astype(str).values
    _lms    = _col('LeadMonth')
    _srcs   = _col('Source')
    _lts    = _col('LeadType')
    _mdls   = _col('ModelName')
    _sts    = _col('State')
    _zones  = _col('Zone', '0')
    _bds    = _col('BuyingDays', '0')
    _cities = _col('CityName')
    _cds    = _col('CreateDate')   # Lead CreateDate — used for Retail Ageing only
    _dls    = all_leads[dl_col].fillna('').astype(str).values if dl_col and dl_col in _c else None
    _sns    = _col('StatusName')

    del all_leads, _c  # free ~500 MB DataFrame now that we have arrays
    _gc.collect()

    for i in range(total):
        if i % 100000 == 0 and i > 0:
            print(f"  {i:,}/{total:,} ({100*i//total}%)", flush=True)

        lid  = to_id(_lids[i])
        lm   = _lms[i].strip()
        src  = _srcs[i].strip() or 'Unknown'
        if src in ('Non-MS', 'Non MS', 'Non- MS'): src = 'Non CPS'
        if src.lower() == 'whatsapp': src = 'WhatsApp'
        lt   = _lts[i].strip() or 'Unknown'
        mdl  = normalize_lead_model(_mdls[i])
        st   = _sts[i].strip().title() or 'Unknown'
        zone = _zones[i].strip() or 'Unknown'
        bd   = _bds[i].strip() or '0'
        city = normalize_city(_cities[i])

        if not lm or not lid: continue

        is_ret = lid in retail_map

        # Diagnostic: capture context for every lead whose model normalizes to Unknown.
        # Stored in _unk_mdl_detail keyed by stripped raw value; reason comes from
        # _unk_mdl_reasons (populated by normalize_purchased_model) or inferred for empty.
        if mdl == 'Unknown':
            _raw_v      = str(_mdls[i])
            _stripped_v = _raw_v.strip()
            _rec = _unk_mdl_detail.setdefault(_stripped_v, {
                'raw_repr': repr(_raw_v), 'leads': 0, 'rets': 0, 'by_month': {}, 'by_src': {}
            })
            _rec['leads'] += 1
            _rec['by_month'].setdefault(lm, [0, 0])[0] += 1
            _rec['by_src'].setdefault(src, [0, 0])[0] += 1
            if is_ret:
                _rec['rets'] += 1
                _rec['by_month'][lm][1] += 1
                _rec['by_src'][src][1] += 1

        li   = ix(lm_idx,   lm_arr,   lm)
        si   = ix(src_idx,  src_arr,  src)
        tti  = ix(lt_idx,   lt_arr,   lt)
        mi   = ix(mdl_idx,  mdl_arr,  mdl)
        sti  = ix(st_idx,   st_arr,   st)
        zi   = ix(zone_idx, zone_arr, zone)
        cti  = ix(city_idx, city_arr, city)
        city_to_state[cti] = sti
        rtype = retail_map[lid]['rtype'] if is_ret else ''

        bump(monthly, li,                   is_ret, rtype)
        bump(sm,      f"{si}|{li}",         is_ret, rtype)
        bump(ltm,     f"{tti}|{si}|{li}",  is_ret, rtype)
        bump(mm,      f"{mi}|{si}|{li}",   is_ret, rtype)
        bump(stm,     f"{sti}|{si}|{li}",  is_ret, rtype)
        bump(mxst,    f"{mi}|{sti}|{li}",  is_ret, rtype)
        bump(mlt,     f"{mi}|{tti}|{li}",  is_ret, rtype)
        bump(stlt,    f"{sti}|{tti}|{li}", is_ret, rtype)
        bump(zm,      f"{zi}|{li}",         is_ret, rtype)
        bump(bdm,     f"{bd}|{si}|{li}",   is_ret, rtype)
        bump(cm,      f"{cti}|{li}",           is_ret, rtype)
        bump(csm,     f"{cti}|{si}|{li}",   is_ret, rtype)
        bump(cxm,     f"{cti}|{mi}|{li}",      is_ret, rtype)
        bump(cxsm,    f"{cti}|{si}|{mi}|{li}", is_ret, rtype)
        bump(stcm,    f"{sti}|{cti}|{li}",  is_ret, rtype)
        bump(univ,    f"{mi}|{si}|{sti}|{tti}|{li}", is_ret, rtype)

        if _dls is not None:
            dl  = _dls[i].strip() or 'Unknown'
            dli = ix(dl_idx, dl_arr, dl)
            bump(cdm,  f"{cti}|{dli}|{li}",      is_ret, rtype)
            bump(cdsm, f"{cti}|{dli}|{si}|{li}", is_ret, rtype)
            bump(stdm, f"{sti}|{dli}|{li}",       is_ret, rtype)
            bump(mxdl, f"{mi}|{dli}|{li}",        is_ret, rtype)
            bump(ltdl, f"{tti}|{dli}|{li}",       is_ret, rtype)
            # Status classification for Geo & Dealer tab (lead-level, not retail-level)
            # 'U' (unclassified) increments neither bucket — intentional.
            _sc = classify_status(_sns[i])
            _sk = f"{cti}|{dli}|{li}"
            if _sk not in dl_sn: dl_sn[_sk] = [0, 0, 0]
            if   _sc == 'B': dl_sn[_sk][1] += 1
            elif _sc == 'L': dl_sn[_sk][2] += 1
            elif _sc == 'O': dl_sn[_sk][0] += 1
            # 'U': not counted in any bucket

        rm  = retail_map[lid].get('rm', '') if is_ret else ''
        um  = rm if rm else lm
        # u_ matrices share the same lm_arr so lead counts stay fixed by create month.
        uli = ix(lm_idx, lm_arr, um)
        ubump(u_monthly, li,                          uli,                          is_ret, rtype)
        ubump(u_sm,      f"{si}|{li}",               f"{si}|{uli}",               is_ret, rtype)
        ubump(u_ltm,     f"{tti}|{si}|{li}",        f"{tti}|{si}|{uli}",        is_ret, rtype)
        ubump(u_mm,      f"{mi}|{si}|{li}",         f"{mi}|{si}|{uli}",         is_ret, rtype)
        ubump(u_stm,     f"{sti}|{si}|{li}",        f"{sti}|{si}|{uli}",        is_ret, rtype)
        ubump(u_mxst,    f"{mi}|{sti}|{li}",        f"{mi}|{sti}|{uli}",        is_ret, rtype)
        ubump(u_mlt,     f"{mi}|{tti}|{li}",        f"{mi}|{tti}|{uli}",        is_ret, rtype)
        ubump(u_stlt,    f"{sti}|{tti}|{li}",       f"{sti}|{tti}|{uli}",       is_ret, rtype)
        ubump(u_zm,      f"{zi}|{li}",               f"{zi}|{uli}",               is_ret, rtype)
        ubump(u_bdm,     f"{bd}|{si}|{li}",         f"{bd}|{si}|{uli}",         is_ret, rtype)
        ubump(u_stcm,    f"{sti}|{cti}|{li}",       f"{sti}|{cti}|{uli}",       is_ret, rtype)
        ubump(u_univ,    f"{mi}|{si}|{sti}|{tti}|{li}", f"{mi}|{si}|{sti}|{tti}|{uli}", is_ret, rtype)
        ubump(u_cm,  f"{cti}|{li}",         f"{cti}|{uli}",         is_ret, rtype)
        ubump(u_csm, f"{cti}|{si}|{li}",   f"{cti}|{si}|{uli}",   is_ret, rtype)
        ubump(u_cxm,  f"{cti}|{mi}|{li}",      f"{cti}|{mi}|{uli}",      is_ret, rtype)
        ubump(u_cxsm, f"{cti}|{si}|{mi}|{li}", f"{cti}|{si}|{mi}|{uli}", is_ret, rtype)

        if dl_col:
            ubump(u_stdm, f"{sti}|{dli}|{li}", f"{sti}|{dli}|{uli}", is_ret, rtype)
            ubump(u_mxdl, f"{mi}|{dli}|{li}",  f"{mi}|{dli}|{uli}",  is_ret, rtype)
            ubump(u_ltdl, f"{tti}|{dli}|{li}", f"{tti}|{dli}|{uli}", is_ret, rtype)
            ubump(u_cdm,  f"{cti}|{dli}|{li}",      f"{cti}|{dli}|{uli}",      is_ret, rtype)
            ubump(u_cdsm, f"{cti}|{dli}|{si}|{li}", f"{cti}|{dli}|{si}|{uli}", is_ret, rtype)

        if is_ret:
            pm  = normalize_purchased_model(retail_map[lid].get('pm', '')) or 'Unknown'
            pmi = ix(mdl_idx, mdl_arr, pm)   # purchased model uses same mdl index
            disp[f"{mi}|{pmi}|{li}"]   = disp.get(f"{mi}|{pmi}|{li}",   0) + 1
            u_disp[f"{mi}|{pmi}|{uli}"] = u_disp.get(f"{mi}|{pmi}|{uli}", 0) + 1

            # ── Retail Ageing (On Create, lead month attribution) ──────────────
            _ram_total += 1
            _rd = retail_map[lid].get('rd')
            _cd = parse_date(_cds[i])
            if _rd is None:
                _ram_no_rd += 1
            elif _cd is None:
                _ram_no_cd += 1
            else:
                _age_days = (_rd - _cd).days
                if _age_days < 0:
                    _ram_neg += 1
                else:
                    _abi = age_bucket(_age_days)
                    _rk  = f"{mi}|{si}|{tti}|{sti}|{cti}|{_abi}|{li}"
                    if _rk not in ram: ram[_rk] = [0, 0, 0]
                    ram[_rk][0] += 1
                    _rt_u = rtype.upper()
                    if 'DMS' in _rt_u:    ram[_rk][1] += 1
                    elif 'CALL' in _rt_u: ram[_rk][2] += 1
                    _ram_valid += 1

    # ── Status classification diagnostics ─────────────────────────────────────
    if _dls is not None:
        total_o = sum(v[0] for v in dl_sn.values())
        total_b = sum(v[1] for v in dl_sn.values())
        total_l = sum(v[2] for v in dl_sn.values())
        total_u = sum(_STATUS_UNKNOWN_COUNTS.values())
        print(f"Status classification: O={total_o:,}  B={total_b:,}  L={total_l:,}  "
              f"U={total_u:,} ({len(_STATUS_UNKNOWN_COUNTS)} unique unknown)", flush=True)
        if _STATUS_UNKNOWN_COUNTS:
            top_unknown = sorted(_STATUS_UNKNOWN_COUNTS.items(), key=lambda x: -x[1])[:20]
            print("  Top unclassified Status_Name values:", flush=True)
            for sn_val, cnt in top_unknown:
                print(f"    {cnt:>8,}  {repr(sn_val)}", flush=True)

    def to_rows(d, key_fn):
        return [[*key_fn(k), v[0], v[1], v[2], v[3]] for k, v in d.items()]

    city_state_arr = [city_to_state.get(i) for i in range(len(city_arr))]

    maps_payload = {
        'lm': lm_arr, 'src': src_arr, 'lt': lt_arr, 'mdl': mdl_arr,
        'st': st_arr, 'zone': zone_arr, 'city': city_arr,
        'city_state': city_state_arr,
        'ab': _AGE_BUCKET_LABELS,
    }
    if dl_col and dl_arr:
        maps_payload['dl'] = dl_arr
        print(f"Dealers: {len(dl_arr):,}  City×Dealer×Month rows: {len(cdm):,}", flush=True)

    payload = {
        't':       pd.Timestamp.now().isoformat(),
        'rt_cols': 1,
        'maps':    maps_payload,
        'monthly': to_rows(monthly, lambda k: [int(k)]),
        'sm':      to_rows(sm,  lambda k: list(map(int, k.split('|')))),
        'ltm':     to_rows(ltm, lambda k: list(map(int, k.split('|')))),
        'mm':      to_rows(mm,  lambda k: list(map(int, k.split('|')))),
        'stm':     to_rows(stm, lambda k: list(map(int, k.split('|')))),
        'mxst':    to_rows(mxst,  lambda k: list(map(int, k.split('|')))),
        'mlt':     to_rows(mlt,   lambda k: list(map(int, k.split('|')))),
        'stlt':    to_rows(stlt,  lambda k: list(map(int, k.split('|')))),
        'stcm':    to_rows(stcm,  lambda k: list(map(int, k.split('|')))),
        'disp':    [[*map(int,k.split('|')), v] for k,v in disp.items()],
        'zm':      to_rows(zm,  lambda k: list(map(int, k.split('|')))),
        'bdm':     to_rows(bdm, lambda k: [int(k.split('|')[0])] + list(map(int, k.split('|')[1:]))),
        'cm':      to_rows(cm,  lambda k: list(map(int, k.split('|')))),
        'csm':     to_rows(csm, lambda k: list(map(int, k.split('|')))),
        'cxm':     to_rows(cxm,  lambda k: list(map(int, k.split('|')))),
        'cxsm':    to_rows(cxsm, lambda k: list(map(int, k.split('|')))),
        'u_cm':    to_rows(u_cm,   lambda k: list(map(int, k.split('|')))),
        'u_csm':   to_rows(u_csm,  lambda k: list(map(int, k.split('|')))),
        'u_cxm':   to_rows(u_cxm,  lambda k: list(map(int, k.split('|')))),
        'u_cxsm':  to_rows(u_cxsm, lambda k: list(map(int, k.split('|')))),
        **({"cdm":  to_rows(cdm,  lambda k: list(map(int, k.split('|')))),
            "cdsm": to_rows(cdsm, lambda k: list(map(int, k.split('|')))),
            "dl_sn": [[*map(int,k.split('|')), *v] for k,v in dl_sn.items()],
            "stdm": to_rows(stdm, lambda k: list(map(int, k.split('|')))),
            "mxdl": to_rows(mxdl, lambda k: list(map(int, k.split('|')))),
            "ltdl": to_rows(ltdl, lambda k: list(map(int, k.split('|')))),
            "u_stdm": to_rows(u_stdm, lambda k: list(map(int, k.split('|')))),
            "u_mxdl": to_rows(u_mxdl, lambda k: list(map(int, k.split('|')))),
            "u_ltdl": to_rows(u_ltdl, lambda k: list(map(int, k.split('|')))),
            "u_cdm":  to_rows(u_cdm,  lambda k: list(map(int, k.split('|')))),
            "u_cdsm": to_rows(u_cdsm, lambda k: list(map(int, k.split('|'))))} if dl_col and dl_arr else {}),
        'u_monthly': to_rows(u_monthly, lambda k: [int(k)]),
        'u_sm':      to_rows(u_sm,  lambda k: list(map(int, k.split('|')))),
        'u_ltm':     to_rows(u_ltm, lambda k: list(map(int, k.split('|')))),
        'u_mm':      to_rows(u_mm,  lambda k: list(map(int, k.split('|')))),
        'u_stm':     to_rows(u_stm, lambda k: list(map(int, k.split('|')))),
        'u_mxst':    to_rows(u_mxst,  lambda k: list(map(int, k.split('|')))),
        'u_mlt':     to_rows(u_mlt,   lambda k: list(map(int, k.split('|')))),
        'u_stlt':    to_rows(u_stlt,  lambda k: list(map(int, k.split('|')))),
        'u_stcm':    to_rows(u_stcm,  lambda k: list(map(int, k.split('|')))),
        'univ':      to_rows(univ,    lambda k: list(map(int, k.split('|')))),
        'u_univ':    to_rows(u_univ,  lambda k: list(map(int, k.split('|')))),
        'u_disp':  [[*map(int,k.split('|')), v] for k,v in u_disp.items()],
        'u_zm':      to_rows(u_zm,  lambda k: list(map(int, k.split('|')))),
        'u_bdm':     to_rows(u_bdm, lambda k: [int(k.split('|')[0])] + list(map(int, k.split('|')[1:]))),
        'ram':       [[*map(int, k.split('|')), *v] for k, v in ram.items()],
        'ram_meta':  {'total': _ram_total, 'valid': _ram_valid,
                      'no_rd': _ram_no_rd, 'no_cd': _ram_no_cd, 'neg': _ram_neg},
    }
    print(f"Done — {total:,} leads  {len(retail_map):,} retails", flush=True)
    print(f"Ageing: retails={_ram_total:,}  valid={_ram_valid:,}  "
          f"no_rd={_ram_no_rd:,}  no_cd={_ram_no_cd:,}  neg={_ram_neg:,}  "
          f"ram_rows={len(ram):,}", flush=True)
    return payload

# ─── Pipeline guard functions (defined here so every stage can call them) ─────

_METRICS_PATH = Path(__file__).parent / 'source_metrics.json'

# Required columns that must be present in every live lead sheet.
# These are the columns requested via LEAD_COLS; missing any of these means the
# Apps Script returned a truncated/corrupted response.
_LEAD_REQUIRED_COLS = {'opty_id', 'Lead_Month', 'Date', 'model'}

# Required columns that must be present in the live retail sheet.
_RETAIL_REQUIRED_COLS = {'sourceLeadId', 'performanceMonth'}


def _fail_exit(stage, reason, stg_path=None, firebase_cloud_state='NO'):
    """Print a structured failure report and exit 1. Production is never modified."""
    _data_dir = Path(__file__).parent.parent / 'data'
    _prod     = _data_dir / 'tvs_payload.json.gz'
    sep = '=' * 60
    print(f"\n{sep}", file=sys.stderr, flush=True)
    print("TVS DATA PIPELINE — FAILED SAFELY", file=sys.stderr, flush=True)
    print(sep, file=sys.stderr, flush=True)
    print(f"Stage:    {stage}", file=sys.stderr, flush=True)
    print(f"Reason:   {reason}", file=sys.stderr, flush=True)
    print(f"\nProduction payload changed: NO", file=sys.stderr, flush=True)
    print(f"Firebase changed:           {firebase_cloud_state}", file=sys.stderr, flush=True)
    print(f"GitHub Pages changed:       NO", file=sys.stderr, flush=True)
    if _prod.exists():
        _mtime = datetime.fromtimestamp(_prod.stat().st_mtime, timezone.utc)
        print(f"\nLast known-good: {_prod.name}  ({_prod.stat().st_size // 1024:,} KB)",
              file=sys.stderr, flush=True)
        print(f"Last modified:   {_mtime.strftime('%Y-%m-%d %H:%M UTC')}",
              file=sys.stderr, flush=True)
    else:
        print(f"\nLast known-good: NONE (first run)", file=sys.stderr, flush=True)
    if stg_path:
        _sp = Path(stg_path)
        if _sp.exists():
            print(f"Diagnostic:      {_sp.name}  (kept for inspection)", file=sys.stderr, flush=True)
    print(f"\nDashboard continues serving the previous known-good payload.", file=sys.stderr, flush=True)
    print(sep, file=sys.stderr, flush=True)
    sys.exit(1)


def _validate_post_response(body):
    """
    Parse and validate the Apps Script POST response.

    Returns (ok: bool, detail: str, parsed: dict|None).

    Accepted success formats
    ─────────────────────────
    • Old contract  — {"ok": true, ...}
      Apps Script explicitly signals success.

    • Structural echo (current contract) — {"t": "<iso>", "rt_cols": <int>, "maps": {"lm": [...], ...}}
      After a successful Firebase write the Apps Script echoes the three header
      fields from the payload it just stored.  Presence of a valid timestamp,
      rt_cols, and a non-empty lm array proves the payload was received, decoded,
      and written — no separate ok flag needed.

    Rejected
    ─────────
    • Empty body
    • Non-JSON body
    • {"ok": false, ...}         — explicit failure
    • {"error": "...", ...}      — error key without ok:true
    • Any JSON that meets neither success criterion
    """
    if not body:
        return False, 'EMPTY_BODY: response was empty', None

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        return False, f'INVALID_JSON: {exc}', None

    if not isinstance(parsed, dict):
        return False, f'NOT_A_DICT: type={type(parsed).__name__}', parsed

    # Explicit failure indicators — always reject
    if parsed.get('ok') is False:
        err = parsed.get('error') or parsed.get('message') or 'no detail'
        return False, f'EXPLICIT_FAIL: ok=false, error={err!r}', parsed
    if 'error' in parsed and parsed.get('ok') is not True:
        return False, f'ERROR_KEY: {parsed["error"]!r}', parsed

    # Explicit success — old contract
    if parsed.get('ok') is True:
        return True, 'OK_TRUE', parsed

    # Structural success — new/current contract
    # Apps Script echoes payload header fields after a confirmed Firebase write.
    t_val    = parsed.get('t', '')
    maps_val = parsed.get('maps')
    rt_cols  = parsed.get('rt_cols')
    if (isinstance(t_val, str) and len(t_val) >= 10
            and isinstance(maps_val, dict)
            and isinstance(maps_val.get('lm'), list)
            and len(maps_val['lm']) >= 1
            and rt_cols is not None):
        n_months = len(maps_val['lm'])
        return True, f'STRUCTURAL_OK: t={t_val[:19]}, lm_months={n_months}', parsed

    # Ambiguous — has valid JSON but no clear success or failure signal
    keys = list(parsed.keys())
    return False, f'AMBIGUOUS_RESPONSE: keys={keys}', parsed


def _load_source_metrics():
    """Load previous-run source row counts from source_metrics.json (or return {})."""
    if _METRICS_PATH.exists():
        try:
            with open(_METRICS_PATH, 'r', encoding='utf-8') as _mf:
                return json.load(_mf)
        except Exception as _me:
            print(f"  NOTE: Could not read source_metrics.json ({_me}); "
                  f"baseline comparison skipped this run.", flush=True)
    return {}


def _check_source_drop(label, current_count, prev_metrics, threshold=0.80):
    """Fail the run if current_count is below threshold * previous count.
    Silently passes if no baseline exists for this label.
    """
    prev = prev_metrics.get(label, {}).get('rows') if isinstance(prev_metrics.get(label), dict) \
        else prev_metrics.get(label)
    if prev is None or prev == 0:
        return  # no baseline yet — first run for this source
    ratio = current_count / prev
    if ratio < threshold:
        _fail_exit(
            f'Source completeness check — {label}',
            f'Row count dropped from {prev:,} to {current_count:,} '
            f'({ratio:.1%} of previous run; minimum acceptable: {threshold:.0%}). '
            f'This almost certainly indicates an incomplete fetch or sheet truncation. '
            f'If this is a genuine data reduction, delete {_METRICS_PATH.name} to reset the baseline.'
        )
    pct = f'{ratio:.1%}'
    print(f"  Source check [{label}]: {current_count:,} rows  (prev {prev:,}, {pct}) ✓", flush=True)


def _save_source_metrics(metrics_dict, run_start):
    """Write source row counts to source_metrics.json after a successful run."""
    try:
        with open(_METRICS_PATH, 'w', encoding='utf-8') as _mf:
            json.dump({**metrics_dict, 'date': run_start.strftime('%Y-%m-%d')}, _mf, indent=2)
        print(f"  Source metrics saved → {_METRICS_PATH.name}", flush=True)
    except Exception as _me:
        print(f"  WARNING: Could not save source_metrics.json: {_me}", flush=True)


# ─── Parallel-fetch helper ────────────────────────────────────────────────────
def _fetch_and_process_lead_sheet(sheet, prev_metrics):
    """Fetch one lead sheet and run STAGE 1–6 (fetch → validate → standardize → filter).

    Designed to run in a daemon thread alongside retail and Retail_Date fetches.
    Returns a dict on success; raises SystemExit on unrecoverable error so the
    daemon-thread wrapper can capture and re-raise it on the main thread.
    """
    t0   = time.monotonic()
    _lbl = sheet['label']
    _resume = {'page': 0, 'rows': [], 'headers': None}

    for _sheet_attempt in range(3):
        try:
            raw = fetch_sheet_via_proxy(
                sheet['id'], _lbl, tab_name=sheet.get('tab'),
                _start_page=_resume['page'],
                _prev_rows=_resume['rows'],
                _prev_headers=_resume['headers'])
            raw.columns = [c.strip() for c in raw.columns]

            print(f"\n  [{_lbl}] STAGE 1 — fetched from sheet: {len(raw):,} rows", flush=True)
            print(f"  [{_lbl}] columns: {list(raw.columns)}", flush=True)

            print(f"  [{_lbl}] STAGE 2 — DataFrame rows: {len(raw):,}", flush=True)
            _missing_cols = _LEAD_REQUIRED_COLS - set(raw.columns)
            if _missing_cols:
                _fail_exit(
                    f'Lead schema — {_lbl}',
                    f'Required columns missing: {sorted(_missing_cols)}. '
                    f'Available columns: {list(raw.columns)}')
            if len(raw) == 0:
                _fail_exit(
                    f'Lead sheet empty — {_lbl}',
                    'Sheet returned 0 rows. This is almost certainly an incomplete fetch '
                    'or a sheet access error.')

            if 'Lead_Month' in raw.columns:
                _raw_lm_full = raw['Lead_Month'].astype(str).str.strip().value_counts(dropna=False).to_dict()
                print(f"  [{_lbl}] STAGE 3 — raw Lead_Month full distribution:", flush=True)
                for _lm_v, _lm_c in sorted(_raw_lm_full.items(), key=lambda x: -x[1]):
                    print(f"    {_lm_v!r:25s}: {_lm_c:,}", flush=True)
            else:
                print(f"  [{_lbl}] STAGE 3 — Lead_Month column NOT FOUND in raw sheet", flush=True)

            _rtype_entries = extract_rtype_map(raw)
            std_all = standardize_leads(raw)

            print(f"  [{_lbl}] STAGE 4 — after standardize_leads: {len(std_all):,} rows", flush=True)
            if len(std_all) != len(raw):
                print(f"  [{_lbl}] WARNING: standardize_leads changed row count "
                      f"({len(raw):,} → {len(std_all):,})", flush=True)

            if 'LeadMonth' in std_all.columns:
                _std_lm_full = std_all['LeadMonth'].value_counts(dropna=False).to_dict()
                print(f"  [{_lbl}] STAGE 5 — post-standardize LeadMonth full distribution:", flush=True)
                for _lm_v, _lm_c in sorted(_std_lm_full.items(), key=lambda x: -x[1]):
                    _mo = month_order(_lm_v)
                    print(f"    {_lm_v!r:25s}: {_lm_c:,}  (month_order={_mo})", flush=True)
            else:
                print(f"  [{_lbl}] STAGE 5 — LeadMonth column NOT present after standardize", flush=True)

            _min = sheet.get('min_mo', ONLINE_START_ORDER)
            _max = sheet.get('max_mo')

            std = std_all[std_all['LeadMonth'].apply(month_order) >= _min]
            if _max is not None:
                std = std[std['LeadMonth'].apply(month_order) <= _max]

            _n_dropped = len(std_all) - len(std)
            print(f"  [{_lbl}] STAGE 6 — after month filter (min_mo={_min}, max_mo={_max}): "
                  f"{len(std):,} rows  ({_n_dropped:,} dropped)", flush=True)

            if _n_dropped > 0:
                _dropped_df = std_all[std_all['LeadMonth'].apply(month_order) < _min]
                if _max is not None:
                    _over_max = std_all[std_all['LeadMonth'].apply(month_order) > _max]
                    _dropped_df = pd.concat([_dropped_df, _over_max], ignore_index=True)
                _drop_by_month = _dropped_df['LeadMonth'].value_counts(dropna=False).to_dict()
                print(f"  [{_lbl}] STAGE 6 — dropped rows by LeadMonth:", flush=True)
                for _lm_v, _lm_c in sorted(_drop_by_month.items(), key=lambda x: -x[1]):
                    _mo = month_order(_lm_v)
                    _reason = 'blank/unresolved' if _mo == 0 else f'month_order={_mo} < min_mo={_min}'
                    print(f"    {_lm_v!r:25s}: {_lm_c:,}  ({_reason})", flush=True)

                _samp_cols = [c for c in ['LeadMonth', 'Date', 'CreateDate', 'SorceLeadId',
                                           'Source', 'ModelName', 'State'] if c in _dropped_df.columns]
                _sample100 = _dropped_df[_samp_cols].head(100)
                print(f"  [{_lbl}] STAGE 6 — sample of dropped rows (up to 100):", flush=True)
                print(_sample100.to_string(index=False), flush=True)

            _range_str = f"{MONTH_NAMES[(_min % 100) - 1]}'{_min // 100:02d}" + (
                f" only" if _max == _min else f"+ ({len(std):,} rows)")
            _duration = time.monotonic() - t0
            print(f"  [{_lbl}] STAGE 6 FINAL: {len(std):,} rows kept [{_range_str}]", flush=True)
            print(f"  [{_lbl}] [COMPLETE] Dataset:{_lbl} Raw:{len(raw):,} "
                  f"Filtered:{len(std):,} Duration:{_duration:.1f}s", flush=True)

            with _fetch_perf_lock:
                _fetch_perf[_lbl] = {'duration_s': _duration, 'rows': len(raw)}

            return {
                'label':         _lbl,
                'rtype_entries': _rtype_entries,
                'std':           std,
                'raw_len':       len(raw),
                'filtered_len':  len(std),
                'duration_s':    _duration,
            }

        except SystemExit:
            raise
        except _PageFetchFailed as e:
            _resume = {'page': e.page, 'rows': e.accumulated_rows, 'headers': e.headers}
            if _sheet_attempt < 2:
                print(f"  WARNING: {_lbl} page {e.page} failed on attempt "
                      f"{_sheet_attempt + 1}; resuming from page {e.page} in 60s "
                      f"({len(e.accumulated_rows):,} rows preserved)…", flush=True)
                time.sleep(60)
            else:
                traceback.print_exc()
                _fail_exit(
                    f'Lead sheet fetch — {_lbl}',
                    f'All 3 sheet-level attempts failed at page {e.page}: {e}')
        except Exception as e:
            _resume = {'page': 0, 'rows': [], 'headers': None}
            if _sheet_attempt < 2:
                print(f"  WARNING: {_lbl} attempt {_sheet_attempt + 1} failed: {e}; retrying in 30s…",
                      flush=True)
                time.sleep(30)
            else:
                traceback.print_exc()
                _fail_exit(
                    f'Lead sheet fetch — {_lbl}',
                    f'All 3 attempts failed: {e}')


# ─── Main ─────────────────────────────────────────────────────────────────────

print("=" * 60, flush=True)
print("TVS Lead Disposition — Daily Data Push", flush=True)
print(f"Run start: {_RUN_START.strftime('%Y-%m-%d %H:%M UTC')}"
      + ("  [DRY RUN]" if DRY_RUN else ""), flush=True)
print("=" * 60, flush=True)

# Load previous-run source counts for baseline comparison
_prev_metrics = _load_source_metrics()
_current_metrics: dict = {}   # populated as each source is fetched

# ── Step 1: Historical data ───────────────────────────────────────────────────
# PRODUCTION: load the committed hist_cache.json.gz (always present on GitHub Actions).
# BOOTSTRAP (local only): if the cache is absent AND local Excel files exist, rebuild it.
_cache_exists = HIST_CACHE_PATH.exists()
_xlsb_present = (not _cache_exists) and any(
    os.path.exists(os.path.join(HIST_DIR, s['path']))
    for s in HIST_LEAD_FILES + HIST_RETAIL_FILES
)

if _xlsb_present and not _cache_exists:
    print(f"\n[1/5] Building hist_cache from local Excel files in {HIST_DIR}…", flush=True)

    print("  Loading historical retail files…", flush=True)
    retail_map = load_hist_retail_map()
    print(f"  Historical retail map: {len(retail_map):,} entries", flush=True)

    print("  Loading historical lead files…", flush=True)
    hist_leads = load_hist_leads()
    print(f"  Historical leads: {len(hist_leads):,} rows", flush=True)

    _cache_data = {
        'generated':  datetime.now(timezone.utc).isoformat(),
        'retail_map': retail_map,
        'leads':      hist_leads.to_dict('records'),
    }
    with gzip.open(HIST_CACHE_PATH, 'wt', encoding='utf-8') as _cf:
        json.dump(_cache_data, _cf, separators=(',', ':'))
    print(f"  hist_cache saved -> {HIST_CACHE_PATH} ({HIST_CACHE_PATH.stat().st_size//1024:,} KB)", flush=True)
    del _cache_data

elif _cache_exists:
    print(f"\n[1/5] Loading hist_cache…", flush=True)
    with gzip.open(HIST_CACHE_PATH, 'rt', encoding='utf-8') as _cf:
        _cache_data = json.load(_cf)
    retail_map = _cache_data['retail_map']
    hist_leads  = pd.DataFrame(_cache_data['leads'])
    print(f"  Generated: {_cache_data.get('generated','?')}", flush=True)
    print(f"  Historical retail map: {len(retail_map):,}  leads: {len(hist_leads):,}", flush=True)
    del _cache_data
    # Normalize ModelName: cache may store raw values (e.g. lowercase variants, iQube ST).
    # normalize_lead_model maps them to canonical names consistent with live leads.
    if 'ModelName' in hist_leads.columns:
        hist_leads['ModelName'] = hist_leads['ModelName'].apply(normalize_lead_model)

else:
    # hist_cache.json.gz is always committed in production — this branch fires only locally
    # if the cache is absent. Hard-fail: without hist data the payload is incomplete.
    _fail_exit(
        'Historical cache load',
        f'hist_cache.json.gz not found at {HIST_CACHE_PATH} and no local Excel fallback. '
        f'In production the cache is always present. On a fresh checkout, ensure it is committed.'
    )
    retail_map = {}   # unreachable — satisfies type checker
    hist_leads  = pd.DataFrame()

# ── Steps 2+4: Parallel fetch — retail master, Retail_Date, and all lead sheets ─
# All four data sources are fetched concurrently using daemon threads.
# Retail and lead sheets are independent at the network level — neither depends on
# the other's data during fetch.  Merging, validation, and aggregation happen
# sequentially after ALL threads complete.
#
# hist_cache is the authoritative source for Apr'25–Jun'26. Live retail must NOT
# overwrite those entries — doing so replaces the accurate hist DMS/Call Out value
# (from the Excel "DMS/Call Out" column) with the live classification, which
# incorrectly reclassifies many historical DMS entries as Call Out.
# Only Jul'26+ entries are the true "online" period; those freely overwrite hist.

print(f"\n[2+4/5] Parallel fetch: retail master + Retail_Date + "
      f"{len(LEAD_SHEETS)} lead sheet(s)…", flush=True)
print("  (All threads running concurrently — fetch output may be interleaved)", flush=True)

_par_results: dict = {}
_par_errors:  dict = {}
_par_lock             = threading.Lock()


def _par_run(key, fn, *args, **kwargs):
    def _worker():
        try:
            _par_results[key] = fn(*args, **kwargs)
        except SystemExit as _e:
            with _par_lock:
                _par_errors[key] = _e
        except Exception as _e:
            traceback.print_exc()
            with _par_lock:
                _par_errors[key] = _e
    t = threading.Thread(target=_worker, daemon=True, name=f'fetch-{key}')
    t.start()
    return t


def _retail_with_perf():
    t0 = time.monotonic()
    df = fetch_retails()
    with _fetch_perf_lock:
        _fetch_perf['retail_raw'] = {'duration_s': time.monotonic() - t0, 'rows': len(df)}
    return df


def _rd_with_perf():
    t0 = time.monotonic()
    m  = fetch_retail_date_map()
    with _fetch_perf_lock:
        _fetch_perf['retail_date'] = {'duration_s': time.monotonic() - t0, 'rows': len(m)}
    return m


_parallel_start = time.monotonic()

_threads = [
    _par_run('retail_raw',  _retail_with_perf),
    _par_run('retail_date', _rd_with_perf),
] + [_par_run(s['label'], _fetch_and_process_lead_sheet, s, _prev_metrics)
     for s in LEAD_SHEETS]

for _t in _threads:
    _t.join()

_parallel_elapsed = time.monotonic() - _parallel_start
print(f"\n  All parallel fetches complete: {_parallel_elapsed:.1f}s wall-clock", flush=True)

# ── [2/5] Retail validation ────────────────────────────────────────────────────
print(f"\n[2/5] Live retail master validation…", flush=True)
if 'retail_raw' in _par_errors:
    _retail_err = _par_errors['retail_raw']
    if isinstance(_retail_err, SystemExit):
        raise _retail_err
    _fail_exit('Live retail fetch', str(_retail_err))

retail_df = _par_results['retail_raw']
_missing_ret_cols = _RETAIL_REQUIRED_COLS - set(retail_df.columns)
if _missing_ret_cols:
    _fail_exit(
        'Retail schema validation',
        f'Required columns missing from retail sheet: {sorted(_missing_ret_cols)}. '
        f'Available: {list(retail_df.columns)}')
_validate_retail_fetch(retail_df, _prev_metrics)

_current_metrics['retail_raw'] = {'rows': len(retail_df)}
_check_source_drop('retail_raw', len(retail_df), _prev_metrics)

# Retail_Date — non-fatal; empty dict means Retail Ageing is skipped for this run.
if 'retail_date' in _par_errors:
    _rd_err = _par_errors['retail_date']
    print(f"  WARNING: Retail_Date fetch failed ({_rd_err}); "
          f"Retail Ageing will be empty this run.", flush=True)
    _rd_map = {}
else:
    _rd_map = _par_results.get('retail_date', {})

online_rmap, unexpected_call_types = build_retail_map(retail_df, rd_map=_rd_map)

# Three-way merge: prioritised by data quality.
#
# CASE A — Jul'26+ (rm >= ONLINE_START):
#   Live retail sheet is fully authoritative (performanceMonth + Call Type).
#   Overwrites any hist_cache entry for the same lid.
#
# CASE B — Pre-Jul'26, lid ALREADY in hist_cache:
#   hist_cache DMS/Call Out is authoritative (from Excel "DMS/Call Out" column;
#   the live sheet's "Call Type" / Purchased-From logic is known to mislabel
#   many historical DMS entries as Call Out, so we do NOT adopt it).
#   However the live performanceMonth is more accurate than the Excel
#   DMS_Retail_Month — update ONLY the retail month, keep the rtype.
#   Guard: only update if the new rm is a valid month >= LEAD_MASTER_START
#   so we never blank-out or back-date a known retail.
#
# CASE C — Pre-Jul'26, lid NOT in hist_cache:
#   New retail that appeared after the Excel export (common for May–Jun'26 leads
#   that retailed after the Excel cut date).  Add it fully from the live sheet.
#
_added_jul26  = 0   # Case A
_updated_rm   = 0   # Case B — rm updated
_kept_rm      = 0   # Case B — rm kept (live rm invalid / pre-scope)
_added_new    = 0   # Case C
for lid, info in online_rmap.items():
    live_rm       = info.get('rm', '')
    live_rm_order = month_order(live_rm)
    if live_rm_order >= ONLINE_START_ORDER:
        # Case A
        retail_map[lid] = info
        _added_jul26 += 1
    elif lid in retail_map:
        # Case B — keep rtype, update rm if valid; always propagate rd for ageing
        live_rd = info.get('rd')
        if live_rm and live_rm_order >= LEAD_MASTER_START_ORDER:
            retail_map[lid] = {**retail_map[lid], 'rm': live_rm, 'rd': live_rd}
            _updated_rm += 1
        else:
            if live_rd is not None:
                retail_map[lid] = {**retail_map[lid], 'rd': live_rd}
            _kept_rm += 1
    else:
        # Case C — new retail not in hist_cache
        retail_map[lid] = info
        _added_new += 1

if unexpected_call_types:
    print(f"  WARNING: {len(unexpected_call_types)} unexpected 'Call Type' values "
          f"(defaulted to DMS):", flush=True)
    for item in unexpected_call_types[:20]:
        print(f"    lid={item['lid']}  call_type={item['call_type']!r}", flush=True)
    if len(unexpected_call_types) > 20:
        print(f"    ... and {len(unexpected_call_types)-20} more", flush=True)

print(f"  Live retail: {len(online_rmap):,} total  "
      f"| {_added_jul26:,} added/replaced (Jul'26+)  "
      f"| {_updated_rm:,} rm updated (pre-Jul'26, existing)  "
      f"| {_kept_rm:,} rm kept (pre-Jul'26, invalid new rm)  "
      f"| {_added_new:,} added new (pre-Jul'26, not in hist)", flush=True)

# Remove retail entries whose Retail_Attribution_Date is before LEAD_MASTER_START (Apr'25).
# Dashboard scope begins Apr'25; Jan'25–Mar'25 retails must not appear anywhere.
_pre_filter = len(retail_map)
retail_map  = {lid: info for lid, info in retail_map.items()
               if month_order(info.get('rm', '')) >= LEAD_MASTER_START_ORDER}
print(f"  Combined total after {LEAD_MASTER_START} filter: {len(retail_map):,}"
      f"  (removed {_pre_filter - len(retail_map):,} pre-{LEAD_MASTER_START} entries)", flush=True)

# ── Jun'26 source correction: Facebook (non-LT1105) → Whatsapp ───────────────
# Business rule: Jun'26 leads from Facebook that are NOT LeadType 1105 are
# misclassified — they should be Whatsapp. LT 1105 Facebook rows are correct.
# Applied here once, after all hist_leads loading paths, before any aggregation.
if len(hist_leads) > 0 and 'Source' in hist_leads.columns and 'LeadType' in hist_leads.columns:
    _jun26_correct_mask = (
        (hist_leads['LeadMonth'] == "Jun'26") &
        (hist_leads['Source'] == 'Facebook') &
        (hist_leads['LeadType'].astype(str) != '1105')
    )
    _jun26_corrected = int(_jun26_correct_mask.sum())
    if _jun26_corrected:
        hist_leads.loc[_jun26_correct_mask, 'Source'] = 'WhatsApp'
        print(f"  Jun'26 source correction: {_jun26_corrected:,} Facebook (non-LT1105) → WhatsApp", flush=True)

# ── [3/5] Historical leads ────────────────────────────────────────────────────
print(f"\n[3/5] Historical leads: {len(hist_leads):,} rows", flush=True)

# ── [4/5] Live lead results — merge from parallel threads ─────────────────────
print(f"\n[4/5] Live lead sheets fetched in parallel — merging results…", flush=True)
lead_dfs  = []
rtype_map = {}

for _sheet in LEAD_SHEETS:
    _lbl = _sheet['label']
    if _lbl in _par_errors:
        _lead_err = _par_errors[_lbl]
        if isinstance(_lead_err, SystemExit):
            raise _lead_err
        _fail_exit(f'Lead sheet fetch — {_lbl}', str(_lead_err))

    _lr = _par_results[_lbl]
    rtype_map.update(_lr['rtype_entries'])
    lead_dfs.append(_lr['std'])
    # Source metrics: compare RAW fetched count to baseline.
    # Blank Lead_Month rows filtered in STAGE 6 are empty sheet rows that Apps Script
    # includes via getLastRow() — NOT real data loss. Raw count confirms completeness.
    _current_metrics[_lbl] = {'rows': _lr['raw_len'], 'filtered_rows': _lr['filtered_len']}
    _check_source_drop(_lbl, _lr['raw_len'], _prev_metrics)
    print(f"", flush=True)

# Override rtype from embedded sheet columns (DMS_Retail_Month / Retail By).
# Only override when Retail By is non-empty AND retail month is Jul'26+ (ONLINE_START).
# Pre-Jul'26 hist retail types are authoritative and must not be overwritten by live sheets.
for lid, info in rtype_map.items():
    if lid in retail_map:
        _rm_ord = month_order(info.get('rm', ''))
        if 0 < _rm_ord < ONLINE_START_ORDER:
            continue   # hist retail month — live sheet must not override
        if info['rtype']:
            retail_map[lid]['rtype'] = info['rtype']
        if info['rm'] and not retail_map[lid]['rm']:
            retail_map[lid]['rm'] = info['rm']

# ── Step 5: Merge, gap-fill, aggregate, push ──────────────────────────────────
print("\n[5/5] Merging leads, gap-fill, aggregating…", flush=True)
online_leads = pd.concat(lead_dfs, ignore_index=True) if lead_dfs else pd.DataFrame()

# ── STAGE 7: online sheets after all month filters ────────────────────────────
print(f"\n  STAGE 7 — online_leads (all live sheets, post-filter): {len(online_leads):,} rows", flush=True)
if 'LeadMonth' in online_leads.columns:
    _ol_lm = online_leads['LeadMonth'].value_counts(dropna=False).to_dict()
    for _lm_v, _lm_c in sorted(_ol_lm.items(), key=lambda x: -x[1]):
        print(f"    {_lm_v!r:25s}: {_lm_c:,}", flush=True)

# ── Live month presence check ─────────────────────────────────────────────────
# LOGIC:
#
#   Prior live months (ONLINE_START through end of the PREVIOUS calendar month):
#     REQUIRED — hard-fail if any are absent, UNLESS the month is in
#     PENDING_LEAD_MONTHS (Lead Master not yet provided → no rows expected).
#     Zero rows for a non-pending prior month means a source sheet failed or
#     lost data silently.
#
#   Current calendar month:
#     - COVERED if either:
#       (a) at least one LEAD_SHEETS entry spans it, OR
#       (b) it is listed in PENDING_LEAD_MONTHS (sheet pending → warn, not fail).
#     - Hard-fail only if neither (a) nor (b) is true.
#     - WARN (not fail) if a covering sheet returned 0 rows. Normal at start of
#       a new month before any CRM leads exist.
#     - WARN clearly if the month is pending — dashboard will show 0 new leads.
#
# MONTH-CLOSE PATTERN:
#   When a month closes, its Lead Master sheet is frozen (id stays in LEAD_SHEETS
#   with matching min_mo=max_mo). The NEXT month starts in PENDING_LEAD_MONTHS
#   until its Lead Master URL is provided. Retail always continues updating.

_ref_m  = ONLINE_START_ORDER % 100    # e.g. 7 (July)
_ref_y  = ONLINE_START_ORDER // 100   # e.g. 26
_cur_m  = _RUN_START.month
_cur_y  = _RUN_START.year % 100       # 2-digit
_prev_m = _cur_m - 1 if _cur_m > 1 else 12
_prev_y = _cur_y if _cur_m > 1 else _cur_y - 1

# Months from ONLINE_START through end of PREVIOUS calendar month — always required.
_prior_live_months: list = []
_m, _y = _ref_m, _ref_y
while (_y * 100 + _m) <= (_prev_y * 100 + _prev_m):
    _prior_live_months.append(f"{MONTH_NAMES[_m - 1]}'{_y:02d}")
    _m += 1
    if _m > 12:
        _m, _y = 1, _y + 1

_cur_month_str   = f"{MONTH_NAMES[_cur_m - 1]}'{_cur_y:02d}"
_cur_month_order = _cur_y * 100 + _cur_m

# Is the current month within the min_mo..max_mo range of at least one LEAD_SHEET,
# OR declared as pending (sheet URL not yet provided)?
_cur_in_lead_sheets = any(
    s.get('min_mo', 0) <= _cur_month_order and
    (s.get('max_mo') is None or s.get('max_mo') >= _cur_month_order)
    for s in LEAD_SHEETS
)
_cur_month_pending  = _cur_month_str in PENDING_LEAD_MONTHS
_cur_month_covered  = _cur_in_lead_sheets or _cur_month_pending

_online_lm_set = (
    set(online_leads['LeadMonth'].unique())
    if 'LeadMonth' in online_leads.columns and len(online_leads) > 0 else set()
)

# Hard-fail for any prior live month with 0 rows — UNLESS it is in PENDING_LEAD_MONTHS
# (Lead Master URL not yet provided; 0 rows expected by design).
_missing_prior = [
    mo for mo in _prior_live_months
    if mo not in _online_lm_set and mo not in PENDING_LEAD_MONTHS
]
if _missing_prior:
    _fail_exit(
        'Live month presence check',
        f'Prior live months with zero rows after filtering: {_missing_prior}. '
        f'Required (prior): {_prior_live_months}. '
        f'Actual months in online_leads: {sorted(_online_lm_set)}. '
        f'One or more source sheets may have returned empty data or failed silently.'
    )

# Hard-fail only if the current month has NO coverage at all.
if not _cur_month_covered:
    _fail_exit(
        'Live month presence check — current month not covered',
        f'Current month {_cur_month_str!r} (order {_cur_month_order}) is not covered by any '
        f'entry in LEAD_SHEETS and is not in PENDING_LEAD_MONTHS. '
        f'Add a new LEAD_SHEETS entry for this month, or add it to PENDING_LEAD_MONTHS '
        f'if the Lead Master URL is not yet available.',
    )

# Pending current month — warn clearly; 0 leads expected.
if _cur_month_pending:
    print(
        f"  WARNING: current month {_cur_month_str!r} is in PENDING_LEAD_MONTHS — "
        f"Lead Master URL has not been provided yet. "
        f"The dashboard will show 0 new leads for {_cur_month_str!r} until the sheet "
        f"is added to LEAD_SHEETS and PENDING_LEAD_MONTHS is updated.", flush=True
    )

# Non-pending current month with 0 rows — warn (normal at start of month).
_cur_month_in_data = _cur_month_str in _online_lm_set
if not _cur_month_in_data and not _cur_month_pending:
    print(
        f"  WARNING: current month {_cur_month_str!r} is covered by LEAD_SHEETS but "
        f"has 0 rows in online_leads. This is expected if no leads have been created "
        f"this month yet in CRM. Continuing — monitor daily.", flush=True
    )

# _expected_live_months drives: reconciliation output, _validate_payload checks,
# DRY RUN and SUCCESS report tables.
# Include current month only when it has actual data (so zero-row months don't
# trigger _validate_payload's "0 leads in payload" error).
_expected_live_months = _prior_live_months.copy()
if _cur_month_in_data:
    _expected_live_months.append(_cur_month_str)

if _prior_live_months:
    print(f"  Live month check — required prior: {_prior_live_months} — all present ✓", flush=True)
print(
    f"  Current month {_cur_month_str!r}: "
    f"{'present ✓' if _cur_month_in_data else '0 rows (new month — monitoring)'}",
    flush=True
)

parts     = [df for df in [hist_leads, online_leads] if len(df) > 0]
all_leads = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

# ── STAGE 8: after merge with historical ─────────────────────────────────────
print(f"  STAGE 8 — after merge: hist={len(hist_leads):,}  online={len(online_leads):,}  combined={len(all_leads):,}", flush=True)

# Deduplicate: same opty_id can appear in both hist Excel and online sheets.
# keep='last' → online data wins for overlapping lids (more up-to-date).
if len(all_leads) > 0 and 'SorceLeadId' in all_leads.columns:
    _before_dedup = len(all_leads)
    all_leads = all_leads.drop_duplicates(subset=['SorceLeadId'], keep='last')
    _dedup_dropped = _before_dedup - len(all_leads)
    print(f"  STAGE 8 — deduplication by SorceLeadId: removed {_dedup_dropped:,} rows "
          f"(hist/online overlap), kept {len(all_leads):,}", flush=True)

# ── STAGE 9: unique leads going into aggregation ─────────────────────────────
print(f"  STAGE 9 — unique leads for aggregation: {len(all_leads):,}", flush=True)
if 'LeadMonth' in all_leads.columns:
    _final_lm = all_leads['LeadMonth'].value_counts(dropna=False).to_dict()
    print(f"  STAGE 9 — LeadMonth distribution in all_leads:", flush=True)
    for _lm_v, _lm_c in sorted(_final_lm.items(), key=lambda x: -x[1]):
        print(f"    {_lm_v!r:25s}: {_lm_c:,}", flush=True)

# Gap-fill is disabled: only real CRM leads appear in the dashboard.
# Retails without a matching CRM lead are counted only when a real lead record exists.
# This eliminates Unknown Source / Unknown Model / Unknown LeadType from all dimensions.
print(f"  Grand total: {len(all_leads):,}", flush=True)

import gc; gc.collect()
print("\nAggregating and pushing…", flush=True)
payload  = build_payload(all_leads, retail_map)

# ── Pre-push payload validation ───────────────────────────────────────────────
def _validate_payload(p):
    """Validate internal consistency of the payload and print a reconciliation table.

    Hard-fails via _fail_exit() if:
      - DMS+CO != Retails for any live month (>= ONLINE_START) in On Create
      - DMS+CO != Retails for any live month in On Update
      - Any expected live month has 0 leads in the payload

    Uses _fail_exit (not sys.exit) so the structured failure report is always printed
    and the staging path is captured when available.  _validate_payload is called before
    the staging file is written, so stg_path is always None at this stage.
    Reference values are printed for manual cross-check; they are NOT hard limits.
    """
    # Month labels are nested under payload['maps']['lm'], not at payload['lm'].
    lm_arr = p.get('maps', {}).get('lm', [])

    # monthly: rows = [lm_idx, leads, retails, dms, callout]
    # u_monthly: rows = [lm_idx, leads, retails, dms, callout]
    oc_by_lm  = {}   # On Create: lm_label -> [leads, rets, dms, co]
    ou_by_lm  = {}   # On Update: lm_label -> [leads, rets, dms, co]

    for row in p.get('monthly', []):
        lm = lm_arr[row[0]] if row[0] < len(lm_arr) else '?'
        prev = oc_by_lm.get(lm, [0,0,0,0])
        oc_by_lm[lm] = [prev[j] + row[1+j] for j in range(4)]

    for row in p.get('u_monthly', []):
        lm = lm_arr[row[0]] if row[0] < len(lm_arr) else '?'
        prev = ou_by_lm.get(lm, [0,0,0,0])
        ou_by_lm[lm] = [prev[j] + row[1+j] for j in range(4)]

    grand_leads = sum(v[0] for v in oc_by_lm.values())
    grand_rets  = sum(v[1] for v in oc_by_lm.values())
    grand_dms   = sum(v[2] for v in oc_by_lm.values())
    grand_co    = sum(v[3] for v in oc_by_lm.values())

    print("\n-- Pre-push payload reconciliation --------------------------------------------------", flush=True)
    print(f"  Grand totals  -- Leads: {grand_leads:,}  Retails: {grand_rets:,}  "
          f"DMS: {grand_dms:,}  CO: {grand_co:,}  DMS+CO: {grand_dms+grand_co:,}",
          flush=True)

    errors = []

    # DMS+CO vs Retails (On Create):
    # - Historical months (pre-ONLINE_START): hist Excel may have blank rtype -> informational only.
    # - Live months (ONLINE_START+): retail master provides Call Type for every row ->
    #   DMS+CO MUST equal Retails. Any gap means a '-' sentinel or unknown Call Type
    #   bypassed normalization and reached aggregation unclassified.
    _grand_unclassified = grand_rets - (grand_dms + grand_co)
    if _grand_unclassified > 0:
        print(f"  NOTE: {_grand_unclassified:,} retail records have unclassified Call Type "
              f"(DMS+CO={grand_dms+grand_co:,} vs Retails={grand_rets:,}). "
              f"Checking per-month for live months...", flush=True)

    for lm, oc in sorted(oc_by_lm.items(), key=lambda x: month_order(x[0])):
        if month_order(lm) < ONLINE_START_ORDER:
            continue   # historical -- blank rtype is normal
        if oc[1] == 0:
            continue   # no retails this month -- no check needed
        _unclass = oc[1] - (oc[2] + oc[3])
        if _unclass != 0:
            errors.append(
                f"LIVE DMS+CO != Retails [{lm} On Create]: "
                f"DMS={oc[2]:,}  CO={oc[3]:,}  DMS+CO={oc[2]+oc[3]:,}  "
                f"Retails={oc[1]:,}  diff={_unclass:+,}"
            )

    # DMS+CO vs Retails (On Update):
    # OU retail month is the performanceMonth (retail month), not lead-creation month.
    # Live retail months (rm >= ONLINE_START) must also be fully classified.
    _ou_grand_rets = sum(v[1] for v in ou_by_lm.values())
    _ou_grand_dms  = sum(v[2] for v in ou_by_lm.values())
    _ou_grand_co   = sum(v[3] for v in ou_by_lm.values())
    _ou_unclass    = _ou_grand_rets - (_ou_grand_dms + _ou_grand_co)
    if _ou_unclass > 0:
        print(f"  NOTE: {_ou_unclass:,} OU retail records have unclassified Call Type. "
              f"Checking per-month for live retail months...", flush=True)

    for lm, ou in sorted(ou_by_lm.items(), key=lambda x: month_order(x[0])):
        if month_order(lm) < ONLINE_START_ORDER:
            continue   # historical OU retail months are exempt
        if ou[1] == 0:
            continue
        _unclass_ou = ou[1] - (ou[2] + ou[3])
        if _unclass_ou != 0:
            errors.append(
                f"LIVE DMS+CO != Retails [{lm} On Update retail month]: "
                f"DMS={ou[2]:,}  CO={ou[3]:,}  DMS+CO={ou[2]+ou[3]:,}  "
                f"Retails={ou[1]:,}  diff={_unclass_ou:+,}"
            )

    # Verify every expected live month has leads in the payload
    _pay_lm_set = set(oc_by_lm.keys())
    for _elmo in globals().get('_expected_live_months', []):
        if _elmo not in _pay_lm_set:
            errors.append(f"Live month {_elmo!r} has 0 leads in payload (expected non-zero)")

    # Print monthly breakdown for all live months (dynamic -- no hardcoded month list).
    for mo in globals().get('_expected_live_months', []):
        if month_order(mo) < ONLINE_START_ORDER:
            continue
        oc = oc_by_lm.get(mo, [0,0,0,0])
        ou = ou_by_lm.get(mo, [0,0,0,0])
        _oc_unclass = oc[1] - (oc[2] + oc[3])
        _ou_unclass_mo = ou[1] - (ou[2] + ou[3])
        print(f"  {mo}  On Create  -- Leads: {oc[0]:>7,}  Retails: {oc[1]:>6,}  "
              f"DMS: {oc[2]:>5,}  CO: {oc[3]:>5,}"
              + (f"  UNCLASS={_oc_unclass:+,}" if _oc_unclass != 0 else ""),
              flush=True)
        print(f"  {mo}  On Update  -- Leads: {ou[0]:>7,}  Retails: {ou[1]:>6,}  "
              f"DMS: {ou[2]:>5,}  CO: {ou[3]:>5,}"
              + (f"  UNCLASS={_ou_unclass_mo:+,}" if _ou_unclass_mo != 0 else ""),
              flush=True)

    # Reference cross-check (informational -- NOT hard limits; update after each certified run).
    # Root cause of the 2026-08-22 payload issue: '-' sentinel in lead-sheet 'Retail By'
    # column was stored verbatim by old extract_rtype_map, overriding the correct 'Call Out'
    # from the retail master. Fixed in commit 97abaeb. These values reflect the corrected run.
    REF = {
        "Jul'26": {'oc_leads': 191541, 'oc_rets': 14182, 'ou_leads': None, 'ou_rets': 19054},
        "Aug'26": {'oc_leads': None,   'oc_rets': None,  'ou_leads': None, 'ou_rets': None},
    }
    print("  Reference cross-check (informational):", flush=True)
    for mo, ref in REF.items():
        oc = oc_by_lm.get(mo, [0,0,0,0])
        ou = ou_by_lm.get(mo, [0,0,0,0])
        if ref['oc_rets'] is not None:
            diff = oc[1] - ref['oc_rets']
            flag = '  <-- DRIFT' if abs(diff) > 500 else ''
            print(f"    {mo} On Create retails: {oc[1]:,}  ref={ref['oc_rets']:,}  "
                  f"diff={diff:+,}{flag}", flush=True)
        if ref['ou_rets'] is not None:
            diff = ou[1] - ref['ou_rets']
            flag = '  <-- DRIFT' if abs(diff) > 500 else ''
            print(f"    {mo} On Update retails: {ou[1]:,}  ref={ref['ou_rets']:,}  "
                  f"diff={diff:+,}{flag}", flush=True)

    if errors:
        print("\n  FATAL: payload internal consistency errors:", flush=True)
        for e in errors:
            print(f"    {e}", flush=True)
        print("-- End payload validation ------------------------------------------------------------", flush=True)
        # Use _fail_exit (not sys.exit) so the structured report is always printed.
        # Staging has not been written yet at this stage, so stg_path is None.
        _fail_exit('Payload validation -- DMS+CO mismatch in live months',
                   '\n'.join(errors))
    print("  Payload validation PASSED.", flush=True)
    print("-- End payload validation ------------------------------------------------------------", flush=True)
    return oc_by_lm, ou_by_lm

_oc, _ou = _validate_payload(payload)

# ─── Staging → Validate → Promote ─────────────────────────────────────────────
# ATOMICITY GUARANTEE: production is NEVER overwritten until Firebase confirms OK.
# The dashboard always shows a fully confirmed payload or the previous known-good one.
_data_dir     = Path(__file__).parent.parent / 'data'
_data_dir.mkdir(exist_ok=True)
_prod_path    = _data_dir / 'tvs_payload.json.gz'
_prev_path    = _data_dir / 'tvs_payload_prev.json.gz'
_staging_path = _data_dir / f'tvs_payload_staging_{_RUN_START.strftime("%Y%m%d_%H%M")}.json.gz'
_local_path   = Path(__file__).parent / 'tvs_last_payload.json'


# ── Serialise to local diagnostic copy (never committed) ──────────────────────
try:
    json_str = json.dumps(payload, separators=(',', ':'))
    print(f"\nPayload size: {len(json_str) // 1024:,} KB", flush=True)
    with open(_local_path, 'w', encoding='utf-8') as _f:
        json.dump(payload, _f)
    print(f"  Diagnostic JSON → {_local_path.name}", flush=True)
except Exception as _ser_err:
    _fail_exit('JSON serialisation', str(_ser_err))

# ── Write STAGING file (not production) ───────────────────────────────────────
try:
    with gzip.open(_staging_path, 'wt', encoding='utf-8', compresslevel=6) as _f:
        json.dump(payload, _f, separators=(',', ':'))
    _stg_kb = _staging_path.stat().st_size // 1024
    print(f"  Staging → {_staging_path.name}  ({_stg_kb:,} KB)", flush=True)
except Exception as _stg_err:
    _fail_exit('Staging write', str(_stg_err))

# ── Validate staging (read back and verify structure) ─────────────────────────
try:
    with gzip.open(_staging_path, 'rt', encoding='utf-8') as _f:
        _stg = json.load(_f)
    _missing = {'maps', 'monthly', 'u_monthly'} - set(_stg.keys())
    if _missing:
        raise RuntimeError(f"Missing required keys: {_missing}")
    if not _stg.get('maps', {}).get('lm'):
        raise RuntimeError("maps.lm is empty — month-label array missing")
    _stg_leads   = sum(r[1] for r in _stg.get('monthly', []))
    _stg_retails = sum(r[2] for r in _stg.get('monthly', []))
    print(f"  Staging readback OK  (leads={_stg_leads:,}  retails={_stg_retails:,})", flush=True)
    del _stg
except Exception as _rb_err:
    _fail_exit('Staging readback', str(_rb_err), _staging_path)

# ── DRY RUN: stop here, discard staging ───────────────────────────────────────
if DRY_RUN:
    _staging_path.unlink(missing_ok=True)
    _elapsed_dr = int((datetime.now(timezone.utc) - _RUN_START).total_seconds())
    print(f"\n{'=' * 60}", flush=True)
    print("TVS DATA PIPELINE — DRY RUN COMPLETE  (production unchanged)", flush=True)
    print(f"{'=' * 60}", flush=True)
    print(f"Timestamp: {_RUN_START.strftime('%Y-%m-%d %H:%M UTC')}  "
          f"Runtime: {_elapsed_dr // 60}m {_elapsed_dr % 60}s", flush=True)

    print(f"\nSOURCE STATUS", flush=True)
    print(f"  Historical leads:    {len(hist_leads):>10,}", flush=True)
    for _lbl_m, _m_info in sorted(_current_metrics.items(), key=lambda x: x[0]):
        _prev_r = _prev_metrics.get(_lbl_m, {}).get('rows') if isinstance(_prev_metrics.get(_lbl_m), dict) \
            else _prev_metrics.get(_lbl_m)
        _prev_str = f"prev {_prev_r:,}" if _prev_r is not None else "no baseline"
        print(f"  {_lbl_m:30s}: {_m_info['rows']:>10,}  ({_prev_str})", flush=True)

    print(f"\nSTAGING VALIDATION", flush=True)
    print(f"  Staging leads:      {_stg_leads:>10,}", flush=True)
    print(f"  Staging retails:    {_stg_retails:>10,}", flush=True)

    print(f"\nLIVE MONTHS RECONCILIATION", flush=True)
    for _mo in _expected_live_months:
        _oc_v = _oc.get(_mo, [0,0,0,0])
        _ou_v = _ou.get(_mo, [0,0,0,0])
        print(f"  {_mo} On Create — Leads: {_oc_v[0]:>8,}  Retails: {_oc_v[1]:>7,}  "
              f"DMS: {_oc_v[2]:>6,}  CO: {_oc_v[3]:>6,}", flush=True)
        print(f"  {_mo} On Update — Leads: {_ou_v[0]:>8,}  Retails: {_ou_v[1]:>7,}  "
              f"DMS: {_ou_v[2]:>6,}  CO: {_ou_v[3]:>6,}", flush=True)

    print(f"\nPRODUCTION: UNCHANGED  (dry run — Firebase not called)", flush=True)

    # ── Fetch telemetry (Phase 13) ───────────────────────────────────────────
    print(f"\nFETCH TELEMETRY", flush=True)
    _seq_total_s_dr = 0.0
    for _flbl, _fp in sorted(_fetch_perf.items()):
        _fd = _fp['duration_s']
        _fr = _fp['rows']
        _seq_total_s_dr += _fd
        print(f"  {_flbl:30s}: {_fd:6.1f}s  {_fr:>10,} rows", flush=True)
    print(f"  {'Parallel wall-clock':30s}: {_parallel_elapsed:6.1f}s  "
          f"(saved {max(0.0, _seq_total_s_dr - _parallel_elapsed):.1f}s vs sequential)", flush=True)
    print(f"  {'Apps Script calls (total)':30s}: {_as_calls_total:>6,}", flush=True)

    # ── UNKNOWN MODEL DIAGNOSTIC ─────────────────────────────────────────────
    # Payload baseline: Unknown model totals as of the last production push (2026-08-24).
    # 1,886 = Aug'26 Unknown leads visible in dashboard (on-create view for Aug only).
    # 1,980 = full payload Unknown leads across all live months (Jul'26: 94 + Aug'26: 1,886).
    _PAYLOAD_UNK_LEADS = 1980
    _PAYLOAD_UNK_RETS  = 70

    print(f"\n{'=' * 60}", flush=True)
    print(f"UNKNOWN MODEL DIAGNOSTIC", flush=True)
    print(f"{'=' * 60}", flush=True)
    print(f"  Payload baseline (last prod push 2026-08-24): {_PAYLOAD_UNK_LEADS:,} leads  {_PAYLOAD_UNK_RETS:,} retails", flush=True)
    print(f"  (1,886 = Aug'26 on-create view; 1,980 = all live months in payload mm-array)", flush=True)

    if _unk_mdl_detail:
        _unk_total_leads = sum(v['leads'] for v in _unk_mdl_detail.values())
        _unk_total_rets  = sum(v['rets']  for v in _unk_mdl_detail.values())
        print(f"\n  This run — {len(_unk_mdl_detail)} distinct raw value(s) produced Unknown:", flush=True)
        print(f"  Total leads: {_unk_total_leads:,}   Total retails: {_unk_total_rets:,}", flush=True)
        _ldiff = _unk_total_leads - _PAYLOAD_UNK_LEADS
        _rdiff = _unk_total_rets  - _PAYLOAD_UNK_RETS
        print(f"  vs baseline: leads {_ldiff:+,}   retails {_rdiff:+,}", flush=True)
        print(flush=True)
        print(f"  {'raw_value (repr)':<52}  {'reason':<12}  {'leads':>7}  {'rets':>5}", flush=True)
        print(f"  {'-' * 84}", flush=True)
        _diag_rows = []
        for _rv, _rd in sorted(_unk_mdl_detail.items(), key=lambda x: -x[1]['leads']):
            _reason = _unk_mdl_reasons.get(_rv, 'EMPTY' if not _rv else 'LEAD_MAP_EMPTY')
            print(f"  {_rd['raw_repr']:<52}  {_reason:<12}  {_rd['leads']:>7,}  {_rd['rets']:>5,}", flush=True)
            for _mo, _mv in sorted(_rd['by_month'].items()):
                print(f"      month {_mo:<10}  leads={_mv[0]:>6,}  rets={_mv[1]:>4,}", flush=True)
            for _src, _sv in sorted(_rd['by_src'].items(), key=lambda x: -x[1][0]):
                print(f"      src   {_src:<16} leads={_sv[0]:>6,}  rets={_sv[1]:>4,}", flush=True)
            _diag_rows.append({
                'raw_repr': _rd['raw_repr'],
                'reason': _reason,
                'leads': _rd['leads'],
                'rets':  _rd['rets'],
                'by_month': {m: {'leads': v[0], 'rets': v[1]} for m, v in _rd['by_month'].items()},
                'by_src':   {s: {'leads': v[0], 'rets': v[1]} for s, v in _rd['by_src'].items()},
            })
        _diag_path = Path(__file__).parent / 'unknown_model_diagnostic.json'
        try:
            _diag_out = {
                'date': _RUN_START.strftime('%Y-%m-%d'),
                'payload_baseline': {'leads': _PAYLOAD_UNK_LEADS, 'rets': _PAYLOAD_UNK_RETS},
                'this_run': {'leads': _unk_total_leads, 'rets': _unk_total_rets},
                'distinct_raw_values': len(_unk_mdl_detail),
                'rows': _diag_rows,
            }
            with open(_diag_path, 'w', encoding='utf-8') as _df:
                json.dump(_diag_out, _df, indent=2, ensure_ascii=False)
            print(f"\n  Diagnostic written to {_diag_path.name}", flush=True)
        except Exception as _de:
            print(f"\n  WARNING: could not write diagnostic JSON: {_de}", flush=True)
    else:
        print(f"\n  No Unknown model leads found in this run.", flush=True)

    print(f"\n{'=' * 60}", flush=True)
    sys.exit(0)

# ── Compress payload for Firebase POST ────────────────────────────────────────
_raw_bytes  = json_str.encode('utf-8')
_compressed = gzip.compress(_raw_bytes, compresslevel=6)
_envelope   = json.dumps({'gz': base64.b64encode(_compressed).decode('ascii')},
                          separators=(',', ':'))
print(f"\nPOSTing to Apps Script… ({len(_envelope) // 1024:,} KB compressed, "
      f"was {len(json_str) // 1024:,} KB raw)", flush=True)
del _raw_bytes, _compressed, json_str

body = None
for _attempt in range(3):
    try:
        _resp = requests.post(
            APPS_SCRIPT_URL,
            data=_envelope.encode('utf-8'),
            params={'secret': SECRET},
            headers={'Content-Type': 'application/json'},
            timeout=(60, 1800),
        )
        _resp.raise_for_status()
        body = _resp.text
        break
    except Exception as _e:
        print(f"  POST attempt {_attempt + 1} failed: {_e}", flush=True)
        if _attempt < 2:
            print("  Retrying in 30s…", flush=True)
            time.sleep(30)
        else:
            traceback.print_exc()
            _fail_exit('Firebase POST (all 3 attempts exhausted)', str(_e), _staging_path)
print(f"Response: {body[:500]}{'…' if body and len(body) > 500 else ''}", flush=True)

# ── Validate Apps Script response ─────────────────────────────────────────────
# Response contract has two accepted forms:
#   Old: {"ok": true, ...}
#   New: {"t": "<iso>", "rt_cols": <int>, "maps": {"lm": [...], ...}}
# Either form proves the payload was received and Firebase was written.
# Any POST that succeeds at the HTTP layer but returns neither form is rejected
# so production is never promoted on an ambiguous write confirmation.
_resp_ok, _resp_detail, _resp_parsed = _validate_post_response(body)
print(f"Response validation: {_resp_detail}", flush=True)
if not _resp_ok:
    # The POST may have reached Apps Script before the response was corrupted.
    # Do NOT resend — risk of duplicate write.  Re-run the full pipeline instead.
    _fail_exit(
        'Firebase response',
        _resp_detail,
        _staging_path,
        firebase_cloud_state='UNKNOWN — POST reached Apps Script; verify manually',
    )

# ── Firebase confirmed — PROMOTE STAGING → PRODUCTION ─────────────────────────
_prev_existed = _prod_path.exists()
if _prev_existed:
    shutil.copy2(_prod_path, _prev_path)
    print(f"  Backed up previous → {_prev_path.name}", flush=True)

shutil.move(str(_staging_path), str(_prod_path))
print(f"  Promoted staging   → {_prod_path.name}  ({_prod_path.stat().st_size // 1024:,} KB)",
      flush=True)

# ── Save source metrics (only on actual publish, not dry-run) ─────────────────
if not DRY_RUN:
    _save_source_metrics(_current_metrics, _RUN_START)

# ── Write Unknown-model diagnostic (production path) ─────────────────────────
# Written alongside source_metrics so it lands in the same Keep-repo-active
# commit.  A failure here must never abort a production push — hence try/except.
if not DRY_RUN and _unk_mdl_detail:
    _diag_path = Path(__file__).parent / 'unknown_model_diagnostic.json'
    try:
        _diag_rows_prod = []
        for _rv, _rd in sorted(_unk_mdl_detail.items(), key=lambda x: -x[1]['leads']):
            _reason = _unk_mdl_reasons.get(_rv, 'EMPTY' if not _rv else 'LEAD_MAP_EMPTY')
            _diag_rows_prod.append({
                'raw_repr':  _rd['raw_repr'],
                'reason':    _reason,
                'leads':     _rd['leads'],
                'rets':      _rd['rets'],
                'by_month':  {m: {'leads': v[0], 'rets': v[1]}
                              for m, v in _rd['by_month'].items()},
                'by_src':    {s: {'leads': v[0], 'rets': v[1]}
                              for s, v in _rd['by_src'].items()},
            })
        _unk_prod_leads = sum(v['leads'] for v in _unk_mdl_detail.values())
        _unk_prod_rets  = sum(v['rets']  for v in _unk_mdl_detail.values())
        _diag_out_prod  = {
            'date':                 _RUN_START.strftime('%Y-%m-%d'),
            'total_unknown_leads':  _unk_prod_leads,
            'total_unknown_rets':   _unk_prod_rets,
            'distinct_raw_values':  len(_unk_mdl_detail),
            'rows':                 _diag_rows_prod,
        }
        with open(_diag_path, 'w', encoding='utf-8') as _dfd:
            json.dump(_diag_out_prod, _dfd, indent=2, ensure_ascii=False)
        print(f"  Unknown model diagnostic → {_diag_path.name}  "
              f"({len(_unk_mdl_detail)} raw value(s), {_unk_prod_leads:,} Unknown leads)",
              flush=True)
    except Exception as _diag_err:
        print(f"  WARNING: could not write unknown_model_diagnostic.json: {_diag_err}",
              flush=True)

# ── Structured success report ─────────────────────────────────────────────────
_elapsed       = int((datetime.now(timezone.utc) - _RUN_START).total_seconds())
_grand_leads   = sum(v[0] for v in _oc.values())
_grand_retails = sum(v[1] for v in _oc.values())
_live_lm_dist  = (online_leads['LeadMonth'].value_counts().to_dict()
                  if 'LeadMonth' in online_leads.columns and len(online_leads) > 0 else {})

print(f"\n{'=' * 60}", flush=True)
print("TVS DATA PIPELINE — SUCCESS", flush=True)
print(f"{'=' * 60}", flush=True)
print(f"Timestamp: {_RUN_START.strftime('%Y-%m-%d %H:%M UTC')}  "
      f"Runtime: {_elapsed // 60}m {_elapsed % 60}s", flush=True)

print(f"\nSOURCE STATUS", flush=True)
print(f"  Historical leads:    {len(hist_leads):>10,}", flush=True)
for _lbl_m, _m_info in sorted(_current_metrics.items(), key=lambda x: x[0]):
    _prev_r = _prev_metrics.get(_lbl_m, {}).get('rows') if isinstance(_prev_metrics.get(_lbl_m), dict) \
        else _prev_metrics.get(_lbl_m)
    _prev_str = f"prev {_prev_r:,}" if _prev_r is not None else "no baseline"
    print(f"  {_lbl_m:30s}: {_m_info['rows']:>10,}  ({_prev_str})", flush=True)

print(f"\nMONTH RECONCILIATION (On Create)", flush=True)
for _mo in sorted(_oc.keys(), key=month_order):
    _oc_v = _oc[_mo]
    _is_live = '(live)' if month_order(_mo) >= ONLINE_START_ORDER else '(hist)'
    print(f"  {_mo} {_is_live:6s} — Leads: {_oc_v[0]:>7,}  Retails: {_oc_v[1]:>6,}  "
          f"DMS: {_oc_v[2]:>5,}  CO: {_oc_v[3]:>5,}", flush=True)

print(f"\nLIVE MONTHS — ON CREATE vs ON UPDATE", flush=True)
for _mo in _expected_live_months:
    _oc_v = _oc.get(_mo, [0,0,0,0])
    _ou_v = _ou.get(_mo, [0,0,0,0])
    print(f"  {_mo} On Create — Leads: {_oc_v[0]:>8,}  Retails: {_oc_v[1]:>7,}  "
          f"DMS: {_oc_v[2]:>6,}  CO: {_oc_v[3]:>6,}", flush=True)
    print(f"  {_mo} On Update — Leads: {_ou_v[0]:>8,}  Retails: {_ou_v[1]:>7,}  "
          f"DMS: {_ou_v[2]:>6,}  CO: {_ou_v[3]:>6,}", flush=True)

print(f"\nAGGREGATION TOTALS", flush=True)
print(f"  Total unique leads:         {_grand_leads:>10,}", flush=True)
print(f"  Total retails (On Create):  {_grand_retails:>10,}", flush=True)
print(f"  Combined retail map:        {len(retail_map):>10,}", flush=True)

print(f"\nPUBLICATION", flush=True)
print(f"  Firebase:  CONFIRMED ({_resp_detail})", flush=True)
print(f"  Payload:   {_prod_path.name}  ({_prod_path.stat().st_size // 1024:,} KB)", flush=True)
if _prev_existed:
    print(f"  Previous:  backed up to {_prev_path.name}", flush=True)

# ── Phase 13: Per-source fetch telemetry ─────────────────────────────────────
print(f"\nFETCH TELEMETRY", flush=True)
_seq_total_s  = 0.0
for _flbl, _fp in sorted(_fetch_perf.items()):
    _fd = _fp['duration_s']
    _fr = _fp['rows']
    _seq_total_s += _fd
    print(f"  {_flbl:30s}: {_fd:6.1f}s  {_fr:>10,} rows", flush=True)
print(f"  {'Parallel wall-clock':30s}: {_parallel_elapsed:6.1f}s  "
      f"(saved {max(0.0, _seq_total_s - _parallel_elapsed):.1f}s vs sequential)", flush=True)
print(f"  {'Apps Script calls (total)':30s}: {_as_calls_total:>6,}", flush=True)

print(f"\nSTATUS: PRODUCTION UPDATED", flush=True)
print(f"{'=' * 60}", flush=True)
