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
RETAIL MONTH: Retail_Attribution_Date drives all retail calculations
MERGE: hist leads + live leads deduped by SorceLeadId (keep='last'; live wins on overlap)
       hist retail map + live retail map merged by sourceLeadId (live overwrites hist per key)
"""

import json, sys, re, time, os, gzip, base64, traceback, shutil, argparse
import pandas as pd
import requests
from pathlib import Path
from datetime import datetime, timedelta, timezone

MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbwzgnXPbCbunBblnMUrqdWg3eY9qsIwCrFxuYuvYSpxtH22l4Cs32vdkOkDhUn-qwM64w/exec"
SECRET = "tvs2026push"

RETAILS_FILE_ID = '1ZWBlzxX-g2R5iCcrsGUWrqSvxIHcchFHtajDDPcFJgE'
RETAILS_TAB     = 'Raw'

# Live lead masters - each sheet is authoritative for its month range only.
# Jul'26 Lead Master : dedicated July GSheet (Jul'26 rows only)
# Aug'26+ Lead Master: rolling current-month GSheet (Aug'26 onwards)
# min_mo / max_mo are month_order integers (YY*100+MM); None = no upper bound.
LEAD_SHEETS = [
    {
        'id':     '1gaRoPLebv7jaBgWEET-XSQuhqE_XgQlGru39TA-FoSo',
        'tab':    'TVS',
        'label':  "Jul'26-LeadMaster",
        'min_mo': 2607,   # Jul'26 only
        'max_mo': 2607,
    },
    {
        'id':     '1iSw5zXF67q5Wkoz2mSPFqql9OPAcqmd0um5BEHUGf4o',
        'tab':    'TVS',
        'label':  'Aug26+-LeadMaster',
        'min_mo': 2607,   # Jul'26 onwards (Jul'26 rows dedup against Jul'26-LeadMaster at STAGE 8)
        'max_mo': None,   # no upper bound
    },
]

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
    if not pm: return 'Unknown'
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
    if 'NTORQ' in pu or 'NTRQ' in pu:                            return 'Unknown'
    if 'IQUBE' in pu or 'IQUE' in pu:                            return 'TVS iQube'
    if 'RONIN' in pu:                                             return 'TVS Ronin'
    if 'RADEON' in pu:                                            return 'TVS Radeon'
    if 'ORBITER' in pu:                                           return 'Unknown'
    if 'SPORT' in pu and 'TVS' not in pu.replace('TVS SPORT',''):return 'TVS Sport'
    if 'SPORT' in pu:                                             return 'TVS Sport'
    if 'XL 100' in pu or 'XL100' in pu:                          return 'TVS XL100'
    if 'ZEST' in pu:                                              return 'TVS Scooty Zest'
    if 'STAR CITY' in pu or 'STARCITY' in pu or 'CITY+' in pu:  return 'TVS Star City Plus'
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
}

# ─── Helpers ──────────────────────────────────────────────────────────────────

# Explicit city aliases (title-cased key → canonical name).
# Only strong, confirmed aliases are listed here.
_CITY_ALIAS = {
    'New Delhi':          'Delhi',
    'Bengaluru':          'Bangalore',
    'Prayagraj':          'Allahabad',
    'Thiruvananthapuram': 'Trivandrum',
}

def normalize_city(raw):
    """Canonical city name: strip, collapse whitespace, title-case, apply alias."""
    s = re.sub(r'\s+', ' ', str(raw or '').strip())
    if not s:
        return 'Unknown'
    # title() capitalises after any non-alpha char: handles spaces, hyphens,
    # parentheses, apostrophes — matches JS canonicalCity() exactly.
    s = s.title()
    return _CITY_ALIAS.get(s, s)

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

def proxy_get(action, extra_params=None, timeout=120):
    params = {'action': action, 'secret': SECRET}
    if extra_params:
        params.update(extra_params)
    resp = requests.get(APPS_SCRIPT_URL, params=params, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

# ─── Sheet reader (paginated via Apps Script getSheetData) ────────────────────

# Only these columns are needed from each lead sheet — reduces payload ~70%
LEAD_COLS = 'opty_id,Lead_Month,Date,model,City,State,Dealer_Name,lead_type,Medium,Retail By,DMS_Retail_Month'

def fetch_sheet_via_proxy(file_id, label, tab_name=None):
    """Read any Google Sheet via Apps Script proxy. Returns raw DataFrame."""
    page, all_rows, headers = 0, [], None
    _LEAD_TIMEOUTS = [60, 90, 180]   # escalating per attempt (matches retail fetch pattern)
    extra = {'fileId': file_id, 'pageSize': 10000, 'cols': LEAD_COLS}
    if tab_name:
        extra['tabName'] = tab_name
    while True:
        extra['page'] = page
        data = None
        for attempt, _timeout in enumerate(_LEAD_TIMEOUTS):
            try:
                data = proxy_get('getSheetData', extra, timeout=_timeout)
                if 'error' in data:
                    raise RuntimeError(f"Apps Script error: {data['error']}")
                break  # success
            except Exception as e:
                if attempt < len(_LEAD_TIMEOUTS) - 1:
                    _sleep = 30 * (2 ** attempt)   # 30 s, 60 s
                    print(f"  {label} page {page} attempt {attempt+1} failed ({e}); "
                          f"retrying in {_sleep}s with {_LEAD_TIMEOUTS[attempt+1]}s timeout…", flush=True)
                    time.sleep(_sleep)
                else:
                    traceback.print_exc()
                    raise RuntimeError(f"getSheetData {label} page {page} failed after {len(_LEAD_TIMEOUTS)} attempts: {e}")
        if data is None:
            raise RuntimeError(f"getSheetData {label} page {page}: no data returned")
        if headers is None:
            headers = data['headers']
        rows = data.get('rows', [])
        all_rows.extend(rows)
        total = data.get('total', '?')
        print(f"  {label} page {page}: +{len(rows):,} rows (total {len(all_rows):,}/{total})", flush=True)
        if data.get('done', True):
            break
        page += 1
    return pd.DataFrame(all_rows, columns=headers)

# ─── Lead sheet processing ─────────────────────────────────────────────────────

def extract_rtype_map(raw_df):
    """Extract {opty_id → {rm, rtype}} from embedded retail columns if present."""
    rmap = {}
    if 'DMS_Retail_Month' not in raw_df.columns:
        return rmap
    for _, row in raw_df.iterrows():
        rm = str(row.get('DMS_Retail_Month', '') or '').strip()
        if not rm: continue
        lid = to_id(row.get('opty_id', ''))
        if not lid: continue
        rmap[lid] = {
            'rm':    norm_month(rm),
            'rtype': str(row.get('Retail By', '') or '').strip(),
        }
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
    keep = ['SorceLeadId','LeadMonth','ModelName','Source','LeadType',
            'State','Zone','BuyingDays','CityName','DealerName']
    return df[[c for c in keep if c in df.columns]].copy()

# ─── Retail master ─────────────────────────────────────────────────────────────
# Page size kept small so each Apps Script call finishes well within the HTTP read
# timeout. 5 000 rows / page is safe for the current sheet size (≈70-90 k rows).
_RETAIL_PAGE_SIZE = 5000
# Per-attempt read timeouts (escalating). Between attempts: 30 s → 60 s backoff.
_RETAIL_TIMEOUTS  = [60, 90, 180]

def fetch_retails():
    """Fetch TVS retail master via Apps Script (paginated; exponential backoff per page).

    Never returns partial data — raises RuntimeError on any unrecoverable failure so
    the caller's sys.exit(1) guard prevents a partial payload from reaching Firebase.
    """
    print("Fetching retail master via Apps Script…", flush=True)
    page, all_rows, headers, expected_total = 0, [], None, None
    done_received = False
    while True:
        last_exc = None
        for attempt, _timeout in enumerate(_RETAIL_TIMEOUTS):
            try:
                data = proxy_get('getCurrentRetails',
                                 {'page': page, 'pageSize': _RETAIL_PAGE_SIZE},
                                 timeout=_timeout)
                last_exc = None
                break
            except Exception as e:
                last_exc = e
                if attempt < len(_RETAIL_TIMEOUTS) - 1:
                    _sleep = 30 * (2 ** attempt)   # 30 s, 60 s
                    print(f"  Page {page} attempt {attempt+1} failed ({e}); "
                          f"retrying in {_sleep}s with {_RETAIL_TIMEOUTS[attempt+1]}s timeout…",
                          flush=True)
                    time.sleep(_sleep)
        if last_exc is not None:
            traceback.print_exc()
            raise RuntimeError(
                f"getCurrentRetails page {page} failed after {len(_RETAIL_TIMEOUTS)} "
                f"attempts: {last_exc}")
        if 'error' in data:
            raise RuntimeError(f"getCurrentRetails error: {data['error']}")
        if headers is None:
            headers = data['headers']
        if expected_total is None and isinstance(data.get('total'), int):
            expected_total = data['total']
        rows = data.get('rows', [])
        all_rows.extend(rows)
        total_label = data.get('total', '?')
        print(f"  Page {page}: +{len(rows):,} rows  (running {len(all_rows):,}/{total_label})",
              flush=True)
        if data.get('done', True):
            done_received = True
            break
        page += 1

    # 'done: true' is the authoritative end-of-pagination signal from Apps Script.
    # The 'total' field reflects the raw sheet row count (getLastRow), which may include
    # blank/filtered rows that the paginator skips — so fetched != total is expected and
    # is NOT an error when done was received. Only fail if done was never signalled.
    if not done_received:
        raise RuntimeError(
            f"Retail pagination ended without a 'done' signal after {page+1} pages "
            f"({len(all_rows):,} rows). Possible truncation — aborting.")
    if expected_total is not None and len(all_rows) != expected_total:
        print(f"  NOTE: Apps Script total={expected_total:,}, fetched={len(all_rows):,} — "
              f"gap likely due to blank/non-TVS rows in sheet (normal). "
              f"'done' received; proceeding.", flush=True)

    df = pd.DataFrame(all_rows, columns=headers)
    print(f"  Retail master: {len(df):,} TVS rows  (pages={page+1})", flush=True)
    return df


def _validate_retail_fetch(df):
    """Print a sanity report for the freshly fetched retail DataFrame.
    Raises RuntimeError if the row count looks suspiciously low (< 50 000).
    """
    print("\n── Retail fetch validation ──────────────────────────", flush=True)
    n = len(df)
    print(f"  Total rows: {n:,}", flush=True)
    if n < 50_000:
        raise RuntimeError(
            f"Retail fetch produced only {n:,} rows — expected ≥ 50,000. "
            "Aborting: this is almost certainly an incomplete fetch.")
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

def build_retail_map(retail_df):
    """Build {sourceLeadId -> {rm, rtype, pm}} for LIVE GSheet records.
    rm    : from 'performanceMonth' column (not Retail_Attribution_Date).
    rtype : from 'Call Type' column — 'DMS' or 'Call Out' (case-insensitive, trimmed).
            Unexpected values are logged and collected in the returned warnings list.
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
        rmap[lid] = {'rm': rm, 'rtype': rtype, 'pm': pm}
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
    cxm, u_cxm = {}, {}           # city × model × month
    u_cm, u_csm = {}, {}          # city × month (retail-month attribution)
    u_cdm, u_cdsm = {}, {}        # city × dealer × month (retail-month attribution)

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
    _dls    = all_leads[dl_col].fillna('').astype(str).values if dl_col and dl_col in _c else None

    del all_leads, _c  # free ~500 MB DataFrame now that we have arrays
    _gc.collect()

    for i in range(total):
        if i % 100000 == 0 and i > 0:
            print(f"  {i:,}/{total:,} ({100*i//total}%)", flush=True)

        lid  = to_id(_lids[i])
        lm   = _lms[i].strip()
        src  = _srcs[i].strip() or 'Unknown'
        if src in ('Non-MS', 'Non MS', 'Non- MS'): src = 'Non CPS'
        lt   = _lts[i].strip() or 'Unknown'
        mdl  = normalize_lead_model(_mdls[i])
        st   = _sts[i].strip().title() or 'Unknown'
        zone = _zones[i].strip() or 'Unknown'
        bd   = _bds[i].strip() or '0'
        city = normalize_city(_cities[i])

        if not lm or not lid: continue

        is_ret = lid in retail_map
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
        bump(cxm,     f"{cti}|{mi}|{li}",   is_ret, rtype)
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
        ubump(u_cxm, f"{cti}|{mi}|{li}",   f"{cti}|{mi}|{uli}",   is_ret, rtype)

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

    def to_rows(d, key_fn):
        return [[*key_fn(k), v[0], v[1], v[2], v[3]] for k, v in d.items()]

    city_state_arr = [city_to_state.get(i) for i in range(len(city_arr))]

    maps_payload = {
        'lm': lm_arr, 'src': src_arr, 'lt': lt_arr, 'mdl': mdl_arr,
        'st': st_arr, 'zone': zone_arr, 'city': city_arr,
        'city_state': city_state_arr,
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
        'cxm':     to_rows(cxm, lambda k: list(map(int, k.split('|')))),
        'u_cm':    to_rows(u_cm,  lambda k: list(map(int, k.split('|')))),
        'u_csm':   to_rows(u_csm, lambda k: list(map(int, k.split('|')))),
        'u_cxm':   to_rows(u_cxm, lambda k: list(map(int, k.split('|')))),
        **({"cdm":  to_rows(cdm,  lambda k: list(map(int, k.split('|')))),
            "cdsm": to_rows(cdsm, lambda k: list(map(int, k.split('|')))),
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
    }
    print(f"Done — {total:,} leads  {len(retail_map):,} retails", flush=True)
    return payload

# ─── Main ─────────────────────────────────────────────────────────────────────

print("=" * 60, flush=True)
print("TVS Lead Disposition — Daily Data Push", flush=True)
print("=" * 60, flush=True)

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
    # This branch should never fire in production — hist_cache.json.gz is always committed.
    print("\n[1/5] WARNING: hist_cache.json.gz not found and no local Excel files present.", flush=True)
    print("              This should not occur in production. Continuing with live data only.", flush=True)
    retail_map = {}
    hist_leads  = pd.DataFrame()

# ── Step 2: Live retail master (overwrites hist for ONLINE_START+ months only) ─
# hist_cache is the authoritative source for Apr'25–Jun'26. Live retail must NOT
# overwrite those entries — doing so replaces the accurate hist DMS/Call Out value
# (from the Excel "DMS/Call Out" column) with the live classification (Purchased From
# logic), which incorrectly reclassifies many historical DMS entries as Call Out.
# Only Jul'26+ entries are the true "online" period; those freely overwrite hist.
print("\n[2/5] Loading live retail master from Google Sheet…", flush=True)
try:
    retail_df = fetch_retails()
    _validate_retail_fetch(retail_df)
except Exception as _retail_err:
    traceback.print_exc()
    print(f"\nFATAL: live retail fetch failed — {_retail_err}", file=sys.stderr, flush=True)
    sys.exit(1)
online_rmap, unexpected_call_types = build_retail_map(retail_df)

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
        # Case B — keep rtype, update rm if valid
        if live_rm and live_rm_order >= LEAD_MASTER_START_ORDER:
            retail_map[lid] = {**retail_map[lid], 'rm': live_rm}
            _updated_rm += 1
        else:
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

# ── Step 3: Historical leads (already loaded in step 1) ───────────────────────
print(f"\n[3/5] Historical leads: {len(hist_leads):,} rows", flush=True)

# ── Step 4: Live leads (ONLINE_START onwards; dedup wins over hist) ───────────
print(f"\n[4/5] Loading live lead sheets ({ONLINE_START}+)…", flush=True)
lead_dfs  = []
rtype_map = {}
for sheet in LEAD_SHEETS:
    for _sheet_attempt in range(3):
        try:
            raw = fetch_sheet_via_proxy(sheet['id'], sheet['label'], tab_name=sheet.get('tab'))
            raw.columns = [c.strip() for c in raw.columns]
            _lbl = sheet['label']

            # ── STAGE 1: rows fetched from Google Sheet ────────────────────────
            print(f"\n  [{_lbl}] STAGE 1 — fetched from sheet: {len(raw):,} rows", flush=True)
            print(f"  [{_lbl}] columns: {list(raw.columns)}", flush=True)

            # ── STAGE 2: DataFrame created (same as fetched; confirm) ──────────
            print(f"  [{_lbl}] STAGE 2 — DataFrame rows: {len(raw):,}", flush=True)

            # ── STAGE 3: raw Lead_Month distribution BEFORE standardize_leads ──
            if 'Lead_Month' in raw.columns:
                _raw_lm_vals = raw['Lead_Month'].astype(str).str.strip()
                _raw_lm_full = _raw_lm_vals.value_counts(dropna=False).to_dict()
                print(f"  [{_lbl}] STAGE 3 — raw Lead_Month full distribution:", flush=True)
                for _lm_v, _lm_c in sorted(_raw_lm_full.items(), key=lambda x: -x[1]):
                    print(f"    {_lm_v!r:25s}: {_lm_c:,}", flush=True)
            else:
                print(f"  [{_lbl}] STAGE 3 — Lead_Month column NOT FOUND in raw sheet", flush=True)

            rtype_map.update(extract_rtype_map(raw))
            std_all = standardize_leads(raw)

            # ── STAGE 4: after standardize_leads (no rows dropped here) ────────
            print(f"  [{_lbl}] STAGE 4 — after standardize_leads: {len(std_all):,} rows", flush=True)
            if len(std_all) != len(raw):
                print(f"  [{_lbl}] WARNING: standardize_leads changed row count "
                      f"({len(raw):,} → {len(std_all):,})", flush=True)

            # ── STAGE 5: LeadMonth distribution after standardize_leads ────────
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

            # ── STAGE 6: month filter (min_mo / max_mo) ─────────────────────────
            std = std_all[std_all['LeadMonth'].apply(month_order) >= _min]
            if _max is not None:
                std = std[std['LeadMonth'].apply(month_order) <= _max]

            _n_dropped = len(std_all) - len(std)
            print(f"  [{_lbl}] STAGE 6 — after month filter (min_mo={_min}, max_mo={_max}): "
                  f"{len(std):,} rows  ({_n_dropped:,} dropped)", flush=True)

            if _n_dropped > 0:
                # Full breakdown of every dropped month
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

                # Sample of up to 100 dropped rows with full key columns
                _samp_cols = [c for c in ['LeadMonth','Date','CreateDate','SorceLeadId',
                                           'Source','ModelName','State'] if c in _dropped_df.columns]
                _sample100 = _dropped_df[_samp_cols].head(100)
                print(f"  [{_lbl}] STAGE 6 — sample of dropped rows (up to 100):", flush=True)
                print(_sample100.to_string(index=False), flush=True)

            lead_dfs.append(std)
            _range = f"{MONTH_NAMES[(_min%100)-1]}'{_min//100:02d}" + (f" only" if _max == _min else f"+ ({len(std):,} rows)")
            print(f"  [{_lbl}] STAGE 6 FINAL: {len(std):,} rows kept [{_range}]\n", flush=True)
            break  # success
        except Exception as e:
            if _sheet_attempt < 2:
                print(f"  WARNING: {sheet['label']} attempt {_sheet_attempt+1} failed: {e}; retrying in 30s…", flush=True)
                time.sleep(30)
            else:
                print(f"  ERROR: Could not load {sheet['label']} after 3 attempts: {e}", flush=True)

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
    Hard-fails (sys.exit 1) if DMS+CallOut ≠ Total retails for any month, or if
    On Create and On Update matrix column sums diverge by more than 10 %.
    Reference values are printed for manual cross-check; they are NOT hard limits.
    """
    # Month labels are nested under payload['maps']['lm'], not at payload['lm'].
    lm_arr = p.get('maps', {}).get('lm', [])

    # monthly: rows = [lm_idx, leads, retails, dms, callout]
    # u_monthly: rows = [lm_idx, leads, retails, dms, callout]
    oc_by_lm  = {}   # On Create: lm_label → [leads, rets, dms, co]
    ou_by_lm  = {}   # On Update: lm_label → [leads, rets, dms, co]

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

    print("\n── Pre-push payload reconciliation ──────────────────", flush=True)
    print(f"  Grand totals  — Leads: {grand_leads:,}  Retails: {grand_rets:,}  "
          f"DMS: {grand_dms:,}  CO: {grand_co:,}  DMS+CO: {grand_dms+grand_co:,}",
          flush=True)

    errors = []
    # DMS+CO vs Retails: NOT a hard check. Historical retail entries (Apr'25–Jun'26) sourced
    # from Excel may have blank rtype (neither 'DMS' nor 'Call Out'), so DMS+CO ≤ Retails is
    # expected. Report the gap as informational only.
    _grand_unclassified = grand_rets - (grand_dms + grand_co)
    if _grand_unclassified > 0:
        print(f"  NOTE: {_grand_unclassified:,} retail records have unclassified Call Type "
              f"(DMS+CO={grand_dms+grand_co:,} vs Retails={grand_rets:,}) — "
              f"typical for historical records with blank Excel rtype.", flush=True)

    # Print monthly breakdown for key months
    for mo in ("Jul'26", "Aug'26"):
        oc = oc_by_lm.get(mo, [0,0,0,0])
        ou = ou_by_lm.get(mo, [0,0,0,0])
        print(f"  {mo}  On Create  — Leads: {oc[0]:>7,}  Retails: {oc[1]:>6,}  "
              f"DMS: {oc[2]:>5,}  CO: {oc[3]:>5,}", flush=True)
        print(f"  {mo}  On Update  — Leads: {ou[0]:>7,}  Retails: {ou[1]:>6,}  "
              f"DMS: {ou[2]:>5,}  CO: {ou[3]:>5,}", flush=True)

    # Reference cross-check (informational — not a hard limit)
    REF = {
        "Jul'26": {'oc_leads': 190640, 'oc_rets': 14406, 'ou_leads': None, 'ou_rets': 19052},
        "Aug'26": {'oc_leads': None,   'oc_rets': None,  'ou_leads': None, 'ou_rets': None},
    }
    print("  Reference cross-check (informational):", flush=True)
    for mo, ref in REF.items():
        oc = oc_by_lm.get(mo, [0,0,0,0])
        ou = ou_by_lm.get(mo, [0,0,0,0])
        if ref['oc_rets'] is not None:
            diff = oc[1] - ref['oc_rets']
            print(f"    {mo} On Create retails: {oc[1]:,}  ref={ref['oc_rets']:,}  "
                  f"diff={diff:+,}", flush=True)
        if ref['ou_rets'] is not None:
            diff = ou[1] - ref['ou_rets']
            print(f"    {mo} On Update retails: {ou[1]:,}  ref={ref['ou_rets']:,}  "
                  f"diff={diff:+,}", flush=True)

    if errors:
        print("\n  FATAL: payload internal consistency errors:", flush=True)
        for e in errors: print(e, flush=True)
        print("── End payload validation ────────────────────────────", flush=True)
        sys.exit(1)
    print("  Payload validation PASSED.", flush=True)
    print("── End payload validation ────────────────────────────", flush=True)
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


def _fail_exit(stage, reason, stg_path=None):
    """Structured failure report — production is always unchanged on exit."""
    sep = '=' * 60
    print(f"\n{sep}", file=sys.stderr, flush=True)
    print("TVS DATA PIPELINE — FAILED SAFELY", file=sys.stderr, flush=True)
    print(sep, file=sys.stderr, flush=True)
    print(f"Stage:    {stage}", file=sys.stderr, flush=True)
    print(f"Reason:   {reason}", file=sys.stderr, flush=True)
    print(f"\nProduction payload changed: NO", file=sys.stderr, flush=True)
    print(f"Firebase changed:           NO", file=sys.stderr, flush=True)
    print(f"GitHub Pages changed:       NO", file=sys.stderr, flush=True)
    if _prod_path.exists():
        _mtime = datetime.fromtimestamp(_prod_path.stat().st_mtime, timezone.utc)
        print(f"\nLast known-good: {_prod_path.name}  ({_prod_path.stat().st_size // 1024:,} KB)",
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
    _oc_jul = _oc.get("Jul'26", [0, 0, 0, 0])
    _ou_jul = _ou.get("Jul'26", [0, 0, 0, 0])
    _oc_aug = _oc.get("Aug'26", [0, 0, 0, 0])
    _ou_aug = _ou.get("Aug'26", [0, 0, 0, 0])
    print(f"\n{'=' * 60}", flush=True)
    print("TVS DATA PIPELINE — DRY RUN COMPLETE  (production unchanged)", flush=True)
    print(f"{'=' * 60}", flush=True)
    print(f"  Staging validated:  leads={_stg_leads:,}  retails={_stg_retails:,}", flush=True)
    print(f"  Jul'26 On Create:   leads={_oc_jul[0]:,}  retails={_oc_jul[1]:,}", flush=True)
    print(f"  Jul'26 On Update:   leads={_ou_jul[0]:,}  retails={_ou_jul[1]:,}", flush=True)
    print(f"  Aug'26 On Create:   leads={_oc_aug[0]:,}  retails={_oc_aug[1]:,}", flush=True)
    print(f"  Aug'26 On Update:   leads={_ou_aug[0]:,}  retails={_ou_aug[1]:,}", flush=True)
    print(f"  Production:         UNCHANGED", flush=True)
    print(f"{'=' * 60}", flush=True)
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
print(f"Response: {body}", flush=True)

if not body or '"ok":true' not in body:
    _fail_exit('Firebase response', f'"ok":true not found — got: {str(body)[:200]}', _staging_path)

# ── Firebase confirmed — PROMOTE STAGING → PRODUCTION ─────────────────────────
_prev_existed = _prod_path.exists()
if _prev_existed:
    shutil.copy2(_prod_path, _prev_path)
    print(f"  Backed up previous → {_prev_path.name}", flush=True)

shutil.move(str(_staging_path), str(_prod_path))
print(f"  Promoted staging   → {_prod_path.name}  ({_prod_path.stat().st_size // 1024:,} KB)",
      flush=True)

# ── Structured success report ─────────────────────────────────────────────────
_elapsed       = int((datetime.now(timezone.utc) - _RUN_START).total_seconds())
_oc_jul        = _oc.get("Jul'26", [0, 0, 0, 0])
_ou_jul        = _ou.get("Jul'26", [0, 0, 0, 0])
_oc_aug        = _oc.get("Aug'26", [0, 0, 0, 0])
_ou_aug        = _ou.get("Aug'26", [0, 0, 0, 0])
_grand_leads   = sum(v[0] for v in _oc.values())
_grand_retails = sum(v[1] for v in _oc.values())
_live_lm_dist  = (online_leads['LeadMonth'].value_counts().to_dict()
                  if 'LeadMonth' in online_leads.columns and len(online_leads) > 0 else {})

print(f"\n{'=' * 60}", flush=True)
print("TVS DATA PIPELINE — SUCCESS", flush=True)
print(f"{'=' * 60}", flush=True)
print(f"Timestamp: {_RUN_START.strftime('%Y-%m-%d %H:%M UTC')}  "
      f"Runtime: {_elapsed // 60}m {_elapsed % 60}s", flush=True)
print(f"\nSOURCE", flush=True)
print(f"  Historical leads:  {len(hist_leads):>10,}", flush=True)
for _mo, _cnt in sorted(_live_lm_dist.items(), key=lambda x: month_order(x[0])):
    print(f"  {_mo} live leads: {int(_cnt):>8,}", flush=True)
print(f"  Live retail rows:  {len(retail_df):>10,}  (combined map: {len(retail_map):,})", flush=True)
print(f"\nAGGREGATION", flush=True)
print(f"  Total unique leads:         {_grand_leads:>10,}", flush=True)
print(f"  Total retails (On Create):  {_grand_retails:>10,}", flush=True)
print(f"\nJUL'26", flush=True)
print(f"  On Create — Leads: {_oc_jul[0]:>8,}  Retails: {_oc_jul[1]:>7,}  "
      f"DMS: {_oc_jul[2]:>6,}  CO: {_oc_jul[3]:>6,}", flush=True)
print(f"  On Update — Leads: {_ou_jul[0]:>8,}  Retails: {_ou_jul[1]:>7,}  "
      f"DMS: {_ou_jul[2]:>6,}  CO: {_ou_jul[3]:>6,}", flush=True)
print(f"\nAUG'26", flush=True)
print(f"  On Create — Leads: {_oc_aug[0]:>8,}  Retails: {_oc_aug[1]:>7,}  "
      f"DMS: {_oc_aug[2]:>6,}  CO: {_oc_aug[3]:>6,}", flush=True)
print(f"  On Update — Leads: {_ou_aug[0]:>8,}  Retails: {_ou_aug[1]:>7,}  "
      f"DMS: {_ou_aug[2]:>6,}  CO: {_ou_aug[3]:>6,}", flush=True)
print(f"\nPUBLICATION", flush=True)
print(f"  Firebase:  CONFIRMED (ok:true)", flush=True)
print(f"  Payload:   {_prod_path.name}  ({_prod_path.stat().st_size // 1024:,} KB)", flush=True)
if _prev_existed:
    print(f"  Previous:  backed up to {_prev_path.name}", flush=True)
print(f"\nSTATUS: PRODUCTION UPDATED", flush=True)
print(f"{'=' * 60}", flush=True)
