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

import json, sys, re, time, os, gzip, base64, traceback
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
        'min_mo': 2608,   # Aug'26 onwards
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
    extra = {'fileId': file_id, 'pageSize': 25000, 'cols': LEAD_COLS}
    if tab_name:
        extra['tabName'] = tab_name
    while True:
        extra['page'] = page
        data = None
        for attempt in range(3):
            try:
                data = proxy_get('getSheetData', extra, timeout=45)
                if 'error' in data:
                    raise RuntimeError(f"Apps Script error: {data['error']}")
                break  # success
            except Exception as e:
                if attempt < 2:
                    print(f"  {label} page {page} attempt {attempt+1} failed ({e}); retrying in 30s…", flush=True)
                    time.sleep(30)
                else:
                    traceback.print_exc()
                    raise RuntimeError(f"getSheetData {label} page {page} failed after 3 attempts: {e}")
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
    keep = ['SorceLeadId','LeadMonth','ModelName','Source','LeadType',
            'State','Zone','BuyingDays','CityName','DealerName']
    return df[[c for c in keep if c in df.columns]].copy()

# ─── Retail master ─────────────────────────────────────────────────────────────

def fetch_retails():
    """Fetch TVS retail master via Apps Script (paginated, 3 retries per page)."""
    print("Fetching retail master via Apps Script…", flush=True)
    page, all_rows, headers = 0, [], None
    while True:
        for attempt in range(3):
            try:
                data = proxy_get('getCurrentRetails', {'page': page, 'pageSize': 25000}, timeout=45)
                break
            except Exception as e:
                if attempt < 2:
                    print(f"  Page {page} attempt {attempt+1} failed ({e}); retrying in 30s…", flush=True)
                    time.sleep(30)
                else:
                    traceback.print_exc()
                    raise RuntimeError(f"getCurrentRetails page {page} failed: {e}")
        if 'error' in data:
            raise RuntimeError(f"getCurrentRetails error: {data['error']}")
        if headers is None:
            headers = data['headers']
        rows = data.get('rows', [])
        all_rows.extend(rows)
        total = data.get('total', '?')
        print(f"  Page {page}: +{len(rows):,} rows (total {len(all_rows):,}/{total})", flush=True)
        if data.get('done', True):
            break
        page += 1
    df = pd.DataFrame(all_rows, columns=headers)
    print(f"  Retail master: {len(df):,} TVS rows", flush=True)
    return df

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
        city = _cities[i].strip() or 'Unknown'

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
except Exception as _retail_err:
    traceback.print_exc()
    print(f"\nFATAL: live retail fetch failed — {_retail_err}", file=sys.stderr, flush=True)
    sys.exit(1)
online_rmap, unexpected_call_types = build_retail_map(retail_df)
_online_added = _online_skipped = 0
for lid, info in online_rmap.items():
    if month_order(info.get('rm', '')) >= ONLINE_START_ORDER:
        retail_map[lid] = info   # Jul'26+ entries: live is authoritative
        _online_added += 1
    else:
        _online_skipped += 1     # pre-Jul'26 entries: hist_cache is authoritative, keep it

if unexpected_call_types:
    print(f"  WARNING: {len(unexpected_call_types)} unexpected 'Call Type' values "
          f"(defaulted to DMS):", flush=True)
    for item in unexpected_call_types[:20]:
        print(f"    lid={item['lid']}  call_type={item['call_type']!r}", flush=True)
    if len(unexpected_call_types) > 20:
        print(f"    ... and {len(unexpected_call_types)-20} more", flush=True)

print(f"  Live retail: {len(online_rmap):,} total  "
      f"| {_online_added:,} added/updated (Jul'26+)  "
      f"| {_online_skipped:,} skipped (hist authoritative for pre-{ONLINE_START})", flush=True)

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
            print(f"  {sheet['label']} columns: {list(raw.columns)}", flush=True)
            rtype_map.update(extract_rtype_map(raw))
            std_all = standardize_leads(raw)
            _min = sheet.get('min_mo', ONLINE_START_ORDER)
            _max = sheet.get('max_mo')
            # Diagnostic: show LeadMonth distribution before filter
            _lm_counts = std_all['LeadMonth'].value_counts().head(10).to_dict() if 'LeadMonth' in std_all.columns else {}
            print(f"  {sheet['label']} pre-filter LeadMonth top-10: {_lm_counts}", flush=True)
            std = std_all[std_all['LeadMonth'].apply(month_order) >= _min]
            if _max is not None:
                std = std[std['LeadMonth'].apply(month_order) <= _max]
            # Diagnostic: log filtered-out rows
            _n_filtered = len(std_all) - len(std)
            if _n_filtered > 0:
                _filtered = std_all[std_all['LeadMonth'].apply(month_order) < _min]
                _filt_counts = _filtered['LeadMonth'].value_counts().head(10).to_dict() if 'LeadMonth' in _filtered.columns else {}
                print(f"  {sheet['label']}: {_n_filtered:,} rows filtered (LeadMonth < {_min}): {_filt_counts}", flush=True)
            lead_dfs.append(std)
            _range = f"{MONTH_NAMES[(_min%100)-1]}'{_min//100:02d}" + (f" only" if _max == _min else f"+ ({len(std):,} rows)")
            print(f"  {sheet['label']}: {len(std):,} rows [{_range}]", flush=True)
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
parts        = [df for df in [hist_leads, online_leads] if len(df) > 0]
all_leads    = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
print(f"  Historical: {len(hist_leads):,}  Online: {len(online_leads):,}  Combined: {len(all_leads):,}", flush=True)

# Deduplicate: same opty_id can appear in both hist Excel and online sheets.
# keep='last' → online data wins for overlapping lids (more up-to-date).
if len(all_leads) > 0 and 'SorceLeadId' in all_leads.columns:
    before = len(all_leads)
    all_leads = all_leads.drop_duplicates(subset=['SorceLeadId'], keep='last')
    if before > len(all_leads):
        print(f"  Deduplicated {before - len(all_leads):,} duplicate lead rows (hist/online overlap)", flush=True)

print(f"  Unique leads: {len(all_leads):,}", flush=True)

# Gap-fill is disabled: only real CRM leads appear in the dashboard.
# Retails without a matching CRM lead are counted only when a real lead record exists.
# This eliminates Unknown Source / Unknown Model / Unknown LeadType from all dimensions.
print(f"  Grand total: {len(all_leads):,}", flush=True)

import gc; gc.collect()
print("\nAggregating and pushing…", flush=True)
payload  = build_payload(all_leads, retail_map)
json_str = json.dumps(payload, separators=(',', ':'))
print(f"\nPayload size: {len(json_str)/1024:.1f} KB", flush=True)

# Save payload locally for cross-validation
_payload_path = Path(__file__).parent / 'tvs_last_payload.json'
with open(_payload_path, 'w', encoding='utf-8') as _f:
    json.dump(payload, _f)
print(f"Payload saved to {_payload_path}", flush=True)

# Save gzip-compressed payload for GitHub Pages hosting (bypasses Apps Script GET redirect)
# Compressed keeps the file ~6 MB instead of ~50+ MB, well under GitHub's 100 MB limit.
_data_dir = Path(__file__).parent.parent / 'data'
_data_dir.mkdir(exist_ok=True)
_static_path = _data_dir / 'tvs_payload.json.gz'
with gzip.open(_static_path, 'wt', encoding='utf-8', compresslevel=6) as _f:
    json.dump(payload, _f, separators=(',', ':'))
print(f"Static payload saved to {_static_path} ({_static_path.stat().st_size/1024:.0f} KB)", flush=True)

# Compress the payload: gzip + base64 → ~5-8 MB instead of ~50 MB.
# Apps Script doPost decompresses the "gz" envelope before processing.
_raw_bytes  = json_str.encode('utf-8')
_compressed = gzip.compress(_raw_bytes, compresslevel=6)
_envelope   = json.dumps({'gz': base64.b64encode(_compressed).decode('ascii')},
                          separators=(',', ':'))
print(f"POSTing to Apps Script… ({len(_envelope)/1024:.0f} KB compressed, "
      f"was {len(json_str)/1024:.0f} KB raw)", flush=True)
del _raw_bytes, _compressed, json_str

body = None
for _attempt in range(3):
    try:
        _resp = requests.post(
            APPS_SCRIPT_URL,
            data=_envelope.encode('utf-8'),
            params={'secret': SECRET},
            headers={'Content-Type': 'application/json'},
            timeout=(60, 1800),   # 60 s connect, 30 min read
        )
        _resp.raise_for_status()
        body = _resp.text
        break
    except Exception as _e:
        print(f"  POST attempt {_attempt+1} failed: {_e}", flush=True)
        if _attempt < 2:
            print("  Retrying in 30s…", flush=True)
            time.sleep(30)
        else:
            traceback.print_exc()
            raise
print(f"Response: {body}", flush=True)

if not body or '"ok":true' not in body:
    print("ERROR: Apps Script did not confirm success!", file=sys.stderr)
    sys.exit(1)

print("=" * 60, flush=True)
print("Done.", flush=True)
