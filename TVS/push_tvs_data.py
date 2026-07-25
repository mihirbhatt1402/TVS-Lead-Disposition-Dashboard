"""
TVS Lead Disposition — Daily Data Push
Runs via GitHub Actions at 12:00 PM IST every day.

DATA SOURCES
  Lead master : 7 hardcoded monthly Google Sheets (Jan–Jul or current month)
  Retail master: Google Sheet 1ZWBlzxX-g2R5iCcrsGUWrqSvxIHcchFHtajDDPcFJgE (Raw tab)

JOIN: lead.opty_id  ↔  retail.sourceLeadId
RETAIL MONTH: retail.Retail_Attribution_Date
"""

import json, sys, re, time
import pandas as pd
import requests
import urllib.request
from pathlib import Path

MONTH_NAMES = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

APPS_SCRIPT_URL = "https://script.google.com/macros/s/AKfycbwzgnXPbCbunBblnMUrqdWg3eY9qsIwCrFxuYuvYSpxtH22l4Cs32vdkOkDhUn-qwM64w/exec"
SECRET = "tvs2026push"

RETAILS_FILE_ID = '1ZWBlzxX-g2R5iCcrsGUWrqSvxIHcchFHtajDDPcFJgE'
RETAILS_TAB     = 'Raw'

# Monthly lead master sheets — last one is current month (tab='TVS')
LEAD_SHEETS = [
    {'id': '1mJEi34xbeYW8q3WITTjyUQDLyREBS2dYpWP3svBoprw', 'tab': 'TVS', 'label': 'LeadSheet-1'},
    {'id': '18LM6v6_BLzmKV2fbdXRI19Xr9vCZjJrLgbr4klP5Zis',  'tab': 'TVS', 'label': 'LeadSheet-2'},
    {'id': '1fBvEbUzi6Tnhjq1SYljKDA4tFjuB8gLakmrTrJ2Mk_E',  'tab': 'TVS', 'label': 'LeadSheet-3'},
    {'id': '1ZvoK_8_0BnavmKNqNKIONMC1hM35BwDwTXwGWwK0QzQ',  'tab': 'TVS', 'label': 'LeadSheet-4'},
    {'id': '1jQbHZLrTCsrItGvV26TyDUQ_BiL3vd-sNpWim4tRKJE',  'tab': 'TVS', 'label': 'LeadSheet-5'},
    {'id': '1tWV-wQ97KCZwrb7yz99s52XF5OIVfzj65gxdDSyAeaQ',  'tab': 'TVS', 'label': 'LeadSheet-6'},
    {'id': '1iSw5zXF67q5Wkoz2mSPFqql9OPAcqmd0um5BEHUGf4o',  'tab': 'TVS', 'label': 'LeadSheet-7 (current)'},
]

ONLINE_START = "May'26"

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

ONLINE_START_ORDER = month_order(ONLINE_START)

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
    extra = {'fileId': file_id, 'pageSize': 50000, 'cols': LEAD_COLS}
    if tab_name:
        extra['tabName'] = tab_name
    while True:
        extra['page'] = page
        for attempt in range(3):
            try:
                data = proxy_get('getSheetData', extra, timeout=300)
                break
            except Exception as e:
                if attempt < 2:
                    print(f"  {label} page {page} attempt {attempt+1} failed ({e}); retrying in 30s…", flush=True)
                    time.sleep(30)
                else:
                    raise RuntimeError(f"getSheetData {label} page {page} failed: {e}")
        if 'error' in data:
            raise RuntimeError(f"getSheetData error [{label}]: {data['error']}")
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
                data = proxy_get('getCurrentRetails', {'page': page, 'pageSize': 25000}, timeout=300)
                break
            except Exception as e:
                if attempt < 2:
                    print(f"  Page {page} attempt {attempt+1} failed ({e}); retrying in 30s…", flush=True)
                    time.sleep(30)
                else:
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
    """Build {sourceLeadId → {rm, rtype, pm}} using Retail_Attribution_Date.
    rtype: 'DMS' when 'Purchased From' is blank, 'Call Out' when it has a value.
    """
    rmap = {}
    has_pf = 'Purchased From' in retail_df.columns
    for _, row in retail_df.iterrows():
        lid = to_id(row.get('sourceLeadId', ''))
        if not lid: continue
        rm = parse_ym(row.get('Retail_Attribution_Date', ''))
        pm = normalize_purchased_model(row.get('purchasedModel', ''))
        if has_pf:
            pf = str(row.get('Purchased From', '') or '').strip()
            rtype = '' if pf else 'DMS'
            if pf: rtype = 'Call Out'
        else:
            rtype = ''
        rmap[lid] = {'rm': rm, 'rtype': rtype, 'pm': pm}
    return rmap

def make_synthetic_leads(retail_df, matched_lids):
    """Create lead rows for retailed IDs absent from all lead sheets."""
    rows = []
    for _, row in retail_df.iterrows():
        lid = to_id(row.get('sourceLeadId', ''))
        if not lid or lid in matched_lids: continue
        rm    = parse_ym(row.get('Retail_Attribution_Date', ''))
        lm    = rm or lid_to_month(lid)
        model = str(row.get('purchasedModel', '') or '').strip() or 'Unknown'
        rows.append({
            'SorceLeadId': lid, 'LeadMonth': lm, 'ModelName': model,
            'Source': 'Unknown', 'LeadType': 'Unknown', 'State': 'Unknown',
            'Zone': 'Unknown', 'BuyingDays': '0', 'CityName': 'Unknown', 'DealerName': 'Unknown',
        })
    cols = ['SorceLeadId','LeadMonth','ModelName','Source','LeadType',
            'State','Zone','BuyingDays','CityName','DealerName']
    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=cols)

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

        if dl_col:
            ubump(u_stdm, f"{sti}|{dli}|{li}", f"{sti}|{dli}|{uli}", is_ret, rtype)
            ubump(u_mxdl, f"{mi}|{dli}|{li}",  f"{mi}|{dli}|{uli}",  is_ret, rtype)
            ubump(u_ltdl, f"{tti}|{dli}|{li}", f"{tti}|{dli}|{uli}", is_ret, rtype)

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
        **({"cdm":  to_rows(cdm,  lambda k: list(map(int, k.split('|')))),
            "cdsm": to_rows(cdsm, lambda k: list(map(int, k.split('|')))),
            "stdm": to_rows(stdm, lambda k: list(map(int, k.split('|')))),
            "mxdl": to_rows(mxdl, lambda k: list(map(int, k.split('|')))),
            "ltdl": to_rows(ltdl, lambda k: list(map(int, k.split('|')))),
            "u_stdm": to_rows(u_stdm, lambda k: list(map(int, k.split('|')))),
            "u_mxdl": to_rows(u_mxdl, lambda k: list(map(int, k.split('|')))),
            "u_ltdl": to_rows(u_ltdl, lambda k: list(map(int, k.split('|'))))} if dl_col and dl_arr else {}),
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

# 1. Online retail master
print("\n[1/3] Loading online retail master…", flush=True)
retail_df  = fetch_retails()
retail_map = build_retail_map(retail_df)
print(f"  Retail map: {len(retail_map):,} entries", flush=True)

# 2. Online leads (May'26+)
print(f"\n[2/3] Loading online lead sheets ({ONLINE_START}+)…", flush=True)
lead_dfs  = []
rtype_map = {}
for sheet in LEAD_SHEETS:
    try:
        raw = fetch_sheet_via_proxy(sheet['id'], sheet['label'], tab_name=sheet['tab'])
        raw.columns = [c.strip() for c in raw.columns]
        rtype_map.update(extract_rtype_map(raw))
        std = standardize_leads(raw)
        std = std[std['LeadMonth'].apply(month_order) >= ONLINE_START_ORDER]
        lead_dfs.append(std)
        print(f"  {sheet['label']}: {len(std):,} rows ({ONLINE_START}+)", flush=True)
    except Exception as e:
        print(f"  WARNING: Could not load {sheet['label']}: {e}", flush=True)

# Apply rtype overrides from embedded sheet columns
for lid, info in rtype_map.items():
    if lid in retail_map:
        retail_map[lid]['rtype'] = info['rtype']
        if info['rm'] and not retail_map[lid]['rm']:
            retail_map[lid]['rm'] = info['rm']

# 3. Gap-fill + aggregate
print("\n[3/3] Combining leads, gap-fill, and aggregating…", flush=True)
all_leads = pd.concat(lead_dfs, ignore_index=True) if lead_dfs else pd.DataFrame()
print(f"  Online leads: {len(all_leads):,}", flush=True)

if len(all_leads) > 0 and 'SorceLeadId' in all_leads.columns:
    before_dedup = len(all_leads)
    all_leads = all_leads.drop_duplicates(subset=['SorceLeadId'], keep='last')
    dupes = before_dedup - len(all_leads)
    if dupes:
        print(f"  Deduplicated {dupes:,} duplicate lead rows", flush=True)

print(f"  Grand total rows: {len(all_leads):,}", flush=True)

_matched_lids = set(all_leads['SorceLeadId'].unique()) if len(all_leads) > 0 else set()
_synth_live = make_synthetic_leads(retail_df, _matched_lids)
if len(_synth_live) > 0:
    print(f"  Synthetic (retail, no lead row): {len(_synth_live):,}", flush=True)
    all_leads = pd.concat([all_leads, _synth_live], ignore_index=True)

print(f"  Grand total after gap-fill: {len(all_leads):,}", flush=True)

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

print("POSTing to Apps Script…", flush=True)
url  = APPS_SCRIPT_URL + "?secret=" + SECRET
data = json_str.encode("utf-8")
req  = urllib.request.Request(url, data=data, method="POST",
       headers={"Content-Type": "application/json"})
body = None
for _attempt in range(3):
    try:
        with urllib.request.urlopen(req, timeout=240) as resp:
            body = resp.read().decode()
        break
    except Exception as _e:
        print(f"  POST attempt {_attempt+1} failed: {_e}", flush=True)
        if _attempt < 2:
            print("  Retrying in 30s…", flush=True)
            time.sleep(30)
        else:
            raise
print(f"Response: {body}", flush=True)

if not body or '"ok":true' not in body:
    print("ERROR: Apps Script did not confirm success!", file=sys.stderr)
    sys.exit(1)

print("=" * 60, flush=True)
print("Done.", flush=True)
