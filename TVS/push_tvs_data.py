"""
TVS Lead Disposition — Daily Data Push
Runs via GitHub Actions at 12:00 PM IST every day.

DATA SOURCES
  Lead master : 7 hardcoded monthly Google Sheets (Jan–Jul or current month)
  Retail master: Google Sheet 1ZWBlzxX-g2R5iCcrsGUWrqSvxIHcchFHtajDDPcFJgE (Raw tab)

JOIN: lead.opty_id  ↔  retail.sourceLeadId
RETAIL MONTH: retail.Retail_Attribution_Date
"""

import json, sys, re, time, os
import pandas as pd
import requests
import urllib.request

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

# Historical lead/retail files (Apr'25–Apr'26). Override via TVS_HIST_DIR env var.
HIST_DIR     = os.environ.get('TVS_HIST_DIR', r'C:\Users\mihir.bhatt\Desktop\New folder (2)')
ONLINE_START = "May'26"  # online sheets are only used from this month onwards

# Lead master column map: sheet column → canonical name
# purchasedModel (raw from retail sheet) → canonical lead-model name
PURCHASED_MODEL_MAP = {
    # Apache RTR 160 4V
    'APACHE  160 4V – PL 2CH USD OBDIIB':          'TVS Apache RTR 160 4V',
    'APACHE  160 4V – PL DISC B.T OBDIIB':         'TVS Apache RTR 160 4V',
    'APACHE  160 4V – PL DISC SPL ED OBDIIB':      'TVS Apache RTR 160 4V',
    'APACHE  160 4V â€“ PL 2CH USD+TFT OBDIIB': 'TVS Apache RTR 160 4V',
    'Apache RTR 160 4V Disc BT':                         'TVS Apache RTR 160 4V',
    'TVS APACHE RTR 160 4V - 2CH ABS BT':               'TVS Apache RTR 160 4V',
    'TVS APACHE RTR 160 4V - RM SPL ED':                 'TVS Apache RTR 160 4V',
    'TVS APACHE RTR 160 4V USD – 2CH':              'TVS Apache RTR 160 4V',
    'TVS APACHE RTR 1604V– RM OBDIIA DRUM B.E':    'TVS Apache RTR 160 4V',
    'TVS APACHE RTR 1604V-OBDIIB DISC BLK.EDI':         'TVS Apache RTR 160 4V',
    'TVS APACHE RTR1604V–OBDIIB SPL ED':           'TVS Apache RTR 160 4V',
    'TVSAPACHERTR1604V–OBDIIB 2CH USD':            'TVS Apache RTR 160 4V',
    # Apache RTR 160 (2V)
    'APACHE 160-2V Disc 2CH A -EDI OBDIIB':             'TVS Apache RTR 160',
    'APACHE 160-4V PL TFT USD 2CH A.EDI':               'TVS Apache RTR 160',
    'APACHE RTR 160 2V RM DISC':                         'TVS Apache RTR 160',
    'TVS APACHE RTR 160 2V DC ABS':                      'TVS Apache RTR 160',
    'TVS APACHE RTR 160-2V RM OBDIIA DRUM B.E':         'TVS Apache RTR 160',
    'TVS APACHE RTR 160-OBDIIB 2V DC ABS':              'TVS Apache RTR 160',
    'TVS APACHE RTR160-OBDIIB 2V DISC':                 'TVS Apache RTR 160',
    'TVS APACHE RTR160-OBDIIB 2V DISC BT':              'TVS Apache RTR 160',
    'TVS APACHE RTR160-OBDIIB 2V DRUM':                 'TVS Apache RTR 160',
    'TVS APACHE RTR160-OBDIIB 2V DRUM BLK.EDI':        'TVS Apache RTR 160',
    'TVS APACHE RTR160-OBDIIB 2V RAC ED':               'TVS Apache RTR 160',
    'TVS APACHE RTR180-OBDIIB DISC':                    'TVS Apache RTR 160',  # per user mapping
    # Apache RTR 180
    'APACHE 180-2V Disc 1CH A -EDI OBDIIB':             'TVS Apache RTR 180',
    'APACHE RTR 180 RM':                                 'TVS Apache RTR 180',
    # Apache RTR 200 4V
    'APACHE  200 4V – PL 2CH USD+TFT OBDIIB':     'TVS Apache RTR 200 4V',
    'APACHE  200 4V â€“ PL 2CH USD+TFT OBDIIB': 'TVS Apache RTR 200 4V',
    'APACHE 200-4V PL TFT USD 2CH A.EDI':              'TVS Apache RTR 200 4V',
    'TVS APACHE RTR 200 4V–OBDIIB 2CH':           'TVS Apache RTR 200 4V',
    # Apache RR 310
    'APACHE RR 310-O2B-M25-DYN+DYPR-GBLK GLD':        'TVS Apache RR 310',
    'APACHE RR310-O2B-M24–BASE W/O QS-RAR':       'TVS Apache RR 310',
    'APACHE RR310-O2B-M24–BASE-RAR':              'TVS Apache RR 310',
    'APACHE RR310-O2B-M24–BASE-SMG':              'TVS Apache RR 310',
    'APACHE RR310-O2B-M24-DYN PRO-SEP-BLU':           'TVS Apache RR 310',
    # Apache RTR 310
    'APACHE RTR 310 – BASE BLK':                  'TVS Apache RTR 310',
    'APACHE RTR 310-O2B-M24- BASE-GL BLK':             'TVS Apache RTR 310',
    'APACHE RTR 310-O2B-M24-BASE-BLK YEL':             'TVS Apache RTR 310',
    'APACHE RTR 310-O2B-M24-BASE-RC-RED':              'TVS Apache RTR 310',
    'APACHE RTR 310-O2B-M24-DYN+DYPR-RC-RED':         'TVS Apache RTR 310',
    'APACHE RTR 310-O2B-M25-DYN+DYPR-GBLK GLD':       'TVS Apache RTR 310',
    # Star City Plus
    'CITY+ DRUM OBDIIB':                                'TVS Star City Plus',
    'StarCity + ES DT BSVI':                            'TVS Star City Plus',
    # Jupiter 125
    'TVS JUPITER 125 DISC DT SXC OBDIIB':              'TVS Jupiter 125',
    'TVS JUPITER 125 DISC OBDIIB':                      'TVS Jupiter 125',
    'TVS JUPITER 125 DISC SXC OBDIIB':                 'TVS Jupiter 125',
    'TVS JUPITER 125 DRUM OBDIIB':                      'TVS Jupiter 125',
    # Jupiter (110)
    'JUPITER 125 BSVI':                                 'TVS Jupiter',  # per user mapping
    'JUPITER ZX DISC SXC':                              'TVS Jupiter',
    'TVS JUPITER110 DISC ALLOY SXC':                   'TVS Jupiter',
    'TVS JUPITER110 DISC ALLOY SXC OBDIIB':            'TVS Jupiter',
    'TVS JUPITER110 DRUM ALLOY':                        'TVS Jupiter',
    'TVS JUPITER110 DRUM ALLOY OBDIIB':                 'TVS Jupiter',
    'TVS JUPITER110 DRUM ALLOY SXC OBDIIB':            'TVS Jupiter',
    'TVS JUPITER110 DRUM OBDIIB':                       'TVS Jupiter',
    'TVS JUPITER110 DRUM SMW OBDIIB':                   'TVS Jupiter',
    # iQube S
    'TVS iQUBE  S15 BEIGE  Fr Disc':                   'TVS iQube S',
    'TVS iQUBE  S15 BLACK Fr Disc':                     'TVS iQube S',
    'TVS iQube 11 Fr. Disc black':                      'TVS iQube S',
    'TVS IQUBE ELECTRIC 9':                             'TVS iQube S',
    'TVS IQube S-Beige':                                'TVS iQube S',
    'TVS IQube S-New':                                  'TVS iQube S',
    'TVS IQUBE ST 17':                                  'TVS iQube S',
    'TVS IQUBE ST 17-Beige':                            'TVS iQube S',
    'TVS IQube UG-Beige':                               'TVS iQube S',
    'TVS IQube UG-New':                                 'TVS iQube S',
    'U546 V2':                                          'TVS iQube S',
    'U759 iQUBE':                                       'TVS iQube S',
    'U759 iQUBE 11 Black':                              'TVS iQube S',
    # NTORQ 125
    'NTORQ 125 DISC – Race Edition BSVI':         'TVS NTORQ 125',
    'NTORQ 125 DISC – SSE':                       'TVS NTORQ 125',
    'NTORQ 125 DISC R.LCD OBD2B':                      'TVS NTORQ 125',
    'NTORQ 125 RACE XP OBDIIB TORQUE ASSIST':          'TVS NTORQ 125',
    'NTORQ 125 RE R.LCD OBD2B':                        'TVS NTORQ 125',
    'NTORQ 125 SSE R.LCD OBD2B':                       'TVS NTORQ 125',
    'NTORQ 125 XT':                                    'TVS NTORQ 125',
    'TVS NTORQ 125 DISC BSVI':                         'TVS NTORQ 125',
    'TVS NTORQ 125 DISC BSVI OBDIIB':                  'TVS NTORQ 125',
    'TVS NTORQ 125 RACE EDT  BSVI OBDIIB':             'TVS NTORQ 125',
    'TVS NTORQ 125 RACE XP BSVI OBDIIB':               'TVS NTORQ 125',
    'TVS NTORQ 125 SUPER SQUAD BSVI OBDIIB':           'TVS NTORQ 125',
    'TVS NTORQ 125 XT BSVI OBDIIB':                    'TVS NTORQ 125',
    # Radeon
    'RADEON DISC DIGI OBDIIB':                         'TVS Radeon',
    'RADEON DRUM BLACK EDITION OBDIIB':                'TVS Radeon',
    'RADEON DRUM DIGI OBDIIB':                         'TVS Radeon',
    'RADEON DRUM OBDIIB':                              'TVS Radeon',
    'TVS RADEON - DIGI DISC ':                         'TVS Radeon',
    'TVS RADEON - DIGI DRUM ':                         'TVS Radeon',
    'TVS RADEON 110 ES MAG BSVI':                      'TVS Radeon',
    # Raider
    'RAIDER - OBDIIB 1CH ABS':                         'TVS Raider',
    'RAIDER DISC IGO I-ECU OBDIIB':                    'TVS Raider',
    'RAIDER DRUM OBDIIB':                              'TVS Raider',
    'RAIDER IGO I-ECU RD WH OBDIIB':                  'TVS Raider',
    'Raider LCD OBDIIB 1CH ABS':                       'TVS Raider',
    'RAIDER SQD EDN I-ECU OBDIIB':                     'TVS Raider',
    'RAIDER SS DISC OBDIIB':                           'TVS Raider',
    'TVS RAIDER DISC':                                 'TVS Raider',
    'TVS RAIDER DISC - LCD SX':                        'TVS Raider',
    'TVS RAIDER DISC - SS':                            'TVS Raider',
    'TVS RAIDER DISC - SSE':                           'TVS Raider',
    'TVS RAIDER DISC CONNECTED':                       'TVS Raider',
    'TVS RAIDER DRUM':                                 'TVS Raider',
    # Ronin
    'TVS RONIN 1CH BASE-FL RED - OBDIIB':              'TVS Ronin',
    'TVS RONIN 1CH BASE-LNG Black - OBDIIB':           'TVS Ronin',
    'TVS RONIN 2CH MID SPECIAL EDI OBDIIB':            'TVS Ronin',
    'TVS RONIN BASE OBIIB 1CH – MATTE WHITE':     'TVS Ronin',
    'TVS RONIN MID 2CH – CHARCOAL EMBR OBDIIB':  'TVS Ronin',
    'TVS RONIN MID 2CH – GLACIER SILVR OBDIIB':  'TVS Ronin',
    # Scooty Zest
    'Scooty Zest – OBDIIB':                       'TVS Scooty Zest',
    'Scooty Zest Matte series – BSVI':            'TVS Scooty Zest',
    'Scooty Zest Matte series – OBDIIB':          'TVS Scooty Zest',
    'TVS ZEST - OBDIIB SXC BLACK':                     'TVS Scooty Zest',
    'TVS ZEST - OBDIIB SXC NARDO GREY':                'TVS Scooty Zest',
    # Sport
    'SPORT ELS REFRESH OBDIIB':                        'TVS Sport',
    'SPORT ES OBDIIB':                                 'TVS Sport',
    'TVS SPORT ELS BSVI':                              'TVS Sport',
    'TVS SPORT ES-U559':                               'TVS Sport',
    # XL100
    'TVS XL 100 COM iTs-BSVI':                        'TVS XL100',
    'TVS XL 100 HD iTs – SBS Spl. Edition':      'TVS XL100',
    'TVS XL 100 HD iTs BSVI':                         'TVS XL100',
    'TVS XL 100 HD OBDIIB':                            'TVS XL100',
    'TVS XL 100 HEAVY DUTY ES':                        'TVS XL100',
}

# Lead ModelName column → canonical model name (from TVS Bike Lookup.xlsx)
LEAD_MODEL_MAP = {
    # APACHE RTR 165
    'APACHE RTR 165 RP': 'APACHE RTR 165',
    # TVS Apache RR 310
    'RR 310': 'TVS Apache RR 310',
    'APACHE RR 310-O2B-M25-DYN+DYPR-GBLK GLD': 'TVS Apache RR 310',
    'APACHE RR 310 BTO - RACE': 'TVS Apache RR 310',
    'APACHE RR 310 BTO-RACE+DYN': 'TVS Apache RR 310',
    'APACHE RR 310 BTO-DYNAMIC': 'TVS Apache RR 310',
    'Apache RR': 'TVS Apache RR 310',
    'APACHE RR310-OBDIIA-M23?BASE-RAR': 'TVS Apache RR 310',
    'APACHE RR310-OBDIIA-M23?DYN PRO-RCR TR': 'TVS Apache RR 310',
    'APACHE RR310-OBDIIA-M23?BASE-SMG': 'TVS Apache RR 310',
    'APACHE RR310-O2B-M24-DYN PRO-SEP-BLU': 'TVS Apache RR 310',
    'APACHE RR310-OBDIIA-M23?BASE W/O QS-RAR': 'TVS Apache RR 310',
    'APACHE RR310-O2B-M24-BASE-GRY': 'TVS Apache RR 310',
    'APACHE RR310-O2B-M24-DYN-SEP-BLU': 'TVS Apache RTR 310',
    'APACHE RR310-OBDIIA-M23?DYN PRO-SEP': 'TVS Apache RR 310',
    'TVS Apache RR': 'TVS Apache RR 310',
    'APACHE RR310-OBDIIA-M23?DYN-RAR': 'TVS Apache RR 310',
    'APACHE RR 310 BTO-RC REP+RC DYN+RD AL': 'TVS Apache RR 310',
    'APACHE RR310-OBDIIA-M23?DYN+DYN PRO-SMG': 'TVS Apache RR 310',
    'APACHE RR 310 BSVI': 'TVS Apache RR 310',
    # TVS Apache RTR 160
    'TVS APACHE RTR 160 2V DC ABS': 'TVS Apache RTR 160',
    'TVS APACHE RTR 160-2V RM OBDIIA DRUM B.E': 'TVS Apache RTR 160',
    'TVS Apache RTR': 'TVS Apache RTR 160',
    'TVS APACHE RTR 160 2V DISC BT RACING EDI': 'TVS Apache RTR 160',
    'TVS Apache RTR160': 'TVS Apache RTR 160',
    'RTR 160': 'TVS Apache RTR 160',
    'TVS APACHE RTR160-OBDIIB 2V DISC BT': 'TVS Apache RTR 160',
    '2024 TVS Apache RTR 160': 'TVS Apache RTR 160',
    'TVS Apache 160': 'TVS Apache RTR 160',
    'APACHE RTR 160 2V BSVI DRUM': 'TVS Apache RTR 160',
    'TVS APACHE RTR160-OBDIIB 2V DRUM BLK.EDI': 'TVS Apache RTR 160',
    'APACHE RTR 160 2V BSVI DISC': 'TVS Apache RTR 160',
    'APACHE RTR 160 2V RM DISC BT': 'TVS Apache RTR 160',
    'APACHE 160-2V Disc 2CH A -EDI OBDIIB': 'TVS Apache RTR 160',
    'TVS APACHE RTR160-OBDIIB 2V RAC ED': 'TVS Apache RTR 160',
    'TVS APACHE RTR 160-OBDIIB 2V DC ABS': 'TVS Apache RTR 160',
    'TVS Apache 2V': 'TVS Apache RTR 160',
    'Apache RTR': 'TVS Apache RTR 160',
    'APACHE RTR 160 2V RM DISC': 'TVS Apache RTR 160',
    'APACHE RTR 160 2V RM DRUM': 'TVS Apache RTR 160',
    'TVS Apache': 'TVS Apache RTR 160',
    'TVS APACHE RTR160-OBDIIB 2V DISC': 'TVS Apache RTR 160',
    'TVS APACHE RTR160-OBDIIB 2V DRUM': 'TVS Apache RTR 160',
    # TVS Apache RTR 160 4V
    'APACHE RTR 160 4V BSVI DRUM': 'TVS Apache RTR 160 4V',
    'APACHE RTR 160 4V BSVI DISC': 'TVS Apache RTR 160 4V',
    'TVS APACHE RTR 160 4V - Disc HP': 'TVS Apache RTR 160 4V',
    'TVS APACHE RTR 160 4V - Drum HP': 'TVS Apache RTR 160 4V',
    'TVS APACHE RTR 160 4V - 2CH ABS BT': 'TVS Apache RTR 160 4V',
    'TVS APACHE RTR 160 4V - RM DISC': 'TVS Apache RTR 160 4V',
    'Apache RTR 160 4V Disc BT': 'TVS Apache RTR 160 4V',
    'TVS APACHE RTR 160 4V - RM SPL ED': 'TVS Apache RTR 160 4V',
    'TVS APACHE RTR 160 4V - RM DRUM': 'TVS Apache RTR 160 4V',
    'APACHE 160-4V PL TFT USD 2CH A.EDI': 'TVS Apache RTR 160 4V',
    # TVS Apache RTR 180
    'APACHE RTR 180 RM-OBIIA': 'TVS Apache RTR 180',
    'APACHE 180-2V Disc 1CH A -EDI OBDIIB': 'TVS Apache RTR 180',
    'APACHE RTR 180 BSVI': 'TVS Apache RTR 180',
    'APACHE RTR 180 RM': 'TVS Apache RTR 180',
    'APACHE RTR 180 RM-OBD IIA': 'TVS Apache RTR 180',
    # TVS Apache RTR 200 4V
    'TVS Apache RTR 200 Fi E100': 'TVS Apache RTR 200 4V',
    'Apache 200 4V 1ch-R Mode': 'TVS Apache RTR 200 4V',
    'Apache 200 4V 2ch-R Mode': 'TVS Apache RTR 200 4V',
    'APACHE RTR 200 BSVI': 'TVS Apache RTR 200 4V',
    'TVS Apache RTR 200': 'TVS Apache RTR 200 4V',
    'APACHE 200-4V PL TFT USD 2CH A.EDI': 'TVS Apache RTR 200 4V',
    '2025 TVS Apache RTR 200 4V': 'TVS Apache RTR 200 4V',
    '2024 TVS Apache RTR 200 4V': 'TVS Apache RTR 200 4V',
    # TVS Apache RTR 310
    'Apache RTR 310': 'TVS Apache RTR 310',
    'APACHE RTR 310-O2B-M24-DYN+DYPR-RC-RED': 'TVS Apache RTR 310',
    '2024 TVS Apache RTR 310': 'TVS Apache RTR 310',
    'APACHE RTR 310-O2B-M25-DYN+DYPR-GBLK GLD': 'TVS Apache RTR 310',
    'APACHE RTR 310-O2B-M24-BASE-BLK YEL': 'TVS Apache RTR 310',
    'APACHE RTR 310-O2B-M24-DYN-PRO+ SP BLU': 'TVS Apache RTR 310',
    'APACHE RTR 310-O2B-M24-DYN-RC-RED': 'TVS Apache RTR 310',
    'APACHE RTR 310-O2B-M24-BASE-RC-RED': 'TVS Apache RTR 310',
    'APACHE RTR 310-O2B-M24- BASE-GL BLK': 'TVS Apache RTR 310',
    'APACHE RTR 310-O2B-M24-DYN PRO-RC-RED TR': 'TVS Apache RTR 310',
    # TVS Jupiter
    'TVS JUPITER110 DRUM': 'TVS Jupiter',
    'JUPITER ZX BSVI': 'TVS Jupiter',
    'JUPITERBSVI SMW INS- OBDIIA': 'TVS Jupiter',
    'JUPITER ZX DISC BSVI': 'TVS Jupiter',
    'Jupiter ZX Disc Ref (BSIV)': 'TVS Jupiter',
    'JUPITER ZX DISC SXC': 'TVS Jupiter',
    'TVS JUPITER CLASSIC DISC': 'TVS Jupiter',
    'TVS JUPITER110 DISC ALLOY SXC': 'TVS Jupiter',
    'TVS JUPITER110 DRUM ALLOY SXC': 'TVS Jupiter',
    'TVS JUPITER110 DRUM OBDIIB': 'TVS Jupiter',
    'TVS JUPITER110 DRUM ALLOY OBDIIB': 'TVS Jupiter',
    'TVS JUPITER110 DRUM ALLOY': 'TVS Jupiter',
    'TVS JUPITER110 DRUM ALLOY SXC OBDIIB': 'TVS Jupiter',
    'TVS JUPITER110 DISC ALLOY SXC OBDIIB': 'TVS Jupiter',
    'TVS JUPITER SMW - INSW': 'TVS Jupiter',
    'TVS JUPITER110 DRUM SMW OBDIIB': 'TVS Jupiter',
    'TVS Jupiter 110 Special Edition': 'TVS Jupiter',
    'JUPITER BSVI': 'TVS Jupiter',
    'TVS Jupiter 110cc': 'TVS Jupiter',
    'JUPITER BSVI - SMW': 'TVS Jupiter',
    'Jupiter 110': 'TVS Jupiter',
    'JUPITER BSVI-AOL': 'TVS Jupiter',
    'Jupiter': 'TVS Jupiter',
    'JUPITER CLASSIC BSVI': 'TVS Jupiter',
    'Jupiter X': 'TVS Jupiter',
    'JUPITER ZX BSVI - AOL': 'TVS Jupiter',
    'JUPITER ZX DRUM SXC': 'TVS Jupiter',
    'JUPITER ZX DISC BSVI-ISS': 'TVS Jupiter',
    # TVS Jupiter 125
    'TVS JUPITER 125 DISC DT SXC OBDIIB': 'TVS Jupiter 125',
    'TVS JUPITER 125 DRUM OBDIIB': 'TVS Jupiter 125',
    'JUPITER 125 DISC SX': 'TVS Jupiter 125',
    'TVS JUPITER 125 DISC SXC OBDIIB': 'TVS Jupiter 125',
    'JUPITER 125 DRUM BSVI': 'TVS Jupiter 125',
    'Jupiter 125': 'TVS Jupiter 125',
    'JUPITER 125 BSVI': 'TVS Jupiter 125',
    'TVS JUPITER 125 DISC OBDIIB': 'TVS Jupiter 125',
    'JUPITER 125 SMW BSVI': 'TVS Jupiter 125',
    # TVS NTORQ 125
    'TVS NTORQ 125 DISC BSVI': 'TVS NTORQ 125',
    'NTORQ 125 SSE R.LCD OBD2B': 'TVS NTORQ 125',
    'NTORQ 125 DISC R.LCD OBD2B': 'TVS NTORQ 125',
    'NTORQ 125 RE R.LCD OBD2B': 'TVS NTORQ 125',
    'TVS NTORQ 125 RACE XP BSVI OBDIIB': 'TVS NTORQ 125',
    'ntorq 125': 'TVS NTORQ 125',
    'TVS NTORQ 125 DISC BSVI OBDIIB': 'TVS NTORQ 125',
    'TVS NTORQ 125 XT BSVI OBDIIB': 'TVS NTORQ 125',
    'NTORQ 125 XT': 'TVS NTORQ 125',
    'NTORQ 125 DRUM NC BSVI': 'TVS NTORQ 125',
    'TVS NTORQ 125 DISC': 'TVS NTORQ 125',
    'TVS NTORQ 125 DRUM BSVI': 'TVS NTORQ 125',
    'TVS NTORQ 125 RACE XP': 'TVS NTORQ 125',
    'NTORQ 125 RACE XP OBDIIB TORQUE ASSIST': 'TVS NTORQ 125',
    'TVS NTORQ 125 SUPER SQUAD BSVI OBDIIB': 'TVS NTORQ 125',
    'Ntorq': 'TVS NTORQ 125',
    'TVS NTorq': 'TVS NTORQ 125',
    # TVS Radeon
    'TVS RADEON 110 ES MAG BSVI-OBD IIA': 'TVS Radeon',
    'TVS RADEON - DISC BSVI': 'TVS Radeon',
    'TVS RADEON BSVI DIGIDrum DT OBDIIA': 'TVS Radeon',
    'RADEON DRUM DIGI OBDIIB': 'TVS Radeon',
    'RADEON DISC DIGI OBDIIB': 'TVS Radeon',
    'TVS RADEON 110 ES MAG REF BSVI': 'TVS Radeon',
    'RADEON DRUM BLACK EDITION OBDIIB': 'TVS Radeon',
    'TVS RADEON BSVI DIGI Disc Dual Tone': 'TVS Radeon',
    'TVS RADEON BSVI DIGI Drum Dual Tone': 'TVS Radeon',
    'TVS RADEON 110 ES MAG DRUM': 'TVS Radeon',
    'TVS RADEON BSVI Disc Dual Tone': 'TVS Radeon',
    'TVS RADEON - DIGI DRUM': 'TVS Radeon',
    'TVS RADEON 110 ES MAG BSVI': 'TVS Radeon',
    'TVS RADEON 110 DUAL TONE': 'TVS Radeon',
    'RADEON DRUM OBDIIB': 'TVS Radeon',
    'Radeon': 'TVS Radeon',
    'TVS RADEON - DIGI DISC': 'TVS Radeon',
    # TVS Raider
    'RAIDER - OBDIIB 1CH ABS': 'TVS Raider',
    'RAIDER SX I-ECU OBDIIB': 'TVS Raider',
    'RAIDER SS DISC OBDIIB': 'TVS Raider',
    'RAIDER DRUM OBDIIB': 'TVS Raider',
    'Raider': 'TVS Raider',
    'TVS RAIDER DISC': 'TVS Raider',
    'TVS RAIDER DISC - SS': 'TVS Raider',
    'RAIDER IGO I-ECU RD WH OBDIIB': 'TVS Raider',
    'RAIDER DISC IGO I-ECU OBDIIB': 'TVS Raider',
    'RAIDER SQD EDN I-ECU OBDIIB': 'TVS Raider',
    'Raider LCD OBDIIB 1CH ABS': 'TVS Raider',
    'TVS RAIDER DRUM': 'TVS Raider',
    'RAIDER DISC OBDIIB': 'TVS Raider',
    'TVS RAIDER DISC - LCD SX': 'TVS Raider',
    'TVS RAIDER DISC - SSE': 'TVS Raider',
    'TVS RAIDER DISC CONNECTED': 'TVS Raider',
    # TVS Ronin
    'TVS RONIN 2CH MID SPECIAL EDI OBDIIB': 'TVS Ronin',
    'TVS RONIN 2CH MID SPECIAL EDITION': 'TVS Ronin',
    'TVS RONIN 2CH MID': 'TVS Ronin',
    'TVS RONIN 1CH BASE+': 'TVS Ronin',
    'TVS RONIN 1CH BASE': 'TVS Ronin',
    'TVS RONIN 1CH BASE-FL RED': 'TVS Ronin',
    'TVS RONIN 1CH BASE-LNG Black - OBDIIB': 'TVS Ronin',
    'TVS Ronin TD': 'TVS Ronin',
    'TVS RONIN 2CH MID SPL': 'TVS Ronin',
    'TVS RONIN 1CH BASE-LNG Black': 'TVS Ronin',
    'TVS RONIN 1CH BASE-FL RED - OBDIIB': 'TVS Ronin',
    'Ronin': 'TVS Ronin',
    # TVS Scooty Pep Plus
    'Scooty Pep+ Spl Edition': 'TVS Scooty Pep Plus',
    'Scooty Pep+ Matte series-BSVI': 'TVS Scooty Pep Plus',
    'Scooty Pep+ -BSVI Tamil Ed': 'TVS Scooty Pep Plus',
    'Scooty Pep+ - BSVI': 'TVS Scooty Pep Plus',
    'Scooty PEP+': 'TVS Scooty Pep Plus',
    # TVS Scooty Zest
    'TVS ZEST - OBDIIB SXC NARDO GREY': 'TVS Scooty Zest',
    'TVS ZEST - OBDIIB SXC BLACK': 'TVS Scooty Zest',
    'TVS Zest 110': 'TVS Scooty Zest',
    'TVS Zest': 'TVS Scooty Zest',
    'Zest': 'TVS Scooty Zest',
    # TVS Sport
    'SPORT ELS REFRESH OBDIIB': 'TVS Sport',
    'Sport': 'TVS Sport',
    'SPORT ES+ OBDIIB': 'TVS Sport',
    'TVS SPORT ES-U559': 'TVS Sport',
    'SPORT ES OBDIIB': 'TVS Sport',
    'TVS SPORT KLS BSVI': 'TVS Sport',
    'TVS SPORT ELS BSVI': 'TVS Sport',
    'TVS SPORT DURALIFE KS SWL BSVI': 'TVS Sport',
    'TVS SPORT ELS BSVI-OBIIA': 'TVS Sport',
    'TVS SPORT ELS BSVI-OBD IIA': 'TVS Sport',
    # TVS Star City Plus
    'StarCity + ES BSVI': 'TVS Star City Plus',
    'CITY+ DISC OBDIIB': 'TVS Star City Plus',
    'TVS StaR city+': 'TVS Star City Plus',
    'StarCity + ES DT BSVI': 'TVS Star City Plus',
    'STARCITY + ES DISC BSVI': 'TVS Star City Plus',
    'CITY+ DRUM OBDIIB': 'TVS Star City Plus',
    'StarCity + BSIV  110 ES MAG WHL': 'TVS Star City Plus',
    'StaR city+': 'TVS Star City Plus',
    # TVS XL100
    'XL100': 'TVS XL100',
    'XL 100': 'TVS XL100',
    'TVS XL 100 HD iTs Spl. Edition-BSVI': 'TVS XL100',
    'TVS XL 100 COM iTs- OBDIIB': 'TVS XL100',
    'TVS XL 100 HD iTs BSVI': 'TVS XL100',
    'TVS XL 100 HD BSVI': 'TVS XL100',
    'TVS XL 100 COM iTs-BSVI': 'TVS XL100',
    'TVS XL 100 COM BSVI': 'TVS XL100',
    'TVS XL 100 HD iTs Winner Edition OBDIIB': 'TVS XL100',
    'TVS XL 100 HD iTs OBDIIB': 'TVS XL100',
    'TVS XL 100 HD iTs Winner Edition': 'TVS XL100',
    'TVS XL 100 HD OBDIIB': 'TVS XL100',
    'TVS XL 100': 'TVS XL100',
    'TVS XL 100 HEAVY DUTY ES': 'TVS XL100',
    # TVS iQube
    'TVS IQUBE ELECTRIC S- MINT BLUE – GLOSSY': 'TVS iQube',
    'TVS iQUBE ELECTRIC ST12 M52V S BLUE': 'TVS iQube',
    'TVS iQube ST 5.1 kWh': 'TVS iQube',
    'iQube': 'TVS iQube',
    'TVS iQube ST 3.4 kWh': 'TVS iQube',
    'TVS iQube S 3.4 kWh': 'TVS iQube',
    'TVS IQube S-New': 'TVS iQube',
    'TVS iQube 3.4 kWh': 'TVS iQube',
    'TVS IQUBE ELECTRIC S- C BRONZE GLOSSY': 'TVS iQube',
    'TVS iQube 2.2 kWh': 'TVS iQube',
    'TVS IQUBE ST 17': 'TVS iQube',
    'TVS IQube UG-New': 'TVS iQube',
    'TVS iQUBE  S15 BEIGE  Fr Disc': 'TVS iQube',
    'TVS iQUBE ELECTRIC ST12 M52V TG MTTE': 'TVS iQube',
    'TVS iQUBE ELECTRIC SMARTXONNECT PEARL W': 'TVS iQube',
    'TVS IQUBE ELECTRIC S-MERCURY GREY–GLOSSY': 'TVS iQube',
    'TVS iQUBE  S15 BLACK Fr Disc': 'TVS iQube',
    'IQUBE ST 12': 'TVS iQube',
    'TVS iQube 11 Fr. Disc Beige': 'TVS iQube',
    'TVS IQube S-Beige': 'TVS iQube',
    'TVS iQUBE ELECTRIC SMARTXONNECT SHINIG.R': 'TVS iQube',
    'TVS iQUBE ELECTRIC S -MINT BLUE – GLOSSY': 'TVS iQube',
    'TVS IQUBE ST 17-Beige': 'TVS iQube',
    'U759 iQUBE': 'TVS iQube',
    'TVS IQube UG-Beige': 'TVS iQube',
    'TVS iQUBE ELECTRIC SMARTXONNECT P.WHITE': 'TVS iQube',
    'U759 iQUBE 11 Black': 'TVS iQube',
    'TVS iQUBE ELECTRIC SMARTXONNECT T.GREY': 'TVS iQube',
    'TVS iQUBE ELECTRIC S -C BRONZE GLOSSY': 'TVS iQube',
    'TVS iQUBE ELECTRIC SMARTXONNECT T GREY': 'TVS iQube',
    'TVS iQUBE ELECTRIC SMARTXONNECT 9 W BRWN': 'TVS iQube',
    'TVS iQube 11 Fr. Disc black': 'TVS iQube',
    'TVS iQube ELECTRIC ST 12 S BLUE': 'TVS iQube',
    'TVS iQube ST': 'TVS iQube',
    'TVS IQUBE ELECTRIC 9': 'TVS iQube',
    'TVS iQube ELECTRIC ST 17 S BLUE': 'TVS iQube',
    'TVS iQube S': 'TVS iQube',
    'TVS iQUBE ELECTRIC SMARTXONNECT 9P.WHITE': 'TVS iQube',
    'TVS iQUBE ELECTRIC S MERCURY GREY': 'TVS iQube',
}

def normalize_lead_model(mdl):
    """Map raw lead ModelName to canonical model name using lookup table."""
    mdl = str(mdl or '').strip()
    if not mdl: return 'Unknown'
    return LEAD_MODEL_MAP.get(mdl, mdl)

def normalize_purchased_model(pm):
    """Map raw purchasedModel string to canonical lead-model name."""
    pm = str(pm or '').strip()
    if not pm: return 'Unknown'
    # Try exact match (handles both proper unicode and corrupted encodings via keyword fallback)
    if pm in PURCHASED_MODEL_MAP:
        return PURCHASED_MODEL_MAP[pm]
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
    if 'IQUBE' in pu or 'IQUE' in pu:                            return 'TVS iQube S'
    if 'RONIN' in pu:                                             return 'TVS Ronin'
    if 'RADEON' in pu:                                            return 'TVS Radeon'
    if 'SPORT' in pu and 'TVS' not in pu.replace('TVS SPORT',''):return 'TVS Sport'
    if 'SPORT' in pu:                                             return 'TVS Sport'
    if 'XL 100' in pu or 'XL100' in pu:                          return 'TVS XL100'
    if 'ZEST' in pu:                                              return 'TVS Scooty Zest'
    if 'STAR CITY' in pu or 'STARCITY' in pu or 'CITY+' in pu:  return 'TVS Star City Plus'
    return pm  # unmapped — keep raw for now

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

# ─── Historical data loaders (file-based, Apr'25–Apr'26) ─────────────────────

HIST_LEAD_FILES = [
    {'path': 'Leads Data Master_Leads_FY_25_26 Part 1.xlsb', 'engine': 'pyxlsb',  'sheet': 'Raw Data'},
    {'path': 'Leads Data Master_Leads_FY_25_26 Part 2.xlsx', 'engine': 'openpyxl', 'sheet': 0},
    {'path': 'Leads Data Master_Leads_FY_25_26 Part 3.xlsx', 'engine': 'openpyxl', 'sheet': 'Sheet1'},
    {'path': 'Leads Data Master_Leads_FY_26_27.xlsb',        'engine': 'pyxlsb',   'sheet': 'Raw Data'},
]

HIST_RETAIL_FILES = [
    {'path': 'Retail Data Master_Retails_FY_25_26.xlsb',       'engine': 'pyxlsb', 'pm_col': 'Purchased Model 2'},
    {'path': 'Retail Data Master_Retails_FY_26_27 (1).xlsb',   'engine': 'pyxlsb', 'pm_col': 'Purchased Model'},
]

_FILE_LEAD_RENAME = {
    'Lead Type':   'LeadType',
    'Dealer_Name': 'DealerName',
    'Lead Month':  'LeadMonth',
}

def standardize_file_leads(df):
    """Normalize file-based lead DataFrame to canonical pipeline format."""
    df = df.rename(columns={k: v for k, v in _FILE_LEAD_RENAME.items() if k in df.columns}).copy()
    df['SorceLeadId'] = df['SorceLeadId'].apply(to_id)
    df['LeadMonth']   = df.get('LeadMonth', pd.Series(dtype=str)).apply(
                            lambda v: norm_month(str(v or '').strip()))
    if 'State' in df.columns:
        df['State'] = df['State'].astype(str).str.strip().str.title()
    if 'BuyingDays' not in df.columns: df['BuyingDays'] = '0'
    if 'Zone'       not in df.columns: df['Zone']       = 'Unknown'
    keep = ['SorceLeadId','LeadMonth','ModelName','Source','LeadType',
            'State','Zone','BuyingDays','CityName','DealerName']
    out = df[[c for c in keep if c in df.columns]].copy()
    # Drop rows with no ID or no month
    out = out[out['SorceLeadId'].str.len() > 0]
    out = out[out['LeadMonth'].str.len() > 0]
    return out

def load_hist_leads():
    """Read all historical lead files, return combined standardized DataFrame."""
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
            months = sorted(df['LeadMonth'].unique())
            print(f"    {len(df):,} leads, months: {months}", flush=True)
            dfs.append(df)
        except Exception as e:
            print(f"  WARNING: Could not load {spec['path']}: {e}", flush=True)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def load_hist_retail_map():
    """Build {SorceLeadId → {rm, rtype, pm}} from historical retail files."""
    rmap = {}
    for spec in HIST_RETAIL_FILES:
        path = os.path.join(HIST_DIR, spec['path'])
        if not os.path.exists(path):
            print(f"  SKIP (not found): {spec['path']}", flush=True)
            continue
        try:
            print(f"  Reading {spec['path']}…", flush=True)
            df = pd.read_excel(path, sheet_name=0, engine=spec['engine'])
            pm_col = spec['pm_col']
            df['xlid'] = df['SorceLeadId'].apply(to_id)
            df['xrm']  = df['Retail Month'].astype(str).str.strip().apply(norm_month)
            df['xpm']  = df[pm_col].apply(normalize_purchased_model)
            df['xrt']  = df['DMS/Call Out'].fillna('').astype(str).str.strip()
            valid = df[df['xlid'].str.len() > 0].copy()
            # Deduplicate: first occurrence of each lid within this file wins
            deduped = valid.drop_duplicates(subset=['xlid'], keep='first')
            records = deduped[['xlid','xrm','xpm','xrt']].to_dict('records')
            added = 0
            for r in records:
                lid = r['xlid']
                if lid in rmap: continue  # earlier file already has this lid
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

    for i, (_, row) in enumerate(all_leads.iterrows()):
        if i % 100000 == 0 and i > 0:
            print(f"  {i:,}/{total:,} ({100*i//total}%)", flush=True)

        lid  = to_id(row.get('SorceLeadId', ''))
        lm   = str(row.get('LeadMonth',  '') or '').strip()
        src  = str(row.get('Source',     '') or '').strip() or 'Unknown'
        lt   = str(row.get('LeadType',   '') or '').strip() or 'Unknown'
        mdl  = normalize_lead_model(row.get('ModelName', ''))
        st   = str(row.get('State',      '') or '').strip().title() or 'Unknown'
        zone = str(row.get('Zone',       '') or '').strip() or 'Unknown'
        bd   = str(row.get('BuyingDays', '') or '0').strip() or '0'
        city = str(row.get('CityName',   '') or '').strip() or 'Unknown'

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

        if dl_col:
            dl  = str(row.get(dl_col, '') or '').strip() or 'Unknown'
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
            pm  = retail_map[lid].get('pm', 'Unknown')
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

# 1. Historical retail files (Apr'25–Apr'26)
print(f"\n[1/5] Loading historical retail files from {HIST_DIR}…", flush=True)
retail_map = load_hist_retail_map()
print(f"  Historical retail map: {len(retail_map):,} entries", flush=True)

# 2. Online retail master — merge on top (online is authoritative for same ID)
print("\n[2/5] Loading online retail master…", flush=True)
retail_df   = fetch_retails()
online_rmap = build_retail_map(retail_df)
new_online  = 0
for lid, info in online_rmap.items():
    retail_map[lid] = info   # online overwrites historical for the same lid
    new_online += 1
print(f"  Online entries merged: {new_online:,}  Combined total: {len(retail_map):,}", flush=True)

# 3. Historical leads (Apr'25–Apr'26) from files
print("\n[3/5] Loading historical lead files (Apr'25–Apr'26)…", flush=True)
hist_leads = load_hist_leads()
print(f"  Historical leads total: {len(hist_leads):,}", flush=True)

# 4. Online leads (May'26+ only)
print(f"\n[4/5] Loading online lead sheets ({ONLINE_START}+)…", flush=True)
lead_dfs  = []
rtype_map = {}
for sheet in LEAD_SHEETS:
    try:
        raw = fetch_sheet_via_proxy(sheet['id'], sheet['label'], tab_name=sheet['tab'])
        raw.columns = [c.strip() for c in raw.columns]
        rtype_map.update(extract_rtype_map(raw))
        std = standardize_leads(raw)
        # Only keep leads from ONLINE_START onwards — historical files cover earlier months
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

# 5. Combine all leads + synthetic gap-fill + aggregate
print("\n[5/5] Combining leads, gap-fill, and aggregating…", flush=True)
online_leads = pd.concat(lead_dfs, ignore_index=True) if lead_dfs else pd.DataFrame()
parts = [df for df in [hist_leads, online_leads] if len(df) > 0]
all_leads = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
print(f"  Historical: {len(hist_leads):,}  Online: {len(online_leads):,}  Total: {len(all_leads):,}",
      flush=True)

matched_lids = {to_id(v) for v in all_leads['SorceLeadId'].dropna() if to_id(v)}
synthetic    = make_synthetic_leads(retail_df, matched_lids)
if len(synthetic):
    all_leads = pd.concat([all_leads, synthetic], ignore_index=True)
    print(f"  Synthetic gap-fill rows: {len(synthetic):,}", flush=True)
print(f"  Grand total rows: {len(all_leads):,}", flush=True)

print("\nAggregating and pushing…", flush=True)
payload  = build_payload(all_leads, retail_map)
json_str = json.dumps(payload, separators=(',', ':'))
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
