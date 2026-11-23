# Databricks notebook source
# MAGIC %md
# MAGIC ## Project Master Base

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing Libraries

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install openpyxl odfpy -q

# COMMAND ----------

import re
import requests
import pandas as pd
import numpy as np
import datetime
import os
from openpyxl import load_workbook
import warnings
warnings.filterwarnings('ignore')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Empty Project Master Table with Placeholders

# COMMAND ----------

### Creation of blueprint of Project Master(empty)
pm_columns = [
        "UPC",
        "Zone",
        "Project Name",
        "Implementation Mode",
        "Current Status",
        "State Name",
        'PIU',
        "PIU Email ID",
        "PIU Contact No.",
        "RO",
        "RO Email ID",
        "RO Contact No.",
        #"MCW Length(in km)",
        "Length-km",
        #"IR Length",
        "IR (Count)",
        #"SR Length",
        "Lane-km (SR)",
        "No of Lanes",
        #"Lane Length(in km)",
        "Lane-km (MCW)",
        "Lane-km",
        "NH No.",
        "AE/IE Name",
        "AE/IE Contract Start Date",
        "AE/IE Contract End Date",
        "AE/IE Coverage Status",
        "Contractor Name",
        "Contractor Contract Start Date",
        "Contractor Contract End Date",
        "Contractor Coverage Status",
        "Alternate UPC",
        "Project Type",
        "Start Chainage Latitude",
        "Start Chainage Longitude",
        "End Chainage Latitude",
        "End Chainage Longitude",
        "Awarded Cost",
        "Total Capital Cost",
        "LOA Date",
        "Agreement Date",
        "Actual Start Date / Appointed Date",
        "NSV Vendor Name",
        "NSV Zone",
        "NSV Coverage Status",
        "DAMS Vendor Name",
        "DAMS Zone",
        "DAMS Coverage Status",
        "DAS Vendor Name",
        "DAS Zone",
        "DAS Coverage Status",
        "Scheduled Completion as per Contract",
        "Completion Status",
        "final_completiondate",
        "Scheme",
        "Likely Completion Date", #to keep as of now
        "Contractor Email ",
        "AE/IE Email",
        'is_clean_data', #TBD
        'is_active' #TBD
]

df_project_master = pd.DataFrame(columns=pm_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC #### Consolidate Master Ingestion

# COMMAND ----------

CM_PATH = "/Workspace/Users/shivam.raj9226@softsensor.ai/Project Master Soumyadeep/Input files/20260519_Consolidated Master-18-09-2026(cleaned).xlsx"

cm = pd.read_excel(
    CM_PATH,
    sheet_name="cleaned",
    engine="openpyxl"
)

print("Read successful")
print(f"CM rows × cols : {cm.shape}")
print("Unique UPCs present: ", cm['UPC'].nunique())

# COMMAND ----------

sorted(cm.columns.tolist())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Merging Zone Sheets with Cleaned Master to get Contract Dates

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cleaning Date Columns

# COMMAND ----------

# ============================================================================
# Normalise "empty-looking" placeholders to real NaN in the raw contract date
# columns of cm_with_dates.
#
# These columns are object dtype holding a MIX of Timestamps and free text.
# '-' / '--' / '   ' / '—' all mean "no date on record", but as non-null
# strings they read as populated: .isna() counts miss them, and they show up
# as 'Placeholder text' in issue_df instead of as missing data.
#
# ============================================================================

RAW_DATE_COLS = [
    'O&M Contractor Start Date of Contract',        # -> Contractor Contract Start Date
    'O&M Contractor End Date of Contract',          # -> Contractor Contract End Date
    'AE/IE Start Date of Contract',      # -> AE/IE Contract Start Date
    'AE/IE End Date of Contract',        # -> AE/IE Contract End Date
]

# Anything made up ONLY of whitespace and/or dash characters. Cannot match a
# value containing a digit or letter, so it can never eat a real date.
#   covers: '' ' ' '   ' '-' '--' ' - ' '–' '—' '\u00a0' (non-breaking space)
BLANKISH = re.compile(r'[-\u2010-\u2015\u2212\s\u00a0\u200b]*')


def blank_placeholders(df, cols, pattern=BLANKISH, verbose=True):
    """Replace whitespace/dash-only STRING cells with np.nan, in place.

    Only str cells are considered — Timestamps, datetimes and numbers are left
    untouched, so this is safe on the mixed-dtype columns.
    Returns a per-column summary of what changed.
    """
    rows = []

    def is_blankish(v):
        return isinstance(v, str) and pattern.fullmatch(v) is not None

    for c in cols:
        if c not in df.columns:
            print(f"  [skip] column not present: {c}")
            continue

        s = df[c]
        blankish = s.map(is_blankish).fillna(False).astype(bool)

        null_before = int(s.isna().sum())

        if verbose and blankish.any():
            # repr() so spaces / NBSP / dash variants are actually visible
            seen = s[blankish].map(repr).value_counts()
            print(f"\n  {c}  —  blanking {int(blankish.sum())} cell(s)")
            for val, n in seen.items():
                print(f"      {n:>5}  {val}")

        df.loc[blankish, c] = np.nan

        rows.append({
            'column':      c,
            'dtype':       str(s.dtype),
            'blanked':     int(blankish.sum()),
            'null_before': null_before,
            'null_after':  int(df[c].isna().sum()),
            'still_text':  int(df[c].map(lambda v: isinstance(v, str)).sum()),
        })

    return pd.DataFrame(rows)


placeholder_summary = blank_placeholders(cm, RAW_DATE_COLS)

print('\n' + '=' * 70)
print(placeholder_summary.to_string(index=False))

# COMMAND ----------

# ============================================================================
# Normalise the four raw contract date columns to ONE format: datetime64[ns].
#
# As read, each column is object dtype holding a mix of:
#   * pd.Timestamp / datetime.datetime  -- cells Excel already stored as dates
#                                          (all of Zone A, part of B / C / E)
#   * 'DD.MM.YYYY' / 'DD-MM-YYYY' / 'DD/MM/YYYY' text, some carrying stray
#     leading/trailing spaces or non-breaking spaces ('30.06.2026\xa0')
#   * 'YYYY-MM-DD 00:00:00' text (a datetime that was written back as a string)
#   * annotated dates, e.g. '30.10.2026 (EOT)'
#   * '-' placeholders (already blanked above) and free text ('Transfer to PWD')
#
# Every string date in this workbook is DAY-FIRST: each '/' and '-' cell left
# as text has a first component > 12 (Excel had already converted the ones it
# could read), and the '.' cells run 1-31 in the first position against 1-12
# in the second. So parse day-first, never month-first.
#
# Values that are not dates -- free text, impossible calendar days such as
# '31.04.2031' -- become NaT and are listed in date_audit_df rather than
# vanishing silently. Trailing annotations are recorded there too.
# ============================================================================

# ISO-ish text openpyxl handed back as a string: '2024-04-26 00:00:00'
_ISO_DATE = re.compile(r'^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})(?:[ T].*)?$')

# Day-first text: '14.08.2024', '15-04-2026', '31/01/2024', '21. 01.2027'.
# Anchored at the start so a trailing note -- '(EOT)' -- is split off, not lost.
_DAYFIRST_DATE = re.compile(r'^(\d{1,2})\s*[./-]\s*(\d{1,2})\s*[./-]\s*(\d{2,4})\b')

# non-breaking space and zero-width space, spelled as escapes so they survive
# copy/paste and diffs
_NBSP, _ZWSP = ' ', '​'


def _strip_invisibles(s):
    """Drop NBSP / zero-width characters that hide inside these cells."""
    return s.replace(_NBSP, ' ').replace(_ZWSP, '').strip()


def parse_contract_date(v):
    """Coerce one raw cell to a date.

    Returns (Timestamp or NaT, leftover_text, reason), where reason is one of
    'datetime' (already a date object), 'parsed' (text -> date), 'blank'
    (NaN or a dash/whitespace placeholder), 'invalid' (looks like a date but
    is not a real calendar day) or 'unparsed' (not a date at all).
    """
    if v is None or v is pd.NaT or (isinstance(v, float) and pd.isna(v)):
        return pd.NaT, '', 'blank'

    # already a date object -> only the 00:00:00 time part needs dropping
    if isinstance(v, (pd.Timestamp, datetime.datetime, datetime.date)):
        return pd.Timestamp(v).normalize(), '', 'datetime'

    if not isinstance(v, str):
        return pd.NaT, str(v), 'unparsed'

    s = _strip_invisibles(v)
    if BLANKISH.fullmatch(s):            # '' / '-' / '--' / whitespace only
        return pd.NaT, '', 'blank'

    m = _ISO_DATE.match(s)
    if m:
        year, month, day = (int(x) for x in m.groups())
    else:
        m = _DAYFIRST_DATE.match(s)
        if not m:
            return pd.NaT, s, 'unparsed'
        day, month, year = (int(x) for x in m.groups())
        if year < 100:                   # defensive: no 2-digit years seen yet
            year += 2000 if year < 70 else 1900

    try:
        ts = pd.Timestamp(year=year, month=month, day=day)
    except ValueError:                   # e.g. '31.04.2031', '30.02.2020'
        return pd.NaT, s, 'invalid'

    leftover = _strip_invisibles(s[m.end():])
    return ts, ('' if BLANKISH.fullmatch(leftover) else leftover), 'parsed'


def normalise_date_columns(df, cols, parser=parse_contract_date):
    """Rewrite `cols` in place as datetime64.

    Returns (summary, audit, reasons) -- `reasons` is a same-index frame of the
    per-cell parse reason, kept so later checks can tell an Excel-converted
    date apart from one this code parsed out of text.
    """
    summary, audit, reasons = [], [], {}

    for c in cols:
        if c not in df.columns:
            print(f"  [skip] column not present: {c}")
            continue

        original = df[c]
        result   = original.map(parser)
        values   = result.map(lambda t: t[0])
        leftover = result.map(lambda t: t[1])
        reason   = result.map(lambda t: t[2])

        # keep a row-level trail of everything that was not a clean date
        flagged = reason.isin(['unparsed', 'invalid']) | ((reason == 'parsed') & (leftover != ''))
        for i in original.index[flagged]:
            audit.append({
                'row':      i,
                'Zone':     df.at[i, 'Zone'] if 'Zone' in df.columns else None,
                'UPC':      df.at[i, 'UPC'] if 'UPC' in df.columns else None,
                'column':   c,
                'original': repr(original.at[i]),
                'parsed':   values.at[i],
                'leftover': leftover.at[i],
                'reason':   reason.at[i],
            })

        df[c] = pd.to_datetime(values)
        reasons[c] = reason

        summary.append({
            'column':           c,
            'already_datetime': int((reason == 'datetime').sum()),
            'text_parsed':      int((reason == 'parsed').sum()),
            'blank':            int((reason == 'blank').sum()),
            'not_a_date':       int(reason.isin(['unparsed', 'invalid']).sum()),
            'non_null_after':   int(df[c].notna().sum()),
            'dtype':            str(df[c].dtype),
        })

    return pd.DataFrame(summary), pd.DataFrame(audit), pd.DataFrame(reasons)


date_summary, date_audit_df, date_reasons = normalise_date_columns(cm, RAW_DATE_COLS)

print('=' * 70)
print(date_summary.to_string(index=False))

if len(date_audit_df):
    print('\n' + '=' * 70)
    print(f'{len(date_audit_df)} cell(s) to review (NaT unless reason == "parsed"):')
    print(date_audit_df.to_string(index=False))


def flag_daymonth_swap(df, cols, reasons, zone_col='Zone', min_cells=20):
    """Warn where Excel looks to have read DD/MM as MM/DD at source.

    Only the cells that arrived ALREADY converted ('datetime') can carry that
    error -- the text ones are parsed day-first here. If every such cell in a
    zone lands on day <= 12 while the zone's text dates spread across 1-31,
    the conversion was almost certainly month-first. Flagged, never rewritten:
    the original text is gone from the workbook, so this needs a human.
    """
    if zone_col not in df.columns:
        return
    for z, idx in df.groupby(zone_col).groups.items():
        days = pd.concat([df.loc[idx, c][reasons.loc[idx, c] == 'datetime'] for c in cols]).dropna().dt.day
        if len(days) >= min_cells and (days <= 12).all():
            print(f'\n[warn] Zone {z}: all {len(days)} Excel-converted dates fall on '
                  f'day <= 12 -- likely DD/MM read as MM/DD at source. Left as-is.')


flag_daymonth_swap(cm, RAW_DATE_COLS, date_reasons)

# COMMAND ----------

cm

# COMMAND ----------

# MAGIC %md
# MAGIC #### Basic Project Details API Ingestion

# COMMAND ----------

TOKEN_URL = "https://datalakeg.nhai.gov.in/nhaiapi/api/MastersAPI/Get"
DATA_URL  = "https://datalakeg.nhai.gov.in/nhaiapi/api/MastersAPI/NHAIBasicDetailsAPIKey"

CREDENTIALS = {
    "username": "guest",
    "password": "Guest@2020",
}

STATIC_HEADERS = {
    "api-key": "0f086a6789192b4d89ebf56ac5cd7587",
    "api-org": "DHMS",
}

# Request timeouts (seconds)
TOKEN_TIMEOUT = 15
DATA_TIMEOUT  = 30

def get_token() -> str:
    response = requests.get(TOKEN_URL, params=CREDENTIALS, timeout=TOKEN_TIMEOUT)
    response.raise_for_status()
    data = response.json()

    if isinstance(data, str):
        return data

    if isinstance(data, dict):
        for key in ("token", "Token", "access_token", "jwt"):
            if key in data:
                return data[key]
        raise KeyError(f"Token key not found. Keys present: {list(data.keys())}")

    raise ValueError(f"Unexpected token response type: {type(data)}")

def fetch_bpd(token: str) -> pd.DataFrame:
    headers = {"Authorization": token, **STATIC_HEADERS}
    response = requests.get(DATA_URL, headers=headers, timeout=DATA_TIMEOUT)
    response.raise_for_status()

    payload = response.json()

    if isinstance(payload, list):
        records = payload

    elif isinstance(payload, dict):
        for key in ("data", "Data", "result", "Result", "records", "Records"):
            if key in payload and isinstance(payload[key], list):
                records = payload[key]
                break
        else:
            records = [payload]  # single-record edge case

    else:
        raise ValueError(f"Unexpected payload type: {type(payload)}")

    return pd.DataFrame(records)

token = get_token()
raw_fetched = fetch_bpd(token)

#Converting fetched data into dataframe
df_raw = raw_fetched['data'].iloc[0]
df = pd.DataFrame(df_raw['data'])

#Cleaning 

ILLEGAL_CHARS_RE = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]')

def clean_str(val):
    if isinstance(val, str):
        return ILLEGAL_CHARS_RE.sub('', val)
    return val

df_cleaned = df.map(clean_str)
print(f"Shape  : {df_cleaned.shape}")

df_cleaned.rename(columns={'upc':'UPC'},inplace=True)
df_bpd = df_cleaned.copy()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Clean

# COMMAND ----------

# MAGIC %md
# MAGIC #### Consolidate Master Alternate UPC mapping into a single column & UPC cleaning

# COMMAND ----------

# UPC Clean

UPC_PATTERN = re.compile(r"[A-Za-z]/\d{4,6}/\d{4,6}/[A-Za-z]{2}(?:-[A-Za-z]+)?")

# --- Clean main UPC column ---
def clean_single_upc(value):
    if pd.isna(value):
        return np.nan
    match = UPC_PATTERN.search(str(value))   # first core UPC found
    return match.group(0).strip() if match else str(value).strip()

cm["UPC"] = cm["UPC"].map(clean_single_upc)
#cm_with_dates["UPC"] = cm_with_dates["UPC"].map(clean_single_upc)

# # Alternate UPC logic--- Combine all alternate UPC columns into one clean, unique, comma-separated column ---
# alt_cols = ['Alternate UPC', 'Alternate UPC 1', 'Alternate UPC 2', 'Alternate UPC 3', 'Alternate UPC 4'] #Cleaned UPCs of alternate upcs

# def combine_alt_upcs(row):
#     seen = []
#     for col in alt_cols:
#         val = row[col]
#         if pd.isna(val):
#             continue
#         for m in UPC_PATTERN.findall(str(val)):   # only valid core UPCs
#             m = m.strip()
#             if m not in seen:                     # keep only unique
#                 seen.append(m)
#     return ", ".join(seen) if seen else np.nan

# cm_with_dates["Alternate_UPCs"] = cm_with_dates.apply(combine_alt_upcs, axis=1)
# alt_upc_table = cm_with_dates[['UPC'] + alt_cols + ['Alternate_UPCs'] + ["All UPCs (cleaned)"]]
# alt_upc_table.head()

# COMMAND ----------

# Show rows where UPC did not match the standard pattern
print("Shape of data:", cm.shape)
missing_upc = cm[cm['UPC'].isna() | (cm['UPC'].astype(str).str.strip() == '')]
print("Count of rows with missing/blank UPC:", len(missing_upc))
# Keep ALL rows — no UPC rows are dropped
cm_with_dates = cm.copy().reset_index(drop=True)
print("Keeping all rows (no UPC rows dropped)")
print("Shape of data:", cm_with_dates.shape)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Basic Project Details UPC "-RCW" handling

# COMMAND ----------

# UPCs containing "RCW" (case-insensitive)
mask_rcw = cm_with_dates['UPC'].astype(str).str.contains('RCW', case=False, na=False)

rcw_upcs = cm_with_dates.loc[mask_rcw, 'UPC'].unique()

# COMMAND ----------

# ── Make df_bpd's UPCs match the "-RCW" variants used in project master ──────
# PM has e.g. 'N/08038/01001/BR-RCW'; the API returns the core 'N/08038/01001/BR'.
# Add a duplicate row under the suffixed UPC so BOTH keys resolve.

# 1. core UPC -> suffixed UPC  (e.g. 'N/08038/01001/BR' -> 'N/08038/01001/BR-RCW')
rcw_map = {u.rsplit('-', 1)[0].strip(): u for u in rcw_upcs}

# 2. rows in df_bpd whose UPC is one of those cores
core_hits = df_bpd['UPC'].astype(str).str.strip().isin(rcw_map)
print("Core UPCs found in df_bpm:", core_hits.sum(), "of", len(rcw_map))
print("Missing from API :", sorted(set(rcw_map) - set(df_bpd['UPC'].astype(str).str.strip())))

# 3. clone those rows with the -RCW UPC and append
rcw_rows = df_bpd[core_hits].copy()
rcw_rows['UPC'] = rcw_rows['UPC'].astype(str).str.strip().map(rcw_map)

df_bpd = pd.concat([df_bpd, rcw_rows], ignore_index=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RO & PIU Mail/Contact Split in Consolidate Master

# COMMAND ----------

# # --- patterns ---------------------------------------------------------------
# EMAIL_RE = re.compile(r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}')
# # phone within a SINGLE token: optional +, digits with internal spaces/dashes/()
# PHONE_RE = re.compile(r'\+?\d[\d\s\-()]{6,}\d')
# SPLIT_RE = re.compile(r'[\/,;\n]+')          # the real separators seen in the data


# def _dedup(seq):
#     out = []
#     for x in seq:
#         if x and x not in out:
#             out.append(x)
#     return out


# def _as_text(val):
#     """Excel gives phone-only cells as int/float -> kill the '.0' tail."""
#     if isinstance(val, float) and val.is_integer():
#         return str(int(val))
#     return str(val)


# def _phones_in(text):
#     """Digit runs of plausible length (India: 10; 11 w/ STD 0; 12-13 w/ 91).
#        Re-splits greedy matches that glued two space-separated numbers together."""
#     out = []
#     for raw in PHONE_RE.findall(text):
#         if 8 <= len(re.sub(r'\D', '', raw)) <= 13:
#             out.append(raw.strip())
#         else:
#             for piece in re.split(r'\s+', raw):
#                 if 8 <= len(re.sub(r'\D', '', piece)) <= 13:
#                     out.append(piece.strip())
#     return out


# def split_email_contact(val):
#     """Split a messy '<name> / <email> / <phone>' cell into (emails_csv, phones_csv).
#        - emails : anything with '@'  (lower-cased, de-duplicated)
#        - phones : 8-13 digit runs, original formatting kept (dashes/spaces/+)
#        - name / designation / label text is dropped."""
#     if pd.isna(val):
#         return np.nan, np.nan

#     emails, phones = [], []
#     for token in SPLIT_RE.split(_as_text(val)):   # break on / , ; and newlines first
#         token = token.strip()
#         if not token:
#             continue

#         # 1. emails in this token
#         emails.extend(e.lower() for e in EMAIL_RE.findall(token))

#         # 2. phones from the token AFTER stripping any email out of it
#         phones.extend(_phones_in(EMAIL_RE.sub(' ', token)))

#     return (', '.join(_dedup(emails)) or np.nan,
#             ', '.join(_dedup(phones)) or np.nan)


# # --- apply to the raw CM columns --------------------------------------------
# CM_CONTACT_COLS = [
#     ('PIU Mail ID / PIU Contact No.',    'PIU Email ID', 'PIU Contact No.'),
#     ('RO / RO Mail ID / RO Contact No.', 'RO Email ID',  'RO Contact No.'),
# ]

# for src_col, email_col, contact_col in CM_CONTACT_COLS:
#     parsed = cm_with_dates[src_col].apply(split_email_contact)
#     cm_with_dates[email_col]   = parsed.str[0]
#     cm_with_dates[contact_col] = parsed.str[1]


# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean Contractor and AE/IE Name

# COMMAND ----------

cm_with_dates['AE/IE Name'].nunique()

# COMMAND ----------

# ============================================================================
# Clean 'AE/IE Name' in cm_with_dates.
#
# THREE separate concerns, kept separate on purpose:
#   1. AUDIT       - report what kinds of dirt exist. Writes nothing.
#   2. clean_name  - SURFACE clean. This is what gets written back. Keeps the
#                    name human-readable; merges no two firms together.
#   3. match_key   - AGGRESSIVE normalisation used ONLY to group variants of
#                    the same firm. NEVER written to the dataframe: it strips
#                    all punctuation and spacing and is unreadable by design.
#
# Step 3 then rewrites each row to the most frequent spelling within its key
# group, so 'M/s Lion Engineering Consultants Private Limited ' and
# 'Lion Engineering Consultant Pvt. Ltd.' collapse to one string.
#
# RUN BEFORE the cm -> pm mapping cell (cm_to_pm).
# ============================================================================
import re
import numpy as np
import pandas as pd

NAME_COL = 'AE/IE Name'      # same code works for 'Contractor / Concessionaire'

# --- the 'messieurs' honorific, in every spelling present in the data --------
# Two alternatives, and the ORDER of the guards matters:
#   slash form : M/s  M/S  m/s  M / s  M/s.   -> the '/' makes it unambiguous
#   bare  form : MS  Ms  ms                   -> MUST be a standalone token.
# \b on BOTH sides of the bare form is what stops it eating the 'MS' in
# 'MSV International' (194 rows) and 'MSPARK Futuristics' (25 rows) - those are
# company names, not honorifics. Do not relax this to a lookahead.
MS_TOKEN = re.compile(r'(?:\b[Mm]\s*/\s*[Ss]\s*\.?|\b[Mm][Ss]\b\.?)\s*')

# ---- 1. AUDIT ---------------------------------------------------------------
CHECKS = {
    'leading/trailing space' : lambda t: t != t.strip(),
    'tab / newline inside'   : lambda t: bool(re.search(r'[\t\n\r]', t)),
    'NBSP / zero-width'      : lambda t: bool(re.search(r'[\u00a0\u200b\u2007\u202f]', t)),
    'double space'           : lambda t: bool(re.search(r'\S  +\S', t)),
    'trailing "."'           : lambda t: t.strip().endswith('.'),
    'trailing , ; - :'       : lambda t: bool(re.search(r'[,;:\-]$', t.strip())),
    'space before . or ,'    : lambda t: bool(re.search(r'\s[.,]', t)),
    'M/s variant present'    : lambda t: bool(MS_TOKEN.search(t)),
    'M/s spelled not "M/s"'  : lambda t: any(m.group(0).strip().rstrip('.') != 'M/s'
                                             for m in MS_TOKEN.finditer(t)),
    'placeholder / no name'  : lambda t: bool(re.fullmatch(r'[\s\-\u2013\u2014]*', t))
                                         or bool(re.search(r'not\s*allot|not\s*award', t, re.I)),
}

txt = cm_with_dates[NAME_COL].dropna().astype(str)
print(f"rows {len(cm_with_dates)} | non-null {len(txt)} | distinct raw {txt.nunique()}")
print(pd.DataFrame([{'issue': k,
                     'rows': int(txt.map(f).sum()),
                     'distinct values': int(txt[txt.map(f)].nunique())}
                    for k, f in CHECKS.items()]
                  ).sort_values('rows', ascending=False).to_string(index=False))

# every distinct spelling of the honorific actually found, so nothing is missed
print("\nhonorific spellings found:",
      dict(pd.Series([m.group(0) for t in txt for m in MS_TOKEN.finditer(t)]).value_counts()))

# ---- 2. SURFACE CLEAN (this is what gets written) ---------------------------
INVISIBLE   = re.compile(r'[\u00a0\u200b\u2007\u202f\t\n\r]')
PLACEHOLDER = re.compile(r'^(?:[\s\-\u2013\u2014]*|n\.?a\.?|nil|none|not\s*applicable)$', re.I)

def clean_name(v):
    if pd.isna(v):
        return np.nan
    t = INVISIBLE.sub(' ', str(v))
    t = re.sub(r'\s+', ' ', t).strip()          # collapse runs of whitespace
    t = MS_TOKEN.sub('M/s ', t)                 # m/s, ms, M/S, MS, M/s. -> 'M/s '
    t = re.sub(r'\s+', ' ', t).strip()          # ...and re-collapse after it
    t = re.sub(r'\s+([.,])', r'\1', t)          # 'Ltd ,' -> 'Ltd,'
    t = re.sub(r'[.,;:\-]+$', '', t).strip()    # ONE trailing-punct rule for all
    if PLACEHOLDER.match(t) or re.search(r'not\s*allot|not\s*award', t, re.I):
        return np.nan                           # '-', 'AE/IE not alloted' = missing
    return t or np.nan

# ---- 3. MATCH KEY (grouping only - never written) ---------------------------
# Singular/plural and legal-form synonyms folded together; spaces dropped last
# so 'L.N.' == 'LN' == 'L N'.
LEGAL = {'private':'pvt','pvt':'pvt','limited':'ltd','ltd':'ltd',
         'incorporated':'inc','inc':'inc','corporation':'corp','corp':'corp',
         'company':'co','co':'co','llp':'llp',
         'associates':'assoc','association':'assoc','associate':'assoc','assoc':'assoc',
         'consultants':'consultant','consultancy':'consultant',
         'services':'service','solutions':'solution','projects':'project',
         'engineers':'engineer','engineering':'engineer',
         'jv':'jv','joint':'jv','venture':'jv'}
# scanned-'l'-for-'I' and misspellings actually present in this column
TYPO  = {'lnfra':'infra','lndo':'indo','lnternational':'international','ln':'in',
         'enginneering':'engineer','consultanats':'consultant',
         'compnies':'companies','malaviya':'malviya'}

def match_key(v):
    if pd.isna(v):
        return np.nan
    # Drop the honorific BEFORE stripping punctuation: 'M/s' would otherwise
    # become the tokens 'm','s' and keep 'M/s Lion ...' from grouping with
    # 'Lion ...'. Runs on the cleaned string, so only 'M/s ' can be present.
    k = MS_TOKEN.sub(' ', str(v)).lower().replace('&', ' and ')
    k = re.sub(r'[^a-z0-9]+', ' ', k)                       # kills . - , / ( )
    return ''.join(LEGAL.get(w, TYPO.get(w, w)) for w in k.split())

cleaned = cm_with_dates[NAME_COL].map(clean_name)
keys    = cleaned.map(match_key)

# ---- review: key groups still holding more than one spelling ---------------
grp   = pd.DataFrame({'key': keys, 'clean': cleaned}).dropna(subset=['key'])
multi = grp.groupby('key')['clean'].nunique()
multi = multi[multi > 1].index
print(f"\n{len(multi)} key-groups hold >1 spelling - review before accepting:")
for k in multi:
    sub = grp.loc[grp['key'] == k, 'clean'].value_counts()
    print(f"\n  [{int(sub.sum())} rows]")
    for val, n in sub.items():
        print(f"     {n:>3}  {val[:110]}")

# ---- canonical spelling = most frequent variant in the group ---------------
# Override any group whose majority spelling you don't like by adding
#   MANUAL[match_key('<any raw variant>')] = '<preferred spelling>'
MANUAL = {}

canon = (grp.groupby(['key', 'clean']).size().rename('n').reset_index()
            .sort_values(['key', 'n', 'clean'], ascending=[True, False, True])
            .drop_duplicates('key').set_index('key')['clean'])
canon.update(pd.Series(MANUAL))

cm_with_dates[NAME_COL] = keys.map(canon)

print(f"\ndistinct: {txt.nunique()} raw -> {cleaned.dropna().nunique()} cleaned"
      f" -> {cm_with_dates[NAME_COL].dropna().nunique()} canonical")
print(f"rows blanked as placeholder: {int(len(txt) - cm_with_dates[NAME_COL].notna().sum())}")


# COMMAND ----------

cm_with_dates['AE/IE Name'].nunique()

# COMMAND ----------

cm_with_dates['Contractor / Concessionaire'].nunique()

# COMMAND ----------

# ============================================================================
# Clean 'AE/IE Name' and 'Contractor / Concessionaire' in cm_with_dates.
#
# THREE separate concerns, kept separate on purpose:
#   1. AUDIT       - report what kinds of dirt exist. Writes nothing.
#   2. clean_name  - SURFACE clean. This is what gets written back. Keeps the
#                    name human-readable; merges no two firms together.
#   3. match_key   - AGGRESSIVE normalisation used ONLY to group variants of
#                    the same firm. NEVER written to the dataframe: it strips
#                    all punctuation and spacing and is unreadable by design.
#
# Step 3 then rewrites each row to the most frequent spelling within its key
# group, so 'M/s Gawar Construction Ltd' / 'Gawar Construction Ltd' /
# 'Gawar Construction Limited' collapse to one string.
#
# The contractor column is messier than AE/IE and gets two extra passes:
#   - role labels ('EPC Contractor - ', 'Maintenance Contractor- ') stripped
#     off the front and kept in a separate '<col> Role Label' column;
#   - narrative / multi-entity cells ('...terminated on 17.03.2023',
#     'EPC Contractor - X \n Present O&M agency - Y') are REPORTED for manual
#     review, never auto-split. Splitting them on the newline would silently
#     invent a second contractor or truncate the first.
#
# RUN BEFORE the cm -> pm mapping cell (cm_to_pm).
# ============================================================================
import re
import numpy as np
import pandas as pd

# --- the 'messieurs' honorific, in every spelling present in the data --------
# Two alternatives, and the ORDER of the guards matters:
#   slash form : M/s  M/S  m/s  M / s  M/s.   -> the '/' makes it unambiguous
#   bare  form : MS  Ms  ms                   -> MUST be a standalone token.
# \b on BOTH sides of the bare form is what stops it eating the 'MS' in
# 'MSV International' (194 rows) and 'MSPARK Futuristics' (25 rows) - those are
# company names, not honorifics. Do not relax this to a lookahead.
MS_TOKEN    = re.compile(r'(?:\b[Mm]\s*/\s*[Ss]\s*\.?|\b[Mm][Ss]\b\.?)\s*')
MS_LEADING  = re.compile(r'^(?:M/s\s+)+')          # repeats: 'M/s M/s X'
INVISIBLE   = re.compile(r'[\u00a0\u200b\u2007\u202f\t\n\r]')
PLACEHOLDER = re.compile(r'^(?:[\s\-\u2013\u2014]*|n\.?a\.?|nil|none|not\s*applicable)$', re.I)
# contractor-only: role played, not the firm's name
ROLE_LABEL  = re.compile(r'^\s*(EPC\s+Contractor|Maintenance\s+Contractor'
                         r'|O\s*&\s*M\s+(?:Contractor|Agency)|DLP|Concessionaire|Contractor)'
                         r'\s*[-:]\s*', re.I)
# contractor-only: cell holds a sentence / two entities, not one name
NARRATIVE   = re.compile(r'terminat|foreclos|tender\s+under|under\s+process|tenure\s+complet'
                         r'|expired\s+on|formerly|formely|cease|presently\s+under|yet\s+to', re.I)

# 'keep'   -> majority vote decides, per firm (mixed across the column)
# 'strip'  -> no canonical name carries 'M/s'
# 'prefix' -> every canonical name carries 'M/s'
# Cosmetic only: all three give the SAME number of canonical names, because
# match_key ignores the honorific regardless.
MS_POLICY = 'keep'

# ---- 1. AUDIT ---------------------------------------------------------------
CHECKS = {
    'leading/trailing space' : lambda t: t != t.strip(),
    'tab / newline inside'   : lambda t: bool(re.search(r'[\t\n\r]', t)),
    'NBSP / zero-width'      : lambda t: bool(re.search(r'[\u00a0\u200b\u2007\u202f]', t)),
    'double space'           : lambda t: bool(re.search(r'\S  +\S', t)),
    'trailing "."'           : lambda t: t.strip().endswith('.'),
    'trailing , ; - :'       : lambda t: bool(re.search(r'[,;:\-]$', t.strip())),
    'space before . or ,'    : lambda t: bool(re.search(r'\s[.,]', t)),
    'M/s variant present'    : lambda t: bool(MS_TOKEN.search(t)),
    'M/s spelled not "M/s"'  : lambda t: any(m.group(0).strip().rstrip('.') != 'M/s'
                                             for m in MS_TOKEN.finditer(t)),
    'role label prefix'      : lambda t: bool(ROLE_LABEL.match(t)),
    'narrative / 2 entities' : lambda t: bool(NARRATIVE.search(t)) or '\n' in t,
    'placeholder / no name'  : lambda t: bool(re.fullmatch(r'[\s\-\u2013\u2014]*', t))
                                         or bool(re.search(r'not\s*allot|not\s*award', t, re.I)),
}

# ---- 2. SURFACE CLEAN (this is what gets written) ---------------------------
def clean_name(v, strip_role=False):
    if pd.isna(v):
        return np.nan
    t = INVISIBLE.sub(' ', str(v))
    t = re.sub(r'\s+', ' ', t).strip()          # collapse runs of whitespace
    if strip_role:
        t = ROLE_LABEL.sub('', t).strip()       # 'EPC Contractor - X' -> 'X'
    t = MS_TOKEN.sub('M/s ', t)                 # m/s, ms, M/S, MS, M/s. -> 'M/s '
    t = re.sub(r'\s+', ' ', t).strip()          # ...and re-collapse after it
    t = re.sub(r'\s+([.,])', r'\1', t)          # 'Ltd ,' -> 'Ltd,'
    t = re.sub(r'[.,;:\-]+$', '', t).strip()    # ONE trailing-punct rule for all
    if PLACEHOLDER.match(t) or re.search(r'not\s*allot|not\s*award', t, re.I):
        return np.nan                           # '-', 'not alloted' = missing
    if   MS_POLICY == 'strip':
        t = MS_LEADING.sub('', t).strip()
    elif MS_POLICY == 'prefix' and t and not t.startswith('M/s '):
        t = 'M/s ' + t
    return t or np.nan

# ---- 3. MATCH KEY (grouping only - never written) ---------------------------
# Singular/plural and legal-form synonyms folded together; spaces dropped last
# so 'L.N.' == 'LN' == 'L N' and 'G R Infraprojects' == 'GR Infraprojects'.
LEGAL = {'private':'pvt','pvt':'pvt','limited':'ltd','ltd':'ltd',
         'incorporated':'inc','inc':'inc','corporation':'corp','corp':'corp',
         'company':'co','co':'co','llp':'llp',
         'associates':'assoc','association':'assoc','associate':'assoc','assoc':'assoc',
         'consultants':'consultant','consultancy':'consultant',
         'services':'service','solutions':'solution','projects':'project',
         'engineers':'engineer','engineering':'engineer',
         'jv':'jv','joint':'jv','venture':'jv',
         # plurals that only show up on the contractor side
         'constructions':'construction','developers':'developer',
         'enterprises':'enterprise','infraprojects':'infraproject',
         'technologies':'technology','industries':'industry',
         'contractors':'contractor','builders':'builder',
         'highways':'highway','infrastructures':'infrastructure'}
# scanned-'l'-for-'I' and misspellings actually present in these columns
TYPO  = {'lnfra':'infra','lndo':'indo','lnternational':'international','ln':'in',
         'enginneering':'engineer','consultanats':'consultant',
         'compnies':'companies','malaviya':'malviya'}

def match_key(v):
    if pd.isna(v):
        return np.nan
    # Drop the honorific BEFORE stripping punctuation: 'M/s' would otherwise
    # become the tokens 'm','s' and keep 'M/s Gawar ...' from grouping with
    # 'Gawar ...'.
    k = MS_TOKEN.sub(' ', str(v)).lower().replace('&', ' and ')
    k = re.sub(r'[^a-z0-9]+', ' ', k)                       # kills . - , / ( )
    return ''.join(LEGAL.get(w, TYPO.get(w, w)) for w in k.split())

# ---- driver -----------------------------------------------------------------
# Override any group whose majority spelling you don't like:
#   MANUAL['AE/IE Name'][match_key('<any raw variant>')] = '<preferred spelling>'
MANUAL = {'AE/IE Name': {}, 'Contractor / Concessionaire': {}}

def clean_name_column(df, col, strip_role=False, show_groups=True):
    raw = df[col]
    txt = raw.dropna().astype(str)
    print("=" * 78)
    print(f"{col}   rows {len(raw)} | non-null {len(txt)} | distinct raw {txt.nunique()}")
    print("=" * 78)
    print(pd.DataFrame([{'issue': k,
                         'rows': int(txt.map(f).sum()),
                         'distinct values': int(txt[txt.map(f)].nunique())}
                        for k, f in CHECKS.items()]
                      ).sort_values('rows', ascending=False).to_string(index=False))
    print("\nhonorific spellings found:",
          dict(pd.Series([m.group(0) for t in txt for m in MS_TOKEN.finditer(t)]).value_counts()))

    # keep the role label rather than throwing it away
    if strip_role:
        df[f'{col} Role Label'] = raw.map(
            lambda v: ROLE_LABEL.match(str(v).strip()).group(1) if pd.notna(v)
                      and ROLE_LABEL.match(str(v).strip()) else np.nan)

    cleaned = raw.map(lambda v: clean_name(v, strip_role=strip_role))
    keys    = cleaned.map(match_key)

    grp   = pd.DataFrame({'key': keys, 'clean': cleaned}).dropna(subset=['key'])
    multi = grp.groupby('key')['clean'].nunique()
    multi = multi[multi > 1].index
    print(f"\n{len(multi)} key-groups hold >1 spelling - review before accepting:")
    if show_groups:
        for k in multi:
            sub = grp.loc[grp['key'] == k, 'clean'].value_counts()
            print(f"\n  [{int(sub.sum())} rows]")
            for val, n in sub.items():
                print(f"     {n:>3}  {val[:110]}")

    canon = (grp.groupby(['key', 'clean']).size().rename('n').reset_index()
                .sort_values(['key', 'n', 'clean'], ascending=[True, False, True])
                .drop_duplicates('key').set_index('key')['clean'])
    canon.update(pd.Series(MANUAL.get(col, {})))
    df[col] = keys.map(canon)

    # rows the cleaner deliberately did NOT try to fix
    review = txt[txt.map(lambda t: bool(NARRATIVE.search(t)) or '\n' in t)]
    print(f"\ndistinct: {txt.nunique()} raw -> {cleaned.dropna().nunique()} cleaned"
          f" -> {df[col].dropna().nunique()} canonical")
    print(f"rows blanked as placeholder : {int(len(txt) - df[col].notna().sum())}")
    print(f"rows for MANUAL review      : {len(review)}  (narrative / two entities in one cell)")
    for v in sorted(set(review))[:10]:
        print(f"     {v[:100]!r}")
    return review


rev_ae  = clean_name_column(cm_with_dates, 'AE/IE Name')
rev_con = clean_name_column(cm_with_dates, 'Contractor / Concessionaire', strip_role=True)


# COMMAND ----------

cm_with_dates['Contractor / Concessionaire'].nunique()

# COMMAND ----------

cm_with_dates.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### CLean length columns

# COMMAND ----------

# ============================================================================
# Normalise the length columns in cm_with_dates to clean numeric columns.
#
# The raw cells are free text: they mix real numbers with unit suffixes
# ('32.5 km'), placeholders ('-', 'NA', 'nil'), and junk. Anything that is not
# a positive number is redundant and becomes NaN.
# ============================================================================
LEN_COLS = ['Length (in km)', 'Lane Length (in km)']

# ---- 1. AUDIT: what is actually in there that isn't a plain number ----------
for col in LEN_COLS:
    raw = cm_with_dates[col]
    non_numeric = raw[raw.notna() & pd.to_numeric(raw, errors='coerce').isna()]
    print(f"\n--- {col} ---")
    print(f"rows {len(cm_with_dates)} | non-null {raw.notna().sum()} | "
          f"non-numeric {len(non_numeric)}")
    if len(non_numeric):
        print(non_numeric.astype(str).str.strip().value_counts().head(30).to_string())

# ---- 2. CLEAN (this is what gets written) -----------------------------------
LEN_PLACEHOLDER = re.compile(
    r'^(?:[\s\-\u2013\u2014\u2212]*|n\.?a\.?|nil|none|not\s*available|'
    r'not\s*applicable|tbd|0+(?:\.0+)?)$', re.I)
UNIT = re.compile(r'\b(?:kms?|k\.?m\.?|kilo\s*met(?:er|re)s?)\b\.?', re.I)

def clean_length(v):
    """Return a positive float, else np.nan."""
    if pd.isna(v):
        return np.nan
    if isinstance(v, (int, float)):
        return float(v) if float(v) > 0 else np.nan

    t = INVISIBLE.sub(' ', str(v))
    t = re.sub(r'\s+', ' ', t).strip()
    if LEN_PLACEHOLDER.match(t):
        return np.nan

    t = UNIT.sub('', t)                       # '32.5 km' -> '32.5'
    t = t.replace(',', '').strip()            # '1,234.5' -> '1234.5'
    m = re.fullmatch(r'[+]?(\d+(?:\.\d+)?)', t)   # reject '12-15', '25/30', 'approx 12'
    if not m:
        return np.nan

    val = float(m.group(1))
    return val if val > 0 else np.nan         # 0 / negative = no length on record

print('\n' + '=' * 70)
for col in LEN_COLS:
    before = cm_with_dates[col].notna().sum()
    cm_with_dates[col] = cm_with_dates[col].map(clean_length).astype('float64')
    after = cm_with_dates[col].notna().sum()
    print(f"{col}: non-null {before} -> {after} "
          f"(blanked as redundant: {before - after})")

print('\n' + cm_with_dates[LEN_COLS].describe().to_string())


# COMMAND ----------

# MAGIC %md
# MAGIC ### Populating fields of Project Master

# COMMAND ----------

# MAGIC %md
# MAGIC #### Populating respective fields of Project Master from Consolidate Master

# COMMAND ----------

cm_with_dates.columns.tolist()

# COMMAND ----------



# COMMAND ----------

# ── Manual column mapping: cm column  ->  df_project_master (pm) column ───────
# Uses the "(Cleaned)" cm columns where available.
cm_to_pm = {
    'UPC':                                         'UPC',
    'Project Name':                                'Project Name',
    'CLEANED Mode of Implementation':              'Implementation Mode',
    'Current Status (Only O&M Projects)':          'Current Status',
    'CLEANED State':                               'State Name',
    'CLEANED PIU':                                 'PIU',
    'CLEANED PIU Mail ID':                         'PIU Email ID',
    'CLEANED PIU Contact No.':                     'PIU Contact No.',
    'CLEANED RO':                                  'RO',
    'CLEANED RO Mail ID':                          'RO Email ID',
    'CLEANED RO Contact No.':                      'RO Contact No.',
    'Length (in km)':                              'Length-km',
    'CLEANED Number of Lanes':                     'No of Lanes', # 'Number of Lanes'
    'Lane Length (in km)':                         'Lane-km (MCW)',
    'NH Number':                                   'NH No.',
    'AE/IE Name':                                  'AE/IE Name',
    'AE/IE Start Date of Contract':                'AE/IE Contract Start Date',
    'AE/IE End Date of Contract':                  'AE/IE Contract End Date',
    'Contractor / Concessionaire':                 'Contractor Name',
    'O&M Contractor Start Date of Contract':       'Contractor Contract Start Date',
    'O&M Contractor End Date of Contract':         'Contractor Contract End Date',
    'Alternate UPC':                               'Alternate UPC',
    'Zone':                                        'Zone'
}

# Build df_project_master: select the mapped cm cols, rename, then reindex to
# the full pm schema so unmapped target columns come through as empty (NaN).
df_project_master = (
    cm_with_dates[list(cm_to_pm)]
      .rename(columns=cm_to_pm)
      .reindex(columns=pm_columns)
      .reset_index(drop=True)
)

# COMMAND ----------

df_project_master.columns

# COMMAND ----------

col = "Length-km"

# Treat NaN, empty strings and whitespace-only cells as missing
missing_mask = df_project_master[col].isna() | (df_project_master[col].astype(str).str.strip() == "")

print(f"Rows with missing {col}: {missing_mask.sum()} of {len(df_project_master)}")

missing_rows = df_project_master.loc[missing_mask]

cols = [c for c in ["UPC", "Project Name", "Length-km"] if c in df_project_master.columns]
display(missing_rows[cols])

# unique UPCs affected
print("Unique UPCs missing length:", missing_rows["UPC"].nunique())


# COMMAND ----------

# MAGIC %md
# MAGIC #### Populating respective fields of Project Master from Basic Project Deatils API Data

# COMMAND ----------

df_bpd[df_bpd["UPC"] == 'N/02026/05005/AS'][['UPC','start_chainage_from_latitude','start_chainage_from_longitude']]

# COMMAND ----------

df_bpd[df_bpd['start_chainage_from_latitude'] == 22.9807]

# COMMAND ----------

# BASIC PROJECT API
bpd_to_pm = {
    'UPC':                            'UPC',
    'project_type':                   'Project Type',
    'start_chainage_from_latitude':   'Start Chainage Latitude',
    'start_chainage_from_longitude':  'Start Chainage Longitude',
    'end_chainage_to_latitude':       'End Chainage Latitude',
    'end_chainage_to_longitude':      'End Chainage Longitude',
    'awarded_cost':                   'Awarded Cost',
    'total_capital_cost':             'Total Capital Cost',
    'loa_date':                       'LOA Date',
    'agreement_date':                 'Agreement Date',
    'appointment_date':               'Actual Start Date / Appointed Date',  #dlpperiodfrom
    'scheduled_completiondate':       'Scheduled Completion as per Contract',
    'final_completiondate':           'final_completiondate',
    'pm_scheme':                      'Scheme',
    'likely_completiondate':          'Likely Completion Date'
}

df_bpm = (
    df_bpd[list(bpd_to_pm)]
    .rename(columns=bpd_to_pm)
    .reset_index(drop=True))

# COMMAND ----------

# ── Fill missing values in df_project_master from df_bpm (matched on UPC) ─────
# Rule: only fill cells that are empty in df_project_master.
#       existing values are never overwritten.

# 1. UPC-keyed lookup from df_bpm (one row per UPC)
bpm_lookup = df_bpm.drop_duplicates(subset='UPC').set_index('UPC')

# 2. Columns common to both frames (excluding the UPC key itself)
common_cols = [c for c in df_project_master.columns
               if c in bpm_lookup.columns and c != 'UPC']

# 3. Treat NaN and blank/whitespace strings as "empty"
def _is_empty(s):
    return s.isna() | s.astype(str).str.strip().isin(['', 'nan', 'None', 'NaT'])

# 4. For each common column, fill empties with the df_bpm value for that UPC
pm = df_project_master.copy()

for col in common_cols:
    filler = pm['UPC'].map(bpm_lookup[col])   # bpm value aligned per row via UPC
    mask   = _is_empty(pm[col])               # cells to fill in project master
    pm[col] = pm[col].mask(mask, filler)      # .mask upcasts dtype as needed

df_project_master2 = pm
print("Rows in project master:", len(df_project_master2))
df_project_master2[['is_clean_data','is_active']]= 1

#df_project_master2.head(3)

# COMMAND ----------

df_project_master2.columns

# COMMAND ----------

df_project_master2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Contract & AE/IE Coverage Status

# COMMAND ----------

df_project_master3 = df_project_master2.copy()

# Current date (normalized to midnight so the comparison is date-based)
today = pd.Timestamp.now().normalize()
six_months_out = today + pd.DateOffset(months=6)


def contract_status(df, start_col, end_col):
    """Combined lifecycle status, evaluated in priority order:
      1. Uncovered   -> start or end date missing
      2. Expired     -> end date already passed
      3. Near Expiry -> end date within 6 months from today
      4. Covered     -> both dates present, not expired, not near expiry
    """
    start = pd.to_datetime(df[start_col], errors='coerce')
    end   = pd.to_datetime(df[end_col],   errors='coerce')

    return np.select(
        [
            start.isna() | end.isna(),     # dates missing   -> Uncovered
            end < today,                   # already ended   -> Expired
            end <= six_months_out,         # within 6 months -> Near Expiry
        ],
        ['Uncovered', 'Expired', 'Near Expiry'],
        default='Covered'                  # both dates, still valid
    )


df_project_master3['Contractor Coverage Status'] = contract_status(
    df_project_master3, 'Contractor Contract Start Date', 'Contractor Contract End Date'
)

df_project_master3['AE/IE Coverage Status'] = contract_status(
    df_project_master3, 'AE/IE Contract Start Date','AE/IE Contract End Date'
)

df_project_master3[['Contractor Coverage Status', 'AE/IE Coverage Status']].apply(pd.Series.value_counts)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Completion Status Calculation

# COMMAND ----------

s = df_project_master3['final_completiondate'].astype('object')

blank = s.isna() | s.astype(str).str.strip().str.lower().isin(
    ['', 'nan', 'none', 'nat', '<na>', 'null']
)

df_project_master3['Completion Status'] = np.where(blank, 'No', 'Yes')
df_project_master3.drop(columns='final_completiondate',inplace=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### OUTPUT

# COMMAND ----------

df_project_master4 = df_project_master3[[
    "UPC",'Zone',"Project Name","Implementation Mode","Current Status","State Name",'PIU',"PIU Email ID","PIU Contact No.",
    "RO","RO Email ID","RO Contact No.","Length-km","IR (Count)","Lane-km (SR)","No of Lanes","Lane-km (MCW)","Lane-km",
    "NH No.","AE/IE Name","AE/IE Contract Start Date","AE/IE Contract End Date","AE/IE Coverage Status","Contractor Name",
    "Contractor Contract Start Date","Contractor Contract End Date","Contractor Coverage Status","Alternate UPC",
    "Project Type","Start Chainage Latitude","Start Chainage Longitude","End Chainage Latitude","End Chainage Longitude",
    "Awarded Cost","Total Capital Cost","LOA Date","Agreement Date","Actual Start Date / Appointed Date","NSV Vendor Name",
    "NSV Zone","NSV Coverage Status","DAMS Vendor Name","DAMS Zone","DAMS Coverage Status","DAS Vendor Name","DAS Zone",
    "DAS Coverage Status","Scheduled Completion as per Contract","Completion Status","Scheme",
    "Likely Completion Date", "Contractor Email ","AE/IE Email",
    'is_clean_data','is_active'
]]

# COMMAND ----------

df_project_master4.to_excel("/Workspace/Users/shivam.raj9226@softsensor.ai/Project Master Soumyadeep/Output/project_master_structure_1.xlsx", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## NSV Contractor Mapping

# COMMAND ----------

df_project_master2 = pd.read_excel("/Workspace/Users/shivam.raj9226@softsensor.ai/Project Master Soumyadeep/Output/project_master_structure_1.xlsx")
df_project_master2.head(2)

# COMMAND ----------

BACKUP_COL = "Backups/Processed Data Link"
GDRIVE_COL = "Report submission google drive (Link)"

BACKUP_FLAG = "backup received flag"
GDRIVE_FLAG = "google drive received"

_HEADER_MARKERS = {"s. no.", "s.no.", "s. no", "zone"}
_COL_ALIASES = {"PIU/PMU": "PIU", "Latest UPC Code": "UPC Code"}

LINK_COLS = {
    BACKUP_COL,
        GDRIVE_COL,
            "Report acceptance email (link)",
                "Report Forwarding Email Link (Tech Div./RO/PIU)",
                }

def _clean(v):
    if v is None:
        return ""
    s = str(v).replace("\xa0", " ").replace("\x97", "-").strip()
    return "" if s.lower() in ("nan", "none") else s


def _canon(h):
    return _COL_ALIASES.get(h, h)


def _dedupe_headers(headers):
    seen, out = {}, []
    for h in headers:
        if h == "":
            out.append("")
            continue
        if h in seen:
            seen[h] += 1
            out.append(f"{h}.{seen[h]}")
        else:
            seen[h] = 0
            out.append(h)
    return out


def _is_serial(v):
    s = str(v).strip()
    if s == "":
        return False
    try:
        float(s); return True
    except ValueError:
        return False


def _cell_val(cell, colname):
    if colname in LINK_COLS and cell.hyperlink is not None:
        return cell.hyperlink.target
    return _clean(cell.value)


def _detect_header_row(ws, scan=4):
    for r in range(1, scan + 1):
        vals = {
            str(ws.cell(r, c).value).strip().lower()
            for c in range(1, ws.max_column + 1)
            if ws.cell(r, c).value is not None
        }
        if vals & _HEADER_MARKERS:
            return r
    return 1

def _read_sheet(ws, zone_hint=None):
    hr = _detect_header_row(ws)
    raw_headers = [_canon(_clean(ws.cell(hr, c).value)) for c in range(1, ws.max_column + 1)]
    headers = _dedupe_headers(raw_headers)

    col_idx = {h: i + 1 for i, h in enumerate(headers) if h != ""}
    sno_col = col_idx.get("S. No.", 1)

    rows, fb, fg = [], [], []
    for r in range(hr + 1, ws.max_row + 1):
        if not _is_serial(ws.cell(r, sno_col).value):
            continue
        rd = {h: _cell_val(ws.cell(r, ci), h) for h, ci in col_idx.items()}
        if rd.get("Zone", "") == "" and zone_hint:
            rd["Zone"] = zone_hint

        def present(cn):
            ci = col_idx.get(cn)
            if ci is None:
                return 0
            cell = ws.cell(r, ci)
            return int(_clean(cell.value) != "" or cell.hyperlink is not None)

        fb.append(present(BACKUP_COL))
        fg.append(present(GDRIVE_COL))
        rows.append(rd)

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df[BACKUP_FLAG] = fb
    df[GDRIVE_FLAG] = fg
    return df

def _read_sheet(ws, zone_hint=None):
    hr = _detect_header_row(ws)
    raw_headers = [_canon(_clean(ws.cell(hr, c).value)) for c in range(1, ws.max_column + 1)]
    headers = _dedupe_headers(raw_headers)

    col_idx = {h: i + 1 for i, h in enumerate(headers) if h != ""}
    sno_col = col_idx.get("S. No.", 1)

    rows, fb, fg = [], [], []
    for r in range(hr + 1, ws.max_row + 1):
        if not _is_serial(ws.cell(r, sno_col).value):
            continue
        rd = {h: _cell_val(ws.cell(r, ci), h) for h, ci in col_idx.items()}
        if rd.get("Zone", "") == "" and zone_hint:
            rd["Zone"] = zone_hint

        def present(cn):
            ci = col_idx.get(cn)
            if ci is None:
                return 0
            cell = ws.cell(r, ci)
            return int(_clean(cell.value) != "" or cell.hyperlink is not None)

        fb.append(present(BACKUP_COL))
        fg.append(present(GDRIVE_COL))
        rows.append(rd)

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df[BACKUP_FLAG] = fb
    df[GDRIVE_FLAG] = fg
    return df

def _read_csv_one(path):
    raw = pd.read_csv(path, header=None, dtype=str, keep_default_na=False, encoding="cp1252")
    hr = 0
    for r in range(min(4, len(raw))):
        if {str(x).strip().lower() for x in raw.iloc[r]} & _HEADER_MARKERS:
            hr = r; break
    df = raw.iloc[hr + 1:].copy()
    df.columns = _dedupe_headers([_canon(_clean(x)) for x in raw.iloc[hr]])
    df = df.loc[:, [c for c in df.columns if c != ""]]
    for c in df.columns:
        df[c] = df[c].map(_clean)
    if "S. No." in df.columns:
        df = df[df["S. No."].map(_is_serial)]
    df = df.reset_index(drop=True)
    df[BACKUP_FLAG] = (df[BACKUP_COL] != "").astype(int) if BACKUP_COL in df.columns else 0
    df[GDRIVE_FLAG] = (df[GDRIVE_COL] != "").astype(int) if GDRIVE_COL in df.columns else 0
    return df

def load_nsv_tracker(path):
    if not os.path.exists(path):
        folder = os.path.dirname(path)
        listing = os.listdir(folder) if os.path.isdir(folder) else ["<folder not found>"]
        raise FileNotFoundError(f"File not found:\n  {path}\nFiles in folder:\n  " + "\n  ".join(listing))
    ext = os.path.splitext(path)[1].lower()
    if ext in (".xlsx", ".xlsm"):
        wb = load_workbook(path, data_only=True)
        frames = []
        for ws in wb.worksheets:
            zh = ws.title.replace("Zone", "").strip() or None
            d = _read_sheet(ws, zh)
            if not d.empty:
                frames.append(d)
                print(f"[ok] sheet '{ws.title}' -> {len(d)} rows")
        return pd.concat(frames, ignore_index=True)
    return _read_csv_one(path)


def split_by_zone(df):
    out = {}
    for z, g in df.groupby("Zone"):
        z = str(z).strip()
        if z and z.lower() != "none":
            out[z] = g.reset_index(drop=True)
    return out

# COMMAND ----------

NSV_TRACKER_PATH = "/Workspace/Users/shivam.raj9226@softsensor.ai/Project Master Soumyadeep/Input files/NSV_Activity_Tracker_ZoneWise_Standardized_17072026.xlsx"
df = load_nsv_tracker(NSV_TRACKER_PATH)
print("Total rows:", len(df), "| columns:", len(df.columns))
nsv_cleaned = df.copy()

# COMMAND ----------

KNOWN_METRICS = {"ravelling", "rutting", "cracks", "roughness"}

def rename_ca_irc_columns(df):
    cols = list(df.columns)
    new = list(cols)
    renamed, drop_idx = {}, []
    for idx, c in enumerate(cols):
        base = str(c).split(".")[0].strip().lower()
        if base.startswith("reporting as per ca/irc"):
            nxt = str(cols[idx + 1]).split(".")[0].strip().lower() if idx + 1 < len(cols) else ""
            if nxt in KNOWN_METRICS:
                metric = str(cols[idx + 1]).split(".")[0].strip()
                target = f"Reporting as per CA/IRC ({metric})"
                new[idx] = target
                renamed[c] = target
            else:
                drop_idx.append(idx)
    df.columns = new
    keep = [i for i in range(len(new)) if i not in drop_idx]
    dropped = [cols[i] for i in drop_idx]
    df = df.iloc[:, keep]
    df = df.loc[:, ~pd.Index(df.columns).duplicated(keep="first")]
    return df, renamed, dropped


nsv_cleaned, renamed, dropped = rename_ca_irc_columns(nsv_cleaned)
print("Renamed:", renamed)
print("Dropped:", dropped or "none")

# COMMAND ----------

# Where the agency is the "Completed by AE/IE" placeholder, use the AE/IE firm instead
mask = nsv_cleaned["NSV Survey Agency Name"].astype(str).str.strip() == "Completed by AE/IE"
nsv_cleaned.loc[mask, "NSV Survey Agency Name"] = nsv_cleaned.loc[mask, "AE/ IE"]

print(f"Rows replaced with AE/IE: {int(mask.sum())}")

AGENCY_COL = "NSV Survey Agency Name"

# Canonicalize known duplicate spellings -> one standard name
AGENCY_NAME_FIXES = {
    "M/s L N Malviya infra Projects Ltd.": "L.N. Malviya infra projects Pvt. Ltd.",
    "L.N. Malviya infra projects Pvt. Ltd.": "L.N. Malviya infra projects Pvt. Ltd.",
    "Adharsila":  "Adharshila",
    "Adharshila": "Adharshila",
}

# Trim whitespace first, then apply the fixes (unmapped names pass through unchanged)
nsv_cleaned[AGENCY_COL] = nsv_cleaned[AGENCY_COL].astype(str).str.strip()
nsv_cleaned[AGENCY_COL] = nsv_cleaned[AGENCY_COL].replace(AGENCY_NAME_FIXES)

# Turn blanks / "nan" into real missing values, then drop those rows
nsv_cleaned[AGENCY_COL] = nsv_cleaned[AGENCY_COL].replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
before = len(nsv_cleaned)
nsv_cleaned = nsv_cleaned[nsv_cleaned[AGENCY_COL].notna()].reset_index(drop=True)

for col in ["Phase", "Cycle"]:
    nsv_cleaned[col] = (
        nsv_cleaned[col].astype(str)
        .str.replace(r"\s+", " ", regex=True)   # collapse multiple spaces into one
        .str.strip()
    )


# Convert both date columns to real datetimes (bad/blank strings become NaT)
nsv_cleaned["contract_start_date"] = pd.to_datetime(
    nsv_cleaned["Start Date of contract (mobilization date)"], errors="coerce", format="mixed"
)
nsv_cleaned["contract_end_date"] = pd.to_datetime(
    nsv_cleaned["End Date of contract (mobilization date)"], errors="coerce", format="mixed"
)

nsv_cleaned = nsv_cleaned.drop(columns=['Start Date of contract (mobilization date)', 'End Date of contract (mobilization date)'])

# COMMAND ----------

df_nsv0 = nsv_cleaned[['UPC code as per O&M master','PIU','Phase','Cycle','NSV Survey Agency Name','contract_start_date','contract_end_date','Zone']].copy()

df_nsv0['Zone'] = 'Zone ' + df_nsv0['Zone'].astype(str).str.strip()

df_nsv0.rename(columns={'UPC code as per O&M master':'UPC', 
                       'NSV Survey Agency Name':'NSV Vendor Name',
                       'contract_start_date':'NSV Contract Start Date',
                       'contract_end_date':'NSV Contract End Date',
                       'Zone':'NSV Zone'
                       },inplace=True)

# COMMAND ----------

# ---- expand multi-UPC cells into one row per UPC --------------------------
UPC_SEP = r"[,\n;]+"          # NOT '/' - that is part of the UPC itself

df_nsv = df_nsv0.copy()        # cell 53 hands back a slice; avoids SettingWithCopy
before = len(df_nsv)

def _split_upc(v):
    parts = re.split(UPC_SEP, str(v).replace("\xa0", " "))
    parts = [p.strip() for p in parts]
    parts = [p for p in parts if p not in ("", "nan", "None")]
    return list(dict.fromkeys(parts))          # dedupe, order preserved

df_nsv["_upc_list"] = df_nsv["UPC"].map(_split_upc)

# guard: only rows that genuinely hold >1 UPC should fan out
exploded_src = df_nsv[df_nsv["_upc_list"].str.len() > 1]
print(f"rows carrying multiple UPCs : {len(exploded_src)}")
for _, r in exploded_src.iterrows():
    print(f"  {r['Phase']}/{r['Cycle']:<8} {r['NSV Vendor Name'][:35]:<35} -> {r['_upc_list']}")

df_nsv["UPC"] = df_nsv["_upc_list"]
df_nsv = df_nsv.explode("UPC", ignore_index=True).drop(columns=["_upc_list"])

print(f"\nrows: {before} -> {len(df_nsv)}  (+{len(df_nsv) - before})")
print(f"unique UPCs: {df_nsv['UPC'].nunique(dropna=True)}")
df_nsv

# COMMAND ----------

# ---- df_nsv_vendor : one row per UPC -------------------------------------
tmp = df_nsv.copy()

# UPC hygiene (some tracker rows carry a blank UPC)
tmp['UPC'] = tmp['UPC'].astype(str).str.strip()
tmp = tmp[~tmp['UPC'].isin(['', 'nan', 'None', 'NA'])]

# numeric rank for phase / cycle so "Phase 10" sorts after "Phase 2"
tmp['_phase_no'] = pd.to_numeric(
    tmp['Phase'].astype(str).str.extract(r'(\d+)')[0], errors='coerce'
).fillna(-1)
tmp['_cycle_no'] = pd.to_numeric(
    tmp['Cycle'].astype(str).str.extract(r'(\d+)')[0], errors='coerce'
).fillna(-1)

# latest phase-cycle first
tmp = tmp.sort_values(['UPC', '_phase_no', '_cycle_no'], ascending=[True, False, False])


def _latest_available(s):
    """First non-null value walking from the latest phase-cycle backwards."""
    s = s.dropna()
    return s.iloc[0] if len(s) else pd.NaT


def _join_unique(s):
    """All distinct vendor names for the UPC, comma separated, order preserved."""
    vals = [v.strip() for v in s.dropna().astype(str) if v.strip() not in ('', 'nan', 'None')]
    return ', '.join(dict.fromkeys(vals))


df_nsv_vendor = (
    tmp.groupby('UPC', as_index=False)
       .agg(**{
           'NSV Vendor Name':          ('NSV Vendor Name', _join_unique),
           'NSV Contract Start Date':  ('NSV Contract Start Date', _latest_available),
           'NSV Contract End Date':    ('NSV Contract End Date',   _latest_available),
           'NSV Zone':                     ('NSV Zone', lambda s: s.dropna().iloc[0] if s.notna().any() else pd.NA),
       })
)

print(f"UPCs: {len(df_nsv_vendor)} | multi-vendor UPCs: "
      f"{df_nsv_vendor['NSV Vendor Name'].str.contains(',').sum()}")
df_nsv_vendor.head()

# COMMAND ----------

# ---- pull NSV vendor / zone / contract dates into df_project_master2 --------
FILL_ONLY_MISSING = True      # False -> NSV values overwrite whatever is there

NSV_MAP = {                   # df_nsv_vendor column -> df_project_master2 column
    'NSV Vendor Name':         'NSV Vendor Name',
    'NSV Zone':                'NSV Zone',
    'NSV Contract Start Date': 'NSV Contract Start Date',
    'NSV Contract End Date':   'NSV Contract End Date',
}

missing = [c for c in NSV_MAP if c not in df_nsv_vendor.columns]
assert not missing, f"df_nsv_vendor is missing {missing}; has {list(df_nsv_vendor.columns)}"

# '-' is a literal placeholder UPC in the tracker; without it here the base key
# below would reduce it to an empty string and match pm's own junk rows
_PLACEHOLDERS = ['', 'nan', 'None', 'NaT', '<NA>', '-']

def _is_empty(s):
    """NaN/NaT plus blank & placeholder strings - same rule as cell 43."""
    return s.isna() | s.astype(str).str.strip().isin(_PLACEHOLDERS)

def _upc_key(s):
    """Trimmed, inner whitespace removed, upper-cased."""
    return (pd.Series(s).astype(str).str.strip()
              .str.replace(r'\s+', '', regex=True)
              .str.upper())

def _base_key(s):
    """Match key = the BASE UPC, i.e. everything before the first hyphen.

    Trailing qualifiers (-RCW, -DPR, ...) are not part of the project identity
    and are spelled inconsistently across the two sources, so both sides are
    matched on the base only:
        'N/08041/01001/UP - RCW' -> 'N/08041/01001/UP'
        'N/08041/01001/UP-RCW'   -> 'N/08041/01001/UP'
    _upc_key has already stripped the spaces, so a plain split on '-' suffices.
    """
    return _upc_key(s).str.split('-').str[0].str.strip()

def _merge_vendors(s):
    """Union of vendor names across merged rows, order preserved.
    Values are already comma-joined per UPC, so split before deduping."""
    out = []
    for v in s.dropna().astype(str):
        for part in v.split(','):
            part = part.strip()
            if part and part not in ('nan', 'None') and part not in out:
                out.append(part)
    return ', '.join(out)

def _first_valid(s):
    s = s.dropna()
    return s.iloc[0] if len(s) else pd.NA


# key on the BASE UPC; drop rows clean_single_upc nulled out, otherwise they all
# collapse onto one key and would match pm's blank-UPC rows
lookup = df_nsv_vendor.copy()
dropped = int(_is_empty(lookup['UPC']).sum())
lookup = lookup[~_is_empty(lookup['UPC'])].copy()
lookup['_key'] = _base_key(lookup['UPC'])
lookup = lookup[lookup['_key'] != '']
print(f"df_nsv_vendor rows with no usable UPC (dropped): {dropped}")

for c in ('NSV Contract Start Date', 'NSV Contract End Date'):
    lookup[c] = pd.to_datetime(lookup[c], errors='coerce')

# Several raw UPCs can share a base ('X' and 'X-RCW'). Merge them rather than
# keeping an arbitrary one: vendors are unioned, the window spans both rows.
dupe_keys = lookup['_key'].value_counts()
dupe_keys = dupe_keys[dupe_keys > 1]
if len(dupe_keys):
    print(f"base-UPC collisions merged: {len(dupe_keys)}")
    for k in dupe_keys.index:
        print(f"  {k}: {lookup.loc[lookup['_key'] == k, 'UPC'].tolist()}")

lookup = lookup.groupby('_key').agg(**{
    'NSV Vendor Name':         ('NSV Vendor Name',         _merge_vendors),
    'NSV Contract Start Date': ('NSV Contract Start Date', 'min'),
    'NSV Contract End Date':   ('NSV Contract End Date',   'max'),
    'NSV Zone':                ('NSV Zone',                _first_valid),
})

pm_base = _base_key(df_project_master2['UPC']).where(~_is_empty(df_project_master2['UPC']))
pm_base = pm_base.where(pm_base != '')

# what the base match buys over the exact-UPC match it replaces
_exact_hits = int(_upc_key(df_project_master2['UPC'])
                  .where(~_is_empty(df_project_master2['UPC']))
                  .isin(set(_upc_key(df_nsv_vendor['UPC']))).sum())
print(f"pm rows matched, exact UPC       : {_exact_hits} of {len(pm_base)}")
print(f"pm rows matched to an NSV vendor : {int(pm_base.isin(lookup.index).sum())} of {len(pm_base)}")
print(f"df_nsv_vendor UPCs not in pm     : {int((~lookup.index.isin(set(pm_base.dropna()))).sum())} of {len(lookup)}")

for src, tgt in NSV_MAP.items():
    vals = pm_base.map(lookup[src])                    # aligned on pm's row index
    if tgt not in df_project_master2.columns:
        df_project_master2[tgt] = pd.NaT if 'Date' in tgt else np.nan
        print(f"  (created column '{tgt}')")

    before = int((~_is_empty(df_project_master2[tgt])).sum())
    if FILL_ONLY_MISSING:
        mask = _is_empty(df_project_master2[tgt]) & vals.notna()
        df_project_master2[tgt] = df_project_master2[tgt].mask(mask, vals)
    else:
        df_project_master2[tgt] = vals.where(vals.notna(), df_project_master2[tgt])
    after = int((~_is_empty(df_project_master2[tgt])).sum())

    print(f"  {tgt:<26} +{after - before:<5} | populated {after:<6} | blank "
          f"{int(_is_empty(df_project_master2[tgt]).sum())}")

# keep the NSV block contiguous, dates after Zone (matches the DAMS/DAS layout)
cols = [c for c in df_project_master2.columns if c not in
        ('NSV Contract Start Date', 'NSV Contract End Date')]
i = cols.index('NSV Zone') + 1
cols[i:i] = ['NSV Contract Start Date', 'NSV Contract End Date']
df_project_master2 = df_project_master2[cols]

df_project_master2.filter(like='NSV').head(10)

# COMMAND ----------

# ---- Survey vendor status per source system (Covered / Uncovered only) ----
# A source system "covers" a UPC when it has a valid contract window that is
# active today (start <= today <= end).
# DELIBERATELY two-state, unlike Contractor / AE-IE Coverage Status, which is
# four-state (Covered / Uncovered / Expired / Near Expiry). Survey vendor
# coverage is only ever asked as "is this UPC being surveyed today?", so an
# ended window and a missing window both read as Uncovered here. Do not extend
# this to Expired / Near Expiry by analogy with contract_status().
# Dynamic: one "<SOURCE> Coverage Status" column per source system in `sources`.
# The column is CREATED here (inserted next to that source's contract dates)
# rather than written into a pre-existing blueprint slot, so this works whether
# or not the blueprint declares a "<SOURCE> Coverage Status" placeholder.

today = pd.Timestamp.now().normalize()

sources = ['NSV']          # add 'DAMS' / 'DAS' here once their dates are merged in


def survey_coverage_status(df, start_col, end_col):
    start = pd.to_datetime(df[start_col], errors='coerce')
    end   = pd.to_datetime(df[end_col],   errors='coerce')
    covered = start.notna() & end.notna() & (start <= today) & (today <= end)
    return np.where(covered, 'Covered', 'Uncovered')


for src in sources:
    start_col, end_col = f'{src} Contract Start Date', f'{src} Contract End Date'
    missing = [c for c in (start_col, end_col) if c not in df_project_master2.columns]
    assert not missing, (
        f"df_project_master2 is missing {missing} - merge df_{src.lower()}_vendor "
        f"into df_project_master2 before computing {src} Coverage Status"
    )

    col    = f'{src} Coverage Status'
    status = survey_coverage_status(df_project_master2, start_col, end_col)

    # drop any same-named blueprint placeholder first, so insert() cannot create
    # a duplicate column name
    if col in df_project_master2.columns:
        df_project_master2 = df_project_master2.drop(columns=col)

    # place it immediately after that source system's contract dates
    pos = df_project_master2.columns.get_loc(end_col) + 1
    df_project_master2.insert(pos, col, status)

status_cols = [f'{src} Coverage Status' for src in sources]
print("Coverage status columns added:", status_cols)
df_project_master2[status_cols].apply(pd.Series.value_counts)

# COMMAND ----------

df_project_master2.drop(columns=['NSV Contract Start Date', 'NSV Contract End Date'],inplace=True)

# COMMAND ----------

df_project_master2.head()

# COMMAND ----------

df_project_master2.to_excel("/Workspace/Users/shivam.raj9226@softsensor.ai/Project Master Soumyadeep/Output/project_master_structure_2.xlsx", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # DAMS PM Zone Mapping

# COMMAND ----------

import openpyxl
from collections import defaultdict

MASTER_PATH = "/Workspace/Users/shivam.raj9226@softsensor.ai/Project Master Soumyadeep/Input files/DAMS_TSP_DSP_Master.xlsx"
PM_PATH = "/Workspace/Users/shivam.raj9226@softsensor.ai/Project Master Soumyadeep/Output/project_master_structure_2.xlsx"
OUT_PATH = "/Workspace/Users/shivam.raj9226@softsensor.ai/Project Master Soumyadeep/Output/project_master_structure_3.xlsx"

MASTER_SHEET = "Sheet1"
PM_SHEET = "Sheet1"

# COMMAND ----------

wb_m = openpyxl.load_workbook(MASTER_PATH, data_only=True)
ws_m = wb_m[MASTER_SHEET]

h_m = [c.value for c in ws_m[1]]
idx_m = {h: i + 1 for i, h in enumerate(h_m)}

ro_info = {}  # RO_Name -> {"tsp": str, "zone": int, "dsps": [ordered unique DSP names]}

for r in range(2, ws_m.max_row + 1):
    ro = ws_m.cell(r, idx_m["RO_Name"]).value
    if ro is None:
        continue
    tsp = ws_m.cell(r, idx_m["TSP_Agency_Name"]).value
    zone = ws_m.cell(r, idx_m["Zone"]).value
    dsp = ws_m.cell(r, idx_m["DSP_Agency_Name"]).value

    if ro not in ro_info:
        ro_info[ro] = {"tsp": tsp, "zone": zone, "dsps": []}
    if dsp is not None and dsp not in ro_info[ro]["dsps"]:
        ro_info[ro]["dsps"].append(dsp)

print(f"Master RO entries: {len(ro_info)}")
for k in sorted(ro_info):
    v = ro_info[k]
    print(f"  {k}: TSP={v['tsp']}  Zone={v['zone']}  #DSP={len(v['dsps'])}")


# COMMAND ----------

def resolve_master_key(ro_raw, upc):
    """Return the RO_Name key to look up in ro_info, or None if unmatched/outlier."""
    if not ro_raw:
        return None
    ro_raw = ro_raw.strip()

    if ro_raw == "RO Chandigarh":
        suffix = (upc or "")[-2:].upper()
        if suffix == "HR":
            return "RO-Chandigarh (HR)"
        elif suffix == "PB":
            return "RO-Chandigarh (PB)"
        else:
            # outlier (e.g. DL) - leave blank for now
            return None

    # generic normalization: "RO Xyz" -> "RO-Xyz"
    return ro_raw.replace("RO ", "RO-", 1)


# COMMAND ----------

wb_p = openpyxl.load_workbook(PM_PATH, data_only=False)
ws_p = wb_p[PM_SHEET]

h_p = [ws_p.cell(1, c).value for c in range(1, ws_p.max_column + 1)]
idx_p = {h: i + 1 for i, h in enumerate(h_p)}

ro_col = idx_p["RO"]
upc_col = idx_p["UPC"]
vendor_col = idx_p["DAMS Vendor Name"]
zone_col = idx_p["DAMS Zone"]
status_col = idx_p["DAMS Coverage Status"]

stats = defaultdict(int)
unmatched_ros = defaultdict(int)

for r in range(2, ws_p.max_row + 1):
    ro_raw = ws_p.cell(r, ro_col).value
    upc = ws_p.cell(r, upc_col).value
    key = resolve_master_key(ro_raw, upc)

    if key is not None and key in ro_info:
        info = ro_info[key]
        vendor_parts = []
        if info["tsp"]:
            vendor_parts.append(str(info["tsp"]))
        vendor_parts.extend(str(d) for d in info["dsps"])
        vendor_str = ", ".join(vendor_parts) if vendor_parts else None
        zone_str = f"Zone {info['zone']}" if info["zone"] is not None else None

        ws_p.cell(r, vendor_col).value = vendor_str
        ws_p.cell(r, zone_col).value = zone_str
        ws_p.cell(r, status_col).value = "Covered" if (vendor_str and zone_str) else "Uncovered"
        stats["matched"] += 1
    else:
        ws_p.cell(r, vendor_col).value = None
        ws_p.cell(r, zone_col).value = None
        ws_p.cell(r, status_col).value = "Uncovered"
        stats["unmatched"] += 1
        unmatched_ros[ro_raw] += 1

print("Row stats:", dict(stats))
print()
print("Unmatched RO breakdown:")
for k, v in sorted(unmatched_ros.items(), key=lambda x: -x[1]):
    print(f"  {k!r}: {v}")


# COMMAND ----------

wb_p.save(OUT_PATH)
print("Saved:", OUT_PATH)


# COMMAND ----------

# MAGIC %md
# MAGIC # DAS ZONE Mapping

# COMMAND ----------

import re
import openpyxl
import pandas as pd
from collections import Counter

MASTER_PATH = "/Workspace/Users/shivam.raj9226@softsensor.ai/Project Master Soumyadeep/Input files/DAS RO-Zone map.ods"        # UPC -> DAS Zone mapping
PM_PATH = "/Workspace/Users/shivam.raj9226@softsensor.ai/Project Master Soumyadeep/Output/project_master_structure_3.xlsx"    # Project Master to update
OUT_PATH = "/Workspace/Users/shivam.raj9226@softsensor.ai/Project Master Soumyadeep/Output/project_master_structure_4.xlsx"   # Output file (overwrite in place; change if you want a new file)

PM_SHEET = "Sheet1"


# ---- 0. UPC normalization ----
# Some UPCs carry a trailing qualifier after a hyphen, e.g. "N/08041/01001/UP - RCW"
# or "N/08025/05001/JH-RCW". The qualifier (RCW / DPR / ...) is not part of the
# project identity and it appears inconsistently on the two sides, so both the
# master and the Project Master are matched on the base UPC only.
def base_upc(value):
    """Return the UPC up to the first hyphen, upper-cased, or None if blank."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    return re.split(r"\s*-\s*", s, maxsplit=1)[0].strip().upper()


# ---- 1. Load the UPC -> Zone map from the ODS file ----
das_df = pd.read_excel(MASTER_PATH, sheet_name="Sheet1", engine="odf")
das_df = das_df.dropna(subset=["UPC Code"])

das_df["UPC Base"] = das_df["UPC Code"].map(base_upc)
das_df = das_df.dropna(subset=["UPC Base"])

# Warn if two different raw UPCs collapse onto the same base (first one wins below)
collisions = {k: v for k, v in Counter(das_df["UPC Base"]).items() if v > 1}
if collisions:
    print(f"WARNING: {len(collisions)} base UPC collision(s) in the DAS master:")
    for b in collisions:
        print(f"  {b}: {das_df.loc[das_df['UPC Base'] == b, 'UPC Code'].tolist()}")

upc_to_zone = {}
for base, zone in zip(das_df["UPC Base"], das_df["Zone"].astype(str).str.strip()):
    upc_to_zone.setdefault(base, zone)   # first occurrence wins

print(f"DAS UPC->Zone map entries: {len(upc_to_zone)} (from {len(das_df)} master rows)")

# ---- 2. Load Project Master and update DAS Zone / DAS Coverage Status ----
wb = openpyxl.load_workbook(PM_PATH, data_only=False)
ws = wb[PM_SHEET]

headers = [ws.cell(1, c).value for c in range(1, ws.max_column + 1)]
idx = {h: i + 1 for i, h in enumerate(headers)}

upc_col = idx["UPC"]
das_zone_col = idx["DAS Zone"]
das_status_col = idx["DAS Coverage Status"]
# DAS Vendor Name column exists but is intentionally left untouched for now

matched = 0
unmatched = 0
unmatched_upcs = []

for r in range(2, ws.max_row + 1):
    upc = ws.cell(r, upc_col).value
    key = base_upc(upc)

    if key is not None and key in upc_to_zone:
        zone_letter = upc_to_zone[key]
        ws.cell(r, das_zone_col).value = f"Zone {zone_letter}"
        matched += 1
    else:
        ws.cell(r, das_zone_col).value = None
        unmatched += 1
        unmatched_upcs.append(upc)

    # Per instructions: mark every row Uncovered for now, regardless of match
    ws.cell(r, das_status_col).value = "Uncovered"

print(f"Matched: {matched}, Unmatched: {unmatched}")
print("Unmatched UPCs:", unmatched_upcs)

wb.save(OUT_PATH)
print("Saved:", OUT_PATH)

# COMMAND ----------

