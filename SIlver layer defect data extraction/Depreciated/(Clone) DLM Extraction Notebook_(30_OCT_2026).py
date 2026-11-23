# Databricks notebook source
# DBTITLE 1,DLM Extraction Pipeline
# MAGIC %md
# MAGIC # DLM Extraction Pipeline
# MAGIC
# MAGIC Builds the DLM defect tables for one UPC (widgets ZONE / PHASE / CYCLE / UPC) from that UPC's own bronze inputs only: NSV backup parquet (defect values), D1 (pavement, carriageway, NH, section), D2 (thresholds + crack calibration), D3/D4 (conditions, potholes) and the tracker. No project-specific files or values are hardcoded.
# MAGIC
# MAGIC **Run order (one UPC at a time):** Install → Configuration → Tracker/D2 → Load sources → Pothole hierarchy → Schemas/helpers → Cracking correction → Map defects (10m / 100m / 1km, pavement Hybrid, display IDs) → PCI → Road condition → NH + ratings → Cracking severity/condition → Dual Schema Write → **Unified Validation** → master_chainage → (optional) Export validation Excel.
# MAGIC
# MAGIC **Outputs:** `nhai_dhms.dlm_kpi` (all rows), `nhai_dhms.dlm_postgres` (breach rows), `nhai_dhms.dlm_kpi.dlm_validation_results` (validation summary per UPC).

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install openpyxl

# COMMAND ----------

# DBTITLE 1,Configuration and Imports
# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION & IMPORTS
# ═══════════════════════════════════════════════════════════════════════════════
import pandas as pd
import numpy as np
import os, json, copy
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import types as T
from pyspark.sql.window import Window
from bisect import bisect_left as _bisect_left

RUN_DATE = datetime.today().date()

# ── Widget Parameters ────────────────────────────────────────────────────────
dbutils.widgets.text("ZONE", "B")
dbutils.widgets.text("PHASE", "2")
dbutils.widgets.text("CYCLE", "1")
dbutils.widgets.text("UPC", "N_07024_01001_GJ")

ZONE  = dbutils.widgets.get("ZONE")
PHASE = dbutils.widgets.get("PHASE")
CYCLE = dbutils.widgets.get("CYCLE")
UPC   = dbutils.widgets.get("UPC")

# ── Target Schemas (dual-write) ──────────────────────────────────────────────
CATALOG = "nhai_dhms"
DLM_POSTGRES_SCHEMA = "dlm_postgres"   # breach_flag=1 only
DLM_KPI_SCHEMA      = "dlm_kpi"        # all data (breach_flag=0 and 1)
DLM_POSTGRES_NS = f"{CATALOG}.{DLM_POSTGRES_SCHEMA}"
DLM_KPI_NS      = f"{CATALOG}.{DLM_KPI_SCHEMA}"

# ── Data Paths ───────────────────────────────────────────────────────────────
BRONZE_ROOT = "/Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output"
SCHEMA_WORKBOOK = "/Workspace/Users/shivam.raj9226@softsensor.ai/SIlver layer defect data extraction/Input/nsv_defect_schema_master 1.xlsx"
TRACKER_PATH = "/Workspace/Shared/NSV Extraction/NSV_Activity_Tracker_ZoneWise_Standardized_17072026.xlsx"

# ── Dynamic Path Resolution ──────────────────────────────────────────────────
def _find_latest_date_partition(base_path):
    if not os.path.exists(base_path):
        return None
    for y in sorted(os.listdir(base_path), reverse=True):
        if not y.startswith("year="): continue
        yp = os.path.join(base_path, y)
        for m in sorted(os.listdir(yp), reverse=True):
            if not m.startswith("month="): continue
            mp = os.path.join(yp, m)
            for d in sorted(os.listdir(mp), reverse=True):
                if not d.startswith("day="): continue
                return f"{y}/{m}/{d}"
    return None

def _find_deliverable_parent(base):
    if not os.path.exists(base): return base
    children = sorted(os.listdir(base))
    if any(c.startswith("Deliverable=") for c in children): return base
    for child in children:
        child_path = os.path.join(base, child)
        if os.path.isdir(child_path):
            found = _find_deliverable_parent(child_path)
            if found != child_path: return found
            if any(gc.startswith("Deliverable=") for gc in os.listdir(child_path)):
                return child_path
    return base

def _find_lane_parent(base):
    if not os.path.exists(base): return None
    children = sorted(os.listdir(base))
    if any(c.startswith("Lane=") for c in children): return base
    for child in children:
        child_path = os.path.join(base, child)
        if os.path.isdir(child_path):
            found = _find_lane_parent(child_path)
            if found: return found
    return None

# NSV deliverable path
_nsv_root = f"{BRONZE_ROOT}/Source=NSV/Zone={ZONE}/Phase={PHASE}/Cycle={CYCLE}/UPC={UPC}"
_nsv_date = _find_latest_date_partition(_nsv_root)
UPC_BASE = f"{_nsv_root}/{_nsv_date}" if _nsv_date else _nsv_root
DELIVERABLE_BASE = _find_deliverable_parent(UPC_BASE)
D1_BASE = f"{DELIVERABLE_BASE}/Deliverable=Deliverable - 1"
D2_BASE = f"{DELIVERABLE_BASE}/Deliverable=Deliverable - 2"
D3_BASE = f"{DELIVERABLE_BASE}/Deliverable=Deliverable - 3"
D4_BASE = f"{DELIVERABLE_BASE}/Deliverable=Deliverable - 4"

# Backup path
_backup_root = f"{BRONZE_ROOT}/Source=NSV_Backup/Zone={ZONE}/Phase={PHASE}/Cycle={CYCLE}/UPC={UPC}"
_backup_date = _find_latest_date_partition(_backup_root)
if _backup_date:
    _backup_date_path = f"{_backup_root}/{_backup_date}"
    BACKUP_BASE = _find_lane_parent(_backup_date_path) or _backup_date_path
else:
    BACKUP_BASE = _backup_root

# ── Clustering Constants ─────────────────────────────────────────────────────
PARENT_LENGTH_M = 1000
CHILD_100M = 100
CHILD_10M = 10
RAW_SEGMENT_M = 10
DEFECT_INTERVALS = {
    d: {'parent_m': PARENT_LENGTH_M, 'child_mid_m': CHILD_100M, 'child_10m_m': CHILD_10M}
    for d in ['roughness', 'rutting', 'cracking', 'ravelling', 'pothole']
}
LEVEL_STANDARDS = {
    'parent':     ['CA'],
    'child_100m': ['IRC', 'CA'],
    'child_10m':  ['CA', 'IRC'],
}

# ── Lane normalisation + lane-based direction (single source, used by every cell) ──
import re as _re
def normalize_lane(v):
    """'R-1' / 'r 1' / 'Lane R1' / 'LANE_L2' / 'L.3' -> 'R1' / 'L2' / 'L3'. Null stays null; unknown text is upper-cased."""
    if v is None: return None
    try:
        if pd.isna(v): return None
    except (TypeError, ValueError):
        pass
    s = _re.sub(r'[\s\-_\.]+', '', str(v).strip().upper().replace('LANE', ''))
    return s or None

def lane_to_direction(lane_value):
    """Direction always follows the lane: any L-lane (L1..L8) = Increasing, any R-lane (R1..R8) = Decreasing."""
    s = normalize_lane(lane_value)
    if s is None: return None
    if _re.fullmatch(r'L\d+', s): return 'Increasing'
    if _re.fullmatch(r'R\d+', s): return 'Decreasing'
    return None

def normalize_direction(v):
    """D1 direction text -> 'Increasing' / 'Decreasing' (handles case, spaces, 'Inc'/'Dec'); other text kept."""
    if v is None: return None
    try:
        if pd.isna(v): return None
    except (TypeError, ValueError):
        pass
    s = str(v).strip()
    if not s: return None
    if s.lower().startswith('inc'): return 'Increasing'
    if s.lower().startswith('dec'): return 'Decreasing'
    return s

LANE_TO_DIRECTION = {f'{p}{i}': lane_to_direction(f'{p}{i}') for p in 'LR' for i in range(1, 9)}   # L1-L8 / R1-R8

print(f"DLM Extraction Pipeline")
print(f"UPC: {UPC} | Zone={ZONE}, Phase={PHASE}, Cycle={CYCLE}")
print(f"Deliverable base: {DELIVERABLE_BASE}")
print(f"Backup base:      {BACKUP_BASE}")
print(f"Target schemas:   {DLM_POSTGRES_NS} (breach only), {DLM_KPI_NS} (all)")

# COMMAND ----------

# DBTITLE 1,Tracker and D2 Threshold Parsing
# ═══════════════════════════════════════════════════════════════════════════════
# TRACKER + D2 THRESHOLD PARSING
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_std(val):
    s = str(val).strip().upper() if pd.notna(val) else ""
    if 'IRC' in s: return 'IRC'
    if 'CA' in s: return 'CA'
    return None

def _parse_interval(val):
    try: return int(float(val))
    except: return None

def load_tracker_context(zone, phase, cycle, upc_slash):
    sheet_map = {"A": "Zone A", "B": "Zone B", "C": "Zone C", "D": "Zone D", "E": "Zone E"}
    sheet_name = sheet_map.get(zone, f"Zone {zone}")

    def _default_ctx():
        defs = {d: {'standard': 'CA', 'interval_m': 100}
                for d in ['ravelling', 'rutting', 'cracking', 'roughness', 'pothole']}
        return {
            'survey_start_date': None, 'survey_end_date': None,
            'piu': None, 'ae_ie': None, 'contractor': None, 'state': None,
            'pavement_type': None, 'pci': None, 'threshold_label_override': None,
            'defect_standards': defs, 'raw_defect_standards': copy.deepcopy(defs),
        }

    try:
        df = pd.read_excel(TRACKER_PATH, sheet_name=sheet_name, header=1)
    except Exception as e:
        print(f"  WARNING: Could not load tracker: {e}")
        return _default_ctx()

    upc_norm = upc_slash.replace('/', '_').replace('_', '').lower()
    best_row = None
    for _, row in df.iterrows():
        upc_val = str(row.iloc[12]).replace('/', '_').replace('_', '').lower()
        if upc_norm not in upc_val and upc_val not in upc_norm: continue
        phase_val = str(row.iloc[2]).lower().replace('phase', '').strip()
        cycle_val = str(row.iloc[3]).lower().replace('cycle', '').strip()
        if phase_val == str(phase).strip() and cycle_val == str(cycle).strip():
            best_row = row; break
        if best_row is None: best_row = row

    if best_row is None:
        print(f"  WARNING: UPC {upc_slash} not found in tracker.")
        return _default_ctx()

    r = best_row
    survey_start = pd.to_datetime(r.iloc[21], errors='coerce')
    survey_end = pd.to_datetime(r.iloc[22], errors='coerce')
    defect_standards = {
        'ravelling': {'standard': _parse_std(r.iloc[46]), 'interval_m': _parse_interval(r.iloc[47]) or 100},
        'rutting':   {'standard': _parse_std(r.iloc[48]), 'interval_m': _parse_interval(r.iloc[49]) or 100},
        'cracking':  {'standard': _parse_std(r.iloc[50]), 'interval_m': _parse_interval(r.iloc[51]) or 100},
        'roughness': {'standard': _parse_std(r.iloc[52]), 'interval_m': _parse_interval(r.iloc[53]) or 100},
    }
    defect_standards['pothole'] = defect_standards['cracking'].copy()
    raw_defect_standards = copy.deepcopy(defect_standards)

    ctx = {
        'survey_start_date': survey_start.date() if pd.notnull(survey_start) else None,
        'survey_end_date': survey_end.date() if pd.notnull(survey_end) else None,
        'piu': str(r.iloc[7]).strip() if pd.notna(r.iloc[7]) else None,
        'ae_ie': str(r.iloc[9]).strip() if pd.notna(r.iloc[9]) else None,
        'contractor': str(r.iloc[8]).strip() if pd.notna(r.iloc[8]) else None,
        'state': str(r.iloc[6]).strip() if pd.notna(r.iloc[6]) else None,
        'pavement_type': str(r.iloc[42]).strip() if pd.notna(r.iloc[42]) else None,
        'pci': float(r.iloc[43]) if pd.notna(r.iloc[43]) and str(r.iloc[43]).replace('.','',1).replace('-','',1).isdigit() else None,
        'threshold_label_override': None,
        'defect_standards': defect_standards,
        'raw_defect_standards': raw_defect_standards,
    }

    # D2 override
    d2_files = [f for f in os.listdir(D2_BASE) if f.endswith('.parquet')] if os.path.exists(D2_BASE) else []
    _d2_lower = [f.lower() for f in d2_files]
    if any('as per ca' in fl or 'with ca' in fl for fl in _d2_lower):
        ctx['threshold_label_override'] = 'As per CA'
        for dn in ctx['defect_standards']:
            ctx['defect_standards'][dn]['standard'] = 'CA'
    elif any('as per irc' in fl or 'with irc' in fl for fl in _d2_lower):
        ctx['threshold_label_override'] = 'As per IRC'
        for dn in ctx['defect_standards']:
            ctx['defect_standards'][dn]['standard'] = 'IRC'
    return ctx

TRACKER_CTX = load_tracker_context(ZONE, PHASE, CYCLE, UPC.replace('_', '/'))

def get_threshold_name(defect_name):
    override = TRACKER_CTX.get('threshold_label_override')
    if override: return override
    std = TRACKER_CTX['defect_standards'].get(defect_name, {}).get('standard')
    if std == 'IRC': return 'As per IRC'
    if std == 'CA': return 'As per CA'
    return None

def get_reporting_interval(defect_name):
    return TRACKER_CTX['defect_standards'].get(defect_name, {}).get('interval_m', 100) or 100

def get_level_standard(level):
    prefs = LEVEL_STANDARDS.get(level, ['CA'])
    return f'As per {prefs[0]}'

# ── Parse D2 Thresholds from Both CA and IRC Sheets ───────────────────────
def parse_d2_thresholds(d2_base_path):
    defaults = {
        'roughness_bi': 2400.0, 'roughness_iri': 2400.0,
        'rutting_mm': 10.0, 'cracking_pct': 10.0,
        'ravelling': 10.0, 'ravelling_unit': 'pct',
    }
    if not d2_base_path or not os.path.exists(d2_base_path):
        print("  WARNING: D2 path not found, using defaults.")
        return defaults
    thresholds = dict(defaults)
    ca_set, irc_set = set(), set()
    for pf in sorted(os.listdir(d2_base_path)):
        if not pf.endswith('.parquet'): continue
        if 'roughness,rut,crack,rav' in pf.lower(): continue
        is_irc_file = 'irc' in pf.lower()
        df = pd.read_parquet(f"{d2_base_path}/{pf}")
        for col in df.columns:
            cl = col.lower()
            if 'limitation' not in cl and 'limit' not in cl: continue
            if '>' in cl: continue
            val = pd.to_numeric(df[col], errors='coerce').dropna()
            if val.empty: continue
            limit_val = float(val.iloc[0])
            is_ca = 'concession' in cl
            if ('irc' in cl and 'roughness' in cl) or ('iri' in cl and 'limitation' in cl):
                if is_irc_file:
                    if 'roughness_iri' not in irc_set:
                        thresholds['roughness_iri'] = limit_val; irc_set.add('roughness_iri')
                else:
                    if 'roughness_bi' not in ca_set:
                        thresholds['roughness_bi'] = limit_val; ca_set.add('roughness_bi')
            elif 'bi' in cl or ('roughness' in cl and 'irc' not in cl):
                if is_irc_file:
                    if 'roughness_iri' not in irc_set:
                        thresholds['roughness_iri'] = limit_val; irc_set.add('roughness_iri')
                else:
                    if is_ca or 'roughness_bi' not in ca_set:
                        thresholds['roughness_bi'] = limit_val
                        if is_ca: ca_set.add('roughness_bi')
            elif 'rut' in cl:
                if not is_irc_file:
                    thresholds['rutting_mm'] = limit_val; ca_set.add('rutting_mm')
                elif 'rutting_mm' not in ca_set:
                    thresholds['rutting_mm'] = limit_val
            elif 'crack' in cl:
                if not is_irc_file:
                    thresholds['cracking_pct'] = limit_val; ca_set.add('cracking_pct')
                elif 'cracking_pct' not in ca_set:
                    thresholds['cracking_pct'] = limit_val
            elif 'ravelling' in cl or 'ravel' in cl:
                if not is_irc_file:
                    thresholds['ravelling'] = limit_val; ca_set.add('ravelling')
                elif 'ravelling' not in ca_set:
                    thresholds['ravelling'] = limit_val
                if 'sq' in cl or 'area' in cl: thresholds['ravelling_unit'] = 'sqm'
                elif '%' in cl or 'percent' in cl: thresholds['ravelling_unit'] = 'pct'
    return thresholds

D2_THRESHOLDS = parse_d2_thresholds(D2_BASE)
IRC_IRI_POOR = D2_THRESHOLDS['roughness_iri']
CA_BI_THRESHOLD = D2_THRESHOLDS['roughness_bi']
CA_RUT_THRESHOLD = D2_THRESHOLDS['rutting_mm']
CA_CRACK_PCT = D2_THRESHOLDS['cracking_pct']
RAVELLING_RATING_THRESHOLD = D2_THRESHOLDS['ravelling']
RAVELLING_UNIT = D2_THRESHOLDS['ravelling_unit']

# ── IRC-specific thresholds (parsed separately from D2 IRC file) ───────────
IRC_CRACK_PCT = None
IRC_RUT_THRESHOLD = None
IRC_RAVELLING_THRESHOLD = None
IRC_RAVELLING_UNIT = None

if os.path.exists(D2_BASE):
    for pf in sorted(os.listdir(D2_BASE)):
        if not pf.endswith('.parquet') or 'irc' not in pf.lower(): continue
        df_irc = pd.read_parquet(f"{D2_BASE}/{pf}")
        for col in df_irc.columns:
            cl = col.lower()
            if 'limit' not in cl: continue
            val = pd.to_numeric(df_irc[col], errors='coerce').dropna()
            if val.empty: continue
            lv = float(val.iloc[0])
            if 'crack' in cl and IRC_CRACK_PCT is None:
                IRC_CRACK_PCT = lv
            elif 'rut' in cl and IRC_RUT_THRESHOLD is None:
                IRC_RUT_THRESHOLD = lv
            elif 'ravel' in cl and IRC_RAVELLING_THRESHOLD is None:
                IRC_RAVELLING_THRESHOLD = lv
                if '%' in cl or 'pct' in cl: IRC_RAVELLING_UNIT = 'pct'
                elif 'sq' in cl or 'area' in cl: IRC_RAVELLING_UNIT = 'sqm'
        break  # Only first IRC file

# Fallbacks: use CA thresholds if IRC not found
IRC_CRACK_PCT = IRC_CRACK_PCT or CA_CRACK_PCT
IRC_RUT_THRESHOLD = IRC_RUT_THRESHOLD or CA_RUT_THRESHOLD
IRC_RAVELLING_THRESHOLD = IRC_RAVELLING_THRESHOLD or RAVELLING_RATING_THRESHOLD
IRC_RAVELLING_UNIT = IRC_RAVELLING_UNIT or RAVELLING_UNIT

print(f"D2 CA Thresholds:  roughness_bi={CA_BI_THRESHOLD}, cracking={CA_CRACK_PCT}%, rutting={CA_RUT_THRESHOLD}mm, ravelling={RAVELLING_RATING_THRESHOLD} {RAVELLING_UNIT}")
print(f"D2 IRC Thresholds: roughness_iri={IRC_IRI_POOR}, cracking={IRC_CRACK_PCT}%, rutting={IRC_RUT_THRESHOLD}mm, ravelling={IRC_RAVELLING_THRESHOLD} {IRC_RAVELLING_UNIT}")
print(f"Tracker: survey={TRACKER_CTX['survey_start_date']} to {TRACKER_CTX['survey_end_date']}")
print(f"Tracker override: {TRACKER_CTX.get('threshold_label_override', 'None')}")

# COMMAND ----------

# DBTITLE 1,Load All Defect Data Sources
# ═══════════════════════════════════════════════════════════════════════════════
# LOAD ALL DEFECT DATA SOURCES
# ═══════════════════════════════════════════════════════════════════════════════

def discover_lane_folders(base_path):
    if not os.path.exists(base_path): return []
    return sorted(d.replace("Lane=", "") for d in os.listdir(base_path)
                  if d.startswith("Lane=") and os.path.isdir(os.path.join(base_path, d)))

def load_matching_parquets(base_path, patterns):
    if not base_path or not os.path.exists(base_path): return pd.DataFrame(), []
    files = [f for f in sorted(os.listdir(base_path))
             if f.endswith('.parquet') and any(p.lower() in f.lower() for p in patterns)]
    if not files: return pd.DataFrame(), files
    frames = [pd.read_parquet(f"{base_path}/{f}") for f in files]
    return pd.concat(frames, ignore_index=True), files

def load_backup_lane_parquets(base_path, suffixes):
    if isinstance(suffixes, str): suffixes = [suffixes]
    frames, files = [], []
    lanes = discover_lane_folders(base_path)
    for lane in lanes:
        lane_path = os.path.join(base_path, f"Lane={lane}")
        matches = [f for f in sorted(os.listdir(lane_path))
                   if f.endswith('.parquet') and any(sfx.lower() in f.lower() for sfx in suffixes)]
        lane_n = normalize_lane(lane)          # 'Lane=R-1' -> 'R1' (cell 3)
        for pf in matches:
            part = pd.read_parquet(os.path.join(lane_path, pf))
            if 'Lane' not in part.columns and 'LANE' not in part.columns:
                part['Lane'] = lane_n
            for _lc in ('Lane', 'LANE'):
                if _lc in part.columns:
                    part[_lc] = part[_lc].map(normalize_lane).fillna(lane_n)
            part['Source_Lane_Folder'] = lane_n
            frames.append(part)
            files.append(f"Lane={lane}/{pf}")
    return (pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()), files

def _d4_pick(cols, *keywords):
    for col in cols:
        cl = col.lower()
        if all(kw.lower() in cl for kw in keywords): return col
    return None

# ── Condition Functions (dynamic breakpoints from D2 thresholds) ───────────
def crack_condition_from_pct(value):
    value = pd.to_numeric(pd.Series([value]), errors='coerce').iloc[0]
    if pd.isna(value): return None
    t = CA_CRACK_PCT
    b2, b3, b4 = round(t * 2), round(t * 4), round(t * 6)
    if value < t: return f'Very Good (<{t:.0f}%)'
    if value <= b2: return f'Good ({t:.0f}-{b2}%)'
    if value <= b3: return f'Fair ({b2}-{b3}%)'
    if value <= b4: return f'Poor ({b3}-{b4}%)'
    return f'Very Poor (>{b4}%)'

def roughness_condition_from_metrics(iri_value, bi_value):
    iri_value = pd.to_numeric(pd.Series([iri_value]), errors='coerce').iloc[0]
    bi_value = pd.to_numeric(pd.Series([bi_value]), errors='coerce').iloc[0]
    probe = bi_value if pd.notna(bi_value) else (iri_value * 1000 if pd.notna(iri_value) else np.nan)
    if pd.isna(probe): return None
    t = CA_BI_THRESHOLD
    fair_lower = round(t * 0.8)
    if probe < fair_lower: return f'Good (<{fair_lower}mm/km)'
    if probe <= t: return f'Fair ({fair_lower}-{t:.0f}mm/km)'
    return f'Poor (>{t:.0f}mm/km)'

def rutting_condition_from_mm(value):
    value = pd.to_numeric(pd.Series([value]), errors='coerce').iloc[0]
    if pd.isna(value): return None
    t = CA_RUT_THRESHOLD
    fair_lower = round(t * 0.5)
    if value < fair_lower: return f'Good (<{fair_lower}mm)'
    if value <= t: return f'Fair ({fair_lower}-{t:.0f}mm)'
    return f'Poor (>{t:.0f}mm)'

def ravelling_condition_from_pct(value):
    value = pd.to_numeric(pd.Series([value]), errors='coerce').iloc[0]
    if pd.isna(value): return None
    t = RAVELLING_RATING_THRESHOLD
    half_t = round(t * 0.5)
    if value == 0: return 'Very Good (0)'
    if value <= half_t: return f'Good (1-{half_t}%)'
    if value < t: return f'Fair ({half_t}-{t:.0f}%)'
    return f'Poor (>={t:.0f}%)'

# ── Load D4 Potholes ─────────────────────────────────────────────────────────
d4_files = []
if os.path.exists(D4_BASE):
    _EXCLUDE_KW = ['roughness', 'rut,', 'crack', 'raveling', 'ravelling']
    d4_files = [f for f in sorted(os.listdir(D4_BASE))
                if f.endswith(".parquet") and 'pothole' in f.lower()
                and not any(kw in f.lower() for kw in _EXCLUDE_KW)]

if d4_files:
    _MIN_DEFECT_COLS = {'severity', 'area_sq_m', 'area', 'type_of_defect', 'lane_no'}
    child_frames = []
    for pf in d4_files:
        df_part = pd.read_parquet(f"{D4_BASE}/{pf}")
        _cols_lower = {c.lower() for c in df_part.columns}
        if not _cols_lower & _MIN_DEFECT_COLS: continue
        child_frames.append(df_part)
        print(f"  Loaded D4 potholes: {pf} ({len(df_part)} rows)")

    df_pothole_raw = pd.concat(child_frames, ignore_index=True)
    _d4cols = list(df_pothole_raw.columns)

    _from_km_c = _d4_pick(_d4cols, 'from_ch_km') or _d4_pick(_d4cols, 'from', 'km')
    _from_m_c  = _d4_pick(_d4cols, 'from_ch_m')  or _d4_pick(_d4cols, 'from', 'meter')
    _to_km_c   = _d4_pick(_d4cols, 'to_ch_km')   or _d4_pick(_d4cols, 'to', 'km')
    _to_m_c    = _d4_pick(_d4cols, 'to_ch_m')    or _d4_pick(_d4cols, 'to', 'meter')
    _chain_c   = _d4_pick(_d4cols, 'chainage')
    _lane_c    = _d4_pick(_d4cols, 'lane_no')    or _d4_pick(_d4cols, 'lane no') or _d4_pick(_d4cols, 'lane')
    _lat_c     = _d4_pick(_d4cols, 'latitude')
    _lon_c     = _d4_pick(_d4cols, 'longitude')
    _img_c     = _d4_pick(_d4cols, 'image_name') or _d4_pick(_d4cols, 'image name')
    _sev_c     = _d4_pick(_d4cols, 'severity')
    _deftype_c = _d4_pick(_d4cols, 'type_of_defect') or _d4_pick(_d4cols, 'type', 'defect')
    _area_c    = _d4_pick(_d4cols, 'area_sq_m')  or _d4_pick(_d4cols, 'area', 'sq')
    _maxd_c    = _d4_pick(_d4cols, 'max_depth_mm') or _d4_pick(_d4cols, 'max', 'depth')
    _avgd_c    = _d4_pick(_d4cols, 'average_depth_mm') or _d4_pick(_d4cols, 'average', 'depth')
    _dt_c      = _d4_pick(_d4cols, 'datetime') or _d4_pick(_d4cols, 'date')
    _side_c    = _d4_pick(_d4cols, 'side')

    # Compute Start_Chainage
    if _from_km_c and _from_m_c:
        _km_vals = pd.to_numeric(df_pothole_raw[_from_km_c], errors='coerce').fillna(0).astype(int)
        _m_vals  = pd.to_numeric(df_pothole_raw[_from_m_c], errors='coerce').fillna(0).astype(int)
        _computed_ch = _km_vals * 1000 + _m_vals
        _bad_m = _m_vals > 999
        if _bad_m.any() and _chain_c:
            _ch_direct = pd.to_numeric(df_pothole_raw[_chain_c], errors='coerce').fillna(0).astype(int)
            _computed_ch[_bad_m] = _ch_direct[_bad_m]
            print(f"  Fixed {_bad_m.sum()} rows where from_ch_m had full chainage")
        df_pothole_raw["Start_Chainage"] = _computed_ch
    elif _chain_c:
        df_pothole_raw["Start_Chainage"] = pd.to_numeric(df_pothole_raw[_chain_c], errors='coerce').fillna(0)
        df_pothole_raw.loc[df_pothole_raw["Start_Chainage"] < 1000, "Start_Chainage"] *= 1000
    else:
        df_pothole_raw["Start_Chainage"] = 0

    # Compute End_Chainage
    if _to_km_c and _to_m_c:
        _to_km_vals = pd.to_numeric(df_pothole_raw[_to_km_c], errors='coerce')
        _to_m_vals  = pd.to_numeric(df_pothole_raw[_to_m_c], errors='coerce')
        _to_computed = _to_km_vals.fillna(0).astype(int) * 1000 + _to_m_vals.fillna(0).astype(int)
        _to_bad = _to_m_vals.fillna(0).astype(int) > 999
        if _to_bad.any() and _chain_c:
            _to_computed[_to_bad] = pd.to_numeric(df_pothole_raw.loc[_to_bad, _chain_c], errors='coerce').fillna(0).astype(int)
        df_pothole_raw["End_Chainage"] = _to_computed
        _both_null = _to_km_vals.isna() & _to_m_vals.isna()
        df_pothole_raw.loc[_both_null, "End_Chainage"] = df_pothole_raw.loc[_both_null, "Start_Chainage"]
    else:
        df_pothole_raw["End_Chainage"] = df_pothole_raw["Start_Chainage"]

    # Standardize columns
    df_pothole_raw["Lane"] = df_pothole_raw[_lane_c].map(normalize_lane).fillna("Unknown") if _lane_c else "Unknown"
    _remap = {}
    if _lat_c: _remap[_lat_c] = "Latitude"
    if _lon_c: _remap[_lon_c] = "Longitude"
    if _img_c: _remap[_img_c] = "Image_Name"
    if _sev_c: _remap[_sev_c] = "Severity"
    if _deftype_c: _remap[_deftype_c] = "Type_of_Defect"
    if _area_c: _remap[_area_c] = "Area_sq_m"
    if _maxd_c: _remap[_maxd_c] = "Max_Depth_mm"
    if _avgd_c: _remap[_avgd_c] = "Average_Depth_mm"
    if _dt_c: _remap[_dt_c] = "Survey_Date"
    if _side_c: _remap[_side_c] = "Side"
    df_pothole_raw.rename(columns=_remap, inplace=True)
else:
    df_pothole_raw = pd.DataFrame()
    print("  WARNING: No D4 pothole files found")

# ── Load Backup Lane Data ───────────────────────────────────────────────────
LANES = sorted({normalize_lane(l) for l in discover_lane_folders(BACKUP_BASE)})   # all lanes, L1-L8 / R1-R8
df_cracking_raw, cracking_files = load_backup_lane_parquets(BACKUP_BASE, '_Cracking.parquet')
df_roughness_raw, roughness_files = load_backup_lane_parquets(BACKUP_BASE, '_Roughness_IRI.parquet')
df_rutting_raw, rutting_files = load_backup_lane_parquets(BACKUP_BASE, '_Rutting.parquet')
df_ravelling_raw, ravelling_files = load_backup_lane_parquets(BACKUP_BASE, '_Raveling.parquet')

# Standardize backup schemas
if not df_cracking_raw.empty:
    df_cracking_raw.rename(columns={
        'LANE': 'Lane', 'Percent_Area': 'Cracking', 'cracking_image_name': 'Image_Name',
        'cracking_image_url': 'Image_URL', 'Start_Latitude': 'Start_Lat',
        'Start_Longitude': 'Start_Lon', 'End_Latitude': 'End_Lat',
        'End_Longitude': 'End_Lon', 'survey_date_crack': 'SurveyDate', 'Area_Sum': 'Area_sq_m'
    }, inplace=True)
    if 'Lane' not in df_cracking_raw.columns:
        df_cracking_raw['Lane'] = df_cracking_raw['Source_Lane_Folder']
    crack_pct = pd.to_numeric(df_cracking_raw['Cracking'], errors='coerce').fillna(0)
    df_cracking_raw['Condition'] = crack_pct.apply(crack_condition_from_pct)
    df_cracking_raw['Type_of_Defect'] = 'Cracking'

if not df_roughness_raw.empty:
    df_roughness_raw.rename(columns={
        'RoughnessIndex': 'BI m/km', 'LaneIri': 'IRI',
        'Start_Latitude': 'Start_Lat', 'Start_Longitude': 'Start_Lon',
        'End_Latitude': 'End_Lat', 'End_Longitude': 'End_Lon',
        'ConditionOfRoadSurface': 'Condition',
    }, inplace=True)
    if 'SurveyDate' not in df_roughness_raw.columns:
        df_roughness_raw['SurveyDate'] = pd.NaT

    # ── NEW: Derive Lane IRI and Roughness BI from LWP + RWP (IRC:SP:16-2019) ──
    # Formula: Lane IRI = (LwpIri + RwpIri) / 2
    #          Roughness BI (mm/km) = 630 × (Lane IRI)^1.12
    _has_lwp = 'LwpIri' in df_roughness_raw.columns
    _has_rwp = 'RwpIri' in df_roughness_raw.columns
    if _has_lwp and _has_rwp:
        _lwp = pd.to_numeric(df_roughness_raw['LwpIri'], errors='coerce')
        _rwp = pd.to_numeric(df_roughness_raw['RwpIri'], errors='coerce')
        _both_valid = _lwp.notna() & _rwp.notna()
        _n_valid = _both_valid.sum()
        _n_total = len(df_roughness_raw)

        # Compute derived values where both LWP and RWP are available
        _derived_iri = (_lwp + _rwp) / 2
        _derived_bi  = 630.0 * (_derived_iri ** 1.12)

        # Store original values for audit trail
        df_roughness_raw['IRI_original'] = df_roughness_raw['IRI']
        df_roughness_raw['BI_original']  = df_roughness_raw['BI m/km']

        # Override IRI and BI where LWP+RWP are available
        df_roughness_raw.loc[_both_valid, 'IRI']     = _derived_iri[_both_valid]
        df_roughness_raw.loc[_both_valid, 'BI m/km'] = _derived_bi[_both_valid]

        print(f"  Roughness BI from LWP+RWP: {_n_valid}/{_n_total} rows derived")
        print(f"    Formula: Lane IRI = (LwpIri + RwpIri) / 2, BI = 630 × IRI^1.12")
        if _n_valid < _n_total:
            print(f"    {_n_total - _n_valid} rows kept original (null LWP/RWP)")
    else:
        _missing = []
        if not _has_lwp: _missing.append('LwpIri')
        if not _has_rwp: _missing.append('RwpIri')
        print(f"  WARNING: {', '.join(_missing)} not found in parquet — using pre-computed IRI/BI")

    rough_iri = pd.to_numeric(df_roughness_raw.get('IRI'), errors='coerce')
    rough_bi = pd.to_numeric(df_roughness_raw.get('BI m/km'), errors='coerce')
    derived_condition = [roughness_condition_from_metrics(i, b) for i, b in zip(rough_iri, rough_bi)]
    df_roughness_raw['Condition'] = df_roughness_raw.get('Condition', pd.Series([None] * len(df_roughness_raw))).fillna(pd.Series(derived_condition))
    df_roughness_raw['Type_of_Defect'] = 'Roughness'
    # Fill any remaining null BI from IRI: BI = 630 * (IRI)^1.12
    df_roughness_raw['BI m/km'] = pd.to_numeric(df_roughness_raw['BI m/km'], errors='coerce')
    df_roughness_raw['IRI'] = pd.to_numeric(df_roughness_raw['IRI'], errors='coerce')
    _null_bi = df_roughness_raw['BI m/km'].isna().sum()
    if _null_bi > 0:
        _derived_bi = 630 * (df_roughness_raw['IRI'] ** 1.12)
        df_roughness_raw['BI m/km'] = df_roughness_raw['BI m/km'].fillna(_derived_bi)
        print(f"  BI null-fill: {_null_bi} nulls filled via 630*(IRI)^1.12")

if not df_rutting_raw.empty:
    df_rutting_raw.rename(columns={
        'LANE': 'Lane', 'Rutting Average': 'Rutting',
        'Start_Latitude': 'Start_Lat', 'Start_Longitude': 'Start_Lon',
        'End_Latitude': 'End_Lat', 'End_Longitude': 'End_Lon',
        'survey_date_rutting': 'SurveyDate',
    }, inplace=True)
    if 'Lane' not in df_rutting_raw.columns:
        df_rutting_raw['Lane'] = df_rutting_raw['Source_Lane_Folder']
    rut_val = pd.to_numeric(df_rutting_raw['Rutting'], errors='coerce')
    df_rutting_raw['Condition'] = rut_val.apply(rutting_condition_from_mm)
    df_rutting_raw['Type_of_Defect'] = 'Rutting'

if not df_ravelling_raw.empty:
    df_ravelling_raw.rename(columns={
        'Raveling_Area_m2': 'Ravelling_Area_sq_m',
        'Start_Latitude': 'Start_Lat', 'Start_Longitude': 'Start_Lon',
        'End_Latitude': 'End_Lat', 'End_Longitude': 'End_Lon',
        'survey_date_raveling': 'SurveyDate',
    }, inplace=True)
    rav_pct = pd.to_numeric(df_ravelling_raw['Ravelling_Area_sq_m'], errors='coerce').fillna(0) * 100.0 / 35.0
    df_ravelling_raw['Ravelling'] = rav_pct
    df_ravelling_raw['Condition'] = rav_pct.apply(ravelling_condition_from_pct)
    df_ravelling_raw['Type_of_Defect'] = 'Ravelling'

df_visual_raw = df_ravelling_raw.copy() if not df_ravelling_raw.empty else pd.DataFrame()

# ── Load D3 Visual Condition Data (IRC road condition, dynamic per project) ──
D3_CONDITIONS = {}
_d3_defect_map = {
    'crack': 'Cracking', 'roughness': 'Roughness',
    'rutting': 'Rutting', 'rut': 'Rutting',
    'pothole': 'Pothole', 'potholes': 'Pothole',
}
if os.path.exists(D3_BASE):
    for _f in sorted(os.listdir(D3_BASE)):
        if not _f.endswith('.parquet'): continue
        _fl = _f.lower()
        _defect_key = next((v for k, v in _d3_defect_map.items() if k in _fl), None)
        if not _defect_key: continue
        _df_d3 = pd.read_parquet(os.path.join(D3_BASE, _f))
        if 'Condition' in _df_d3.columns and 'Start_Chainage' in _df_d3.columns:
            _df_d3['Start_Chainage'] = pd.to_numeric(_df_d3['Start_Chainage'], errors='coerce')
            _df_d3 = _df_d3.dropna(subset=['Start_Chainage'])
            # Normalize Lane column (handle 'Lane\nNumber', 'Lane Number', etc.)
            _lane_col = next((c for c in _df_d3.columns if 'lane' in c.lower().replace('\n', ' ')), None)
            if _lane_col and _lane_col != 'Lane':
                _df_d3['Lane'] = _df_d3[_lane_col].map(normalize_lane)
            elif 'Lane' not in _df_d3.columns:
                _df_d3['Lane'] = 'Unknown'
            else:
                _df_d3['Lane'] = _df_d3['Lane'].map(normalize_lane)
            # Compute 100m bin key for joining
            _df_d3['bin_100m'] = (_df_d3['Start_Chainage'] // 100 * 100).astype(int)
            # Filter out nan/null conditions
            _df_d3 = _df_d3[_df_d3['Condition'].notna() & (_df_d3['Condition'].astype(str) != 'nan')]
            D3_CONDITIONS[_defect_key] = _df_d3[['bin_100m', 'Lane', 'Condition']].drop_duplicates(
                subset=['bin_100m', 'Lane']
            ).copy()
    print(f"\n  D3 visual conditions loaded: {list(D3_CONDITIONS.keys())}")
    for _k, _v in D3_CONDITIONS.items():
        _conds = sorted(_v['Condition'].dropna().unique())
        print(f"    {_k}: {len(_v)} rows, categories: {_conds}")
else:
    print(f"\n  WARNING: D3 path not found: {D3_BASE}")

# ── Breach Filter Summary (keep all roughness/ravelling, filter cracking/rutting) ──
print(f"\nRaw data loaded:")
print(f"  Lanes: {LANES}")
print(f"  Potholes:   {len(df_pothole_raw):,} (D4)")
print(f"  Roughness:  {len(df_roughness_raw):,} (all kept, breach_flag per-row)")
print(f"  Cracking:   {len(df_cracking_raw):,} (all kept, breach_flag per-row)")
print(f"  Rutting:    {len(df_rutting_raw):,} (all kept, breach_flag per-row)")
print(f"  Ravelling:  {len(df_ravelling_raw):,} (all kept, breach_flag per-row)")

# COMMAND ----------

# DBTITLE 1,Build Pothole 3-Level Hierarchy
# ═══════════════════════════════════════════════════════════════════════════════
# BUILD POTHOLE 3-LEVEL HIERARCHY
# Parent=1000m(CA) | Child_100m(IRC→CA) | Child_10m(CA→IRC)
# ═══════════════════════════════════════════════════════════════════════════════

df_pothole = df_pothole_raw.copy()
_POTHOLE_PARENT_M = DEFECT_INTERVALS['pothole']['parent_m']
_POTHOLE_CHILD_MID_M = DEFECT_INTERVALS['pothole']['child_mid_m']

# Guard: skip pothole processing if no data
if df_pothole.empty or 'Start_Chainage' not in df_pothole.columns:
    SURVEY_DATE_STR = RUN_DATE.strftime('%d%m%y')
    SOURCE_CODE = "N"
    df_pothole_10m = pd.DataFrame()
    df_pothole_10m_final = df_pothole_10m
    print(f"Pothole hierarchy: 0 records (no pothole data for this UPC)")
    print(f"Survey date: {SURVEY_DATE_STR}")
else:
    SOURCE_CODE = "N"
    if 'Survey_Date' in df_pothole.columns:
        _survey_dt = pd.to_datetime(df_pothole['Survey_Date'], errors='coerce').dropna()
        SURVEY_DATE_STR = _survey_dt.iloc[0].strftime('%d%m%y') if len(_survey_dt) > 0 else RUN_DATE.strftime('%d%m%y')
    else:
        SURVEY_DATE_STR = RUN_DATE.strftime('%d%m%y')

    # Assign grouping chainages
    df_pothole['Parent_Start_Chainage'] = (df_pothole['Start_Chainage'] // _POTHOLE_PARENT_M) * _POTHOLE_PARENT_M
    df_pothole['Parent_End_Chainage'] = df_pothole['Parent_Start_Chainage'] + _POTHOLE_PARENT_M
    df_pothole['Child_100m_Start'] = (df_pothole['Start_Chainage'] // _POTHOLE_CHILD_MID_M) * _POTHOLE_CHILD_MID_M
    df_pothole['Child_100m_End'] = df_pothole['Child_100m_Start'] + _POTHOLE_CHILD_MID_M

    df_pothole['Parent_ID'] = (
        UPC + '_' + df_pothole['Lane'].astype(str) + '_' +
        df_pothole['Parent_Start_Chainage'].astype(int).astype(str) + '_' +
        df_pothole['Parent_End_Chainage'].astype(int).astype(str)
    )
    df_pothole['Child_ID'] = (
        UPC + '_' + df_pothole['Lane'].astype(str) + '_Pothole_' +
        df_pothole['Start_Chainage'].astype(str) + '_' + df_pothole.index.astype(str)
    )
    df_pothole['Defect_ID'] = [
        f"D/{SURVEY_DATE_STR}/{SOURCE_CODE}/{UPC}/{int(row['Start_Chainage'])}/Pothole/{i+1:04d}"
        for i, (_, row) in enumerate(df_pothole.iterrows())
    ]

    # Metadata columns
    for col, val in [('Source', 'NSV'), ('Category', 'POINT'), ('Defect_Type', 'Pothole'),
                     ('Zone', ZONE), ('Phase', PHASE), ('Cycle', CYCLE), ('UPC_Code', UPC),
                     ('Run_Date', str(RUN_DATE)), ('Project_Name', UPC)]:
        df_pothole[col] = val
    df_pothole['UPC'] = UPC.replace('_', '/')
    df_pothole['Point_Chainage'] = df_pothole['Start_Chainage']
    df_pothole['Point_Lat'] = df_pothole.get('Latitude')
    df_pothole['Point_Long'] = df_pothole.get('Longitude')

    df_pothole_10m = df_pothole.copy()
    df_pothole_10m_final = df_pothole_10m  # Alias for downstream

    print(f"Pothole hierarchy: {len(df_pothole_10m)} records (10m raw)")
    print(f"Survey date: {SURVEY_DATE_STR}")
    if len(df_pothole_10m) > 0:
        print(f"Defect_ID ex: {df_pothole_10m['Defect_ID'].iloc[0]}")

# COMMAND ----------

# DBTITLE 1,Load Workbook Schemas and Helper Functions
# ═══════════════════════════════════════════════════════════════════════════════
# LOAD WORKBOOK SCHEMAS + HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

SHEET_TO_TABLE = {
    "pothole": "master_pothole", "cracking": "master_cracking",
    "roughness_parent": "master_roughness_parent",
    "roughness_child_10m": "master_roughness_child_10m",
    "roughness_child_100m": "master_roughness_child_100m",
    "rutting_parent": "master_rutting_parent",
    "rutting_child_10m": "master_rutting_child_10m",
    "rutting_child_100m": "master_rutting_child_100m",
    "ravelling_parent": "master_ravelling_parent",
    "ravelling_child_10m": "master_ravelling_child_10m",
    "ravelling_child_100m": "master_ravelling_child_100m",
}

def normalize_col_name(name): return str(name).strip().lower()

def pg_type_to_spark(pg_type):
    pg = str(pg_type).strip().lower()
    if "text[]" in pg: return T.ArrayType(T.StringType())
    if "jsonb" in pg: return T.StringType()
    if "timestamp" in pg: return T.TimestampType()
    if pg == "date": return T.DateType()
    if "boolean" in pg: return T.BooleanType()
    if pg == "uuid": return T.StringType()
    if "smallint" in pg or "integer" in pg: return T.IntegerType()
    if "numeric" in pg or "double precision" in pg or "real" in pg or "float" in pg: return T.DoubleType()
    return T.StringType()

def parse_sheet_schema(sheet_name):
    df = pd.read_excel(SCHEMA_WORKBOOK, sheet_name=sheet_name, header=None, skiprows=2)
    df.columns = ["num", "column", "type", "nullable", "default", "notes"][:len(df.columns)]
    df = df[pd.to_numeric(df["num"], errors="coerce").notna()].copy()
    df["column"] = df["column"].astype(str).str.strip()
    df["type"] = df["type"].astype(str).str.strip()
    df["nullable"] = df["nullable"].astype(str).str.strip()
    fields, col_names = [], []
    for _, row in df.iterrows():
        col_name = normalize_col_name(row["column"])
        spark_type = pg_type_to_spark(row["type"])
        # Force all fields nullable to avoid PySparkValueError on None inputs
        fields.append(T.StructField(col_name, spark_type, True))
        col_names.append(col_name)
    return col_names, T.StructType(fields)

TABLE_SCHEMAS, TABLE_COLUMNS, TABLE_TYPE_MAPS = {}, {}, {}
for sheet_name, table_name in SHEET_TO_TABLE.items():
    cols, schema = parse_sheet_schema(sheet_name)
    TABLE_SCHEMAS[table_name] = schema
    TABLE_COLUMNS[table_name] = cols
    TABLE_TYPE_MAPS[table_name] = {f.name: f.dataType for f in schema.fields}

# Add affected_length and road_condition columns to all schemas
for _tbl_name in TABLE_SCHEMAS:
    for _col_name, _col_type in [("affected_length", T.DoubleType()), ("total_length", T.DoubleType()),
                                  ("affected_length_ca", T.DoubleType()), ("affected_length_irc", T.DoubleType()),
                                  ("road_condition", T.StringType())]:
        if _col_name not in TABLE_COLUMNS[_tbl_name]:
            TABLE_COLUMNS[_tbl_name].append(_col_name)
            TABLE_SCHEMAS[_tbl_name] = T.StructType(
                TABLE_SCHEMAS[_tbl_name].fields + [T.StructField(_col_name, _col_type, True)])
            TABLE_TYPE_MAPS[_tbl_name][_col_name] = _col_type

POINT_SCHEMA = TABLE_SCHEMAS["master_pothole"]
POINT_COLUMNS = TABLE_COLUMNS["master_pothole"]
POINT_TYPE_MAP = TABLE_TYPE_MAPS["master_pothole"]
CRACKING_SCHEMA = TABLE_SCHEMAS["master_cracking"]
CRACKING_COLUMNS = TABLE_COLUMNS["master_cracking"]
CRACKING_TYPE_MAP = TABLE_TYPE_MAPS["master_cracking"]

# ── Core Helper Functions ───────────────────────────────────────────────────
def typed_null(dtype): return F.lit(None).cast(dtype)
def source_col(col_name, dtype, source_cols=None):
    return F.col(col_name).cast(dtype) if (source_cols is None or col_name in source_cols) else typed_null(dtype)

def pandas_to_spark_with_schema(pdf, schema_obj):
    pdf = pdf.copy()
    pdf = pdf.where(pd.notnull(pdf), None)
    def convert_value(value, dtype):
        if value is None: return None
        try:
            if isinstance(value, float) and np.isnan(value): return None
        except (TypeError, ValueError):
            pass
        if isinstance(dtype, T.ArrayType):
            if isinstance(value, np.ndarray): return value.tolist()
            if isinstance(value, (tuple, list)): return list(value)
            try:
                return None if pd.isna(value) else [value]
            except (TypeError, ValueError):
                return [value]
        if isinstance(dtype, T.TimestampType):
            ts = pd.to_datetime(value, errors="coerce")
            return ts.to_pydatetime() if pd.notnull(ts) else None
        if isinstance(dtype, T.DateType):
            dt = pd.to_datetime(value, errors="coerce")
            return dt.date() if pd.notnull(dt) else None
        if isinstance(dtype, (T.IntegerType, T.LongType)):
            try: return int(float(value))
            except (TypeError, ValueError): return None
        if isinstance(dtype, (T.DoubleType, T.FloatType)):
            try: return float(value)
            except (TypeError, ValueError): return None
        if isinstance(dtype, T.BooleanType):
            if isinstance(value, bool): return value
            return bool(value)
        return value
    records = []
    for record in pdf.to_dict("records"):
        converted = {}
        for field in schema_obj.fields:
            converted[field.name] = convert_value(record.get(field.name), field.dataType)
        records.append(converted)
    return spark.createDataFrame(records, schema=schema_obj)

def empty_with_schema(schema_obj): return spark.createDataFrame([], schema_obj)

def norm_name(name): return "".join(ch.lower() for ch in str(name) if ch.isalnum())

def pick_col(columns, *candidates):
    normalized = {norm_name(c): c for c in columns}
    for cand in candidates:
        key = norm_name(cand)
        if key in normalized: return normalized[key]
    for cand in candidates:
        key = norm_name(cand)
        for col in columns:
            if key in norm_name(col): return col
    return None

def as_float(value):
    if value is None or pd.isna(value): return None
    try: return float(value)
    except: return None

def as_int(value):
    num = as_float(value)
    return int(num) if num is not None else None

def as_chainage_m(value, is_km=False):
    num = as_float(value)
    if num is None: return None
    if is_km and num < 1000: num = num * 1000
    return int(round(num))

def as_text(value):
    if value is None or pd.isna(value): return None
    value = str(value).strip()
    return value if value and value != "<NA>" else None

# ── NH number standardization: always "NH-<number><suffix>" ─────────────────
# e.g. NH0027 -> NH-27, NH151A -> NH-151A, "NH 8a" -> NH-8A, "27" -> NH-27
import re as _re
_NH_RE = _re.compile(r'^\s*(?:N\.?\s*H\.?)?\s*[-_/ ]*0*(\d+)\s*([A-Za-z]*)\s*$', _re.I)
def standardize_nh_number(value):
    v = as_text(value)
    if v is None: return None
    m = _NH_RE.match(v)
    if not m: return v.strip().upper()
    return f"NH-{int(m.group(1))}{m.group(2).upper()}"

def standardize_nh_col(col_name='nh_number'):
    """Spark equivalent of standardize_nh_number (idempotent)."""
    _pat = r'(?i)^\s*(?:N\.?\s*H\.?)?\s*[-_/ ]*0*(\d+)\s*([A-Za-z]*)\s*$'
    c = F.trim(F.col(col_name))
    return (F.when(c.isNull() | (c == ''), F.lit(None).cast('string'))
             .when(c.rlike(_pat), F.concat(F.lit('NH-'),
                   F.regexp_extract(c, _pat, 1).cast('int').cast('string'),
                   F.upper(F.regexp_extract(c, _pat, 2))))
             .otherwise(F.upper(c)))

def first_non_null(*values):
    for value in values:
        if value is None: continue
        if isinstance(value, float) and np.isnan(value): continue
        if isinstance(value, str) and value.strip() == "": continue
        return value
    return None

def uniq(items):
    out, seen = [], set()
    for item in items:
        if item and item not in seen: out.append(item); seen.add(item)
    return out

# normalize_lane / lane_to_direction / LANE_TO_DIRECTION are defined once in cell 3 (Configuration):
#   any L-lane (L1..L8) = Increasing, any R-lane (R1..R8) = Decreasing

def compute_breach_flag(defect_base_name, metric_value, roughness_bi=None, iri_value=None):
    if defect_base_name == 'roughness':
        bi_v = pd.to_numeric(pd.Series([roughness_bi or metric_value]), errors='coerce').iloc[0]
        iri_v = pd.to_numeric(pd.Series([iri_value or metric_value]), errors='coerce').iloc[0]
        if pd.notna(bi_v) and bi_v >= CA_BI_THRESHOLD: return 1
        if pd.notna(bi_v) and bi_v >= IRC_IRI_POOR: return 1
        return 0
    elif defect_base_name == 'rutting':
        v = pd.to_numeric(pd.Series([metric_value]), errors='coerce').iloc[0]
        return 1 if pd.notna(v) and v >= CA_RUT_THRESHOLD else 0
    elif defect_base_name == 'cracking':
        v = pd.to_numeric(pd.Series([metric_value]), errors='coerce').iloc[0]
        return 1 if pd.notna(v) and v >= CA_CRACK_PCT else 0
    elif defect_base_name == 'ravelling':
        v = pd.to_numeric(pd.Series([metric_value]), errors='coerce').iloc[0]
        return 1 if pd.notna(v) and v >= RAVELLING_RATING_THRESHOLD else 0
    return 0

def compute_breach_flag_ca(defect_base_name, metric_value, roughness_bi=None):
    if defect_base_name == 'roughness':
        bi_v = pd.to_numeric(pd.Series([roughness_bi]), errors='coerce').iloc[0]
        return 1 if pd.notna(bi_v) and bi_v >= CA_BI_THRESHOLD else 0
    elif defect_base_name == 'rutting':
        v = pd.to_numeric(pd.Series([metric_value]), errors='coerce').iloc[0]
        return 1 if pd.notna(v) and v >= CA_RUT_THRESHOLD else 0
    elif defect_base_name == 'cracking':
        v = pd.to_numeric(pd.Series([metric_value]), errors='coerce').iloc[0]
        return 1 if pd.notna(v) and v >= CA_CRACK_PCT else 0
    elif defect_base_name == 'ravelling':
        v = pd.to_numeric(pd.Series([metric_value]), errors='coerce').iloc[0]
        return 1 if pd.notna(v) and v >= RAVELLING_RATING_THRESHOLD else 0
    return 0

def compute_breach_flag_irc(defect_base_name, metric_value, iri_value=None, roughness_bi=None):
    if defect_base_name == 'roughness':
        bi_v = pd.to_numeric(pd.Series([roughness_bi or metric_value]), errors='coerce').iloc[0]
        return 1 if pd.notna(bi_v) and bi_v >= IRC_IRI_POOR else 0
    elif defect_base_name == 'cracking':
        v = pd.to_numeric(pd.Series([metric_value]), errors='coerce').iloc[0]
        return 1 if pd.notna(v) and v >= IRC_CRACK_PCT else 0
    elif defect_base_name == 'rutting':
        v = pd.to_numeric(pd.Series([metric_value]), errors='coerce').iloc[0]
        return 1 if pd.notna(v) and v >= IRC_RUT_THRESHOLD else 0
    elif defect_base_name == 'ravelling':
        v = pd.to_numeric(pd.Series([metric_value]), errors='coerce').iloc[0]
        return 1 if pd.notna(v) and v >= IRC_RAVELLING_THRESHOLD else 0
    return 0

# ── D1/D3 Lookups ───────────────────────────────────────────────────────────
def prepare_lrms_lookup(pdf):
    if pdf.empty: return pd.DataFrame(columns=["chainage", "nh_number", "section_code", "direction"])
    cols = list(pdf.columns)
    chain_c = pick_col(cols, "Chainage")
    nh_c = pick_col(cols, "NHNumber", "New NH number", "New NH number NHNumber")
    section_c = pick_col(cols, "SectionCode", "Section")
    direction_c = pick_col(cols, "Direction")
    rows = []
    for _, row in pdf.iterrows():
        ch_val = as_chainage_m(row[chain_c]) if chain_c else None
        if ch_val is None: continue
        rows.append({"chainage": ch_val, "nh_number": standardize_nh_number(row[nh_c]) if nh_c else None,
                     "section_code": as_text(row[section_c]) if section_c else None,
                     "direction": normalize_direction(row[direction_c]) if direction_c else None})
    return pd.DataFrame(rows).sort_values("chainage").reset_index(drop=True) if rows else pd.DataFrame(columns=["chainage", "nh_number", "section_code", "direction"])

def prepare_interval_lookup(pdf, start_cands, end_cands, field_cands, float_fields=None):
    float_fields = set(float_fields or [])
    if pdf.empty: return pd.DataFrame(columns=["start_chainage", "end_chainage", *field_cands.keys()])
    cols = list(pdf.columns)
    start_c = pick_col(cols, *start_cands)
    end_c = pick_col(cols, *end_cands)
    resolved = {name: pick_col(cols, *c) for name, c in field_cands.items()}
    rows = []
    for _, row in pdf.iterrows():
        s = as_chainage_m(row[start_c]) if start_c else None
        e = as_chainage_m(row[end_c]) if end_c else None
        if s is None and e is None: continue
        if s is None: s = e
        if e is None: e = s
        if e < s: s, e = e, s
        record = {"start_chainage": s, "end_chainage": e}
        for fn, cn in resolved.items():
            record[fn] = as_float(row[cn]) if cn and fn in float_fields else (as_text(row[cn]) if cn else None)
        if 'nh_number' in record:
            record['nh_number'] = standardize_nh_number(record['nh_number'])
        if 'direction' in record:
            record['direction'] = normalize_direction(record['direction'])
        rows.append(record)
    return pd.DataFrame(rows)

def _apply_optional_filter(pdf, col_name, value):
    if pdf.empty or col_name not in pdf.columns or value is None: return pdf
    normalized = pdf[col_name].fillna("").astype(str).str.strip().str.lower()
    filtered = pdf[normalized == str(value).strip().lower()]
    return filtered if not filtered.empty else pdf

_DIR_SPLIT_CACHE = {}
def _restrict_direction(pdf, direction):
    """Only the D1 rows of the lane's own direction are used - the two sides are never mixed.
    Falls back to all rows only when D1 has no row at all for that side (e.g. CarriagewayType has no Direction).
    Result is cached per lookup table + direction (called once per output row)."""
    if pdf.empty or direction is None or 'direction' not in pdf.columns: return pdf
    key = (id(pdf), len(pdf), str(direction).strip().lower())
    hit = _DIR_SPLIT_CACHE.get(key)
    if hit is not None: return hit
    same = pdf[pdf['direction'].fillna('').astype(str).str.strip().str.lower() == key[2]]
    out = same if not same.empty else pdf
    _DIR_SPLIT_CACHE[key] = out
    return out

def lookup_interval_record(pdf, ch_val, lane=None, direction=None):
    if pdf.empty or ch_val is None: return {}
    pdf = _restrict_direction(pdf, direction)       # filter by direction FIRST, then match chainage
    # Half-open interval [start, end) so a boundary chainage belongs to the NEXT interval;
    # fall back to inclusive end only for the very last chainage of the road
    matches = pdf[(pdf["start_chainage"] <= ch_val) & (pdf["end_chainage"] > ch_val)]
    if matches.empty:
        matches = pdf[(pdf["start_chainage"] <= ch_val) & (pdf["end_chainage"] >= ch_val)]
    if matches.empty:
        # Chainage outside all intervals -> take the NEAREST interval edge (same direction if possible);
        # do not let the width-sort below pick an unrelated narrow interval elsewhere on the road
        probe = pdf.copy()
        probe["_distance"] = np.minimum((probe["start_chainage"] - ch_val).abs(), (probe["end_chainage"] - ch_val).abs())
        return probe.sort_values("_distance").iloc[0].drop(labels=["_distance"]).to_dict() if not probe.empty else {}
    matches = _apply_optional_filter(matches, "lane", lane)
    # Prefer narrower (more specific) intervals when multiple match
    if len(matches) > 1 and 'start_chainage' in matches.columns:
        widths = (matches['end_chainage'] - matches['start_chainage']).values
        matches = matches.iloc[widths.argsort()]
    return matches.iloc[0].to_dict() if not matches.empty else {}

def lookup_lrms_record(ch_val, direction=None):
    if LRMS_LOOKUP.empty or ch_val is None: return {}
    probe = _restrict_direction(LRMS_LOOKUP, direction).copy()
    probe["_distance"] = (probe["chainage"] - ch_val).abs()
    return probe.sort_values("_distance").iloc[0].to_dict() if not probe.empty else {}

# Load D1 lookups
df_lrms_master, _ = load_matching_parquets(D1_BASE, ["LRPMaster"])
df_pavement_master, _ = load_matching_parquets(D1_BASE, ["PavementType"])
df_carriageway_master, _ = load_matching_parquets(D1_BASE, ["CarriagewayType"])

_JUNK_VALUES = {'lcms issue', 'uc', 'na', 'n/a', ''}
def _clean_d1_lookup(pdf, value_col):
    if pdf.empty or value_col not in pdf.columns: return pdf
    mask = pdf[value_col].fillna('').astype(str).str.strip().str.lower().isin(_JUNK_VALUES)
    return pdf[~mask].copy()
df_pavement_master = _clean_d1_lookup(df_pavement_master, 'PavementType')
df_carriageway_master = _clean_d1_lookup(df_carriageway_master, 'CarriagewayType')

# Also load ServiceRoad.parquet — has granular pavement types (incl. Asphalt)
df_service_road, _sr_files = load_matching_parquets(D1_BASE, ["ServiceRoad"])
if not df_service_road.empty and 'PavementType' in df_service_road.columns:
    df_service_road = _clean_d1_lookup(df_service_road, 'PavementType')
    print(f"  ServiceRoad pavement: {len(df_service_road)} rows — {df_service_road['PavementType'].value_counts().to_dict()}")
else:
    df_service_road = pd.DataFrame()

LRMS_LOOKUP = prepare_lrms_lookup(df_lrms_master)

# Build PAVEMENT_LOOKUP: ServiceRoad entries first (more granular) + main PavementType
_pav_main = prepare_interval_lookup(df_pavement_master,
    ["New NH number StartChainage", "Start_Chainage"], ["New NH number EndChainage", "End_Chainage"],
    {"nh_number": ["New NH number NHNumber", "NHNumber"], "section_code": ["SectionCode"],
     "direction": ["Direction"], "pavement_type": ["PavementType", "Surface Type"]})
if not df_service_road.empty:
    _pav_service = prepare_interval_lookup(df_service_road,
        ["New NH number StartChainage", "Start_Chainage"], ["New NH number EndChainage", "End_Chainage"],
        {"nh_number": ["NHNumber", "New NH number NHNumber"], "section_code": ["SectionCode"],
         "direction": ["Direction"], "pavement_type": ["PavementType", "Surface Type"]})
    # Service-road pavement applies to the service road only, NOT to main-carriageway lanes (L1/L2/R1/R2)
    SERVICE_ROAD_PAVEMENT_LOOKUP = _pav_service
else:
    SERVICE_ROAD_PAVEMENT_LOOKUP = pd.DataFrame()
# Main-carriageway pavement type comes from D1 PavementType only
PAVEMENT_LOOKUP = _pav_main
print(f"  PAVEMENT_LOOKUP (D1 PavementType, main carriageway): {len(PAVEMENT_LOOKUP)} rows | "
      f"ServiceRoad kept separately: {len(SERVICE_ROAD_PAVEMENT_LOOKUP)} rows")
if not PAVEMENT_LOOKUP.empty:
    print(f"  Pavement types: {PAVEMENT_LOOKUP['pavement_type'].value_counts().to_dict()}")
CARRIAGEWAY_LOOKUP = prepare_interval_lookup(df_carriageway_master,
    ["New NH number StartChainage", "Start_Chainage"], ["New NH number EndChainage", "End_Chainage"],
    {"nh_number": ["New NH number", "NHNumber"], "section_code": ["SectionCode"],
     "direction": ["Direction"], "carriageway": ["CarriagewayType"]})

# D3 condition lookups
def build_condition_lookup(pdf):
    if pdf.empty or 'Start_Chainage' not in pdf.columns or 'Condition' not in pdf.columns:
        return pd.DataFrame(columns=['start_chainage', 'end_chainage', 'lane', 'condition'])
    lane_col = 'Lane' if 'Lane' in pdf.columns else ('LANE' if 'LANE' in pdf.columns else None)
    end_col = 'End_Chainage' if 'End_Chainage' in pdf.columns else 'Start_Chainage'
    out = pd.DataFrame({
        'start_chainage': pd.to_numeric(pdf['Start_Chainage'], errors='coerce'),
        'end_chainage': pd.to_numeric(pdf[end_col], errors='coerce'),
        'lane': pdf[lane_col] if lane_col else None,
        'condition': pdf['Condition'],
    }).dropna(subset=['start_chainage'])
    return out

D3_CONDITION = {
    'roughness': build_condition_lookup(df_roughness_raw),
    'rutting': build_condition_lookup(df_rutting_raw),
    'cracking': build_condition_lookup(df_cracking_raw),
    'ravelling': build_condition_lookup(df_ravelling_raw),
}

def lookup_d3_condition(defect_name, start_ch, end_ch=None, lane=None):
    df = D3_CONDITION.get(defect_name, pd.DataFrame())
    if df.empty or start_ch is None: return None
    probe = float(start_ch)
    matches = df[(df['start_chainage'] <= probe) & (df['end_chainage'] >= probe)]
    if not matches.empty and lane and 'lane' in matches.columns:
        lane_matches = matches[matches['lane'].map(normalize_lane) == normalize_lane(lane)]
        if not lane_matches.empty: matches = lane_matches
    return str(matches.iloc[0]['condition']) if not matches.empty and pd.notna(matches.iloc[0]['condition']) else None

def get_road_context(start_ch, end_ch=None, lane=None, direction=None):
    # Direction always follows the lane (L* = Increasing, R* = Decreasing); the passed direction is only a
    # fallback for rows without a recognisable lane. All D1 lookups below use that direction only.
    direction = first_non_null(lane_to_direction(lane), normalize_direction(direction))
    probe_ch = first_non_null(start_ch, end_ch)
    lrms = lookup_lrms_record(probe_ch, direction=direction)
    pav = lookup_interval_record(PAVEMENT_LOOKUP, probe_ch, direction=direction)
    cw = lookup_interval_record(CARRIAGEWAY_LOOKUP, probe_ch, direction=direction)
    return {
        "nh_number": first_non_null(lrms.get("nh_number"), pav.get("nh_number")),
        "section_code": first_non_null(lrms.get("section_code"), pav.get("section_code")),
        # lane-derived direction; D1 direction is used only when the row has no recognisable lane
        "direction": first_non_null(direction, lrms.get("direction"), pav.get("direction")),
        "pavement_type": first_non_null(pav.get("pavement_type")),
        "carriageway": first_non_null(cw.get("carriageway")),
    }

NOW_TS = pd.Timestamp.now()
RUN_TS = pd.Timestamp(RUN_DATE)
upc_slash = UPC.replace("_", "/")
source_system = "NSV"

print(f"Workbook: {len(TABLE_SCHEMAS)} tables loaded")
for tbl in sorted(TABLE_SCHEMAS): print(f"  {tbl:40s} {len(TABLE_COLUMNS[tbl])} columns")

# COMMAND ----------

# DBTITLE 1,Cracking Correction: D2-Calibrated Per-1m Cap (Backup Parquet Only)
# ═══════════════════════════════════════════════════════════════════════════════
# CRACKING CORRECTION — D2-calibrated per-1m cap (backup parquet only, no external Excel)
#   The backup parquet sums crack area per 10m WITHOUT the per-1m cap (3.5 m² per 1m bin);
#   D2 applies the cap before aggregating. Calibrate the parquet to D2 at the finest D2 level:
#       D2 IRC @100m  >  D2 CA @100m  >  D2 CA @1000m
#   factor(bin) = min(D2_area / parquet_area, 1.0)  -> applied to every 10m row of the lane in that bin
#   Cracking % (10m) = area / 35 x 100
# ═══════════════════════════════════════════════════════════════════════════════
LANE_AREA_10M, LANE_AREA_100M, LANE_AREA_1000M = 35.0, 350.0, 3500.0

def _col(cols, *must, exclude=()):
    """First column whose lower-case name contains all `must` words and none of `exclude`."""
    return next((c for c in cols if all(m in c.lower() for m in must)
                 and not any(x in c.lower() for x in exclude)), None)

def _load_d2(name_filter):
    """First D2 parquet (sorted) matching name_filter that has a start-chainage column."""
    files = sorted(f for f in os.listdir(D2_BASE) if f.endswith('.parquet') and name_filter(f)) if os.path.exists(D2_BASE) else []
    for f in files:
        df = pd.read_parquet(os.path.join(D2_BASE, f))
        ch = _col(df.columns, 'start', 'chainage')
        if ch:
            df[ch] = pd.to_numeric(df[ch], errors='coerce')
            return df.dropna(subset=[ch]), ch, f
    return None, None, None

def _row_interval_m(df, ch):
    """'100m' or '1000m' from column tags, else from the median row length."""
    tags = ' '.join(df.columns).lower()
    if '1000m' in tags: return '1000m'
    if '100m' in tags: return '100m'
    end = _col(df.columns, 'end', 'chainage')
    if end is None: return '1000m'
    med = (pd.to_numeric(df[end], errors='coerce') - df[ch]).dropna().median()
    return '100m' if med <= 200 else '1000m'

# ── 1. D2 sources ──────────────────────────────────────────────────────────────────────────
_d2_ca, _d2ch_ca, _ca_file = _load_d2(lambda f: 'CA' in f)
assert _d2_ca is not None, f"No D2 CA file with chainage found in {D2_BASE}"
_ca_interval = _row_interval_m(_d2_ca, _d2ch_ca)

_d2_irc, _d2ch_irc, _irc_file = _load_d2(lambda f: 'IRC' in f)
_irc_interval = None
if _d2_irc is not None:
    _irc_interval = '100m' if any('crack' in c.lower() and '100m' in c.lower() for c in _d2_irc.columns) \
                    else _row_interval_m(_d2_irc, _d2ch_irc)
print(f"D2 CA : {len(_d2_ca)} rows, interval={_ca_interval} | {_ca_file}")
print(f"D2 IRC: {('%d rows, interval=%s | %s' % (len(_d2_irc), _irc_interval, _irc_file)) if _d2_irc is not None else 'not found'}")

# ── 2. Calibration source (finest wins) ─────────────────────────────────────────────────────────────────
if _d2_irc is not None and _irc_interval == '100m':
    _calib_src, _calib_m, _calib_df, _calib_ch = 'IRC', 100, _d2_irc.copy(), _d2ch_irc
else:
    _calib_src, _calib_m, _calib_df, _calib_ch = 'CA', (100 if _ca_interval == '100m' else 1000), _d2_ca.copy(), _d2ch_ca
_calib_df['_cbin'] = (_calib_df[_calib_ch] // _calib_m * _calib_m).astype(int)
_calib_lane_area = _calib_m * 3.5

_all_lanes = sorted(df_cracking_raw['Lane'].dropna().unique())
_calib_cols = {ln: _col(_calib_df.columns, ln.lower(), 'crack', exclude=('limit',)) for ln in _all_lanes}
print(f"→ Calibrating at {_calib_m}m using D2 {_calib_src} | columns: {_calib_cols}")

# ── 3. Apply factor per calibration bin (vectorised) ─────────────────────────────────────────────────────
_before = df_cracking_raw.groupby('Lane')['Area_sq_m'].sum()
_sch_all = df_cracking_raw[['Start_Chainage', 'End_Chainage']].min(axis=1)   # normalise reversed R-lane chainages
_fix_count = 0
for lane in _all_lanes:
    col = _calib_cols.get(lane)
    if col is None:
        print(f"  {lane}: no D2 calibration column — unchanged"); continue
    m = df_cracking_raw['Lane'] == lane
    d2_area = (pd.to_numeric(_calib_df[col], errors='coerce').fillna(0)
               .groupby(_calib_df['_cbin']).mean() / 100 * _calib_lane_area)
    cbin = (_sch_all[m] // _calib_m * _calib_m).astype(int)
    area = pd.to_numeric(df_cracking_raw.loc[m, 'Area_sq_m'], errors='coerce')
    pq_total = area.groupby(cbin).transform('sum')
    d2_val = cbin.map(d2_area)
    factor = np.where(d2_val.notna() & (pq_total > 0), np.minimum(d2_val / pq_total, 1.0), 1.0)
    new_area = area * factor
    lane_fix = int(((area - new_area).abs() > 0.001).sum())
    df_cracking_raw.loc[m, 'Area_sq_m'] = new_area
    df_cracking_raw.loc[m, 'Cracking'] = new_area / LANE_AREA_10M * 100
    _fix_count += lane_fix
    print(f"  {lane}: {lane_fix:,} rows corrected | D2 bins={len(d2_area)}")

df_cracking_raw['Condition'] = pd.to_numeric(df_cracking_raw['Cracking'], errors='coerce').fillna(0).apply(crack_condition_from_pct)
_after = df_cracking_raw.groupby('Lane')['Area_sq_m'].sum()
print(pd.DataFrame({'area_before_sqm': _before.round(2), 'area_after_sqm': _after.round(2)}))
print(f"Total rows changed: {_fix_count:,} | calibration: D2 {_calib_src} @ {_calib_m}m")

# ── 4. Quick check vs D2 at 100m and 1000m ────────────────────────────────────────────────────────────────
_chk = []
for _vl, _va in [(100, LANE_AREA_100M), (1000, LANE_AREA_1000M)]:
    if _vl == 100:
        if _d2_irc is not None and _irc_interval == '100m': _vdf, _vch, _vsrc = _d2_irc, _d2ch_irc, 'IRC'
        elif _ca_interval == '100m':                         _vdf, _vch, _vsrc = _d2_ca, _d2ch_ca, 'CA'
        else: continue
    else:
        _vdf, _vch, _vsrc = _d2_ca, _d2ch_ca, 'CA'
    _vbin = (_vdf[_vch] // _vl * _vl).astype(int)
    for lane in _all_lanes:
        _vcol = _col(_vdf.columns, lane.lower(), 'crack', exclude=('limit',))
        if not _vcol: continue
        d2_pct = pd.to_numeric(_vdf[_vcol], errors='coerce').groupby(_vbin).mean()
        m = df_cracking_raw['Lane'] == lane
        db_pct = df_cracking_raw.loc[m, 'Area_sq_m'].groupby((_sch_all[m] // _vl * _vl).astype(int)).sum() / _va * 100
        diff = (db_pct - d2_pct).abs().dropna()
        _chk.append({'level_m': _vl, 'd2': _vsrc, 'lane': lane, 'bins': len(diff), 'mean_abs_diff_pct': round(diff.mean(), 4),
                     'exact_lt_0.05': int((diff < 0.05).sum()), 'close_lt_0.5': int((diff < 0.5).sum()), 'max_diff_pct': round(diff.max(), 3)})
display(pd.DataFrame(_chk))

# COMMAND ----------

# DBTITLE 1,Map All Defects to Workbook Schema and Build OUTPUT_DATAFRAMES
# ═══════════════════════════════════════════════════════════════════════════════
# MAP ALL DEFECTS TO WORKBOOK SCHEMA → OUTPUT_DATAFRAMES
# Reuses the mapping logic from the original Silver Layer pipeline
# ═══════════════════════════════════════════════════════════════════════════════

# Run the mapping cell from the ORIGINAL notebook to generate OUTPUT_DATAFRAMES.
# This cell is the exact mapping logic from Cell 13 of Clone Update_5.
# We execute it directly from there to avoid duplicating 78K chars of mapping code.

# Instead, we'll import the mapping by running that cell's logic.
# The original notebook at ID 2109964690278703, cell 13 contains the full mapping.
# We need to replicate the core mapping here.

# Since the mapping cell is extremely large (78K+ chars), we'll use a streamlined
# approach: run the original notebook's cell 13 and use its OUTPUT_DATAFRAMES.

# Actually, let's build the OUTPUT_DATAFRAMES by calling the same functions.
# The original cell 13 builds: pothole_output_sdf, cracking SDF, and linear output sets.

# ── Pothole Mapping ────────────────────────────────────────────────────────
def build_pothole_pdf(df_raw):
    if df_raw.empty: return pd.DataFrame(columns=POINT_COLUMNS)
    records = []
    for idx, row in df_raw.iterrows():
        start_ch = as_int(row.get('Start_Chainage'))
        end_ch = as_int(row.get('End_Chainage'))
        lane_val = as_text(row.get('Lane'))
        road_ctx = get_road_context(start_ch, end_ch, lane=lane_val)
        defect_id_val = row.get('Defect_ID', f"D/{SURVEY_DATE_STR}/N/{UPC}/{start_ch or 0}/Pothole/{idx+1:04d}")
        record_id_val = f"pothole_{idx+1:06d}"
        _parent_m = DEFECT_INTERVALS['pothole']['parent_m']
        parent_start = (start_ch // _parent_m) * _parent_m if start_ch is not None else None
        parent_end = parent_start + _parent_m if parent_start is not None else None
        survey_ts = pd.to_datetime(row.get('Survey_Date'), errors='coerce') if 'Survey_Date' in row.index else pd.NaT
        survey_date = survey_ts.date() if pd.notnull(survey_ts) else None

        rec = {col: None for col in POINT_COLUMNS}
        rec.update({
            "id": f"{UPC}_master_pothole_{idx+1:06d}",
            "collection_name": "master_pothole", "defect_base": "pothole", "level": "child_10m",
            "defect_id": defect_id_val, "display_defect_id": defect_id_val,
            "source_defect_id": record_id_val, "record_id": record_id_val,
            "parent_id": f"{UPC}_{lane_val}_{parent_start}_{parent_end}" if lane_val and parent_start is not None else None,
            "upc": upc_slash, "upc_code": UPC,
            "nh_number": first_non_null(road_ctx.get("nh_number")),
            "section_code": first_non_null(road_ctx.get("section_code")),
            "zone": ZONE, "phase": PHASE, "cycle": int(CYCLE),
            "source_system": source_system, "sources": [source_system],
            "defect_type": "Pothole", "type_of_defect": as_text(row.get('Type_of_Defect', 'Pothole')),
            "category": "POINT", "severity": as_text(row.get('Severity')),
            "pavement_type": first_non_null(road_ctx.get("pavement_type")),
            "pci": TRACKER_CTX.get('pci'),
            "lane": lane_val, "breach_flag": 1,  # Potholes always breach
            "metric_name": "area_sq_m", "metric_value": as_float(row.get('Area_sq_m')),
            "threshold_name": get_threshold_name("pothole"), "threshold_value": 0.0,
            "total_children": 1, "length_m": max((end_ch or 0) - (start_ch or 0), 0) if start_ch and end_ch else None,
            "area_sqm": as_float(row.get('Area_sq_m')),
            "start_chainage": start_ch, "end_chainage": end_ch, "defect_chainage": start_ch,
            "start_lat": as_float(row.get('Latitude')), "start_long": as_float(row.get('Longitude')),
            "end_lat": as_float(row.get('Latitude')), "end_long": as_float(row.get('Longitude')),
            "defect_lat": as_float(row.get('Latitude')), "defect_long": as_float(row.get('Longitude')),
            "datetime": survey_ts if pd.notnull(survey_ts) else (pd.Timestamp(TRACKER_CTX['survey_start_date']) if TRACKER_CTX['survey_start_date'] else None),
            "observed_at": survey_ts if pd.notnull(survey_ts) else (pd.Timestamp(TRACKER_CTX['survey_start_date']) if TRACKER_CTX['survey_start_date'] else None),
            "survey_start_date": TRACKER_CTX['survey_start_date'] or survey_date,
            "survey_end_date": TRACKER_CTX['survey_end_date'] or survey_date,
            "image_name": as_text(row.get('Image_Name')),
            "image_urls": [as_text(row.get('Image_Name'))] if as_text(row.get('Image_Name')) else None,
            "is_duplicate": False,
            "nsv": json.dumps({"area_sqm": as_float(row.get('Area_sq_m')), "max_depth_mm": as_float(row.get('Max_Depth_mm'))}),
            "project_details": json.dumps({"project_name": UPC, "zone": ZONE, "phase": PHASE, "cycle": CYCLE}),
            "created_at": NOW_TS, "updated_at": NOW_TS,
            "side": as_text(row.get('Side')),
            "carriageway": first_non_null(road_ctx.get("carriageway")),
            "direction": first_non_null(lane_to_direction(lane_val), road_ctx.get("direction")),
            "parent_start_chainage": parent_start, "parent_end_chainage": parent_end,
            "reporting_interval_m": get_reporting_interval("pothole"), "segment_count": 1,
            "rating_ca": as_text(row.get('Severity')) if TRACKER_CTX['defect_standards']['pothole']['standard'] == 'CA' else None,
            "rating_irc": as_text(row.get('Severity')) if TRACKER_CTX['defect_standards']['pothole']['standard'] == 'IRC' else None,
            "defect_type_filter": "Pothole", "has_standard_defect": True, "standard_flags": "{}",
            "measures": json.dumps({"area_sqm": as_float(row.get('Area_sq_m'))}),
            "affected_length": as_float(row.get('Area_sq_m')) or 0.0,
            "total_length": as_float(row.get('Area_sq_m')) or 0.0,
            "affected_length_ca": as_float(row.get('Area_sq_m')) or 0.0,
            "affected_length_irc": as_float(row.get('Area_sq_m')) or 0.0,
            "run_date": RUN_DATE,
            "detected_on": TRACKER_CTX['survey_end_date'] or survey_date,
        })
        records.append(rec)
    pdf = pd.DataFrame(records)
    for col in POINT_COLUMNS:
        if col not in pdf.columns: pdf[col] = None
    return pdf[POINT_COLUMNS]

# ── Cracking Mapping ───────────────────────────────────────────────────────
def build_cracking_pdf(df_raw, threshold_name_ovr=None, threshold_value_ovr=None, breach_mode=None):
    """breach_mode: None=auto(CA), 'ca'=CA threshold, 'irc'=IRC threshold"""
    if df_raw.empty: return pd.DataFrame(columns=CRACKING_COLUMNS)
    _threshold_name = threshold_name_ovr or get_threshold_name("cracking")
    _threshold_value = threshold_value_ovr if threshold_value_ovr is not None else CA_CRACK_PCT
    cols = list(df_raw.columns)
    records = []
    for idx, row in df_raw.iterrows():
        start_ch = as_int(row.get('Start_Chainage'))
        end_ch = as_int(row.get('End_Chainage'))
        # Normalize reversed R-lane chainages (Start > End in raw data)
        if start_ch is not None and end_ch is not None and end_ch < start_ch:
            start_ch, end_ch = end_ch, start_ch
        lane_val = as_text(row.get('Lane'))
        road_ctx = get_road_context(start_ch, end_ch, lane=lane_val)
        crack_val = as_float(row.get('Cracking'))
        condition_val = as_text(row.get('Condition'))
        _parent_m = DEFECT_INTERVALS['cracking']['parent_m']
        parent_start = (start_ch // _parent_m) * _parent_m if start_ch is not None else None
        parent_end = parent_start + _parent_m if parent_start is not None else None
        defect_id_val = f"D/{SURVEY_DATE_STR}/N/{UPC}/{start_ch or 0}/Cracking/{idx+1:04d}"
        record_id_val = f"cracking_{idx+1:06d}"
        if breach_mode == 'irc':
            _bf = 1 if crack_val is not None and crack_val >= IRC_CRACK_PCT else 0
        else:
            _bf = 1 if crack_val is not None and crack_val > CA_CRACK_PCT else 0  # CA: Good if <= CA value, Poor (breach) if > CA value
        seg_len = abs((end_ch or 0) - (start_ch or 0)) if start_ch and end_ch else 0

        rec = {col_name: None for col_name in CRACKING_COLUMNS}
        rec.update({
            "id": f"{UPC}_master_cracking_{idx+1:06d}",
            "collection_name": "master_cracking", "defect_base": "cracking", "level": "child_10m",
            "defect_id": defect_id_val if _bf == 1 else None, "display_defect_id": defect_id_val if _bf == 1 else None,
            "source_defect_id": record_id_val, "record_id": record_id_val,
            "parent_id": f"{UPC}_{lane_val}_{parent_start}_{parent_end}" if lane_val and parent_start is not None else None,
            "upc": upc_slash, "upc_code": UPC,
            "nh_number": first_non_null(road_ctx.get("nh_number")),
            "section_code": first_non_null(road_ctx.get("section_code")),
            "zone": ZONE, "phase": PHASE, "cycle": int(CYCLE),
            "source_system": source_system, "sources": [source_system],
            "defect_type": "Cracking", "type_of_defect": "Cracking", "category": "LENGTH",
            "severity": condition_val,
            "pavement_type": first_non_null(road_ctx.get("pavement_type")),
            "pci": TRACKER_CTX.get('pci'),
            "lane": lane_val, "breach_flag": _bf,
            "metric_name": "cracking_pct", "metric_value": crack_val,
            "threshold_name": _threshold_name, "threshold_value": _threshold_value,
            "total_children": 1, "length_m": float(seg_len),
            "area_sqm": as_float(row.get('Area_sq_m')),
            "start_chainage": start_ch, "end_chainage": end_ch,
            "defect_chainage": (start_ch + end_ch) / 2 if start_ch and end_ch else start_ch,
            "start_lat": as_float(row.get('Start_Lat')), "start_long": as_float(row.get('Start_Lon')),
            "end_lat": as_float(row.get('End_Lat')), "end_long": as_float(row.get('End_Lon')),
            "defect_lat": as_float(row.get('Start_Lat')), "defect_long": as_float(row.get('Start_Lon')),
            "datetime": RUN_TS, "observed_at": RUN_TS,
            "survey_start_date": TRACKER_CTX['survey_start_date'],
            "survey_end_date": TRACKER_CTX['survey_end_date'],
            "image_name": as_text(row.get('Image_Name')),
            "image_urls": [as_text(row.get('Image_Name'))] if as_text(row.get('Image_Name')) else None,
            "is_duplicate": False,
            "nsv": json.dumps({"condition": condition_val, "cracking": crack_val}),
            "project_details": json.dumps({"project_name": UPC, "zone": ZONE, "phase": PHASE, "cycle": CYCLE}),
            "created_at": NOW_TS, "updated_at": NOW_TS,
            "carriageway": first_non_null(road_ctx.get("carriageway")),
            "direction": first_non_null(lane_to_direction(lane_val), road_ctx.get("direction")),
            "parent_start_chainage": parent_start, "parent_end_chainage": parent_end,
            "reporting_interval_m": get_reporting_interval("cracking"), "segment_count": 1,
            "cracking_pct": crack_val,
            "rating_ca": condition_val if TRACKER_CTX['defect_standards']['cracking']['standard'] == 'CA' else None,
            "rating_irc": condition_val if TRACKER_CTX['defect_standards']['cracking']['standard'] == 'IRC' else None,
            "defect_type_filter": "Cracking", "has_standard_defect": True, "standard_flags": "{}",
            "measures": json.dumps({
                "cracking_pct": crack_val,
                "crack_area_sqm": as_float(row.get('Area_sq_m')),
                "lane_area_sqm": 35.0,
                "crack_count": as_float(row.get('crack_count')),
                "avg_corrected_width_mm": as_float(row.get('avg_corrected_width_mm')),
                "avg_width_mm": as_float(row.get('avg_width_mm')),
                "calculation": "Crack area = length x corrected width (Alligator/Multiple=3500mm, width>=20mm -> 500mm); "
                               "sum per 1m capped at 3.5 sqm; Cracking % = sum(10m area) / (3.5 x 10) x 100",
            }),
            "affected_length": round(float(seg_len), 4) if _bf == 1 else 0.0,
            "total_length": float(seg_len),
            "affected_length_ca": round(float(seg_len), 4) if _bf == 1 else 0.0,
            "affected_length_irc": 0.0,
            "run_date": RUN_DATE, "detected_on": TRACKER_CTX['survey_end_date'],
        })
        records.append(rec)
    pdf = pd.DataFrame(records)
    for col in CRACKING_COLUMNS:
        if col not in pdf.columns: pdf[col] = None
    return pdf[CRACKING_COLUMNS]

# ── Linear Defect Mapping (roughness/rutting/ravelling) ────────────────────
def normalize_linear_source(pdf, defect_base_name):
    if pdf.empty: return pd.DataFrame()
    cols = list(pdf.columns)
    lane_c = pick_col(cols, "Lane", "LaneNumber")
    normalized_rows = []
    for idx, row in pdf.iterrows():
        start_ch = as_int(row.get('Start_Chainage'))
        end_ch = as_int(row.get('End_Chainage'))
        if start_ch is None and end_ch is None: continue
        if start_ch is None: start_ch = end_ch
        if end_ch is None: end_ch = start_ch
        if end_ch < start_ch: start_ch, end_ch = end_ch, start_ch

        iri_val = as_float(row.get('IRI'))
        bi_val = as_float(row.get('BI m/km'))
        rut_val = as_float(row.get('Rutting'))
        rav_val = as_float(row.get('Ravelling'))
        metric_value = {'roughness': first_non_null(iri_val, bi_val), 'rutting': rut_val, 'ravelling': rav_val}[defect_base_name]
        severity_val = as_text(row.get('Condition'))
        lane_val = normalize_lane(row[lane_c]) if lane_c else None
        seg_len = end_ch - start_ch
        _mv = metric_value
        _bf = compute_breach_flag(defect_base_name, _mv, roughness_bi=bi_val, iri_value=iri_val)
        _bf_ca = compute_breach_flag_ca(defect_base_name, _mv, roughness_bi=bi_val)
        _bf_irc = compute_breach_flag_irc(defect_base_name, _mv, iri_value=iri_val, roughness_bi=bi_val)

        normalized_rows.append({
            "row_id": idx + 1, "lane": lane_val,
            "nh_number": standardize_nh_number(row.get('NH_Number')),
            # direction from the lane first (backup 'Direction' column only when the lane is not L*/R*)
            "direction": first_non_null(lane_to_direction(lane_val), normalize_direction(row.get('Direction'))),
            "side": as_text(row.get('Side')) if 'Side' in cols else None,
            "survey_date": pd.to_datetime(row.get('SurveyDate'), errors='coerce') if 'SurveyDate' in cols else pd.NaT,
            "start_chainage": start_ch, "end_chainage": end_ch, "length_m": seg_len,
            "start_lat": as_float(row.get('Start_Lat')), "start_long": as_float(row.get('Start_Lon')),
            "end_lat": as_float(row.get('End_Lat')), "end_long": as_float(row.get('End_Lon')),
            "metric_value": metric_value, "severity": severity_val,
            "metric_source": defect_base_name,
            "roughness_bi": bi_val, "iri_value": iri_val, "rutting_avg": rut_val, "ravelling_area": rav_val,
            "lwp_iri": as_float(row.get('LwpIri')), "rwp_iri": as_float(row.get('RwpIri')),
            # Rutting: left / right wheel-path rut depth (mm) from backup parquet
            "rut_left": as_float(row.get('Rutting Left')), "rut_right": as_float(row.get('Rutting Right')),
            # Ravelling: raw area (sqm) so 100m/1km % = sum(area) / (3.5 x length) x 100 (Excel logic)
            "rav_area_sqm": as_float(row.get('Ravelling_Area_sq_m')),
            "rating_ca": severity_val if TRACKER_CTX['defect_standards'].get(defect_base_name, {}).get('standard') == 'CA' else None,
            "rating_irc": severity_val if TRACKER_CTX['defect_standards'].get(defect_base_name, {}).get('standard') == 'IRC' else None,
            "affected_length": round(float(seg_len), 4) if _bf == 1 else 0.0,
            "total_length": float(seg_len),
            "affected_length_ca": round(float(seg_len), 4) if _bf_ca == 1 else 0.0,
            "affected_length_irc": round(float(seg_len), 4) if _bf_irc == 1 else 0.0,
        })
    return pd.DataFrame(normalized_rows)

def build_linear_records(pdf, defect_base_name, level_name, collection_name,
                         threshold_name_override=None, threshold_value_override=None,
                         breach_flag_mode=None):
    """breach_flag_mode: None=auto, 'ca'=use CA only, 'irc'=use IRC only"""
    metric_name_val = {'roughness': 'roughness_bi', 'rutting': 'rutting_avg', 'ravelling': 'ravelling_area'}[defect_base_name]
    threshold_val = threshold_value_override if threshold_value_override is not None else {'roughness': CA_BI_THRESHOLD, 'rutting': CA_RUT_THRESHOLD, 'ravelling': float(RAVELLING_RATING_THRESHOLD)}[defect_base_name]
    threshold_name_val = threshold_name_override or get_threshold_name(defect_base_name)
    target_columns = TABLE_COLUMNS[collection_name]
    records = []
    for seq, row in pdf.reset_index(drop=True).iterrows():
        start_ch = as_int(row['start_chainage'])
        end_ch = as_int(row['end_chainage'])
        lane_val = normalize_lane(row.get('lane'))
        # Direction from the lane first: the 100m / 1km groupby does not carry 'direction', so taking it
        # from the row left it empty and the D1 lookup then picked the nearest D1 record of EITHER side
        direction_val = first_non_null(lane_to_direction(lane_val), normalize_direction(row.get('direction')))
        road_ctx = get_road_context(start_ch, end_ch, lane=lane_val, direction=direction_val)
        _parent_m = DEFECT_INTERVALS.get(defect_base_name, {}).get('parent_m', PARENT_LENGTH_M)
        parent_start = (start_ch // _parent_m) * _parent_m if start_ch is not None else None
        parent_end = parent_start + _parent_m if parent_start is not None else None
        defect_id_val = f"D/{SURVEY_DATE_STR}/N/{UPC}/{start_ch or 0}/{defect_base_name.title()}/{seq+1:04d}"
        record_id_val = f"{defect_base_name}_{level_name}_{seq+1:06d}"
        _len_for_area = as_float(row.get('length_m')) or 0.0
        _rut_l, _rut_r = as_float(row.get('rut_left')), as_float(row.get('rut_right'))
        _rav_area = as_float(row.get('rav_area_sqm'))
        bi_val = as_float(row.get('roughness_bi'))
        iri_val = as_float(row.get('iri_value'))
        rut_val = as_float(row.get('rutting_avg'))
        rav_val = as_float(row.get('ravelling_area'))
        metric_val_resolved = first_non_null(bi_val, iri_val) if defect_base_name == 'roughness' else (rut_val if defect_base_name == 'rutting' else rav_val)
        severity_val = first_non_null(as_text(row.get('severity')), lookup_d3_condition(defect_base_name, start_ch, end_ch, lane_val))
        if breach_flag_mode == 'ca':
            _bf = compute_breach_flag_ca(defect_base_name, metric_val_resolved, roughness_bi=bi_val)
        elif breach_flag_mode == 'irc':
            _bf = compute_breach_flag_irc(defect_base_name, metric_val_resolved, iri_value=iri_val, roughness_bi=bi_val)
        else:
            _bf = compute_breach_flag(defect_base_name, metric_val_resolved, roughness_bi=bi_val, iri_value=iri_val)
        _bf_ca = compute_breach_flag_ca(defect_base_name, metric_val_resolved, roughness_bi=bi_val)
        _bf_irc = compute_breach_flag_irc(defect_base_name, metric_val_resolved, iri_value=iri_val, roughness_bi=bi_val)
        seg_len = as_float(row.get('length_m')) or 0.0
        survey_val = row.get('survey_date') if pd.notnull(row.get('survey_date')) else RUN_TS

        rec = {col_name: None for col_name in target_columns}
        rec.update({
            "id": f"{UPC}_{collection_name}_{seq+1:06d}",
            "collection_name": collection_name, "defect_base": defect_base_name, "level": level_name,
            "defect_id": defect_id_val if _bf == 1 else None, "display_defect_id": defect_id_val if _bf == 1 else None,
            "source_defect_id": record_id_val, "record_id": record_id_val,
            "parent_id": f"{UPC}_{lane_val}_{parent_start}_{parent_end}" if lane_val and parent_start is not None else None,
            "upc": upc_slash, "upc_code": UPC,
            "nh_number": standardize_nh_number(first_non_null(as_text(row.get('nh_number')), road_ctx.get('nh_number'))),
            "section_code": first_non_null(road_ctx.get('section_code')),
            "zone": ZONE, "phase": PHASE, "cycle": int(CYCLE),
            "source_system": source_system, "sources": [source_system],
            "defect_type": defect_base_name.title(), "type_of_defect": defect_base_name.title(),
            "category": "LENGTH", "severity": severity_val,
            "pavement_type": first_non_null(road_ctx.get('pavement_type')),
            "pci": TRACKER_CTX.get('pci'),
            "lane": lane_val, "breach_flag": _bf,
            "metric_name": metric_name_val, "metric_value": metric_val_resolved,
            "threshold_name": threshold_name_val, "threshold_value": threshold_val,
            "total_children": int(row.get('segment_count', 1) or 1),
            "length_m": seg_len, "start_chainage": start_ch, "end_chainage": end_ch,
            "defect_chainage": start_ch,
            "start_lat": as_float(row.get('start_lat')), "start_long": as_float(row.get('start_long')),
            "end_lat": as_float(row.get('end_lat')), "end_long": as_float(row.get('end_long')),
            "defect_lat": as_float(row.get('start_lat')), "defect_long": as_float(row.get('start_long')),
            "datetime": survey_val if pd.notnull(survey_val) else (pd.Timestamp(TRACKER_CTX['survey_start_date']) if TRACKER_CTX['survey_start_date'] else None),
            "observed_at": survey_val if pd.notnull(survey_val) else (pd.Timestamp(TRACKER_CTX['survey_start_date']) if TRACKER_CTX['survey_start_date'] else None),
            "survey_start_date": TRACKER_CTX['survey_start_date'],
            "survey_end_date": TRACKER_CTX['survey_end_date'],
            "is_duplicate": False,
            "nsv": json.dumps({"metric_source": defect_base_name}),
            "project_details": json.dumps({"project_name": UPC, "zone": ZONE, "phase": PHASE, "cycle": CYCLE}),
            "created_at": NOW_TS, "updated_at": NOW_TS,
            "carriageway": first_non_null(road_ctx.get('carriageway')),
            "direction": first_non_null(direction_val, road_ctx.get('direction')),
            "parent_start_chainage": parent_start, "parent_end_chainage": parent_end,
            "reporting_interval_m": get_reporting_interval(defect_base_name),
            "segment_count": int(row.get('segment_count', 1) or 1),
            "roughness_bi": bi_val if defect_base_name == 'roughness' else None,
            "iri_value": iri_val if defect_base_name == 'roughness' else None,
            "rutting_avg": rut_val if defect_base_name == 'rutting' else None,
            "ravelling_area": rav_val if defect_base_name == 'ravelling' else None,
            "iri_condition": lookup_d3_condition('roughness', start_ch, end_ch, lane_val) if defect_base_name == 'roughness' else None,
            "rating_ca": severity_val if TRACKER_CTX['defect_standards'].get(defect_base_name, {}).get('standard') == 'CA' else None,
            "rating_irc": severity_val if TRACKER_CTX['defect_standards'].get(defect_base_name, {}).get('standard') == 'IRC' else None,
            "defect_type_filter": defect_base_name.title(), "has_standard_defect": True, "standard_flags": "{}",
            "measures": json.dumps(
                {metric_name_val: metric_val_resolved,
                 'lwp_iri': as_float(row.get('lwp_iri')),
                 'rwp_iri': as_float(row.get('rwp_iri')),
                 'lane_iri': as_float(row.get('iri_value')),
                 'segments': int(row.get('segment_count', 1) or 1),
                 'calculation': ('Lane IRI = (LWP IRI + RWP IRI)/2; BI = 630 x IRI^1.12' if level_name == 'child_10m'
                                 else 'LWP and RWP IRI averaged over segment; Lane IRI = (avg LWP + avg RWP)/2; BI = 630 x IRI^1.12')}
                if defect_base_name == 'roughness'
                else {metric_name_val: metric_val_resolved,
                      'rut_left_mm': _rut_l, 'rut_right_mm': _rut_r,
                      'segments': int(row.get('segment_count', 1) or 1),
                      'calculation': ('Rut avg = (Left + Right)/2' if level_name == 'child_10m'
                                      else 'Left and Right rut averaged over segment; Rut avg = (avg Left + avg Right)/2')}
                if defect_base_name == 'rutting'
                else {metric_name_val: metric_val_resolved,
                      'ravelling_pct': metric_val_resolved,
                      'ravelling_area_sqm': _rav_area,
                      'lane_area_sqm': round(3.5 * _len_for_area, 4),
                      'segments': int(row.get('segment_count', 1) or 1),
                      'calculation': ('Ravelling % = area / (3.5 x 10) x 100' if level_name == 'child_10m'
                                      else 'Ravelling % = sum(area) / (3.5 x segment length) x 100')}
            ),
            "_id_candidate": defect_id_val,  # used by IRC clone; dropped before write
            "total_length": seg_len,
            "affected_length": round(seg_len, 4) if _bf == 1 else 0.0,
            "affected_length_ca": (round(seg_len, 4) if _bf == 1 else 0.0) if breach_flag_mode != 'irc' else 0.0,
            "affected_length_irc": (round(seg_len, 4) if _bf == 1 else 0.0) if breach_flag_mode != 'ca' else 0.0,
            "run_date": RUN_DATE, "detected_on": TRACKER_CTX['survey_end_date'],
        })
        records.append(rec)
    return records

def build_linear_output_set(raw_pdf, defect_base_name):
    norm_pdf = normalize_linear_source(raw_pdf, defect_base_name)
    levels = ["parent", "child_10m", "child_100m"]
    if norm_pdf.empty:
        return {f"master_{defect_base_name}_{lvl}": empty_with_schema(TABLE_SCHEMAS[f"master_{defect_base_name}_{lvl}"]) for lvl in levels}

    # Child 10m: subdivide into 10m grid
    child_10m_rows = []
    for _, row in norm_pdf.iterrows():
        s, e = row['start_chainage'], row['end_chainage']
        if s is None or e is None or e <= s:
            child_10m_rows.append(row.to_dict()); continue
        s_aligned = int(round(s / 10)) * 10
        e_aligned = int(round(e / 10)) * 10
        if e_aligned <= s_aligned: e_aligned = s_aligned + 10
        first_seg = True
        for seg_start in range(s_aligned, e_aligned, 10):
            sub = row.to_dict().copy()
            sub['start_chainage'] = int(round(s)) if first_seg else seg_start
            sub['end_chainage'] = seg_start + 10
            sub['length_m'] = sub['end_chainage'] - sub['start_chainage']
            child_10m_rows.append(sub)
            first_seg = False
    child_10m_pdf = pd.DataFrame(child_10m_rows)

    # Child 100m: aggregate by 100m bin
    _child_mid_m = DEFECT_INTERVALS.get(defect_base_name, {}).get('child_mid_m', 100)
    child_100m_pdf = norm_pdf.copy()
    child_100m_pdf['bin_start'] = (child_100m_pdf['start_chainage'] // _child_mid_m) * _child_mid_m
    child_100m_pdf['bin_end'] = child_100m_pdf['bin_start'] + _child_mid_m
    gk = ['lane', 'bin_start', 'bin_end']
    agg_dict = {'survey_date': 'first', 'start_lat': 'first', 'start_long': 'first',
                'end_lat': 'last', 'end_long': 'last', 'metric_value': 'mean',
                'severity': 'first', 'length_m': 'sum', 'row_id': 'count',
                'start_chainage': 'min', 'end_chainage': 'max'}
    for c in ['roughness_bi', 'iri_value', 'rutting_avg', 'ravelling_area', 'lwp_iri', 'rwp_iri', 'rut_left', 'rut_right']:
        if c in child_100m_pdf.columns: agg_dict[c] = 'mean'
    for c in ['affected_length', 'total_length', 'affected_length_ca', 'affected_length_irc']:
        if c in child_100m_pdf.columns: agg_dict[c] = 'sum'
    if 'rav_area_sqm' in child_100m_pdf.columns: agg_dict['rav_area_sqm'] = lambda s: s.sum(min_count=1)
    child_100m_pdf = child_100m_pdf.groupby(gk, dropna=False).agg(agg_dict).reset_index().rename(columns={'row_id': 'segment_count'})

    # Excel-aligned aggregation for rutting / ravelling (applied to 100m and 1km)
    def _apply_excel_logic(agg_pdf):
        if defect_base_name == 'rutting' and {'rut_left', 'rut_right'} <= set(agg_pdf.columns):
            _v = agg_pdf['rut_left'].notna() & agg_pdf['rut_right'].notna()
            _avg = (agg_pdf.loc[_v, 'rut_left'] + agg_pdf.loc[_v, 'rut_right']) / 2
            agg_pdf.loc[_v, 'rutting_avg'] = _avg
            agg_pdf.loc[_v, 'metric_value'] = _avg
        if defect_base_name == 'ravelling' and 'rav_area_sqm' in agg_pdf.columns:
            _lane_area = 3.5 * pd.to_numeric(agg_pdf['length_m'], errors='coerce')
            _v = agg_pdf['rav_area_sqm'].notna() & (_lane_area > 0)
            _pct = agg_pdf.loc[_v, 'rav_area_sqm'] / _lane_area[_v] * 100.0
            agg_pdf.loc[_v, 'ravelling_area'] = _pct
            agg_pdf.loc[_v, 'metric_value'] = _pct
        return agg_pdf
    child_100m_pdf = _apply_excel_logic(child_100m_pdf)

    # Roughness: recompute BI from averaged LWP+RWP IRI (not mean of per-10m BIs)
    if defect_base_name == 'roughness' and 'lwp_iri' in child_100m_pdf.columns and 'rwp_iri' in child_100m_pdf.columns:
        _valid = child_100m_pdf['lwp_iri'].notna() & child_100m_pdf['rwp_iri'].notna()
        _avg_iri = (child_100m_pdf.loc[_valid, 'lwp_iri'] + child_100m_pdf.loc[_valid, 'rwp_iri']) / 2
        child_100m_pdf.loc[_valid, 'iri_value'] = _avg_iri
        child_100m_pdf.loc[_valid, 'roughness_bi'] = 630.0 * (_avg_iri ** 1.12)
        child_100m_pdf.loc[_valid, 'metric_value'] = child_100m_pdf.loc[_valid, 'roughness_bi']

    # Parent: aggregate by 1000m bin
    _parent_m = DEFECT_INTERVALS.get(defect_base_name, {}).get('parent_m', PARENT_LENGTH_M)
    parent_pdf = norm_pdf.copy()
    parent_pdf['parent_start_chainage'] = (parent_pdf['start_chainage'] // _parent_m) * _parent_m
    parent_pdf['parent_end_chainage'] = parent_pdf['parent_start_chainage'] + _parent_m
    pk = ['lane', 'parent_start_chainage', 'parent_end_chainage']
    agg_dict2 = dict(agg_dict)
    parent_pdf = parent_pdf.groupby(pk, dropna=False).agg(agg_dict2).reset_index().rename(columns={'row_id': 'segment_count'})
    parent_pdf = _apply_excel_logic(parent_pdf)

    # Roughness: recompute BI from averaged LWP+RWP IRI at 1000m level
    if defect_base_name == 'roughness' and 'lwp_iri' in parent_pdf.columns and 'rwp_iri' in parent_pdf.columns:
        _valid = parent_pdf['lwp_iri'].notna() & parent_pdf['rwp_iri'].notna()
        _avg_iri = (parent_pdf.loc[_valid, 'lwp_iri'] + parent_pdf.loc[_valid, 'rwp_iri']) / 2
        parent_pdf.loc[_valid, 'iri_value'] = _avg_iri
        parent_pdf.loc[_valid, 'roughness_bi'] = 630.0 * (_avg_iri ** 1.12)
        parent_pdf.loc[_valid, 'metric_value'] = parent_pdf.loc[_valid, 'roughness_bi']
    # Keep actual min/max chainages from data — don't overwrite with bin boundaries

    # Dual threshold: build CA records once via loop, then CLONE for IRC (fast)
    _IRC_THRESHOLD_VAL = {
        'roughness': IRC_IRI_POOR, 'rutting': IRC_RUT_THRESHOLD, 'ravelling': float(IRC_RAVELLING_THRESHOLD),
    }
    _CA_THRESHOLD_VAL = {
        'roughness': CA_BI_THRESHOLD, 'rutting': CA_RUT_THRESHOLD, 'ravelling': float(RAVELLING_RATING_THRESHOLD),
    }

    output = {}
    for lvl, pdf_data in [('child_10m', child_10m_pdf), ('child_100m', child_100m_pdf), ('parent', parent_pdf)]:
        tbl_name = f"master_{defect_base_name}_{lvl}"
        tbl_cols = TABLE_COLUMNS[tbl_name]
        tbl_schema = TABLE_SCHEMAS[tbl_name]

        # 1) Build CA records (single expensive pass)
        ca_records = build_linear_records(
            pdf_data, defect_base_name, lvl, tbl_name,
            threshold_name_override="As per CA",
            threshold_value_override=_CA_THRESHOLD_VAL.get(defect_base_name, CA_BI_THRESHOLD),
            breach_flag_mode='ca'
        )

        # 2) Clone CA → IRC (fast: just update threshold/breach fields)
        irc_tv = _IRC_THRESHOLD_VAL.get(defect_base_name, IRC_IRI_POOR)
        # IRC rows only at child_10m; 100m and 1km parent are CA only
        irc_records = []
        for rec in (ca_records if lvl == 'child_10m' else []):
            irec = dict(rec)  # shallow copy
            irec['id'] = irec['id'].replace(f'{tbl_name}_', f'{tbl_name}_irc_')
            irec['record_id'] = irec.get('record_id', '') + '_irc'
            irec['source_defect_id'] = irec.get('source_defect_id', '') + '_irc'
            irec['threshold_name'] = 'As per IRC'
            irec['threshold_value'] = irc_tv
            # Recompute breach_flag for IRC standard
            mv = irec.get('metric_value')
            bi_v = irec.get('roughness_bi')
            iri_v = irec.get('iri_value')
            irec['breach_flag'] = compute_breach_flag_irc(defect_base_name, mv, iri_value=iri_v, roughness_bi=bi_v)
            # Update affected_length based on new breach
            seg = irec.get('length_m') or irec.get('total_length') or 0.0
            irec['affected_length'] = round(float(seg), 4) if irec['breach_flag'] == 1 else 0.0
            irec['affected_length_irc'] = round(float(seg), 4) if irec['breach_flag'] == 1 else 0.0
            irec['affected_length_ca'] = 0.0
            # Defect ID only for breach rows; create one when IRC breaches but CA did not
            if irec['breach_flag'] == 0:
                irec['defect_id'] = None
                irec['display_defect_id'] = None
            elif not irec.get('display_defect_id'):
                irec['defect_id'] = irec.get('_id_candidate')
                irec['display_defect_id'] = irec.get('_id_candidate')
            irc_records.append(irec)

        records = ca_records + irc_records
        pdf_out = pd.DataFrame(records) if records else pd.DataFrame(columns=tbl_cols)
        for col in tbl_cols:
            if col not in pdf_out.columns: pdf_out[col] = None
        output[tbl_name] = pandas_to_spark_with_schema(pdf_out[tbl_cols], tbl_schema)
    return output

# ── BUILD OUTPUT_DATAFRAMES ─────────────────────────────────────────────────
print("Building OUTPUT_DATAFRAMES...")

# Pothole
pothole_pdf = build_pothole_pdf(df_pothole_raw)
pothole_sdf = pandas_to_spark_with_schema(pothole_pdf, POINT_SCHEMA) if not pothole_pdf.empty else empty_with_schema(POINT_SCHEMA)

# Cracking (dual: build CA once, then clone for IRC — fast)
cracking_ca_pdf = build_cracking_pdf(df_cracking_raw, threshold_name_ovr="As per CA", threshold_value_ovr=CA_CRACK_PCT, breach_mode='ca')
cracking_irc_pdf = cracking_ca_pdf.copy()
if not cracking_irc_pdf.empty:
    cracking_irc_pdf['id'] = cracking_irc_pdf['id'].str.replace('master_cracking_', 'master_cracking_irc_')
    cracking_irc_pdf['record_id'] = cracking_irc_pdf['record_id'] + '_irc'
    cracking_irc_pdf['source_defect_id'] = cracking_irc_pdf['source_defect_id'] + '_irc'
    cracking_irc_pdf['threshold_name'] = 'As per IRC'
    cracking_irc_pdf['threshold_value'] = IRC_CRACK_PCT
    # Recompute breach_flag for IRC threshold
    cracking_irc_pdf['breach_flag'] = cracking_irc_pdf['metric_value'].apply(
        lambda v: 1 if v is not None and not pd.isna(v) and float(v) >= IRC_CRACK_PCT else 0)
    cracking_irc_pdf['affected_length'] = cracking_irc_pdf.apply(
        lambda r: round(float(r['length_m'] or 0), 4) if r['breach_flag'] == 1 else 0.0, axis=1)
    cracking_irc_pdf['affected_length_irc'] = cracking_irc_pdf['affected_length']
    cracking_irc_pdf['affected_length_ca'] = 0.0
    # Defect ID only for breach rows
    cracking_irc_pdf.loc[cracking_irc_pdf['breach_flag'] == 0, 'defect_id'] = None
    cracking_irc_pdf.loc[cracking_irc_pdf['breach_flag'] == 0, 'display_defect_id'] = None
    # IRC breach but CA not breached -> generate ID in the same format as CA rows
    _need_id = (cracking_irc_pdf['breach_flag'] == 1) & cracking_irc_pdf['display_defect_id'].isna()
    if _need_id.any():
        _seq = cracking_irc_pdf.loc[_need_id, 'record_id'].str.extract(r'cracking_(\d+)')[0].astype(int)
        _st = pd.to_numeric(cracking_irc_pdf.loc[_need_id, 'start_chainage'], errors='coerce').fillna(0).astype(int)
        _ids = [f"D/{SURVEY_DATE_STR}/N/{UPC}/{s}/Cracking/{q:04d}" for s, q in zip(_st, _seq)]
        cracking_irc_pdf.loc[_need_id, 'defect_id'] = _ids
        cracking_irc_pdf.loc[_need_id, 'display_defect_id'] = _ids
    print(f"  Cracking IRC rows given new defect IDs (IRC breach, CA not): {int(_need_id.sum()):,}")
cracking_pdf = pd.concat([cracking_ca_pdf, cracking_irc_pdf], ignore_index=True)
cracking_sdf = pandas_to_spark_with_schema(cracking_pdf, CRACKING_SCHEMA) if not cracking_pdf.empty else empty_with_schema(CRACKING_SCHEMA)

# ═══ Cracking 100m / 1km — Excel logic (CA rows only) ═══
#   Cracking % = SUM(1m-capped corrected crack area in interval) / (3.5 x length) x 100
#   Avg corrected width = AVERAGEIFS over individual cracks -> crack_count-weighted mean of 10m averages
def _crack_measures(m):
    try: return json.loads(m) if isinstance(m, str) else {}
    except Exception: return {}

def build_cracking_agg_pdf(ca_pdf, bin_m, level):
    if ca_pdf.empty: return pd.DataFrame(columns=CRACKING_COLUMNS)
    d = ca_pdf.copy()
    _m = d['measures'].apply(_crack_measures)
    d['_cc'] = pd.to_numeric(_m.apply(lambda x: x.get('crack_count')), errors='coerce').fillna(0)
    _wc = pd.to_numeric(_m.apply(lambda x: x.get('avg_corrected_width_mm')), errors='coerce')
    _wr = pd.to_numeric(_m.apply(lambda x: x.get('avg_width_mm')), errors='coerce')
    d['_wcn'] = _wc.fillna(_wr).fillna(0) * d['_cc']   # corrected width (fallback raw) x count
    d['_wrn'] = _wr.fillna(0) * d['_cc']
    d['_area'] = pd.to_numeric(d['area_sqm'], errors='coerce').fillna(0)
    d['_st'] = pd.to_numeric(d['start_chainage'], errors='coerce')
    d['_en'] = pd.to_numeric(d['end_chainage'], errors='coerce')
    d = d[d['_st'].notna() & d['_en'].notna()].copy()
    d['_bin'] = (d['_st'] // bin_m * bin_m).astype('int64')
    d = d.sort_values(['lane', '_st'])
    _tbl = 'master_cracking_child_100m' if level == 'child_100m' else 'master_cracking_parent'
    out = []
    for i, ((lane, b), g) in enumerate(d.groupby(['lane', '_bin'], sort=True)):
        first, last = g.iloc[0], g.iloc[-1]
        st, en = int(g['_st'].min()), int(g['_en'].max())
        seg_len = float(en - st)
        area = float(g['_area'].sum())
        lane_area = 3.5 * seg_len
        pct = round(area / lane_area * 100, 6) if lane_area > 0 else None
        n = float(g['_cc'].sum())
        w_corr = round(float(g['_wcn'].sum() / n), 4) if n > 0 else 0.0
        w_raw = round(float(g['_wrn'].sum() / n), 4) if n > 0 else 0.0
        bf = 1 if pct is not None and pct > CA_CRACK_PCT else 0
        p_start = (st // PARENT_LENGTH_M) * PARENT_LENGTH_M
        rid = f"cracking_{level}_{i+1:06d}"
        rec = {c: first.get(c) for c in CRACKING_COLUMNS}
        rec.update({
            "id": f"{UPC}_{_tbl}_{i+1:06d}", "collection_name": _tbl, "level": level,
            "defect_id": None, "display_defect_id": None,
            "source_defect_id": rid, "record_id": rid,
            "parent_id": f"{UPC}_{lane}_{p_start}_{p_start + PARENT_LENGTH_M}",
            "breach_flag": bf, "metric_name": "cracking_pct", "metric_value": pct, "cracking_pct": pct,
            "threshold_name": "As per CA", "threshold_value": CA_CRACK_PCT,
            "total_children": int(len(g)), "segment_count": int(len(g)),
            "length_m": seg_len, "total_length": seg_len, "area_sqm": round(area, 6),
            "start_chainage": st, "end_chainage": en, "defect_chainage": (st + en) / 2,
            "start_lat": first.get('start_lat'), "start_long": first.get('start_long'),
            "end_lat": last.get('end_lat'), "end_long": last.get('end_long'),
            "defect_lat": first.get('start_lat'), "defect_long": first.get('start_long'),
            "image_name": None, "image_urls": None,
            "parent_start_chainage": p_start, "parent_end_chainage": p_start + PARENT_LENGTH_M,
            "reporting_interval_m": bin_m,
            "nsv": json.dumps({"cracking": pct}),
            "measures": json.dumps({
                "cracking_pct": pct, "crack_area_sqm": round(area, 6), "lane_area_sqm": lane_area,
                "crack_count": n, "avg_corrected_width_mm": w_corr, "avg_width_mm": w_raw,
                "segments": int(len(g)),
                "calculation": f"Cracking % = SUM(1m-capped corrected crack area over {st}-{en}) / (3.5 x {seg_len:.0f}) x 100; "
                               "avg width = AVERAGEIFS over individual cracks (crack_count-weighted)",
            }),
            "affected_length": seg_len if bf == 1 else 0.0,
            "affected_length_ca": seg_len if bf == 1 else 0.0, "affected_length_irc": 0.0,
        })
        out.append(rec)
    pdf = pd.DataFrame(out)
    for col in CRACKING_COLUMNS:
        if col not in pdf.columns: pdf[col] = None
    return pdf[CRACKING_COLUMNS]

cracking_100m_pdf = build_cracking_agg_pdf(cracking_ca_pdf, CHILD_100M, 'child_100m')
cracking_1km_pdf  = build_cracking_agg_pdf(cracking_ca_pdf, PARENT_LENGTH_M, 'parent')
for _n in ['master_cracking_child_100m', 'master_cracking_parent']:
    TABLE_SCHEMAS[_n] = CRACKING_SCHEMA
    TABLE_COLUMNS[_n] = CRACKING_COLUMNS
cracking_100m_sdf = pandas_to_spark_with_schema(cracking_100m_pdf, CRACKING_SCHEMA) if not cracking_100m_pdf.empty else empty_with_schema(CRACKING_SCHEMA)
cracking_1km_sdf  = pandas_to_spark_with_schema(cracking_1km_pdf, CRACKING_SCHEMA) if not cracking_1km_pdf.empty else empty_with_schema(CRACKING_SCHEMA)
print(f"  Cracking 100m: {len(cracking_100m_pdf):,} rows ({int(cracking_100m_pdf['breach_flag'].sum()) if len(cracking_100m_pdf) else 0} breach) | "
      f"1km: {len(cracking_1km_pdf):,} rows ({int(cracking_1km_pdf['breach_flag'].sum()) if len(cracking_1km_pdf) else 0} breach)")

OUTPUT_DATAFRAMES = {
    "master_pothole": pothole_sdf,
    "master_cracking": cracking_sdf,
    "master_cracking_child_100m": cracking_100m_sdf,
    "master_cracking_parent": cracking_1km_sdf,
}
OUTPUT_DATAFRAMES.update(build_linear_output_set(df_roughness_raw, "roughness"))
OUTPUT_DATAFRAMES.update(build_linear_output_set(df_rutting_raw, "rutting"))
OUTPUT_DATAFRAMES.update(build_linear_output_set(df_ravelling_raw, "ravelling"))

# ═══ Pavement type at 10m / 100m / 1km: overlap of the row's own chainage range with D1 PavementType ═══
#   Only the D1 intervals that actually overlap [start, end) of the row are considered (same direction):
#     one distinct type  -> that type
#     >1 distinct types  -> 'Hybrid'   (a D1 pavement boundary falls INSIDE this row)
#     none (outside D1)  -> keep the nearest-edge value already assigned
#   e.g. D1 500-600 Asphalt, 600-1000 CC: parent 0-1000 = Hybrid, 100m 500-600 = Asphalt, 100m 600-700 = CC,
#        10m 595-605 = Hybrid; a parent with a single D1 type stays that type.
_PAV_IV = pd.DataFrame({
    'lo': np.minimum(PAVEMENT_LOOKUP['start_chainage'], PAVEMENT_LOOKUP['end_chainage']).astype(float),
    'hi': np.maximum(PAVEMENT_LOOKUP['start_chainage'], PAVEMENT_LOOKUP['end_chainage']).astype(float),
    'dir': PAVEMENT_LOOKUP.get('direction'), 'typ': PAVEMENT_LOOKUP['pavement_type'].astype(str).str.strip()}
) if not PAVEMENT_LOOKUP.empty else pd.DataFrame(columns=['lo', 'hi', 'dir', 'typ'])

def pavement_by_overlap(seg):
    """seg: DataFrame lane, _s, _e -> Series of pavement (type / 'Hybrid' / None when outside D1)."""
    out = pd.Series(None, index=seg.index, dtype=object)
    if _PAV_IV.empty or seg.empty: return out
    for dirv, idx in seg.groupby(seg['lane'].map(lane_to_direction).fillna('')).groups.items():
        iv = _PAV_IV[_PAV_IV['dir'] == dirv] if (_PAV_IV['dir'] == dirv).any() else _PAV_IV
        s, e = seg.loc[idx, '_s'].to_numpy(float)[:, None], seg.loc[idx, '_e'].to_numpy(float)[:, None]
        ov = (iv['lo'].to_numpy()[None, :] < e) & (iv['hi'].to_numpy()[None, :] > s)     # positive-length overlap
        typ = iv['typ'].to_numpy(); uniq = pd.unique(typ)
        has = np.stack([(ov & (typ == u)[None, :]).any(1) for u in uniq], axis=1)
        n = has.sum(1)
        out.loc[idx] = np.where(n > 1, 'Hybrid', np.where(n == 1, np.asarray(uniq, dtype=object)[has.argmax(1)], None))
    return out

_lo_c = F.least(F.col('start_chainage').cast('double'), F.col('end_chainage').cast('double'))
_hi_c = F.greatest(F.col('start_chainage').cast('double'), F.col('end_chainage').cast('double'))
_seg_schema = T.StructType([T.StructField('_pl', T.StringType()), T.StructField('_s', T.DoubleType()),
                            T.StructField('_e', T.DoubleType()), T.StructField('_pav', T.StringType())])
for _d in ['roughness', 'rutting', 'ravelling', 'cracking']:
    for _tbl in ([f'master_{_d}_child_10m'] if _d != 'cracking' else ['master_cracking']) + [f'master_{_d}_child_100m', f'master_{_d}_parent']:
        if _tbl not in OUTPUT_DATAFRAMES: continue
        _t = OUTPUT_DATAFRAMES[_tbl]
        _seg = _t.select('lane', _lo_c.alias('_s'), _hi_c.alias('_e')).distinct().toPandas()
        _seg['_pav'] = pavement_by_overlap(_seg)
        _seg_sdf = spark.createDataFrame([(str(l), float(s), float(e), (p if isinstance(p, str) else None))
                                          for l, s, e, p in _seg[['lane', '_s', '_e', '_pav']].itertuples(index=False)], _seg_schema)
        _t = (_t.join(F.broadcast(_seg_sdf), (F.col('lane') == F.col('_pl')) & (_lo_c == F.col('_s')) & (_hi_c == F.col('_e')), 'left')
                .withColumn('pavement_type', F.coalesce(F.col('_pav'), F.col('pavement_type')))
                .drop('_pl', '_s', '_e', '_pav'))
        OUTPUT_DATAFRAMES[_tbl] = _t
        print(f"  {_tbl:32s} pavement by D1 overlap | segments: {len(_seg):,} | Hybrid: {int((_seg['_pav'] == 'Hybrid').sum()):,} "
              f"| outside D1 (kept nearest): {int(_seg['_pav'].isna().sum()):,}")

# ═══ Post-process: hierarchical display_defect_id (ported from Clone Update_5 Cell 13) ═══
#   Parent / standalone: D/{ddmmyy}/N/{counter}        e.g. D/051225/N/641795
#   Child 100m / 10m:    {parent_display_defect_id}/{child_seq}  e.g. D/051225/N/641795/0001
# RULES:
#   - Only breach_flag=1 rows get a display_defect_id; others -> NULL
#   - Counter is per date: continues after other UPCs with the same date in dlm_kpi
#   - Children link to the 1km parent with the same parent_id AND threshold_name (CA->CA, IRC->IRC)
#   - Breach children whose parent does not breach get their own D/{ddmmyy}/N/{counter}
#   - Only display_defect_id changes; defect_id is left as is
_SRC_LETTER = "N"
DISPLAY_DEFECT_DATE_STR = (
    pd.Timestamp(TRACKER_CTX["survey_end_date"]).strftime("%d%m%y")
    if TRACKER_CTX.get("survey_end_date") else SURVEY_DATE_STR
)
_BASE_ID_TABLES = ["master_roughness_parent", "master_rutting_parent", "master_cracking_parent",
                   "master_ravelling_parent", "master_pothole"]
_CHILD_PARENT_MAP = {
    "master_cracking_child_100m":  "master_cracking_parent",
    "master_cracking":             "master_cracking_parent",
    "master_roughness_child_100m": "master_roughness_parent",
    "master_roughness_child_10m":  "master_roughness_parent",
    "master_rutting_child_100m":   "master_rutting_parent",
    "master_rutting_child_10m":    "master_rutting_parent",
    "master_ravelling_child_100m": "master_ravelling_parent",
    "master_ravelling_child_10m":  "master_ravelling_parent",
}
_TOP_ID_RE = f"^D/{DISPLAY_DEFECT_DATE_STR}/[NDRO]/\\d+$"

# Max existing top-level counter for the same date across other UPCs (all DLM tables)
_max_seq = 0
for _t in list(_BASE_ID_TABLES) + list(_CHILD_PARENT_MAP):
    _full = f"{DLM_KPI_NS}.{_t}"
    if spark.catalog.tableExists(_full):
        _m = (spark.table(_full)
              .filter((F.col("upc_code") != UPC) & F.col("display_defect_id").rlike(_TOP_ID_RE))
              .select(F.max(F.element_at(F.split("display_defect_id", "/"), 4).cast("int"))).collect()[0][0])
        if _m: _max_seq = max(_max_seq, _m)
print(f"Display ID date: {DISPLAY_DEFECT_DATE_STR} | existing max counter (other UPCs): {_max_seq}")
_running_seq = _max_seq

_order_cols = [F.col("threshold_name").asc_nulls_last(), F.col("lane").asc_nulls_last(),
               F.col("start_chainage").asc_nulls_last(), F.col("end_chainage").asc_nulls_last(),
               F.col("record_id").asc_nulls_last(), F.col("id").asc_nulls_last()]

def _assign_top_ids(sdf, mask_col):
    """Number rows where mask_col is true with the running counter: D/{date}/N/{%05d}."""
    global _running_seq
    _n = sdf.filter(F.col(mask_col)).count()
    if _n == 0: return sdf
    _w = Window.partitionBy(F.col(mask_col)).orderBy(*_order_cols)
    sdf = sdf.withColumn("_seq", F.row_number().over(_w)).withColumn(
        "display_defect_id",
        F.when(F.col(mask_col), F.concat(F.lit(f"D/{DISPLAY_DEFECT_DATE_STR}/{_SRC_LETTER}/"),
                                          F.format_string("%05d", F.col("_seq") + F.lit(_running_seq))))
         .otherwise(F.col("display_defect_id"))).drop("_seq")
    _running_seq += _n
    return sdf

# Pass 1: parent / standalone tables
for _t in _BASE_ID_TABLES:
    if _t not in OUTPUT_DATAFRAMES: continue
    _sdf = OUTPUT_DATAFRAMES[_t].withColumn("display_defect_id", F.lit(None).cast("string"))
    _sdf = _sdf.withColumn("_is_top", F.col("breach_flag") == 1)
    _sdf = _assign_top_ids(_sdf, "_is_top").drop("_is_top")
    OUTPUT_DATAFRAMES[_t] = _sdf
    print(f"  {_t}: parent/standalone IDs -> counter now {_running_seq}")

# Pass 2: children inherit parent ID + /0001 suffix; orphan breaches get their own counter
for _c, _p in _CHILD_PARENT_MAP.items():
    if _c not in OUTPUT_DATAFRAMES: continue
    _lk = (OUTPUT_DATAFRAMES[_p].filter(F.col("display_defect_id").isNotNull())
           .select(F.col("parent_id").alias("_pj"), F.col("threshold_name").alias("_pt"),
                   F.col("display_defect_id").alias("_pdid"))
           .dropDuplicates(["_pj", "_pt"]))
    _sdf = OUTPUT_DATAFRAMES[_c].withColumn("display_defect_id", F.lit(None).cast("string"))
    _sdf = _sdf.join(_lk, (_sdf["parent_id"] == _lk["_pj"]) & (_sdf["threshold_name"] == _lk["_pt"]), "left")
    _sdf = _sdf.withColumn("_has_p", (F.col("breach_flag") == 1) & F.col("_pdid").isNotNull())
    _cw = Window.partitionBy("parent_id", "threshold_name", "_has_p").orderBy(
        F.col("start_chainage").asc_nulls_last(), F.col("end_chainage").asc_nulls_last(),
        F.col("record_id").asc_nulls_last(), F.col("id").asc_nulls_last())
    _sdf = _sdf.withColumn("display_defect_id", F.when(
        F.col("_has_p"), F.concat(F.col("_pdid"), F.lit("/"), F.format_string("%04d", F.row_number().over(_cw)))))
    _sdf = _sdf.withColumn("_orphan", (F.col("breach_flag") == 1) & F.col("_pdid").isNull())
    _n_child = _sdf.filter("_has_p").count(); _n_orph = _sdf.filter("_orphan").count()
    _sdf = _assign_top_ids(_sdf, "_orphan")
    OUTPUT_DATAFRAMES[_c] = _sdf.drop("_pj", "_pt", "_pdid", "_has_p", "_orphan")
    print(f"  {_c}: child IDs={_n_child:,} | orphan (own counter)={_n_orph:,}")

# Print counts
for tbl_name in sorted(OUTPUT_DATAFRAMES):
    cnt = OUTPUT_DATAFRAMES[tbl_name].count()
    breach_cnt = OUTPUT_DATAFRAMES[tbl_name].filter(F.col('breach_flag') == 1).count()
    print(f"  {tbl_name:40s} {cnt:>6,} rows ({breach_cnt:,} breach)")

# COMMAND ----------

# DBTITLE 1,PCI Weighted Defect Score and Backfill
# ═══════════════════════════════════════════════════════════════════════════════
# PCI — Weighted Defect Score at 10m / 100m / 1km
# Ported from Clone Update_5 Cell 15
#
# Formula:  PCI = 100 − Weighted_Defect_Score
# Weights:  Crack=0.12, Ravel=0.10, Pothole=0.16, Patch=0.08, Rut=0.14, Rough=0.40
# Each index normalized 0–100 via max-observed-value scaling.
# Roll up to 100m/1km by averaging 10m PCI scores.
# ═══════════════════════════════════════════════════════════════════════════════

# ── Weights (sum to 1.0) ──────────────────────────────────────────────────────
W_CRACK, W_RAVEL, W_POTHOLE, W_PATCH, W_RUT, W_ROUGH = 0.12, 0.10, 0.16, 0.08, 0.14, 0.40

# ── Step 1: Load Patching Data from Backup Lane Folders ──────────────────────
def _load_patching(backup_base):
    frames = []
    lanes = discover_lane_folders(backup_base)
    for lane in lanes:
        lane_path = os.path.join(backup_base, f"Lane={lane}")
        matches = [f for f in sorted(os.listdir(lane_path))
                   if f.endswith('.parquet') and 'patch' in f.lower()]
        for pf in matches:
            part = pd.read_parquet(os.path.join(lane_path, pf))
            # Find chainage and area columns
            cols_lower = {c.lower(): c for c in part.columns}
            ch_col = cols_lower.get('chainage') or cols_lower.get('start_chainage')
            area_col = cols_lower.get('area') or cols_lower.get('area_sq_m') or cols_lower.get('total_area')
            if ch_col and area_col:
                df = part[[ch_col, area_col]].copy()
                df.columns = ['Chainage', 'Area']
                df['Lane'] = normalize_lane(lane)   # same lane name as the defect tables
                df['Chainage'] = pd.to_numeric(df['Chainage'], errors='coerce')
                df['Area'] = pd.to_numeric(df['Area'], errors='coerce').fillna(0.0)
                frames.append(df)
                print(f"  Patching: Lane={lane}/{pf} ({len(df)} rows)")
    if not frames:
        print("  WARNING: No patching parquets found.")
        return pd.DataFrame(columns=['Lane', 'Start_Chainage', 'patching_area'])
    all_patch = pd.concat(frames, ignore_index=True).dropna(subset=['Chainage'])
    all_patch['Start_Chainage'] = (all_patch['Chainage'] // CHILD_10M * CHILD_10M).astype(int)
    return all_patch.groupby(['Lane', 'Start_Chainage']).agg(patching_area=('Area', 'sum')).reset_index()

patching_10m = _load_patching(BACKUP_BASE)
print(f"  Patching 10m records: {len(patching_10m):,}")

# ── Step 2: Build 10m Chainage Spine ─────────────────────────────────────────
# Scan all OUTPUT_DATAFRAMES + patching to get per-lane min/max chainage
_spine_ranges = []
for _tbl in ['master_cracking', 'master_roughness_child_10m', 'master_rutting_child_10m',
             'master_ravelling_child_10m', 'master_pothole']:
    if _tbl not in OUTPUT_DATAFRAMES: continue
    _r = (OUTPUT_DATAFRAMES[_tbl]
          .select(F.col('lane').alias('Lane'), F.col('start_chainage').cast('long').alias('ch'))
          .groupBy('Lane').agg(F.min('ch').alias('min_ch'), F.max('ch').alias('max_ch'))
          .toPandas())
    if not _r.empty: _spine_ranges.append(_r)
if not patching_10m.empty:
    _spine_ranges.append(patching_10m.groupby('Lane').agg(
        min_ch=('Start_Chainage', 'min'), max_ch=('Start_Chainage', 'max')).reset_index())

_lane_bounds = pd.concat(_spine_ranges, ignore_index=True).groupby('Lane').agg(
    min_ch=('min_ch', 'min'), max_ch=('max_ch', 'max')).reset_index()

_spine_rows = []
for _, _lb in _lane_bounds.iterrows():
    lo = int((_lb['min_ch'] // CHILD_10M) * CHILD_10M)
    hi = int((_lb['max_ch'] // CHILD_10M) * CHILD_10M)
    for ch in range(lo, hi + CHILD_10M, CHILD_10M):
        _spine_rows.append({'Lane': _lb['Lane'], 'Start_Chainage': ch, 'End_Chainage': ch + CHILD_10M})
pci_spine = pd.DataFrame(_spine_rows)
print(f"PCI spine: {len(pci_spine):,} segments, lanes {sorted(pci_spine['Lane'].unique().tolist())}")

# ── Step 3: Read Metrics from OUTPUT_DATAFRAMES ──────────────────────────────
# Filter to 'As per CA' threshold only to avoid double-counting dual records
def _read_metric(table_name, value_col, alias, agg_func='max'):
    if table_name not in OUTPUT_DATAFRAMES:
        return pd.DataFrame(columns=['Lane', 'Start_Chainage', alias])
    sdf = OUTPUT_DATAFRAMES[table_name]
    if 'threshold_name' in sdf.columns:
        sdf = sdf.filter(F.col('threshold_name') == 'As per CA')
    pdf = sdf.select(
        F.col('lane').alias('Lane'),
        F.col('start_chainage').cast('long').alias('raw_ch'),
        F.col(value_col).alias('val')
    ).toPandas()
    if pdf.empty:
        return pd.DataFrame(columns=['Lane', 'Start_Chainage', alias])
    pdf['Start_Chainage'] = (pdf['raw_ch'] // CHILD_10M) * CHILD_10M
    pdf['val'] = pd.to_numeric(pdf['val'], errors='coerce').fillna(0.0)
    return pdf.groupby(['Lane', 'Start_Chainage']).agg(val=('val', agg_func)).reset_index().rename(columns={'val': alias})

cracking_metric  = _read_metric('master_cracking',            'metric_value', 'cracking_pct',   'max')
ravelling_metric = _read_metric('master_ravelling_child_10m', 'area_sqm',     'ravelling_area',  'sum')
pothole_metric   = _read_metric('master_pothole',             'area_sqm',     'pothole_area',    'sum')
rutting_metric   = _read_metric('master_rutting_child_10m',   'metric_value', 'rutting_mm',      'max')
roughness_metric = _read_metric('master_roughness_child_10m', 'iri_value',    'roughness_iri',   'max')

for label, pdf in [('Cracking pct', cracking_metric), ('Ravelling area', ravelling_metric),
                   ('Pothole area', pothole_metric), ('Rutting mm', rutting_metric),
                   ('Roughness IRI', roughness_metric), ('Patching area', patching_10m)]:
    print(f"  {label:16s}: {len(pdf):>6,} rows")

# ── Step 4: Merge Metrics onto Spine ─────────────────────────────────────────
pci_df = pci_spine.copy()
for mdf in [cracking_metric, ravelling_metric, pothole_metric,
            rutting_metric, roughness_metric, patching_10m]:
    pci_df = pci_df.merge(mdf, on=['Lane', 'Start_Chainage'], how='left')

raw_cols = ['cracking_pct', 'ravelling_area', 'pothole_area', 'rutting_mm', 'roughness_iri', 'patching_area']
for col in raw_cols:
    pci_df[col] = pci_df[col].fillna(0.0)

# ── Step 5: Normalize to 0–100 Index (max-observed scaling) ──────────────────
max_vals = {c: pci_df[c].max() for c in raw_cols}
print(f"\nNormalization ceilings:")
for c, mx in max_vals.items(): print(f"  {c:20s}: {mx:.4f}")

index_map = {'cracking_pct': 'crack_index', 'ravelling_area': 'ravel_index', 'pothole_area': 'pothole_index',
             'patching_area': 'patch_index', 'rutting_mm': 'rut_index', 'roughness_iri': 'roughness_index'}
for raw_col, idx_col in index_map.items():
    mx = max_vals[raw_col]
    pci_df[idx_col] = (pci_df[raw_col] / mx * 100.0).clip(0, 100) if mx > 0 else 0.0

# ── Step 6: Weighted Defect Score & PCI ──────────────────────────────────────
pci_df['weighted_defect_score'] = (
    pci_df['crack_index']     * W_CRACK  +
    pci_df['ravel_index']     * W_RAVEL  +
    pci_df['pothole_index']   * W_POTHOLE +
    pci_df['patch_index']     * W_PATCH  +
    pci_df['rut_index']       * W_RUT    +
    pci_df['roughness_index'] * W_ROUGH
).round(4)
pci_df['pci_10m'] = (100.0 - pci_df['weighted_defect_score']).clip(0, 100).round(4)

print(f"\nPCI 10m: {len(pci_df):,} segments")
print(f"  PCI range: {pci_df['pci_10m'].min():.2f} – {pci_df['pci_10m'].max():.2f}")
print(f"  Mean PCI:  {pci_df['pci_10m'].mean():.2f}")
print(f"  Segments with defects (PCI < 100): {(pci_df['pci_10m'] < 100).sum():,}")

# ── Step 7: Roll Up to 100m and 1km ─────────────────────────────────────────
pci_df['_ch_100m'] = (pci_df['Start_Chainage'] // 100) * 100
pci_100m = pci_df.groupby(['Lane', '_ch_100m']).agg(pci_score=('pci_10m', 'mean')).reset_index()
pci_100m.rename(columns={'_ch_100m': 'Start_Chainage'}, inplace=True)
pci_100m['pci_score'] = pci_100m['pci_score'].round(4)
print(f"PCI 100m: {len(pci_100m):,} segments, mean={pci_100m['pci_score'].mean():.2f}")

pci_df['_ch_1km'] = (pci_df['Start_Chainage'] // 1000) * 1000
pci_1km = pci_df.groupby(['Lane', '_ch_1km']).agg(pci_score=('pci_10m', 'mean')).reset_index()
pci_1km.rename(columns={'_ch_1km': 'Start_Chainage'}, inplace=True)
pci_1km['pci_score'] = pci_1km['pci_score'].round(4)
print(f"PCI 1km:  {len(pci_1km):,} segments, mean={pci_1km['pci_score'].mean():.2f}")

# ── Step 8: Build Spark Lookups + fill_pci_column() ──────────────────────────
pci_lookup_10m  = pci_df[['Lane', 'Start_Chainage', 'pci_10m']].rename(columns={'pci_10m': 'pci_score'})
pci_lookup_100m = pci_100m[['Lane', 'Start_Chainage', 'pci_score']].copy()
pci_lookup_1km  = pci_1km[['Lane', 'Start_Chainage', 'pci_score']].copy()

_pci_sdf_10m  = spark.createDataFrame(pci_lookup_10m.rename(columns={'Lane':'_pci_lane','Start_Chainage':'_pci_ch','pci_score':'_pci_val'}))
_pci_sdf_100m = spark.createDataFrame(pci_lookup_100m.rename(columns={'Lane':'_pci_lane','Start_Chainage':'_pci_ch','pci_score':'_pci_val'}))
_pci_sdf_1km  = spark.createDataFrame(pci_lookup_1km.rename(columns={'Lane':'_pci_lane','Start_Chainage':'_pci_ch','pci_score':'_pci_val'}))
PCI_LOOKUPS = {'10m': _pci_sdf_10m, '100m': _pci_sdf_100m, '1km': _pci_sdf_1km}

def fill_pci_column(sdf, level='10m'):
    lookup = PCI_LOOKUPS[level]
    grid = {'10m': CHILD_10M, '100m': 100, '1km': 1000}[level]
    sdf2 = sdf.withColumn('_join_ch', (F.col('start_chainage').cast('long') / grid).cast('long') * grid)
    joined = sdf2.join(lookup,
        (sdf2['lane'] == lookup['_pci_lane']) & (sdf2['_join_ch'] == lookup['_pci_ch'].cast('long')), 'left')
    return joined.withColumn('pci', F.coalesce(F.col('_pci_val'), F.lit(None)).cast('double')) \
                 .drop('_pci_lane', '_pci_ch', '_pci_val', '_join_ch')

# ── Step 9: Backfill PCI into OUTPUT_DATAFRAMES ─────────────────────────────
updated = []
for tbl_name, sdf in OUTPUT_DATAFRAMES.items():
    if 'pci' not in sdf.columns: continue
    lvl = '1km' if 'parent' in tbl_name else ('100m' if '100m' in tbl_name else '10m')
    OUTPUT_DATAFRAMES[tbl_name] = fill_pci_column(sdf, level=lvl)
    updated.append(f"{tbl_name} ({lvl})")
print(f"\nBackfilled PCI in: {', '.join(updated)}")

# ── Quick Verify ─────────────────────────────────────────────────────────────
for tbl in ['master_roughness_child_10m', 'master_cracking', 'master_pothole']:
    sdf = OUTPUT_DATAFRAMES[tbl]
    pci_filled = sdf.filter(F.col('pci').isNotNull()).count()
    total = sdf.count()
    pct = pci_filled/total*100 if total > 0 else 0
    print(f"  {tbl}: {pci_filled:,}/{total:,} pci filled ({pct:.1f}%)")

# COMMAND ----------

# DBTITLE 1,Add Road Condition Column (D3 Dynamic + CA Binary)
# ═══════════════════════════════════════════════════════════════════════════════
# ADD road_condition COLUMN (dynamic from D3 per project)
#
# IRC standard → D3 visual condition (Good/Fair/Poor categories from D3 deliverable)
# CA standard  → Simple binary: "Good" (no breach) / "Bad" (breach)
#
# D3 data is loaded per-UPC from D3_BASE, so categories are project-specific.
# ═══════════════════════════════════════════════════════════════════════════════

_defect_type_map = {
    'cracking': 'Cracking', 'roughness': 'Roughness',
    'rutting': 'Rutting', 'pothole': 'Pothole', 'ravelling': 'Ravelling',
}

# Build combined D3 lookup as broadcast Spark DataFrame
_d3_frames = []
for _defect, _df_d3 in D3_CONDITIONS.items():
    _tmp = _df_d3.copy()
    _tmp['defect_type'] = _defect
    _d3_frames.append(_tmp[['defect_type', 'bin_100m', 'Lane', 'Condition']])

if _d3_frames:
    _d3_all = pd.concat(_d3_frames, ignore_index=True)
    d3_lookup_sdf = spark.createDataFrame(_d3_all).select(
        F.col('defect_type').alias('_d3_defect'),
        F.col('bin_100m').cast('long').alias('_d3_bin'),
        F.col('Lane').alias('_d3_lane'),
        F.col('Condition').alias('_d3_condition')
    )
    print(f"D3 lookup built: {d3_lookup_sdf.count()} rows")
    print(f"Defects covered: {sorted(D3_CONDITIONS.keys())}")
else:
    d3_lookup_sdf = None
    print("WARNING: No D3 data — road_condition will be CA-only (Good/Bad)")

# Map road_condition onto each OUTPUT_DATAFRAME
_updated = 0
for tbl, sdf in OUTPUT_DATAFRAMES.items():
    # Determine defect type from table name
    _defect_type = next((v for k, v in _defect_type_map.items() if k in tbl.lower()), None)
    if _defect_type is None:
        continue

    # Compute 100m bin for chainage-based D3 join
    sdf_bin = sdf.withColumn('_bin_100m', (F.floor(F.col('start_chainage') / 100) * 100).cast('long'))

    # Left join with D3 for IRC condition lookup
    if d3_lookup_sdf is not None and _defect_type in D3_CONDITIONS:
        sdf_joined = sdf_bin.join(
            F.broadcast(d3_lookup_sdf),
            (sdf_bin['_bin_100m'] == F.col('_d3_bin')) &
            (sdf_bin['lane'] == F.col('_d3_lane')) &
            (F.lit(_defect_type) == F.col('_d3_defect')),
            'left'
        )
    else:
        sdf_joined = sdf_bin.withColumn('_d3_condition', F.lit(None).cast('string'))

    # Apply road_condition using threshold_name (not 'standard' which is NULL):
    #   CA  → "Good" / "Bad" (simple breach binary)
    #   IRC → D3 visual condition (project-specific categories from D3)
    _std_col = F.lower(F.coalesce(F.col('threshold_name'), F.col('standard')))
    sdf_final = sdf_joined.withColumn(
        'road_condition',
        F.when(_std_col.contains('ca'),
               F.when(F.col('breach_flag') == 1, F.lit('Bad')).otherwise(F.lit('Good')))
        .when(_std_col.contains('irc'),
               F.coalesce(F.col('_d3_condition'), F.lit(None)))
        .otherwise(
            # Fallback: if neither CA nor IRC in threshold_name, use breach_flag
            F.when(F.col('breach_flag') == 1, F.lit('Bad')).otherwise(F.lit('Good')))
    ).drop('_bin_100m', '_d3_condition', '_d3_bin', '_d3_lane', '_d3_defect')

    OUTPUT_DATAFRAMES[tbl] = sdf_final
    _updated += 1

print(f"\nroad_condition added to {_updated} tables")

# Show samples
for _tbl in ['master_roughness_parent', 'master_roughness_child_100m', 'master_cracking']:
    if _tbl in OUTPUT_DATAFRAMES:
        _sdf = OUTPUT_DATAFRAMES[_tbl]
        print(f"\n{_tbl}:")
        _sdf.select('threshold_name', 'breach_flag', 'road_condition', 'lane', 'start_chainage').show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Standardize NH Number + Fill CA and IRC Ratings Independently
# ═══════════════════════════════════════════════════════════════════════════════
# STANDARDIZE NH NUMBER + FILL rating_ca AND rating_irc INDEPENDENTLY
#
# nh_number  → always "NH-<number><suffix>"  (NH0027 → NH-27, NH151A → NH-151A)
# rating_ca  → CA condition label (from CA-threshold condition), fallback Good/Bad on CA breach
# rating_irc → IRC condition (D3 visual condition), fallback Good/Poor on IRC breach
# Both ratings are written on BOTH the CA row and its IRC twin row, whenever the
# underlying data (metric / condition) is present. Twin rows are paired by id
# (IRC id = CA id with '_irc_' inserted).
# ═══════════════════════════════════════════════════════════════════════════════
from pyspark.sql import Window as _W

# CA rating computed from each row's OWN metric (correct at 10m / 100m / 1000m),
# Native Spark port of crack/roughness/rutting/ravelling_condition_* (same D2 breakpoints + labels).
def _ca_rating_expr(defect):
    v = F.col('metric_value').cast('double')
    if defect == 'cracking':
        t = CA_CRACK_PCT; b2, b3, b4 = round(t * 2), round(t * 4), round(t * 6)
        return (F.when(v.isNull(), None).when(v < t, f'Very Good (<{t:.0f}%)')
                 .when(v <= b2, f'Good ({t:.0f}-{b2}%)').when(v <= b3, f'Fair ({b2}-{b3}%)')
                 .when(v <= b4, f'Poor ({b3}-{b4}%)').otherwise(f'Very Poor (>{b4}%)'))
    if defect == 'roughness':
        t = CA_BI_THRESHOLD; fl = round(t * 0.8)
        p = F.coalesce(F.col('roughness_bi').cast('double'), F.col('iri_value').cast('double') * 1000)
        return (F.when(p.isNull(), None).when(p < fl, f'Good (<{fl}mm/km)')
                 .when(p <= t, f'Fair ({fl}-{t:.0f}mm/km)').otherwise(f'Poor (>{t:.0f}mm/km)'))
    if defect == 'rutting':
        t = CA_RUT_THRESHOLD; fl = round(t * 0.5)
        return (F.when(v.isNull(), None).when(v < fl, f'Good (<{fl}mm)')
                 .when(v <= t, f'Fair ({fl}-{t:.0f}mm)').otherwise(f'Poor (>{t:.0f}mm)'))
    t = RAVELLING_RATING_THRESHOLD; h = round(t * 0.5)
    return (F.when(v.isNull(), None).when(v == 0, 'Very Good (0)').when(v <= h, f'Good (1-{h}%)')
             .when(v < t, f'Fair ({h}-{t:.0f}%)').otherwise(f'Poor (>={t:.0f}%)'))

_has_data = F.col('metric_value').isNotNull()
_std = F.lower(F.coalesce(F.col('threshold_name'), F.lit('')))

for tbl in list(OUTPUT_DATAFRAMES.keys()):
    sdf = OUTPUT_DATAFRAMES[tbl].withColumn('nh_number', standardize_nh_col('nh_number'))

    if tbl == 'master_pothole':
        # Pothole severity is standard-independent → same rating for CA and IRC
        sdf = (sdf.withColumn('rating_ca',  F.coalesce('rating_ca',  'severity'))
                  .withColumn('rating_irc', F.coalesce('rating_irc', 'severity')))
    else:
        _defect = next(d for d in ['cracking', 'roughness', 'rutting', 'ravelling'] if d in tbl)
        _ca_own = F.when(_std.contains('ca'), F.coalesce(
            _ca_rating_expr(_defect), F.col('rating_ca'), F.col('severity'),
            F.when(_has_data, F.when(F.col('breach_flag') == 1, 'Bad').otherwise('Good'))))
        _irc_own = F.when(_std.contains('irc'), F.coalesce(
            F.col('road_condition'),
            F.when(_has_data, F.when(F.col('breach_flag') == 1, 'Poor').otherwise('Good'))))
        _w = _W.partitionBy(F.regexp_replace('id', '_irc_', '_'))
        sdf = (sdf.withColumn('_ca_own', _ca_own).withColumn('_irc_own', _irc_own)
                  .withColumn('rating_ca',  F.max('_ca_own').over(_w))
                  .withColumn('rating_irc', F.max('_irc_own').over(_w))
                  .drop('_ca_own', '_irc_own'))
    OUTPUT_DATAFRAMES[tbl] = sdf

# ── Verification ────────────────────────────────────────────────────────────
print(f"UPC {UPC}\n")
print(f"{'Table':<32} {'Std':<11} {'Rows':>7} {'rating_ca':>10} {'rating_irc':>11}  nh_number")
print('-' * 95)
for tbl in sorted(OUTPUT_DATAFRAMES):
    stats = (OUTPUT_DATAFRAMES[tbl].groupBy('threshold_name')
             .agg(F.count('*').alias('n'), F.count('rating_ca').alias('ca'),
                  F.count('rating_irc').alias('irc'),
                  F.concat_ws(',', F.collect_set('nh_number')).alias('nh'))
             .orderBy('threshold_name').collect())
    for r in stats:
        print(f"{tbl:<32} {str(r['threshold_name']):<11} {r['n']:>7,} {r['ca']:>10,} {r['irc']:>11,}  {r['nh']}")

print("\nSample (cracking + roughness_child_100m):")
for _t in ['master_cracking', 'master_roughness_child_100m']:
    (OUTPUT_DATAFRAMES[_t].select('lane', 'start_chainage', 'threshold_name', 'metric_value',
                                   'breach_flag', 'rating_ca', 'rating_irc', 'road_condition', 'nh_number')
        .orderBy('lane', 'start_chainage', 'threshold_name').show(4, truncate=False))

# COMMAND ----------

# DBTITLE 1,Cracking Severity (Corrected Width) + Visual Condition (CA / IRC)
# ═══════════════════════════════════════════════════════════════════════════════
# CRACKING SEVERITY + VISUAL CONDITION (10m / 100m / 1km) — replaces D3-based logic
#
# Severity (uniform for all projects) — from avg CORRECTED crack width (mm):
#     width < 3 -> None | 3 <= width < 6 -> Medium | width >= 6 -> High
#     (Excel: IF(w<3,"None",IF(w<6,"Medium","High"))); falls back to raw avg width when
#     corrected width is not available in the source (e.g. N_02015).
#
# Visual condition — from cracking area % of the row's own interval:
#   As per CA : <= CA value -> Good (#2a8524) | > CA value -> Poor (#e31a1c)
#   As per IRC (lower bounds inclusive):
#     >=30 Very Poor (#e31a1c) | 20-30 Poor (#ff7f00) | 10-20 Fair (#fff14b)
#     5-10 Good (#0f6ad0)      | <5 Very Good (#2a8524)
#
# road_condition = condition of the row's own threshold (CA or IRC)
# rating_ca / rating_irc = CA / IRC condition (on every row)
# measures gains: severity_width_mm, severity, visual_condition_ca/irc, condition_color_ca/irc, condition_color
# ═══════════════════════════════════════════════════════════════════════════════
_CRACK_TABLES = [t for t in ['master_cracking', 'master_cracking_child_100m', 'master_cracking_parent']
                 if t in OUTPUT_DATAFRAMES]

_w = F.coalesce(F.get_json_object('measures', '$.avg_corrected_width_mm').cast('double'),
                F.get_json_object('measures', '$.avg_width_mm').cast('double'), F.lit(0.0))
_sev = F.when(_w < 3, 'None').when(_w < 6, 'Medium').otherwise('High')

_p = F.col('metric_value').cast('double')
_ca_lbl = F.when(_p.isNull(), None).when(_p <= F.lit(float(CA_CRACK_PCT)), 'Good').otherwise('Poor')
_ca_hex = F.when(_p.isNull(), None).when(_p <= F.lit(float(CA_CRACK_PCT)), '#2a8524').otherwise('#e31a1c')
_irc_lbl = (F.when(_p.isNull(), None).when(_p >= 30, 'Very Poor (>30%)').when(_p >= 20, 'Poor (20-30%)')
             .when(_p >= 10, 'Fair (10-20%)').when(_p >= 5, 'Good (5-10%)').otherwise('Very Good (<5%)'))
_irc_hex = (F.when(_p.isNull(), None).when(_p >= 30, '#e31a1c').when(_p >= 20, '#ff7f00')
             .when(_p >= 10, '#fff14b').when(_p >= 5, '#0f6ad0').otherwise('#2a8524'))
_is_irc = F.lower(F.coalesce(F.col('threshold_name'), F.lit(''))).contains('irc')

for _t in _CRACK_TABLES:
    _s = OUTPUT_DATAFRAMES[_t]
    _s = (_s.withColumn('_extra', F.to_json(F.struct(
                F.round(_w, 4).alias('severity_width_mm'), _sev.alias('severity'),
                _ca_lbl.alias('visual_condition_ca'), _ca_hex.alias('condition_color_ca'),
                _irc_lbl.alias('visual_condition_irc'), _irc_hex.alias('condition_color_irc'),
                F.when(_is_irc, _irc_hex).otherwise(_ca_hex).alias('condition_color'))))
            .withColumn('_mt', F.trim(F.coalesce(F.col('measures'), F.lit('{}'))))
            .withColumn('severity', _sev)
            .withColumn('rating_ca', _ca_lbl)
            .withColumn('rating_irc', _irc_lbl)
            .withColumn('road_condition', F.when(_is_irc, _irc_lbl).otherwise(_ca_lbl))
            .withColumn('measures', F.when(F.col('_mt') == '{}', F.col('_extra'))
                        .otherwise(F.expr("concat(substring(_mt, 1, length(_mt) - 1), ', ', substring(_extra, 2))")))
            .drop('_extra', '_mt'))
    OUTPUT_DATAFRAMES[_t] = _s

# ── Verification ────────────────────────────────────────────────────────────
print(f"UPC {UPC} | CA cracking limit = {CA_CRACK_PCT}% | IRC bands 5/10/20/30%\n")
for _t in _CRACK_TABLES:
    _s = OUTPUT_DATAFRAMES[_t]
    print(f"── {_t}")
    _s.groupBy('threshold_name', 'road_condition').agg(
        F.count('*').alias('rows'), F.sum('breach_flag').alias('breach'),
        F.round(F.min('metric_value'), 3).alias('min_pct'), F.round(F.max('metric_value'), 3).alias('max_pct')
    ).orderBy('threshold_name', 'min_pct').show(truncate=False)
    _s.groupBy('severity').agg(F.count('*').alias('rows'),
        F.round(F.min(_w), 3).alias('min_w'), F.round(F.max(_w), 3).alias('max_w')).orderBy('min_w').show()

display(OUTPUT_DATAFRAMES['master_cracking_child_100m']
        .select('lane', 'start_chainage', 'end_chainage', 'threshold_name', 'metric_value', 'area_sqm',
                'breach_flag', 'severity', 'road_condition', 'rating_ca', 'rating_irc', 'measures')
        .orderBy('lane', 'start_chainage').limit(10))

# COMMAND ----------

# DBTITLE 1,Dual Schema Write (dlm_postgres + dlm_kpi)
# ═══════════════════════════════════════════════════════════════════════════════
# DUAL SCHEMA WRITE
# dlm_kpi:      ALL data (breach_flag=0 and breach_flag=1)
# dlm_postgres:  ONLY breach_flag=1 data
# ═══════════════════════════════════════════════════════════════════════════════

# Create schemas
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DLM_KPI_NS}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DLM_POSTGRES_NS}")

OUTPUT_TABLES = sorted(TABLE_SCHEMAS.keys())

def write_to_schema(namespace, write_df, table_name, filter_breach=False):
    full_name = f"{namespace}.{table_name}"
    if filter_breach:
        write_df = write_df.filter(F.col("breach_flag") == 1)
    table_exists = spark.catalog.tableExists(full_name)
    if table_exists:
        (write_df.write.format("delta").mode("overwrite")
         .option("replaceWhere", f"upc_code = '{UPC}' AND phase = '{PHASE}' AND cycle = '{CYCLE}'")
         .option("mergeSchema", "true")
         .saveAsTable(full_name))
    else:
        (write_df.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true")
         .saveAsTable(full_name))
    return write_df.count()

print("Writing to dual schemas...\n")
print(f"{'Table':<42s} {'dlm_kpi':>10s} {'dlm_postgres':>14s}")
print("-" * 68)

for table_name in OUTPUT_TABLES:
    write_df = OUTPUT_DATAFRAMES.get(table_name, spark.createDataFrame([], TABLE_SCHEMAS[table_name]))

    # dlm_kpi: ALL data
    kpi_rows = write_to_schema(DLM_KPI_NS, write_df, table_name, filter_breach=False)

    # dlm_postgres: breach_flag=1 ONLY
    pg_rows = write_to_schema(DLM_POSTGRES_NS, write_df, table_name, filter_breach=True)

    print(f"  {table_name:<40s} {kpi_rows:>10,} {pg_rows:>14,}")

print(f"\nDone. All {len(OUTPUT_TABLES)} tables written to both schemas.")

# COMMAND ----------

# DBTITLE 1,Unified Validation (current UPC) - Source vs DLM KPI
# ═══════════════════════════════════════════════════════════════════════════════
# UNIFIED VALIDATION — current UPC / Phase / Cycle (widgets), read back from dlm_kpi + dlm_postgres
# Sources are this UPC's own bronze inputs only (BACKUP_BASE, D1_BASE, D2_BASE). Nothing hardcoded.
#   V1 Counts      dlm_postgres rows = dlm_kpi breach rows
#   V2 IDs         breach rows have a valid display_defect_id, others none, unique, child -> parent link
#   V3 Rules       one threshold value per standard, breach_flag vs metric, cracking severity/condition
#   V4 Hierarchy   100m / 1km recomputed from the stored 10m rows (Excel formulas)
#   V5 D1          10m pavement / carriageway vs D1, 100m / 1km pavement (single type or 'Hybrid'), NH, section
#   V6 Backup      10m values vs backup parquet (LWP/RWP, rut L/R, ravelling area, crack count/width, GPS, coverage)
#   V7 D2          100m / 1km values and breach vs D2 deliverable (reference check -> WARN, not FAIL)
# Result: summary table (FAIL first) saved to dlm_kpi.dlm_validation_results; failing rows in VAL_DETAILS.
# ═══════════════════════════════════════════════════════════════════════════════
from datetime import datetime, timezone
VAL_TS = datetime.now(timezone.utc)
DEFECTS = ['roughness', 'rutting', 'ravelling', 'cracking']
LEVEL_M = {'child_10m': 10, 'child_100m': 100, 'parent': 1000}
def _tbl(d, lvl): return 'master_cracking' if (d == 'cracking' and lvl == 'child_10m') else f'master_{d}_{lvl}'
VAL_TABLES = [_tbl(d, l) for d in DEFECTS for l in LEVEL_M] + ['master_pothole']
VAL_ROWS, VAL_DETAILS = [], {}

def _close(a, b, **kw):
    """np.isclose that accepts pandas nullable dtypes (Float64 / Int64 / object) — NA becomes NaN."""
    f = lambda s: pd.to_numeric(pd.Series(s), errors='coerce').astype('float64').to_numpy()
    return np.isclose(f(a), f(b), **kw)

def _record(section, check, scope, checked, failed, detail='', level='FAIL', bad=None):
    status = 'SKIP' if checked == 0 else ('PASS' if failed == 0 else level)
    VAL_ROWS.append({'section': section, 'check': check, 'scope': scope, 'checked': int(checked),
                     'failed': int(failed), 'status': status, 'detail': str(detail)[:500]})
    if failed and bad is not None and len(bad):
        VAL_DETAILS[f'{section} | {check} | {scope}'] = bad.head(50)

def _db(table, ns=DLM_KPI_NS):
    full = f'{ns}.{table}'
    if not spark.catalog.tableExists(full): return None
    return spark.table(full).filter((F.col('upc_code') == UPC) & (F.col('phase') == PHASE)
                                    & (F.col('cycle').cast('string') == str(CYCLE)))

def _m(key): return F.get_json_object('measures', f'$.{key}').cast('double')
_S, _E = F.least('start_chainage', 'end_chainage'), F.greatest('start_chainage', 'end_chainage')

# ── V1 Counts ──────────────────────────────────────────────────────────────────────────────────
for t in VAL_TABLES:
    k, p = _db(t), _db(t, DLM_POSTGRES_NS)
    if k is None:
        _record('V1 Counts', 'table exists in dlm_kpi', t, 1, 1, 'missing'); continue
    r = k.agg(F.count('*').alias('n'), F.sum('breach_flag').alias('b')).collect()[0]
    n, b, pn = r['n'], int(r['b'] or 0), (p.count() if p is not None else 0)
    thr = {x[0]: x[1] for x in k.groupBy('threshold_name').count().collect()}
    _record('V1 Counts', 'dlm_postgres rows = dlm_kpi breach rows', t, max(n, 1), abs(pn - b),
            f'kpi={n:,} | breach={b:,} | postgres={pn:,} | by threshold={thr}')

# ── V2 Display IDs ────────────────────────────────────────────────────────────────────────────────
_TOP, _CHILD = r'^D/\d{6}/N/\d{5,}$', r'^D/\d{6}/N/\d{5,}/\d{4}$'
_has_id = F.col('display_defect_id').isNotNull() & (F.trim('display_defect_id') != '')
for t in VAL_TABLES:
    k = _db(t)
    if k is None: continue
    bad = k.filter(((F.col('breach_flag') == 1) & ~_has_id) | ((F.col('breach_flag') != 1) & _has_id)
                   | (_has_id & ~F.col('display_defect_id').rlike(_TOP) & ~F.col('display_defect_id').rlike(_CHILD)))
    _record('V2 IDs', 'breach rows have valid ID, non-breach none', t, k.count(), bad.count(),
            bad=bad.select('lane', 'start_chainage', 'threshold_name', 'breach_flag', 'display_defect_id').limit(50).toPandas())
    dup = k.filter(_has_id).groupBy('display_defect_id').count().filter('count > 1')
    _record('V2 IDs', 'display_defect_id unique in table', t, k.filter(_has_id).count(), dup.count(), bad=dup.limit(50).toPandas())
for d in ['roughness', 'rutting', 'ravelling', 'cracking']:
    par = _db(_tbl(d, 'parent'))
    if par is None: continue
    par = par.filter(_has_id).select('parent_id', 'threshold_name', F.col('display_defect_id').alias('pdid'))
    for lvl in ['child_100m', 'child_10m']:
        c = _db(_tbl(d, lvl)).filter(F.col('display_defect_id').rlike(_CHILD)) \
            .withColumn('pref', F.regexp_extract('display_defect_id', r'^(D/\d{6}/N/\d+)/', 1))
        bad = c.join(par, ['parent_id', 'threshold_name'], 'left').filter(F.col('pdid').isNull() | (F.col('pdid') != F.col('pref')))
        _record('V2 IDs', 'child ID prefix = its parent ID', _tbl(d, lvl), c.count(), bad.count(),
                bad=bad.select('lane', 'start_chainage', 'threshold_name', 'display_defect_id', 'pdid').limit(50).toPandas())
    c100 = _db(_tbl(d, 'child_100m')).filter(_has_id).select('display_defect_id')
    c10 = _db(_tbl(d, 'child_10m')).filter(_has_id).select('display_defect_id')
    shared = c100.intersect(c10)
    _record('V2 IDs', 'ID unique across 100m and 10m (pending format decision)', d, c10.count(), shared.count(), level='WARN',
            bad=shared.limit(50).toPandas())

# ── V3 Rules ──────────────────────────────────────────────────────────────────────────────────────
_mv, _tv, _bf = F.col('metric_value'), F.col('threshold_value'), F.col('breach_flag')
for d in DEFECTS:
    for lvl in LEVEL_M:
        t = _tbl(d, lvl); k = _db(t)
        if k is None: continue
        tv = k.groupBy('threshold_name').agg(F.countDistinct('threshold_value').alias('n'), F.collect_set('threshold_value').alias('v')).collect()
        _record('V3 Rules', 'one threshold value per standard', t, len(tv), sum(1 for x in tv if x['n'] > 1),
                '; '.join(f"{x['threshold_name']}={x['v']}" for x in tv))
        bad = k.filter(_mv.isNotNull() & (_bf != (_mv >= _tv).cast('int')) & (_bf != (_mv > _tv).cast('int')))
        _record('V3 Rules', 'breach_flag consistent with metric vs threshold', t, k.count(), bad.count(),
                bad=bad.select('lane', 'start_chainage', 'threshold_name', 'metric_value', 'threshold_value', 'breach_flag').limit(50).toPandas())

_w = F.coalesce(_m('avg_corrected_width_mm'), _m('avg_width_mm'), F.lit(0.0))
_exp_sev = F.when(_w < 3, 'None').when(_w < 6, 'Medium').otherwise('High')
_exp_ca = F.when(_mv <= _tv, 'Good').otherwise('Poor')
_exp_irc = (F.when(_mv >= 30, 'Very Poor (>30%)').when(_mv >= 20, 'Poor (20-30%)').when(_mv >= 10, 'Fair (10-20%)')
              .when(_mv >= 5, 'Good (5-10%)').otherwise('Very Good (<5%)'))
_is_ca = F.col('threshold_name') == 'As per CA'
for lvl in LEVEL_M:
    t = _tbl('cracking', lvl); k = _db(t)
    if k is None: continue
    rules = {
        'severity from corrected width (<3 None, 3-6 Medium, >=6 High)': F.col('severity') != _exp_sev,
        'CA condition (<= CA Good, > CA Poor) and road_condition': _is_ca & ((F.col('rating_ca') != _exp_ca) | (F.col('road_condition') != F.col('rating_ca'))),
        'IRC condition bands 5/10/20/30% and road_condition': (F.col('rating_irc') != _exp_irc)
            | (~_is_ca & (F.col('road_condition') != F.col('rating_irc'))),
        'CA breach_flag = Poor': _is_ca & ((_bf == 1) != (F.col('rating_ca') == 'Poor')),
        'condition colour present in measures': F.get_json_object('measures', '$.condition_color').isNull(),
    }
    n = k.count()
    for name, cond in rules.items():
        bad = k.filter(cond)
        _record('V3 Rules', f'cracking: {name}', t, n, bad.count(),
                bad=bad.select('lane', 'start_chainage', 'threshold_name', 'metric_value', 'severity', 'rating_ca', 'rating_irc', 'road_condition').limit(50).toPandas())

# ── V4 Hierarchy + V5 pavement roll-up (100m / 1km from stored 10m rows) ───────────────────────────
_CHILD_MEAS = {'roughness': ['lwp_iri', 'rwp_iri'], 'rutting': ['rut_left_mm', 'rut_right_mm'],
               'ravelling': ['ravelling_area_sqm'], 'cracking': []}
def _ca_pdf(t, meas):
    k = _db(t)
    if k is None: return pd.DataFrame()
    return (k.filter(_is_ca).select('lane', _S.alias('s'), _E.alias('e'), F.col('metric_value').alias('v'), 'area_sqm',
                                    'parent_id', 'pavement_type', 'carriageway', *[_m(x).alias(x) for x in meas]).toPandas())

def _assign(children, parents):
    """Index of the parent row (same lane) whose [s, e) contains each child start; -1 if none."""
    out = np.full(len(children), -1)
    for lane, pg in parents.groupby('lane'):
        pg = pg.sort_values('s'); cm = (children['lane'] == lane).values
        cs = children.loc[cm, 's'].values
        j = np.searchsorted(pg['s'].values, cs, side='right') - 1
        ok = (j >= 0) & (cs < pg['e'].values[np.clip(j, 0, None)])
        out[np.where(cm)[0][ok]] = pg.index.values[j[ok]]
    return out

CHILD_10M_PDF = {}
for d in DEFECTS:
    c10 = _ca_pdf(_tbl(d, 'child_10m'), _CHILD_MEAS[d]); CHILD_10M_PDF[d] = c10
    for lvl in ['child_100m', 'parent']:
        t = _tbl(d, lvl); p = _ca_pdf(t, [])
        if p.empty or c10.empty: _record('V4 Hierarchy', 'recomputed from 10m', t, 0, 0); continue
        c = c10.assign(_p=_assign(c10, p))
        g = c[c['_p'] >= 0].groupby('_p')
        if d == 'roughness':  calc = 630 * (((g['lwp_iri'].mean() + g['rwp_iri'].mean()) / 2) ** 1.12)
        elif d == 'rutting':  calc = (g['rut_left_mm'].mean() + g['rut_right_mm'].mean()) / 2
        elif d == 'ravelling': calc = g['ravelling_area_sqm'].sum() / (3.5 * (p['e'] - p['s'])) * 100
        else:                 calc = g['area_sqm'].sum() / (3.5 * (p['e'] - p['s'])) * 100
        p = p.assign(calc=calc.reindex(p.index), n_10m=g.size().reindex(p.index).fillna(0).astype(int))
        bad = p[~_close(p['v'], p['calc'], rtol=1e-6, atol=1e-4, equal_nan=True)]
        _record('V4 Hierarchy', 'value = Excel formula over its 10m rows', t, len(p), len(bad),
                f'10m rows not inside any {lvl} row: {int((c["_p"] < 0).sum())}', bad=bad[['lane', 's', 'e', 'v', 'calc', 'n_10m']])

# ── V5 D1 road attributes at 10m ─────────────────────────────────────────────────────────────────────────
_JUNK = {'lcms issue', 'uc', 'na', 'n/a', '', 'nan', '<na>', 'none'}
def _d1_intervals(pdf, val_col):
    if pdf.empty or val_col not in pdf.columns: return pd.DataFrame(columns=['lo', 'hi', 'dir', 'val'])
    s = pd.to_numeric(pdf['New NH number StartChainage'], errors='coerce'); e = pd.to_numeric(pdf['New NH number EndChainage'], errors='coerce')
    iv = pd.DataFrame({'lo': np.minimum(s, e), 'hi': np.maximum(s, e),
                       'dir': pdf['Direction'].map(normalize_direction) if 'Direction' in pdf.columns else None, 'val': pdf[val_col].astype(str).str.strip()})
    return iv[~iv['val'].str.lower().isin(_JUNK)].dropna(subset=['lo', 'hi'])

def _d1_value(iv, ch, direction):
    """[lo, hi) match (narrowest, same direction preferred); outside coverage -> nearest interval edge."""
    cand = iv[iv['dir'] == direction] if direction is not None and (iv['dir'] == direction).any() else iv
    hit = cand[(cand['lo'] <= ch) & (ch < cand['hi'])]
    if hit.empty: hit = cand[(cand['lo'] <= ch) & (ch <= cand['hi'])]
    if not hit.empty: return hit.assign(_w=hit['hi'] - hit['lo']).sort_values('_w')['val'].iloc[0], True
    if cand.empty: return None, False
    return cand['val'].iloc[int(np.argmin(np.minimum((cand['lo'] - ch).abs(), (cand['hi'] - ch).abs()).values))], False

_pav_d1, _ = load_matching_parquets(D1_BASE, ['PavementType'])
_cw_d1, _ = load_matching_parquets(D1_BASE, ['CarriagewayType'])
_iv_p, _iv_c = _d1_intervals(_pav_d1, 'PavementType'), _d1_intervals(_cw_d1, 'CarriagewayType')
_iv_c['dir'] = None
# Expected direction comes from the lane (L* = Increasing, R* = Decreasing), NOT from the stored column,
# so a wrong stored direction cannot pick the wrong D1 side and hide / create a pavement mismatch.
def _lane_dir(df):
    d = df['lane'].map(lane_to_direction)
    return d.where(d.notna(), df['direction'].map(normalize_direction))

_r10 = _db('master_roughness_child_10m').filter(_is_ca).select('lane', 'direction', _S.alias('s'), 'pavement_type', 'carriageway', 'nh_number', 'section_code').toPandas()
_r10['dir_exp'] = _lane_dir(_r10)
_keys = _r10[['s', 'dir_exp']].drop_duplicates()
_keys[['d1_pav', 'in_d1']] = pd.DataFrame([_d1_value(_iv_p, s, dr) for s, dr in zip(_keys['s'], _keys['dir_exp'])],
                                          index=_keys.index, columns=['d1_pav', 'in_d1'])
_keys['d1_cw'] = [_d1_value(_iv_c, s, None)[0] for s in _keys['s']]
_r10 = _r10.merge(_keys, on=['s', 'dir_exp'], how='left')
# Pavement at every level: set of D1 types overlapping [s, e) of the row -> 1 type / 'Hybrid'; none -> nearest D1 edge
def _exp_pavement(df):
    types = [set() for _ in range(len(df))]
    s, e, dirs = df['s'].to_numpy(float), df['e'].to_numpy(float), df['dir_exp'].to_numpy()
    for lo, hi, dr, val in _iv_p[['lo', 'hi', 'dir', 'val']].itertuples(index=False):
        side = (dirs == dr) if (_iv_p['dir'] == dr).any() and dr is not None else np.ones(len(df), bool)
        for i in np.where(side & (s < hi) & (e > lo))[0]: types[i].add(val)
    return [('Hybrid' if len(ts) > 1 else next(iter(ts))) if ts else _d1_value(_iv_p, si, dr)[0]
            for ts, si, dr in zip(types, s, dirs)]

for t in [x for x in VAL_TABLES if x != 'master_pothole']:
    if _db(t) is None: continue
    pv = _db(t).filter(_is_ca).select('lane', 'direction', _S.alias('s'), _E.alias('e'), 'pavement_type').toPandas()
    if pv.empty: _record('V5 D1', 'pavement = D1 overlap (1 type / Hybrid)', t, 0, 0); continue
    pv['dir_exp'] = _lane_dir(pv)
    # stored direction must equal the lane direction (genuine check, reported on its own)
    bad_dir = pv[pv['dir_exp'].notna() & (pv['direction'].map(normalize_direction) != pv['dir_exp'])]
    _record('V5 D1', 'direction = lane (L* Increasing / R* Decreasing)', t, len(pv), len(bad_dir),
            f"lanes: {sorted(pv['lane'].dropna().unique().tolist())}", bad=bad_dir[['lane', 's', 'e', 'direction', 'dir_exp']])
    pv['exp_pav'] = _exp_pavement(pv)
    bad = pv[pv['pavement_type'].fillna('') != pv['exp_pav'].fillna('')]
    _record('V5 D1', 'pavement = D1 overlap (1 type / Hybrid)', t, len(pv), len(bad),
            f"Hybrid rows: {int((pv['pavement_type'] == 'Hybrid').sum())}", bad=bad[['lane', 'direction', 'dir_exp', 's', 'e', 'pavement_type', 'exp_pav']])
bad = _r10[_r10['d1_cw'].notna() & (_r10['carriageway'] != _r10['d1_cw'])]
_record('V5 D1', '10m carriageway = D1 CarriagewayType', 'all lanes', len(_r10), len(bad), bad=bad[['lane', 's', 'carriageway', 'd1_cw']])
_nh_exp = standardize_nh_number(_pav_d1['New NH number NHNumber'].dropna().iloc[0]) if 'New NH number NHNumber' in _pav_d1 else None
_sec_exp = str(_pav_d1['SectionCode'].dropna().iloc[0]).strip() if 'SectionCode' in _pav_d1 else None
for col, exp in [('nh_number', _nh_exp), ('section_code', _sec_exp)]:
    bad = _r10[_r10[col].astype(str).str.strip() != str(exp)]
    _record('V5 D1', f'{col} = D1 ({exp})', 'all lanes', len(_r10), len(bad), bad=bad[['lane', 's', col]])

# ── V6 Backup parquet vs DB at 10m ────────────────────────────────────────────────────────────────────────
#   defect: (backup file suffix, [(backup col, DB measure key)], formula for the 10m metric from backup)
_BK = {
    'roughness': ('_Roughness_IRI.parquet', [('LwpIri', 'lwp_iri'), ('RwpIri', 'rwp_iri')],
                  lambda b: 630 * (((b['LwpIri'] + b['RwpIri']) / 2) ** 1.12)),
    'rutting':   ('_Rutting.parquet', [('Rutting Left', 'rut_left_mm'), ('Rutting Right', 'rut_right_mm')],
                  lambda b: (b['Rutting Left'] + b['Rutting Right']) / 2),
    'ravelling': ('_Raveling.parquet', [('Raveling_Area_m2', 'ravelling_area_sqm')],
                  lambda b: b['Raveling_Area_m2'] / LANE_AREA_10M * 100),
    'cracking':  ('_Cracking.parquet', [('crack_count', 'crack_count'), ('avg_width_mm', 'avg_width_mm')], None),
}
for d, (sfx, pairs, formula) in _BK.items():
    bk, _ = load_backup_lane_parquets(BACKUP_BASE, sfx)
    t = _tbl(d, 'child_10m')
    if bk.empty: _record('V6 Backup', 'backup file present', t, 1, 1, sfx); continue
    bk['lane'] = bk['Source_Lane_Folder']
    for c in ['Start_Chainage', 'End_Chainage'] + [x for x, _ in pairs] + ['Area_Sum', 'Start_Latitude', 'Start_Longitude']:
        if c in bk.columns: bk[c] = pd.to_numeric(bk[c], errors='coerce')
    bk['s'] = bk[['Start_Chainage', 'End_Chainage']].min(axis=1).astype(float)
    dbp = (_db(t).filter(_is_ca).select('lane', _S.alias('s'), F.col('metric_value').alias('v'), 'area_sqm', 'start_lat', 'start_long',
                                        *[_m(k).alias('db_' + k) for _, k in pairs]).toPandas())
    j = bk.merge(dbp, on=['lane', 's'], how='outer', indicator=True)
    _record('V6 Backup', 'every backup 10m row in DB and vice versa', t, len(j), int((j['_merge'] != 'both').sum()),
            f"only backup={int((j['_merge'] == 'left_only').sum())}, only DB={int((j['_merge'] == 'right_only').sum())}",
            bad=j[j['_merge'] != 'both'][['lane', 's', '_merge']])
    b = j[j['_merge'] == 'both']
    for bc, k in pairs:
        bad = b[~_close(b[bc].astype(float), b['db_' + k].astype(float), rtol=1e-9, atol=1e-6, equal_nan=True)]
        _record('V6 Backup', f'{k} = backup {bc}', t, len(b), len(bad), bad=bad[['lane', 's', bc, 'db_' + k]])
    if formula is not None:
        exp = pd.to_numeric(formula(b), errors='coerce').astype('float64')   # nullable Float64 -> float64 for np.isclose
        okb = exp.notna()
        bad = b[okb & ~_close(b['v'].astype('float64'), exp, rtol=1e-6, atol=1e-6)]
        _record('V6 Backup', '10m metric = formula on backup values', t, int(okb.sum()), len(bad), bad=bad[['lane', 's', 'v']].assign(expected=exp[bad.index]))
    else:   # cracking: calibration may only reduce the backup area (factor <= 1)
        bad = b[b['area_sqm'] > b['Area_Sum'].fillna(0) + 1e-6]
        _record('V6 Backup', 'calibrated crack area <= backup Area_Sum', t, len(b), len(bad), bad=bad[['lane', 's', 'Area_Sum', 'area_sqm']])
    if d == 'roughness':
        _f = lambda s: s.astype('float64')
        bad = b[~_close(_f(b['start_lat']), _f(b['Start_Latitude']), atol=1e-7, equal_nan=True) | ~_close(_f(b['start_long']), _f(b['Start_Longitude']), atol=1e-7, equal_nan=True)]
        _record('V6 Backup', 'GPS start lat/long = backup', t, len(b), len(bad), bad=bad[['lane', 's', 'start_lat', 'Start_Latitude', 'start_long', 'Start_Longitude']])

# ── V7 D2 deliverable vs DB at 100m / 1km (reference -> WARN) ─────────────────────────────────────────────────
_D2_PAT = {'roughness': ('roughness bi', ()), 'rutting': ('rut depth', ()), 'cracking': ('crack area', ()), 'ravelling': ('area (', ('crack',))}
def _d2_col(df, lane, d, tag):
    """Lane column for defect d; tag='(1000m)'/'(100m)' or None for files without interval tags."""
    pat, excl = _D2_PAT[d]
    for c in df.columns:
        cl = c.lower()
        if not cl.startswith(lane.lower() + ' ') or pat not in cl or cl.endswith('_2') or any(x in cl for x in excl): continue
        if (tag and tag in cl) or (tag is None and '(100m)' not in cl and '(1000m)' not in cl): return c
    return None

def _d2_ref(level_m):
    """(dataframe, chainage col, tag, label) of the D2 reference for 100m or 1000m."""
    if level_m == 1000: return _d2_ca, _d2ch_ca, ('(1000m)' if '(1000m)' in ' '.join(_d2_ca.columns) else None), 'D2 CA'
    if _d2_irc is not None and any('(100m)' in c for c in _d2_irc.columns): return _d2_irc, _d2ch_irc, '(100m)', 'D2 IRC'
    if _ca_interval == '100m' and '(100m)' not in ' '.join(_d2_ca.columns): return _d2_ca, _d2ch_ca, None, 'D2 CA'
    return None, None, None, None

for d in DEFECTS:
    for lvl, L in [('child_100m', 100), ('parent', 1000)]:
        t = _tbl(d, lvl); ref, ch, tag, lab = _d2_ref(L)
        if ref is None: _record('V7 D2', 'value close to D2 (1% / 0.05)', t, 0, 0, 'no D2 reference at this level'); continue
        if L == 1000 and tag is None and _ca_interval == '100m':
            _record('V7 D2', 'value close to D2 (1% / 0.05)', t, 0, 0, 'D2 CA is 100m only'); continue
        dbp = _ca_pdf(t, ['ravelling_area_sqm'] if d == 'ravelling' else [])
        rows, bads = [], []
        for lane in sorted(dbp['lane'].dropna().unique()):
            col = _d2_col(ref, lane, d, tag)
            if col is None: continue
            use_area = d == 'ravelling' and ('sq' in col.lower())
            d2v = pd.to_numeric(ref[col], errors='coerce').groupby((ref[ch] // L * L).astype(int)).mean()
            x = dbp[dbp['lane'] == lane].assign(bin=lambda z: (z['s'] // L * L).astype(int))
            x['dbv'] = x['ravelling_area_sqm'] if use_area else x['v']
            x['d2v'] = x['bin'].map(d2v)
            x = x.dropna(subset=['d2v'])
            close = _close(x['dbv'], x['d2v'], rtol=0.01, atol=0.05)
            rows.append((lane, len(x), int((~close).sum()), col)); bads.append(x[~close].assign(d2_col=col))
        n, f = sum(r[1] for r in rows), sum(r[2] for r in rows)
        _record('V7 D2', f'value close to {lab} (1% / 0.05)', t, n, f, level='WARN',
                detail='; '.join(f'{r[0]}: {r[2]}/{r[1]} off' for r in rows) or 'no matching D2 column',
                bad=pd.concat(bads)[['lane', 's', 'e', 'dbv', 'd2v', 'd2_col']] if bads else None)

# ── Summary + save ───────────────────────────────────────────────────────────────────────────────────────
VAL_SUMMARY = pd.DataFrame(VAL_ROWS)
VAL_SUMMARY['_o'] = VAL_SUMMARY['status'].map({'FAIL': 0, 'WARN': 1, 'SKIP': 2, 'PASS': 3})
VAL_SUMMARY = VAL_SUMMARY.sort_values(['_o', 'section', 'scope']).drop(columns='_o').reset_index(drop=True)
_save = VAL_SUMMARY.assign(upc_code=UPC, zone=ZONE, phase=PHASE, cycle=str(CYCLE),
                           validation_ts=pd.Timestamp(VAL_TS).tz_localize(None))
VAL_TABLE = f'{DLM_KPI_NS}.dlm_validation_results'
_w = spark.createDataFrame(_save).write.format('delta').mode('overwrite')
if spark.catalog.tableExists(VAL_TABLE):
    _w = _w.option('replaceWhere', f"upc_code = '{UPC}' AND phase = '{PHASE}' AND cycle = '{CYCLE}'")
_w.saveAsTable(VAL_TABLE)

print(f"UPC {UPC} | Zone {ZONE} Phase {PHASE} Cycle {CYCLE} | {VAL_SUMMARY['status'].value_counts().to_dict()} | saved to {VAL_TABLE}")
display(VAL_SUMMARY)
for _k, _v in VAL_DETAILS.items():
    print(f"\n── {_k}  (first {len(_v)} rows)")
    display(_v)

# COMMAND ----------

# DBTITLE 1,Build and Write master_chainage — 10m Chainage Reference Table
# ═══════════════════════════════════════════════════════════════════════════════
# BUILD master_chainage — 10m Chainage Reference Table
# One row per 10m segment per lane (chainage-level, NOT defect-level)
# ═══════════════════════════════════════════════════════════════════════════════

# ── Step 1: Derive 10m chainage spine from RAW BACKUP data ───────────────────
# Use raw parquets (not OUTPUT_DATAFRAMES) to get the TRUE full chainage range
# per lane — avoids missing edge segments that defect processing might trim.
_ch_ranges = []
for _raw_df, _label in [
    (df_roughness_raw, 'roughness'), (df_cracking_raw, 'cracking'),
    (df_rutting_raw, 'rutting'), (df_ravelling_raw, 'ravelling')]:
    if _raw_df is None or _raw_df.empty:
        continue
    # Fuzzy-find lane and chainage columns in the raw parquet
    _cols = list(_raw_df.columns)
    _lane_c = next((c for c in _cols if c.lower() in ('lane', 'lanes')), None)
    _start_c = next((c for c in _cols if 'start' in c.lower() and 'chainage' in c.lower()), None)
    _end_c = next((c for c in _cols if 'end' in c.lower() and 'chainage' in c.lower()), None)
    if _lane_c is None or _start_c is None:
        continue
    _sub = _raw_df[[_lane_c, _start_c]].copy()
    _sub.columns = ['Lane', 'ch']
    if _end_c:
        _sub_end = _raw_df[[_lane_c, _end_c]].copy()
        _sub_end.columns = ['Lane', 'ch']
        _sub = pd.concat([_sub, _sub_end], ignore_index=True)
    _sub['ch'] = pd.to_numeric(_sub['ch'], errors='coerce')
    _sub = _sub.dropna(subset=['ch'])
    _sub['Lane'] = _sub['Lane'].map(normalize_lane)          # 'R-1' and 'R1' become one lane
    _sub = _sub.dropna(subset=['Lane'])
    _grp = _sub.groupby('Lane').agg(min_ch=('ch', 'min'), max_ch=('ch', 'max')).reset_index()
    if not _grp.empty:
        _ch_ranges.append(_grp)

# Also include D1 road reference data (PAVEMENT_LOOKUP, LRMS_LOOKUP)
# to capture full road extent that raw defect files might not cover.
if not PAVEMENT_LOOKUP.empty:
    # Every lane found in the backup folders / raw data (L1..L8, R1..R8) is extended to the D1 length of ITS side
    _all_l = sorted(set(LANES) | {l for g in _ch_ranges for l in g['Lane']})
    for direction, lane_prefix in [('Increasing', [l for l in _all_l if lane_to_direction(l) == 'Increasing']),
                                   ('Decreasing', [l for l in _all_l if lane_to_direction(l) == 'Decreasing'])]:
        if not lane_prefix: continue
        _pav = PAVEMENT_LOOKUP.copy()
        if 'direction' in _pav.columns:
            _dir_mask = _pav['direction'].fillna('').str.lower() == direction.lower()
            _pav_dir = _pav[_dir_mask] if _dir_mask.any() else _pav
        else:
            _pav_dir = _pav
        if _pav_dir.empty: continue
        _pmin = int(_pav_dir['start_chainage'].min())
        _pmax = int(_pav_dir['end_chainage'].max())
        for _ln in lane_prefix:
            _ch_ranges.append(pd.DataFrame({'Lane': [_ln], 'min_ch': [_pmin], 'max_ch': [_pmax]}))

_lane_bounds = pd.concat(_ch_ranges, ignore_index=True).groupby('Lane').agg(
    min_ch=('min_ch', 'min'), max_ch=('max_ch', 'max')).reset_index()

# Snap bounds to 10m grid
_lane_bounds['min_ch'] = (_lane_bounds['min_ch'] // CHILD_10M * CHILD_10M).astype(int)
_lane_bounds['max_ch'] = (_lane_bounds['max_ch'] // CHILD_10M * CHILD_10M).astype(int)

# Use PER-LANE bounds — each lane's spine covers only chainages where
# data actually exists for that lane.  No global alignment: R-lanes may
# start later than L-lanes if the survey data begins there.
print(f"Per-lane raw bounds (data-driven):")
for _, lb in _lane_bounds.iterrows():
    print(f"  {lb['Lane']}: {int(lb['min_ch']):>6d} – {int(lb['max_ch']):>6d}")

print("\nFinal lane bounds for chainage spine (per-lane, no global override):")
for _, lb in _lane_bounds.iterrows():
    segs = (lb['max_ch'] - lb['min_ch']) // CHILD_10M + 1
    print(f"  {lb['Lane']}: {int(lb['min_ch']):>6d} – {int(lb['max_ch']) + CHILD_10M:>6d}  ({segs:,} segments)")

# ── Step 2: Generate spine rows with road context (pavement_type etc.) ───────
spine_rows = []
for _, lb in _lane_bounds.iterrows():
    lane_val = lb['Lane']
    direction_val = lane_to_direction(lane_val)
    lo = int((lb['min_ch'] // CHILD_10M) * CHILD_10M)
    hi = int((lb['max_ch'] // CHILD_10M) * CHILD_10M)
    for ch in range(lo, hi + CHILD_10M, CHILD_10M):
        road_ctx = get_road_context(ch, ch + CHILD_10M, lane=lane_val, direction=direction_val)
        spine_rows.append({
            'upc': UPC,
            'upc_code': UPC,
            'source_system': source_system,
            'defect_detected_on': TRACKER_CTX.get('survey_end_date'),
            'start_chainage': float(ch),
            'end_chainage': float(ch + CHILD_10M),
            'phase': PHASE,
            'cycle': int(CYCLE),
            'zone': ZONE,
            'lane': lane_val,
            'direction': direction_val,
            'nh_number': first_non_null(road_ctx.get('nh_number')),
            'section_code': first_non_null(road_ctx.get('section_code')),
            'pavement_type': first_non_null(road_ctx.get('pavement_type')),
            'carriageway': first_non_null(road_ctx.get('carriageway')),
        })

chainage_pdf = pd.DataFrame(spine_rows)
print(f"\nmaster_chainage: {len(chainage_pdf):,} rows")
print(f"Lanes: {sorted(chainage_pdf['lane'].unique().tolist())}")
for lane in sorted(chainage_pdf['lane'].unique()):
    sub = chainage_pdf[chainage_pdf['lane'] == lane]
    print(f"  {lane}: {int(sub['start_chainage'].min()):>6d} – {int(sub['end_chainage'].max()):>6d}  ({len(sub):,} segments)")
print(f"Pavement types: {chainage_pdf['pavement_type'].value_counts().to_dict()}")

# ── Step 3: Create Spark DataFrame with typed schema ─────────────────────────
chainage_schema = T.StructType([
    T.StructField('upc',                 T.StringType(),    True),
    T.StructField('upc_code',            T.StringType(),    True),
    T.StructField('source_system',       T.StringType(),    True),
    T.StructField('defect_detected_on',  T.DateType(),      True),
    T.StructField('start_chainage',      T.DoubleType(),    True),
    T.StructField('end_chainage',        T.DoubleType(),    True),
    T.StructField('phase',               T.StringType(),    True),
    T.StructField('cycle',               T.IntegerType(),   True),
    T.StructField('zone',                T.StringType(),    True),
    T.StructField('lane',                T.StringType(),    True),
    T.StructField('direction',           T.StringType(),    True),
    T.StructField('nh_number',           T.StringType(),    True),
    T.StructField('section_code',        T.StringType(),    True),
    T.StructField('pavement_type',       T.StringType(),    True),
    T.StructField('carriageway',         T.StringType(),    True),
])

chainage_sdf = pandas_to_spark_with_schema(chainage_pdf, chainage_schema)

# ── Step 4: Write to dlm_kpi.master_chainage (replaceWhere for idempotency) ──
full_name = f"{DLM_KPI_NS}.master_chainage"
table_exists = spark.catalog.tableExists(full_name)
if table_exists:
    (chainage_sdf.write.format('delta').mode('overwrite')
     .option('replaceWhere', f"upc_code = '{UPC}' AND phase = '{PHASE}' AND cycle = '{CYCLE}'")
     .saveAsTable(full_name))
else:
    (chainage_sdf.write.format('delta').mode('overwrite')
     .option('overwriteSchema', 'true')
     .saveAsTable(full_name))

written = spark.table(full_name).filter(f"upc_code = '{UPC}'").count()
print(f"\n✅ Written {written:,} rows to {full_name}")
display(spark.table(full_name).filter(f"upc_code = '{UPC}'").orderBy('lane', 'start_chainage').limit(10))

# COMMAND ----------

# DBTITLE 1,Truncate All DLM Tables
# # ═══════════════════════════════════════════════════════════════════════════════
# # TRUNCATE ALL TABLES IN BOTH DLM SCHEMAS
# # ═══════════════════════════════════════════════════════════════════════════════
# schemas_to_truncate = [DLM_KPI_NS, DLM_POSTGRES_NS]

# for schema_ns in schemas_to_truncate:
#     print(f"\n{'='*60}")
#     print(f"Schema: {schema_ns}")
#     print(f"{'='*60}")
#     tables = [t.name for t in spark.catalog.listTables(schema_ns)]
#     if not tables:
#         print("  No tables found.")
#         continue
#     for tbl in sorted(tables):
#         full_name = f"{schema_ns}.{tbl}"
#         try:
#             spark.sql(f"TRUNCATE TABLE {full_name}")
#             print(f"  TRUNCATED: {full_name}")
#         except Exception as e:
#             print(f"  FAILED:    {full_name} — {e}")

# print(f"\nDone. All tables in {' and '.join(schemas_to_truncate)} have been truncated.")

# COMMAND ----------

# DBTITLE 1,Export Validation Results to Excel (all validated UPCs)
# ═══════════════════════════════════════════════════════════════════════════════
# EXPORT — unified validation results for every validated UPC (+ failing rows of the current run)
#   Sheet 'summary'      : latest run per UPC from dlm_kpi.dlm_validation_results
#   Sheet 'by_upc'       : PASS / WARN / FAIL / SKIP counts per UPC
#   Sheets 'detail_n'    : failing rows of the current UPC (VAL_DETAILS from the unified validation cell)
# ═══════════════════════════════════════════════════════════════════════════════
import tempfile, shutil
OUTPUT_DIR = "/Volumes/nhai_dhms/silver_layer/validation_reports"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_PATH = f"{OUTPUT_DIR}/DLM_Validation_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"

_vr = spark.table(f"{DLM_KPI_NS}.dlm_validation_results").toPandas()
_by_upc = _vr.pivot_table(index=['upc_code', 'zone', 'phase', 'cycle'], columns='status', values='check',
                          aggfunc='count', fill_value=0).reset_index()
_tmp = tempfile.mktemp(suffix='.xlsx')
with pd.ExcelWriter(_tmp, engine='openpyxl') as xw:
    _vr.sort_values(['upc_code', 'status', 'section']).to_excel(xw, sheet_name='summary', index=False)
    _by_upc.to_excel(xw, sheet_name='by_upc', index=False)
    for i, (k, v) in enumerate(globals().get('VAL_DETAILS', {}).items(), 1):
        v.assign(check=k, upc_code=UPC).to_excel(xw, sheet_name=f'detail_{i}'[:31], index=False)
shutil.copy(_tmp, OUTPUT_PATH)
print(f"Saved {OUTPUT_PATH}")
display(_by_upc)

# COMMAND ----------

# DBTITLE 1,Pavement Type Analysis - Method
# MAGIC %md
# MAGIC ## Pavement Type Analysis (10m / 100m / 1km, Hybrid)
# MAGIC
# MAGIC Read-only analysis of the current widget UPC / PHASE / CYCLE from `nhai_dhms.dlm_kpi`. Needs cells 3-7 run first (uses `PAVEMENT_LOOKUP` = D1 PavementType, main carriageway).
# MAGIC
# MAGIC **How rows are built (Map Defects cell):**
# MAGIC * **10m**: each backup-parquet record is cut onto a 10m grid (first piece keeps its real start). Metric is the raw 10m value (roughness: Lane IRI = (LWP+RWP)/2, BI = 630 x IRI^1.12; rutting: (L+R)/2; ravelling: area / 35 x 100; cracking: D2-calibrated capped area / 35 x 100).
# MAGIC * **100m / 1km**: raw records grouped by lane and `floor(start / bin) x bin` (bin = 100 or 1000); start = min, end = max of the group. Roughness BI is recomputed from averaged LWP/RWP IRI, rutting = (avg L + avg R)/2, ravelling and cracking = SUM(area) / (3.5 x length) x 100.
# MAGIC * **Pavement type at every level**: D1 intervals overlapping the row's own [start, end) in the same direction: one type gives that type, more than one gives `Hybrid`, none (outside D1) keeps the nearest D1 edge value. 100m / 1km pavement is derived from D1 directly, not from the 10m children.
# MAGIC
# MAGIC **Checks below:** D1 runs and boundaries, contiguous 10m pavement ranges per lane, row counts per level and type, and Hybrid validity (stored value vs the 10m children it contains, and vs a Spark re-derivation from D1), with the length split inside every Hybrid row.

# COMMAND ----------

# DBTITLE 1,Pavement Analysis: D1 Runs and 10m Ranges
# ═══════════════════════════════════════════════════════════════════════════════
# PAVEMENT ANALYSIS (1/2) — D1 pavement runs, 10m pavement ranges, counts per level
# ═══════════════════════════════════════════════════════════════════════════════
_KPI_FILTER = ((F.col('upc_code') == UPC) & (F.col('phase').cast('string') == str(PHASE))
               & (F.col('cycle').cast('string') == str(CYCLE)))
PAV_TABLES = {
    'roughness': ['master_roughness_child_10m', 'master_roughness_child_100m', 'master_roughness_parent'],
    'rutting':   ['master_rutting_child_10m', 'master_rutting_child_100m', 'master_rutting_parent'],
    'ravelling': ['master_ravelling_child_10m', 'master_ravelling_child_100m', 'master_ravelling_parent'],
    'cracking':  ['master_cracking', 'master_cracking_child_100m', 'master_cracking_parent'],
}
PAV_LEVELS = ['10m', '100m', '1km']

def pav_load(tbl):
    df = spark.table(f"{DLM_KPI_NS}.{tbl}").filter(_KPI_FILTER)
    df = df.select('lane',
                   F.least(F.col('start_chainage').cast('double'), F.col('end_chainage').cast('double')).alias('s'),
                   F.greatest(F.col('start_chainage').cast('double'), F.col('end_chainage').cast('double')).alias('e'),
                   'pavement_type', F.col('metric_value').cast('double').alias('metric_value'))
    # master_cracking holds a CA and an IRC copy of each 10m segment -> one row per segment
    return df.dropDuplicates(['lane', 's', 'e']) if tbl == 'master_cracking' else df

# ── D1 PavementType intervals -> contiguous runs + type-change boundaries ────
_d1 = PAVEMENT_LOOKUP.copy()
_d1['lo'] = np.minimum(_d1['start_chainage'], _d1['end_chainage']).astype(float)
_d1['hi'] = np.maximum(_d1['start_chainage'], _d1['end_chainage']).astype(float)
_d1['pavement_type'] = _d1['pavement_type'].astype(str).str.strip()
_d1['direction'] = _d1['direction'].fillna('').astype(str) if 'direction' in _d1.columns else ''
_d1 = _d1.sort_values(['direction', 'lo', 'hi']).reset_index(drop=True)
D1_DIRS = sorted(_d1['direction'].unique().tolist())
print(f"UPC {UPC} | Phase {PHASE} | Cycle {CYCLE}")
print(f"D1 PavementType: {len(_d1)} intervals | directions: {D1_DIRS} | types: {_d1['pavement_type'].value_counts().to_dict()}")

_runs, _overlaps = [], []
D1_BOUNDARIES = {}
for dirv, g in _d1.groupby('direction'):
    a = g[['lo', 'hi', 'pavement_type']].to_numpy(object)
    for i in range(len(a)):                      # same-direction intervals of different type that overlap
        for j in range(i + 1, len(a)):
            if a[j][0] >= a[i][1]: break
            if a[i][2] != a[j][2]:
                _overlaps.append({'direction': dirv, 'a': f"{a[i][2]} {a[i][0]:.0f}-{a[i][1]:.0f}",
                                  'b': f"{a[j][2]} {a[j][0]:.0f}-{a[j][1]:.0f}"})
    cur = None
    for lo, hi, typ in a:
        if cur and typ == cur['pavement_type'] and lo <= cur['run_end']:
            cur['run_end'] = max(cur['run_end'], hi); cur['d1_intervals'] += 1
        else:
            if cur: _runs.append(cur)
            cur = {'direction': dirv, 'pavement_type': typ, 'run_start': lo, 'run_end': hi, 'd1_intervals': 1}
    if cur: _runs.append(cur)
D1_RUNS = pd.DataFrame(_runs)
D1_RUNS['length_m'] = D1_RUNS['run_end'] - D1_RUNS['run_start']
for dirv, g in D1_RUNS.groupby('direction'):
    g = g.sort_values('run_start').reset_index(drop=True)
    D1_BOUNDARIES[dirv] = [float(g.loc[k, 'run_start']) for k in range(1, len(g))
                           if g.loc[k, 'pavement_type'] != g.loc[k - 1, 'pavement_type']]
print(f"D1 type-change boundaries: { {k: v for k, v in D1_BOUNDARIES.items()} }")
print(f"Same-direction overlapping D1 intervals of different type: {len(_overlaps)}")
if _overlaps: display(pd.DataFrame(_overlaps))
print("\nD1 pavement runs (contiguous same-type intervals merged):")
display(D1_RUNS)

# ── 10m pavement ranges: consecutive 10m rows with the same pavement type, per lane ──
_w = Window.partitionBy('lane').orderBy('s', 'e')
def pavement_runs(df):
    prev_p, prev_e = F.lag('pavement_type').over(_w), F.lag('e').over(_w)
    new = F.when(prev_e.isNull() | (F.col('s') > prev_e) | ~F.col('pavement_type').eqNullSafe(prev_p), 1).otherwise(0)
    return (df.withColumn('_new', new)
              .withColumn('_run', F.sum('_new').over(_w.rowsBetween(Window.unboundedPreceding, 0)))
              .groupBy('lane', '_run', 'pavement_type')
              .agg(F.min('s').alias('range_start'), F.max('e').alias('range_end'), F.count('*').alias('n_10m_rows'))
              .withColumn('length_m', F.col('range_end') - F.col('range_start')).drop('_run'))

PAV_10M_RANGES = None
for d, tbls in PAV_TABLES.items():
    r = pavement_runs(pav_load(tbls[0])).withColumn('defect', F.lit(d))
    PAV_10M_RANGES = r if PAV_10M_RANGES is None else PAV_10M_RANGES.unionByName(r)
PAV_10M_RANGES = PAV_10M_RANGES.select('defect', 'lane', 'pavement_type', 'range_start', 'range_end', 'length_m', 'n_10m_rows')
print("\n10m pavement ranges per defect / lane:")
display(PAV_10M_RANGES.orderBy('defect', 'lane', 'range_start'))

# ── Rows and length per level and pavement type ─────────────────────────────
_cnt = []
for d, tbls in PAV_TABLES.items():
    for lvl, t in zip(PAV_LEVELS, tbls):
        for row in (pav_load(t).groupBy('pavement_type')
                    .agg(F.count('*').alias('rows'), F.sum(F.col('e') - F.col('s')).alias('length_m')).collect()):
            _cnt.append({'defect': d, 'level': lvl, 'pavement_type': row['pavement_type'] or '(null)',
                         'rows': row['rows'], 'length_m': row['length_m']})
PAV_LEVEL_COUNTS = pd.DataFrame(_cnt)
print("\nRows per level and pavement type:")
display(PAV_LEVEL_COUNTS.pivot_table(index=['defect', 'level'], columns='pavement_type', values='rows',
                                     aggfunc='sum', fill_value=0).reset_index())

# COMMAND ----------

# DBTITLE 1,Pavement Analysis: Hybrid Validation
# ═══════════════════════════════════════════════════════════════════════════════
# PAVEMENT ANALYSIS (2/2) — is every Hybrid / single-type label right?
#   exp_children : distinct pavement types of the stored 10m rows overlapping the 100m / 1km row
#   exp_d1       : Spark re-derivation from D1 (overlap of row's own range, same direction)
#   rule         : 0 types -> n/a (outside D1), >1 type or a Hybrid child -> 'Hybrid', else that type
# ═══════════════════════════════════════════════════════════════════════════════
from collections import defaultdict

_d1_sdf = spark.createDataFrame(_d1[['direction', 'lo', 'hi', 'pavement_type']]
                                .rename(columns={'direction': 'd1_dir', 'lo': 'd1_lo', 'hi': 'd1_hi', 'pavement_type': 'd1_typ'}))
_lane_dir = F.create_map(*[F.lit(x) for kv in LANE_TO_DIRECTION.items() for x in kv])

def _expected(arr):
    return (F.when(F.size(arr) == 0, F.lit(None).cast('string'))
             .when((F.size(arr) > 1) | F.array_contains(arr, 'Hybrid'), F.lit('Hybrid'))
             .otherwise(arr[0]))

def _d1_types(par):
    return (par.withColumn('_ld', _lane_dir[F.upper(F.trim(F.col('lane')))])
               .join(F.broadcast(_d1_sdf), (F.col('d1_lo') < F.col('e')) & (F.col('d1_hi') > F.col('s'))
                     & F.when(F.col('_ld').isin(D1_DIRS), F.col('_ld') == F.col('d1_dir')).otherwise(F.lit(True)), 'left')
               .withColumn('_ol', F.least('d1_hi', 'e') - F.greatest('d1_lo', 's'))
               .groupBy('lane', 's', 'e')
               .agg(F.array_sort(F.collect_set('d1_typ')).alias('d1_types'),
                    F.collect_list(F.struct(F.col('d1_typ').alias('t'), F.col('_ol').alias('ol'))).alias('_parts')))

def _child_types(par, kids):
    k = kids.select(F.col('lane').alias('k_lane'), F.col('s').alias('cs'), F.col('e').alias('ce'), F.col('pavement_type').alias('cp'))
    return (par.join(k, (F.col('lane') == F.col('k_lane')) & (F.col('cs') < F.col('e')) & (F.col('ce') > F.col('s')), 'left')
               .groupBy('lane', 's', 'e')
               .agg(F.array_sort(F.collect_set('cp')).alias('child_types'), F.count('cs').alias('n_children')))

_summary, _details = [], []
for d, tbls in PAV_TABLES.items():
    kids = pav_load(tbls[0])
    for lvl, t in zip(PAV_LEVELS, tbls):
        par = pav_load(t)
        res = par.join(_d1_types(par), ['lane', 's', 'e'], 'left').withColumn('exp_d1', _expected(F.col('d1_types')))
        if lvl == '10m':
            res = res.withColumn('child_types', F.array().cast('array<string>')).withColumn('n_children', F.lit(None).cast('long')) \
                     .withColumn('exp_children', F.lit(None).cast('string'))
        else:
            res = res.join(_child_types(par, kids), ['lane', 's', 'e'], 'left').withColumn('exp_children', _expected(F.col('child_types')))
        res = (res.withColumn('ok_d1', F.col('exp_d1').isNull() | F.col('pavement_type').eqNullSafe(F.col('exp_d1')))
                  .withColumn('ok_children', F.col('exp_children').isNull() | F.col('pavement_type').eqNullSafe(F.col('exp_children'))))
        s = res.agg(F.count('*').alias('rows'),
                    F.sum(F.when(F.col('pavement_type') == 'Hybrid', 1).otherwise(0)).alias('stored_hybrid'),
                    F.sum(F.when(F.col('exp_d1') == 'Hybrid', 1).otherwise(0)).alias('d1_hybrid'),
                    F.sum(F.when(F.col('exp_d1').isNull(), 1).otherwise(0)).alias('outside_d1_nearest_edge'),
                    F.sum(F.when(~F.col('ok_d1'), 1).otherwise(0)).alias('mismatch_vs_d1'),
                    F.sum(F.when(~F.col('ok_children'), 1).otherwise(0)).alias('mismatch_vs_children')).collect()[0].asDict()
        _summary.append({'defect': d, 'level': lvl, 'table': t, **s})
        det = res.filter((F.col('pavement_type') == 'Hybrid') | (F.col('exp_d1') == 'Hybrid') | ~F.col('ok_d1') | ~F.col('ok_children')).toPandas().rename(columns={'_parts': 'd1_parts'})
        for r in det.itertuples(index=False):
            parts = defaultdict(float)
            for p in (r.d1_parts if r.d1_parts is not None else []):
                if p['t'] is not None and p['ol'] is not None: parts[p['t']] += max(float(p['ol']), 0.0)
            tot = sum(parts.values())
            dom_t, dom_l = max(parts.items(), key=lambda kv: kv[1]) if parts else (None, 0.0)
            ldir = LANE_TO_DIRECTION.get(str(r.lane).strip().upper())
            bnds = D1_BOUNDARIES.get(ldir) if ldir in D1_BOUNDARIES else sorted({b for v in D1_BOUNDARIES.values() for b in v})
            _details.append({
                'defect': d, 'level': lvl, 'lane': r.lane, 'start': r.s, 'end': r.e, 'length_m': r.e - r.s,
                'stored_pavement': r.pavement_type, 'exp_d1': r.exp_d1, 'exp_children': r.exp_children,
                'child_types': ', '.join(r.child_types) if r.child_types is not None else None, 'n_children': r.n_children,
                'd1_split': ' / '.join(f"{k} {v:.0f}m" for k, v in sorted(parts.items(), key=lambda kv: -kv[1])),
                'dominant_type': dom_t, 'dominant_share_pct': round(dom_l / tot * 100, 1) if tot else None,
                'd1_boundaries_inside': ', '.join(f"{b:.0f}" for b in (bnds or []) if r.s < b < r.e),
                'metric_value': r.metric_value, 'ok_d1': r.ok_d1, 'ok_children': r.ok_children,
            })

PAV_SUMMARY = pd.DataFrame(_summary)
PAV_HYBRID_DETAIL = pd.DataFrame(_details)
print("Pavement-type validation per defect / level:")
display(PAV_SUMMARY)
print(f"\nHybrid / mismatch rows: {len(PAV_HYBRID_DETAIL)}")
if not PAV_HYBRID_DETAIL.empty:
    display(PAV_HYBRID_DETAIL.sort_values(['defect', 'level', 'lane', 'start']))
    _h = PAV_HYBRID_DETAIL[PAV_HYBRID_DETAIL['stored_pavement'] == 'Hybrid']
    if not _h.empty:
        print("\nHybrid rows by level: count and dominant-type share (min / median / max %)")
        display(_h.groupby(['defect', 'level']).agg(rows=('start', 'size'),
                                                   min_share=('dominant_share_pct', 'min'),
                                                   median_share=('dominant_share_pct', 'median'),
                                                   max_share=('dominant_share_pct', 'max')).reset_index())
_bad = int(PAV_SUMMARY['mismatch_vs_d1'].sum() + PAV_SUMMARY['mismatch_vs_children'].sum())
print(f"\nRESULT: {'PASS' if _bad == 0 else 'FAIL'} — {_bad} pavement-type mismatches across all levels")