# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

# DBTITLE 1,Configuration and Imports
# ═══════════════════════════════════════════════════════════════════════════════
# SILVER LAYER — Delta Tables Pipeline (Parent-Child Defect Clustering)
# ═══════════════════════════════════════════════════════════════════════════════
# UPC: N_02016_01012_GJ | Zone B | Phase 1 | Cycle 2
# Schema: nhai_dhms.silver_layer_nsv_schema2
#
# Output: 11 Delta tables (parent / child_100m / child_10m per defect)
#
#   Defect       Parent   Child_100m   Child_10m   Notes
#   ─────────    ──────   ──────────   ─────────   ──────────────────────────────
#   Pothole        —          —         3,654      Point defects from D4
#   Cracking       —          —         3,031      All breach CA ≥5%
#   Roughness     42         204          542      All breach IRC (IRI>4.5)
#   Rutting        2           2            2      Breach CA ≥20mm
#   Ravelling     49         458        4,567      No CA violation (breach_flag=0)
#
# Ravelling: Full survey data retained (4,567 rows at 10m) even though no
# segment exceeds the 25 sq.m. CA threshold. breach_flag=0 on all rows.
# Filter on breach_flag=1 to get only violating defects; keep breach_flag=0
# if complete survey coverage is needed.
#
# Columns added from D1: carriageway, direction, pavement_type
# Affected length columns: affected_length, total_length,
#                          affected_length_ca, affected_length_irc
#o
# Source strategy:
#   * D2 thresholds / comparison → Source=NSV deliverable path
#   * D4 potholes                → Source=NSV deliverable path when present
#   * Fallback potholes          → Source=NSV_Backup lane folders
#   * All other defects          → Source=NSV_Backup lane folders
# ═══════════════════════════════════════════════════════════════════════════════

import pandas as pd
import numpy as np
import os
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# ── Pipeline Configuration ───────────────────────────────────────────────────
RUN_DATE = datetime.today().date()

# Accept all key parameters as widgets (for parallel orchestration)
dbutils.widgets.text("ZONE", "D")
dbutils.widgets.text("PHASE", "1")
dbutils.widgets.text("CYCLE", "2")
dbutils.widgets.text("UPC", "N_02004_29002_AP")

ZONE  = dbutils.widgets.get("ZONE")
PHASE = dbutils.widgets.get("PHASE")
CYCLE = dbutils.widgets.get("CYCLE")
UPC   = dbutils.widgets.get("UPC")

# Target catalog/schema
CATALOG = "nhai_dhms"
SCHEMA = "silver_layer"

# ── Data Paths ───────────────────────────────────────────────────────────────
BRONZE_ROOT = "/Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output"


def _find_latest_date_partition(base_path):
    """Walk year=/month=/day= subdirectories and return the latest partition path."""
    if not os.path.exists(base_path):
        return None
    for y in sorted(os.listdir(base_path), reverse=True):
        if not y.startswith("year="):
            continue
        yp = os.path.join(base_path, y)
        for m in sorted(os.listdir(yp), reverse=True):
            if not m.startswith("month="):
                continue
            mp = os.path.join(yp, m)
            for d in sorted(os.listdir(mp), reverse=True):
                if not d.startswith("day="):
                    continue
                return f"{y}/{m}/{d}"
    return None


# Deliverable partition for this UPC (latest available date — not hardcoded)
_nsv_root = f"{BRONZE_ROOT}/Source=NSV/Zone={ZONE}/Phase={PHASE}/Cycle={CYCLE}/UPC={UPC}"
_nsv_date = _find_latest_date_partition(_nsv_root)
if _nsv_date:
    UPC_BASE = f"{_nsv_root}/{_nsv_date}"
    print(f"  NSV date partition: {_nsv_date}")
else:
    UPC_BASE = _nsv_root
    print(f"  WARNING: No NSV date partition found for {UPC}")


def _find_deliverable_parent(base):
    """Walk down from base to find the directory that contains Deliverable= folders.
    Handles varying structures like Project=Reprocessed/ or other intermediate partitions
    between the date partition and the Deliverable= folders."""
    if not os.path.exists(base):
        return base
    children = sorted(os.listdir(base))
    if any(c.startswith("Deliverable=") for c in children):
        return base  # this directory contains the deliverables
    # Recurse into subdirectories (Project=, etc.)
    for child in children:
        child_path = os.path.join(base, child)
        if os.path.isdir(child_path):
            found = _find_deliverable_parent(child_path)
            if found != child_path:  # found Deliverable= deeper
                return found
            # Also check if this child itself has Deliverable= folders
            grandchildren = os.listdir(child_path)
            if any(gc.startswith("Deliverable=") for gc in grandchildren):
                return child_path
    return base  # fallback: return original if nothing found


DELIVERABLE_BASE = _find_deliverable_parent(UPC_BASE)
if DELIVERABLE_BASE != UPC_BASE:
    _rel = DELIVERABLE_BASE.replace(UPC_BASE + "/", "")
    print(f"  Deliverable parent: {_rel}")
D1_BASE = f"{DELIVERABLE_BASE}/Deliverable=Deliverable - 1"
D2_BASE = f"{DELIVERABLE_BASE}/Deliverable=Deliverable - 2"
D3_BASE = f"{DELIVERABLE_BASE}/Deliverable=Deliverable - 3"
D4_BASE = f"{DELIVERABLE_BASE}/Deliverable=Deliverable - 4"

# Backup partition for this UPC (latest available date, dynamic folder walk)
_backup_root = f"{BRONZE_ROOT}/Source=NSV_Backup/Zone={ZONE}/Phase={PHASE}/Cycle={CYCLE}/UPC={UPC}"
_backup_date = _find_latest_date_partition(_backup_root)


def _find_lane_parent(base):
    """Walk down from base to find the directory that contains Lane= folders.
    Handles varying structures like Project=Backup/Pavement=.../Variant=...
    or Pavement=.../Variant=... directly, or any other nesting."""
    if not os.path.exists(base):
        return None
    children = sorted(os.listdir(base))
    if any(c.startswith("Lane=") for c in children):
        return base  # this directory contains the lanes
    # Recurse into subdirectories (Project=, Pavement=, Variant=, etc.)
    for child in children:
        child_path = os.path.join(base, child)
        if os.path.isdir(child_path):
            found = _find_lane_parent(child_path)
            if found:
                return found
    return None


if _backup_date:
    _backup_date_path = f"{_backup_root}/{_backup_date}"
    BACKUP_BASE = _find_lane_parent(_backup_date_path) or _backup_date_path
    _rel = BACKUP_BASE.replace(_backup_date_path + "/", "")
    print(f"  Backup date partition: {_backup_date}")
    print(f"  Backup lane parent:   {_rel}")
else:
    BACKUP_BASE = _backup_root
    print(f"  WARNING: No Backup date partition found for {UPC}")

# ── Clustering Parameters ────────────────────────────────────────────────────
PARENT_LENGTH_M = None
CHILD_100M = 100
CHILD_10M = 10
RAW_SEGMENT_M = 10

LANES = []

print("Silver Layer Delta Tables Pipeline")
print(f"UPC: {UPC} | Zone={ZONE}, Phase={PHASE}, Cycle={CYCLE}")
print(f"Deliverable base: {DELIVERABLE_BASE}")
print(f"Backup base:      {BACKUP_BASE}")
print(f"Target: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Truncate UPC from Silver Tables Before Re-Ingest
# # ───────────────────────────────────────────────────────────────────────────────
# # PRE-STEP: Truncate this UPC from all silver_defect_data tables
# # so display_defect_id counter resets cleanly for re-ingestion
# # ───────────────────────────────────────────────────────────────────────────────
# from delta.tables import DeltaTable
# from pyspark.sql import functions as F

# _TRUNC_SCHEMA = "nhai_dhms.silver_defect_data"
# _TRUNC_TABLES = [
#     "master_cracking", "master_pothole",
#     "master_ravelling_child_100m", "master_ravelling_child_10m", "master_ravelling_parent",
#     "master_roughness_child_100m", "master_roughness_child_10m", "master_roughness_parent",
#     "master_rutting_child_100m", "master_rutting_child_10m", "master_rutting_parent",
# ]

# print(f"Truncating UPC '{UPC}' from {_TRUNC_SCHEMA}...\n")
# _total_deleted = 0
# for _t in _TRUNC_TABLES:
#     _fq = f"{_TRUNC_SCHEMA}.{_t}"
#     if not spark.catalog.tableExists(_fq):
#         print(f"  ⏭  {_t:<40s} table does not exist — skipped")
#         continue
#     _before = spark.table(_fq).filter(F.col("upc_code") == UPC).count()
#     if _before > 0:
#         dt = DeltaTable.forName(spark, _fq)
#         dt.delete(f"upc_code = '{UPC}'")
#         print(f"  ✅ {_t:<40s} deleted {_before:>6,} rows")
#         _total_deleted += _before
#     else:
#         print(f"  ⏭  {_t:<40s} 0 rows — skipped")

# print(f"\n  Total deleted: {_total_deleted:,} rows")
# print(f"  UPC '{UPC}' is now clean — ready for re-ingestion")

# COMMAND ----------

# DBTITLE 1,Load 10m Unified Segments (Bronze)
# ───────────────────────────────────────────────────────────────────────────────
# STEP 1: Load Pothole Data
# Prefer D4 deliverable potholes when present, otherwise fall back to
# Source=NSV_Backup lane-wise Potholes.parquet files.
# ───────────────────────────────────────────────────────────────────────────────


def discover_lane_folders(base_path):
    if not os.path.exists(base_path):
        return []
    return sorted(
        d.replace("Lane=", "")
        for d in os.listdir(base_path)
        if d.startswith("Lane=") and os.path.isdir(os.path.join(base_path, d))
    )


POTHOLE_SOURCE_MODE = None

# ── First preference: D4 deliverable point potholes ──────────────────────────
d4_files = []
if os.path.exists(D4_BASE):
    # Filter: must contain 'pothole' but NOT be a combined defect sheet
    _EXCLUDE_KW = ['roughness', 'rut,', 'crack', 'raveling', 'ravelling']
    d4_files = [f for f in sorted(os.listdir(D4_BASE))
                if f.endswith(".parquet")
                and 'pothole' in f.lower()
                and not any(kw in f.lower() for kw in _EXCLUDE_KW)]

def _d4_pick(cols, *keywords):
    """Find a column whose name contains ALL keywords (case-insensitive)."""
    for col in cols:
        cl = col.lower()
        if all(kw.lower() in cl for kw in keywords):
            return col
    return None


if d4_files:
    POTHOLE_SOURCE_MODE = "deliverable_d4"
    # Minimum columns a real pothole sheet must have (at least severity OR area)
    _MIN_DEFECT_COLS = {'severity', 'area_sq_m', 'area', 'type_of_defect', 'lane_no'}
    child_frames = []
    for pf in d4_files:
        df_part = pd.read_parquet(f"{D4_BASE}/{pf}")
        _cols_lower = {c.lower() for c in df_part.columns}
        if not _cols_lower & _MIN_DEFECT_COLS:
            print(f"  SKIP D4 (no defect columns): {pf} ({len(df_part)} rows, cols={list(df_part.columns)})")
            continue
        child_frames.append(df_part)
        print(f"  Loaded D4 potholes: {pf} ({len(df_part)} rows)")

    df_pothole_raw = pd.concat(child_frames, ignore_index=True)
    _d4cols = list(df_pothole_raw.columns)

    # Flexible column resolution (handles both short and verbose column names)
    _from_km_c = _d4_pick(_d4cols, 'from_ch_km') or _d4_pick(_d4cols, 'from', 'km')
    _from_m_c  = _d4_pick(_d4cols, 'from_ch_m')  or _d4_pick(_d4cols, 'from', 'meter') or _d4_pick(_d4cols, 'from_ch_m')
    _to_km_c   = _d4_pick(_d4cols, 'to_ch_km')   or _d4_pick(_d4cols, 'to', 'km')
    _to_m_c    = _d4_pick(_d4cols, 'to_ch_m')    or _d4_pick(_d4cols, 'to', 'meter') or _d4_pick(_d4cols, 'to_ch_m')
    _chain_c   = _d4_pick(_d4cols, 'chainage')    # Direct chainage column (fallback)
    _lane_c    = _d4_pick(_d4cols, 'lane_no')    or _d4_pick(_d4cols, 'lane no') or _d4_pick(_d4cols, 'lane')
    _nh_c      = _d4_pick(_d4cols, 'nh_no')      or _d4_pick(_d4cols, 'nh no') or _d4_pick(_d4cols, 'nh')
    _lat_c     = _d4_pick(_d4cols, 'latitude')
    _lon_c     = _d4_pick(_d4cols, 'longitude')
    _img_c     = _d4_pick(_d4cols, 'image_name') or _d4_pick(_d4cols, 'image name') or _d4_pick(_d4cols, 'image')
    _sev_c     = _d4_pick(_d4cols, 'severity')
    _deftype_c = _d4_pick(_d4cols, 'type_of_defect') or _d4_pick(_d4cols, 'type of defect') or _d4_pick(_d4cols, 'type', 'defect')
    _area_c    = _d4_pick(_d4cols, 'area_sq_m')  or _d4_pick(_d4cols, 'area', 'sq')
    _maxd_c    = _d4_pick(_d4cols, 'max_depth_mm') or _d4_pick(_d4cols, 'max depth') or _d4_pick(_d4cols, 'max', 'depth')
    _avgd_c    = _d4_pick(_d4cols, 'average_depth_mm') or _d4_pick(_d4cols, 'average depth') or _d4_pick(_d4cols, 'average', 'depth')
    _dt_c      = _d4_pick(_d4cols, 'datetime')   or _d4_pick(_d4cols, 'date')
    _side_c    = _d4_pick(_d4cols, 'side')

    # Compute Start_Chainage: prefer km+m arithmetic; fallback to Chainage column
    # Guard: if from_ch_m > 999 it contains the FULL chainage (not just meters),
    # so fall back to the direct `chainage` column for those rows.
    if _from_km_c and _from_m_c:
        _km_vals = pd.to_numeric(df_pothole_raw[_from_km_c], errors='coerce').fillna(0).astype(int)
        _m_vals  = pd.to_numeric(df_pothole_raw[_from_m_c], errors='coerce').fillna(0).astype(int)
        _computed_ch = _km_vals * 1000 + _m_vals
        # Detect bad from_ch_m rows (full chainage stored instead of meter part)
        _bad_m = _m_vals > 999
        if _bad_m.any() and _chain_c:
            _ch_direct = pd.to_numeric(df_pothole_raw[_chain_c], errors='coerce').fillna(0).astype(int)
            _computed_ch[_bad_m] = _ch_direct[_bad_m]
            print(f"  Fixed {_bad_m.sum()} rows where from_ch_m had full chainage (used chainage column instead)")
        df_pothole_raw["Start_Chainage"] = _computed_ch
    elif _chain_c:
        df_pothole_raw["Start_Chainage"] = pd.to_numeric(df_pothole_raw[_chain_c], errors='coerce').fillna(0)
        # If Chainage looks like km (< 1000), convert to meters
        df_pothole_raw.loc[df_pothole_raw["Start_Chainage"] < 1000, "Start_Chainage"] *= 1000
    else:
        df_pothole_raw["Start_Chainage"] = 0

    if _to_km_c and _to_m_c:
        _to_km_vals = pd.to_numeric(df_pothole_raw[_to_km_c], errors='coerce')
        _to_m_vals  = pd.to_numeric(df_pothole_raw[_to_m_c], errors='coerce')
        _to_computed = _to_km_vals.fillna(0).astype(int) * 1000 + _to_m_vals.fillna(0).astype(int)
        # Same guard: if to_ch_m > 999, it's the full chainage
        _to_bad = _to_m_vals.fillna(0).astype(int) > 999
        if _to_bad.any() and _chain_c:
            _to_computed[_to_bad] = pd.to_numeric(df_pothole_raw.loc[_to_bad, _chain_c], errors='coerce').fillna(0).astype(int)
        df_pothole_raw["End_Chainage"] = _to_computed
        # Point defects (potholes) have null TO → End = Start
        _both_null = _to_km_vals.isna() & _to_m_vals.isna()
        df_pothole_raw.loc[_both_null, "End_Chainage"] = df_pothole_raw.loc[_both_null, "Start_Chainage"]
    else:
        df_pothole_raw["End_Chainage"] = df_pothole_raw["Start_Chainage"]

    # Map to standard column names
    df_pothole_raw["Lane"] = df_pothole_raw[_lane_c].fillna("Unknown") if _lane_c else "Unknown"
    _remap = {}
    if _nh_c:      _remap[_nh_c] = "NH_Number"
    if _lat_c:     _remap[_lat_c] = "Latitude"
    if _lon_c:     _remap[_lon_c] = "Longitude"
    if _img_c:     _remap[_img_c] = "Image_Name"
    if _sev_c:     _remap[_sev_c] = "Severity"
    if _deftype_c: _remap[_deftype_c] = "Type_of_Defect"
    if _area_c:    _remap[_area_c] = "Area_sq_m"
    if _maxd_c:    _remap[_maxd_c] = "Max_Depth_mm"
    if _avgd_c:    _remap[_avgd_c] = "Average_Depth_mm"
    if _dt_c:      _remap[_dt_c] = "Survey_Date"
    if _side_c:    _remap[_side_c] = "Side"
    df_pothole_raw.rename(columns=_remap, inplace=True)
else:
    # ── Fallback: D2 pothole comparison sheet ─────────────────────────────────
    # D2 'Comparison as per CA' contains a pothole sheet with per-pothole rows.
    # We identify it as the D2 parquet that has 'Severity' and 'Area' columns
    # but does NOT have the wide lane-wise roughness/cracking comparison columns.
    POTHOLE_SOURCE_MODE = "deliverable_d2"
    d2_pothole_frames = []
    if os.path.exists(D2_BASE):
        for pf in sorted(os.listdir(D2_BASE)):
            if not pf.endswith(".parquet"):
                continue
            df_tmp = pd.read_parquet(f"{D2_BASE}/{pf}")
            cols_lower = {c.lower() for c in df_tmp.columns}
            # The pothole sheet has 'severity' + area/depth but NOT the wide
            # comparison columns (e.g. 'limitation of roughness').
            has_severity = "severity" in cols_lower
            has_area = any("area" in c for c in cols_lower)
            is_wide_comparison = any("limitation" in c for c in cols_lower)
            is_crack_sheet = 'crack' in pf.lower()
            if has_severity and has_area and not is_wide_comparison and not is_crack_sheet:
                d2_pothole_frames.append(df_tmp)
                print(f"  Loaded D2 pothole sheet: {pf[:70]}... ({len(df_tmp)} rows)")

    if not d2_pothole_frames:
        raise FileNotFoundError(
            f"No D2 pothole sheet found at {D2_BASE}. "
            "Expected a parquet with Severity + Area columns."
        )

    # Prefer the larger file (full pothole list) over the filtered subset
    d2_pothole_frames.sort(key=len, reverse=True)
    df_pothole_raw = d2_pothole_frames[0].copy()
    print(f"  Using D2 pothole sheet: {len(df_pothole_raw):,} rows")

    # Normalize D2 column names to the pipeline's expected schema
    col_map = {
        "Chainage": "Start_Chainage",
        "chainage": "Start_Chainage",
        "Lane No.": "Lane",
        "Lane Number": "Lane",
        "lane_no": "Lane",
        "Area (in Sq m)": "Area_sq_m",
        "area_sq_m": "Area_sq_m",
        "Max Depth (in mm)": "Max_Depth_mm",
        "max_depth_mm": "Max_Depth_mm",
        "Average Depth (in mm)": "Average_Depth_mm",
        "average_depth_mm": "Average_Depth_mm",
        "Severity": "Severity",
        "severity": "Severity",
        "NH Number": "NH_Number",
        "nh_no": "NH_Number",
        "image_name": "Image_Name",
        "latitude": "Latitude",
        "longitude": "Longitude",
        "datetime": "Survey_Date",
        "side": "Side",
        "type_of_defect": "Type_of_Defect",
    }
    # Both 'nh_no' and 'NH Number' map to NH_Number, creating a duplicate
    # that breaks downstream groupby. Keep 'NH Number' (more populated),
    # drop 'nh_no'.
    if 'nh_no' in df_pothole_raw.columns and 'NH Number' in df_pothole_raw.columns:
        nh_no_nulls = df_pothole_raw['nh_no'].isna().sum()
        nh_num_nulls = df_pothole_raw['NH Number'].isna().sum()
        if nh_num_nulls <= nh_no_nulls:
            df_pothole_raw = df_pothole_raw.drop(columns=['nh_no'])
            print(f"  Dropped 'nh_no' (nulls={nh_no_nulls}), kept 'NH Number' (nulls={nh_num_nulls})")
        else:
            df_pothole_raw = df_pothole_raw.drop(columns=['NH Number'])
            print(f"  Dropped 'NH Number' (nulls={nh_num_nulls}), kept 'nh_no' (nulls={nh_no_nulls})")
    df_pothole_raw.rename(
        columns={k: v for k, v in col_map.items() if k in df_pothole_raw.columns},
        inplace=True,
    )
    df_pothole_raw["Start_Chainage"] = pd.to_numeric(
        df_pothole_raw["Start_Chainage"], errors="coerce"
    )
    # D2 pothole sheet has a single chainage per defect (point), so End = Start
    if "End_Chainage" not in df_pothole_raw.columns:
        df_pothole_raw["End_Chainage"] = df_pothole_raw["Start_Chainage"]
    df_pothole_raw["Type_of_Defect"] = df_pothole_raw.get(
        "Type_of_Defect", pd.Series("Pothole", index=df_pothole_raw.index)
    ).fillna("Pothole")
    for col in ["Survey_Date", "Side", "Image_Name", "Latitude", "Longitude"]:
        if col not in df_pothole_raw.columns:
            df_pothole_raw[col] = None

# ── Chainage range validation: drop outlier records with implausible chainages ──
# Some D4 source records have from_ch_km values from other road sections
# (e.g. 608 km, 638 km for a 287–327 km road). Use IQR fencing to detect
# and remove these before they propagate into silver tables with bad PCI nulls.
_q1 = df_pothole_raw['Start_Chainage'].quantile(0.25)
_q3 = df_pothole_raw['Start_Chainage'].quantile(0.75)
_iqr = max(_q3 - _q1, 10000)  # floor at 10 km to avoid over-filtering tight clusters
_ch_lo = _q1 - 3.0 * _iqr
_ch_hi = _q3 + 3.0 * _iqr
_before_count = len(df_pothole_raw)
_outlier_mask = (df_pothole_raw['Start_Chainage'] < _ch_lo) | (df_pothole_raw['Start_Chainage'] > _ch_hi)
_outliers = df_pothole_raw[_outlier_mask]
if len(_outliers) > 0:
    print(f"  \u26a0 Chainage outliers detected ({len(_outliers)} records outside {_ch_lo:.0f}\u2013{_ch_hi:.0f}):")
    for _, _row in _outliers.iterrows():
        print(f"    Lane={_row.get('Lane','?')}  Start_Chainage={_row['Start_Chainage']}  Area={_row.get('Area_sq_m','?')}")
    df_pothole_raw = df_pothole_raw[~_outlier_mask].reset_index(drop=True)
    print(f"  Dropped {_before_count - len(df_pothole_raw)} outlier pothole records")

print(f"\nPothole source mode: {POTHOLE_SOURCE_MODE}")
print(f"Total pothole records: {len(df_pothole_raw):,}")
print(f"Lanes: {sorted(df_pothole_raw['Lane'].dropna().astype(str).unique().tolist())}")
print(f"Chainage range: {df_pothole_raw['Start_Chainage'].min()} to {df_pothole_raw['End_Chainage'].max()}")
print(f"Defect types: {df_pothole_raw['Type_of_Defect'].dropna().astype(str).unique().tolist()}")
print(f"Severity values: {sorted(df_pothole_raw['Severity'].dropna().astype(str).unique().tolist())}")
if 'Image_Name' in df_pothole_raw.columns:
    print(f"Image_Name sample: {df_pothole_raw['Image_Name'].dropna().head(2).tolist()}")

# COMMAND ----------

# DBTITLE 1,Load and Filter Defect Sources by IRC/CA Thresholds
# ───────────────────────────────────────────────────────────────────────────────
# STEP 1B: Load additional defect sources
# Non-pothole defects for this UPC come from Source=NSV_Backup lane folders.
# D2 remains the deliverable source for thresholds / comparison metadata.
# ───────────────────────────────────────────────────────────────────────────────
 
 
def load_matching_parquets(base_path, patterns):
    if not base_path or not os.path.exists(base_path):
        return pd.DataFrame(), []
    files = [
        f for f in sorted(os.listdir(base_path))
        if f.endswith('.parquet') and any(p.lower() in f.lower() for p in patterns)
    ]
    if not files:
        return pd.DataFrame(), files
    frames = [pd.read_parquet(f"{base_path}/{f}") for f in files]
    return pd.concat(frames, ignore_index=True), files
 
 
def load_backup_lane_parquets(base_path, suffixes):
    if isinstance(suffixes, str):
        suffixes = [suffixes]
    frames, files = [], []
    lanes = discover_lane_folders(base_path)
    for lane in lanes:
        lane_path = os.path.join(base_path, f"Lane={lane}")
        matches = [
            f for f in sorted(os.listdir(lane_path))
            if f.endswith('.parquet') and any(sfx.lower() in f.lower() for sfx in suffixes)
        ]
        for pf in matches:
            part = pd.read_parquet(os.path.join(lane_path, pf))
            if 'Lane' not in part.columns and 'LANE' not in part.columns:
                part['Lane'] = lane
            part['Source_Lane_Folder'] = lane
            frames.append(part)
            files.append(f"Lane={lane}/{pf}")
    return (pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()), files
 
 
# ═══ Parse D2 Thresholds EARLY so condition functions use dynamic breakpoints ═══
def parse_d2_thresholds(d2_base_path):
    """Scan D2 parquets for 'Limitation' columns and return threshold dict.
    IRC-100m file → roughness_iri (IRC threshold for IRI-based filtering).
    CA (Concession Agreement) file → roughness_bi (CA threshold for BI-based filtering).
    CA values also take priority for other defect thresholds."""
    defaults = {
        'roughness_bi': 2400.0, 'roughness_iri': 2400.0,
        'rutting_mm': 10.0, 'cracking_pct': 10.0,
        'ravelling': 10.0, 'ravelling_unit': 'pct',
    }
    if not d2_base_path or not os.path.exists(d2_base_path):
        print("  WARNING: D2 path not found, using default thresholds.")
        return defaults
    thresholds = dict(defaults)
    ca_set = set()   # Track keys already set from a Concession Agreement column
    irc_set = set()  # Track keys already set from an IRC-named file (higher priority)
    for pf in sorted(os.listdir(d2_base_path)):
        if not pf.endswith('.parquet'):
            continue
        # Skip combined detail sheet — its thresholds differ from the dedicated CA/IRC sheets
        # Use shorter pattern to handle truncated filenames (e.g. 'Ravellin' instead of 'Ravelling')
        if 'roughness,rut,crack,rav' in pf.lower():
            continue
        is_irc_file = 'irc' in pf.lower()  # File named IRC-100m etc.
        df = pd.read_parquet(f"{d2_base_path}/{pf}")
        for col in df.columns:
            cl = col.lower()
            if 'limitation' not in cl and 'limit' not in cl:
                continue
            # Skip combined-sheet comparison columns (e.g. "Roughness > Limitation of BI ...")
            # These are breach cutoff values, not actual CA/IRC thresholds
            if '>' in cl:
                continue
            val = pd.to_numeric(df[col], errors='coerce').dropna()
            if val.empty:
                continue
            limit_val = float(val.iloc[0])
            is_ca = 'concession' in cl  # Concession Agreement source
            if ('irc' in cl and 'roughness' in cl) or ('iri' in cl and 'limitation' in cl):
                # Column says "IRC" but check the FILE to route correctly
                if is_irc_file:
                    if 'roughness_iri' not in irc_set:
                        thresholds['roughness_iri'] = limit_val
                        irc_set.add('roughness_iri')
                else:
                    # CA file — column mislabelled as "IRC" → treat as CA BI threshold
                    if 'roughness_bi' not in ca_set:
                        thresholds['roughness_bi'] = limit_val
                        ca_set.add('roughness_bi')
            elif 'bi' in cl or ('roughness' in cl and 'irc' not in cl):
                if is_irc_file:
                    # IRC file's BI limitation → roughness_iri (IRC threshold)
                    # Column may lack 'irc'/'iri' keywords, e.g.
                    # "Limitation of BI as per Concession Agreement (in mm/km)"
                    if 'roughness_iri' not in irc_set:
                        thresholds['roughness_iri'] = limit_val
                        irc_set.add('roughness_iri')
                else:
                    # Non-IRC (CA) file's BI limitation → roughness_bi (CA threshold)
                    if is_ca or 'roughness_bi' not in ca_set:
                        thresholds['roughness_bi'] = limit_val
                        if is_ca:
                            ca_set.add('roughness_bi')
            elif 'rut' in cl:
                if not is_irc_file:
                    # CA (non-IRC) file — highest priority
                    thresholds['rutting_mm'] = limit_val
                    ca_set.add('rutting_mm')
                elif 'rutting_mm' not in ca_set:
                    # IRC file — fallback only if CA hasn't set it
                    thresholds['rutting_mm'] = limit_val
            elif 'crack' in cl:
                if not is_irc_file:
                    # CA (non-IRC) file — highest priority
                    thresholds['cracking_pct'] = limit_val
                    ca_set.add('cracking_pct')
                elif 'cracking_pct' not in ca_set:
                    # IRC file — fallback only if CA hasn't set it
                    thresholds['cracking_pct'] = limit_val
            elif 'ravelling' in cl or 'ravel' in cl:
                if not is_irc_file:
                    # CA (non-IRC) file — highest priority
                    thresholds['ravelling'] = limit_val
                    ca_set.add('ravelling')
                elif 'ravelling' not in ca_set:
                    # IRC file — fallback only if CA hasn't set it
                    thresholds['ravelling'] = limit_val
                if 'sq' in cl or 'area' in cl:
                    thresholds['ravelling_unit'] = 'sqm'
                elif '%' in cl or 'percent' in cl:
                    thresholds['ravelling_unit'] = 'pct'
    return thresholds
 
D2_THRESHOLDS = parse_d2_thresholds(D2_BASE)
print(f"  D2 Thresholds (early parse): {D2_THRESHOLDS}")
 
IRC_IRI_POOR = D2_THRESHOLDS['roughness_iri']
CA_BI_THRESHOLD = D2_THRESHOLDS['roughness_bi']
CA_RUT_THRESHOLD = D2_THRESHOLDS['rutting_mm']
CA_CRACK_PCT = D2_THRESHOLDS['cracking_pct']
RAVELLING_RATING_THRESHOLD = D2_THRESHOLDS['ravelling']
RAVELLING_UNIT = D2_THRESHOLDS['ravelling_unit']
 
 
# ═══ Dynamic Condition Functions (breakpoints from D2 thresholds, not hardcoded) ═══
def crack_condition_from_pct(value):
    """Cracking condition — breakpoints derived from D2 CA_CRACK_PCT."""
    value = pd.to_numeric(pd.Series([value]), errors='coerce').iloc[0]
    if pd.isna(value):
        return None
    t = CA_CRACK_PCT
    b2, b3, b4 = round(t * 2), round(t * 4), round(t * 6)
    if value < t:
        return f'Very Good (<{t:.0f}%)'
    if value <= b2:
        return f'Good ({t:.0f}-{b2}%)'
    if value <= b3:
        return f'Fair ({b2}-{b3}%)'
    if value <= b4:
        return f'Poor ({b3}-{b4}%)'
    return f'Very Poor (>{b4}%)'
 
 
def roughness_condition_from_metrics(iri_value, bi_value):
    """Roughness condition — breakpoints derived from D2 CA_BI_THRESHOLD."""
    iri_value = pd.to_numeric(pd.Series([iri_value]), errors='coerce').iloc[0]
    bi_value = pd.to_numeric(pd.Series([bi_value]), errors='coerce').iloc[0]
    probe = bi_value if pd.notna(bi_value) else (iri_value * 1000 if pd.notna(iri_value) else np.nan)
    if pd.isna(probe):
        return None
    t = CA_BI_THRESHOLD
    fair_lower = round(t * 0.8)
    if probe < fair_lower:
        return f'Good (<{fair_lower}mm/km)'
    if probe <= t:
        return f'Fair ({fair_lower}-{t:.0f}mm/km)'
    return f'Poor (>{t:.0f}mm/km)'
 
 
def rutting_condition_from_mm(value):
    """Rutting condition — breakpoints derived from D2 CA_RUT_THRESHOLD."""
    value = pd.to_numeric(pd.Series([value]), errors='coerce').iloc[0]
    if pd.isna(value):
        return None
    t = CA_RUT_THRESHOLD
    fair_lower = round(t * 0.5)
    if value < fair_lower:
        return f'Good (<{fair_lower}mm)'
    if value <= t:
        return f'Fair ({fair_lower}-{t:.0f}mm)'
    return f'Poor (>{t:.0f}mm)'
 
 
def ravelling_condition_from_pct(value):
    """Ravelling condition — breakpoints derived from D2 RAVELLING_RATING_THRESHOLD."""
    value = pd.to_numeric(pd.Series([value]), errors='coerce').iloc[0]
    if pd.isna(value):
        return None
    t = RAVELLING_RATING_THRESHOLD
    half_t = round(t * 0.5)
    if value == 0:
        return 'Very Good (0)'
    if value <= half_t:
        return f'Good (1-{half_t}%)'
    if value < t:
        return f'Fair ({half_t}-{t:.0f}%)'
    return f'Poor (>={t:.0f}%)'
 
 
# Load lane-wise backup files
LANES = discover_lane_folders(BACKUP_BASE)
df_cracking_raw, cracking_files = load_backup_lane_parquets(BACKUP_BASE, '_Cracking.parquet')
df_roughness_raw, roughness_files = load_backup_lane_parquets(BACKUP_BASE, '_Roughness_IRI.parquet')
df_rutting_raw, rutting_files = load_backup_lane_parquets(BACKUP_BASE, '_Rutting.parquet')
df_ravelling_raw, ravelling_files = load_backup_lane_parquets(BACKUP_BASE, '_Raveling.parquet')
 
# ── Standardize backup schemas so downstream mapping can stay generic ────────
if not df_cracking_raw.empty:
    df_cracking_raw.rename(columns={
        'LANE': 'Lane',
        'Percent_Area': 'Cracking',
        'cracking_image_name': 'Image_Name',
        'cracking_image_url': 'Image_URL',
        'Start_Latitude': 'Start_Lat',
        'Start_Longitude': 'Start_Lon',
        'End_Latitude': 'End_Lat',
        'End_Longitude': 'End_Lon',
        'survey_date_crack': 'SurveyDate',
        'Area_Sum': 'Area_sq_m'
    }, inplace=True)
    if 'Lane' not in df_cracking_raw.columns:
        df_cracking_raw['Lane'] = df_cracking_raw['Source_Lane_Folder']
    crack_pct = pd.to_numeric(df_cracking_raw['Cracking'], errors='coerce').fillna(0)
    df_cracking_raw['Condition'] = crack_pct.apply(crack_condition_from_pct)
    df_cracking_raw['Type_of_Defect'] = 'Cracking'
 
if not df_roughness_raw.empty:
    df_roughness_raw.rename(columns={
        'RoughnessIndex': 'BI m/km',
        'LaneIri': 'IRI',
        'Start_Latitude': 'Start_Lat',
        'Start_Longitude': 'Start_Lon',
        'End_Latitude': 'End_Lat',
        'End_Longitude': 'End_Lon',
        'ConditionOfRoadSurface': 'Condition',
    }, inplace=True)
    if 'SurveyDate' not in df_roughness_raw.columns:
        df_roughness_raw['SurveyDate'] = pd.NaT
    rough_iri = pd.to_numeric(df_roughness_raw.get('IRI'), errors='coerce')
    rough_bi = pd.to_numeric(df_roughness_raw.get('BI m/km'), errors='coerce')
    derived_condition = [roughness_condition_from_metrics(i, b) for i, b in zip(rough_iri, rough_bi)]
    df_roughness_raw['Condition'] = df_roughness_raw.get('Condition', pd.Series([None] * len(df_roughness_raw))).fillna(pd.Series(derived_condition))
    df_roughness_raw['Type_of_Defect'] = 'Roughness'
 
if not df_rutting_raw.empty:
    df_rutting_raw.rename(columns={
        'LANE': 'Lane',
        'Rutting Average': 'Rutting',
        'Start_Latitude': 'Start_Lat',
        'Start_Longitude': 'Start_Lon',
        'End_Latitude': 'End_Lat',
        'End_Longitude': 'End_Lon',
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
        'Start_Latitude': 'Start_Lat',
        'Start_Longitude': 'Start_Lon',
        'End_Latitude': 'End_Lat',
        'End_Longitude': 'End_Lon',
        'survey_date_raveling': 'SurveyDate',
    }, inplace=True)
    # Approximate percentage over a 10m x 3.5m lane area so D2's 10% rule can be applied.
    rav_pct = pd.to_numeric(df_ravelling_raw['Ravelling_Area_sq_m'], errors='coerce').fillna(0) * 100.0 / 35.0
    df_ravelling_raw['Ravelling'] = rav_pct
    df_ravelling_raw['Condition'] = rav_pct.apply(ravelling_condition_from_pct)
    df_ravelling_raw['Type_of_Defect'] = 'Ravelling'
 
# Keep df_visual_raw for downstream survey-date fallback logic
df_visual_raw = df_ravelling_raw.copy() if not df_ravelling_raw.empty else pd.DataFrame()
 
# ── D2 thresholds already parsed above (before condition functions) ──────────
def _parse_d2_thresholds_LEGACY(d2_base_path):
    """LEGACY — kept for reference. Active parse is above."""
    defaults = {
        'roughness_bi': 2400.0,
        'roughness_iri': 2400.0,
        'rutting_mm': 10.0,
        'cracking_pct': 10.0,
        'ravelling': 10.0,
        'ravelling_unit': 'pct',
    }
    if not d2_base_path or not os.path.exists(d2_base_path):
        print("  WARNING: D2 path not found, using default thresholds.")
        return defaults
 
    thresholds = dict(defaults)
    for pf in sorted(os.listdir(d2_base_path)):
        if not pf.endswith('.parquet'):
            continue
        df = pd.read_parquet(f"{d2_base_path}/{pf}")
        for col in df.columns:
            cl = col.lower()
            if 'limitation' not in cl and 'limit' not in cl:
                continue
            val = pd.to_numeric(df[col], errors='coerce').dropna()
            if val.empty:
                continue
            limit_val = float(val.iloc[0])
            if ('irc' in cl and 'roughness' in cl) or ('iri' in cl and 'limitation' in cl):
                thresholds['roughness_iri'] = limit_val
            elif 'bi' in cl or ('roughness' in cl and 'irc' not in cl):
                thresholds['roughness_bi'] = limit_val
            elif 'rut' in cl:
                thresholds['rutting_mm'] = limit_val
            elif 'crack' in cl:
                thresholds['cracking_pct'] = limit_val
            elif 'ravelling' in cl or 'ravel' in cl:
                thresholds['ravelling'] = limit_val
                if 'sq' in cl or 'area' in cl:
                    thresholds['ravelling_unit'] = 'sqm'
                elif '%' in cl or 'percent' in cl:
                    thresholds['ravelling_unit'] = 'pct'
    return thresholds
 
# D2_THRESHOLDS, IRC_IRI_POOR, CA_BI_THRESHOLD etc. already set above
# Dynamic CRACKING_BREACH_CONDITIONS from D2 thresholds
_t = CA_CRACK_PCT
CRACKING_BREACH_CONDITIONS = [
    f'Fair ({round(_t*2)}-{round(_t*4)}%)',
    f'Poor ({round(_t*4)}-{round(_t*6)}%)',
    f'Very Poor (>{round(_t*6)}%)',
]
print(f"  Dynamic breach conditions: {CRACKING_BREACH_CONDITIONS}")
 
print("\nRaw backup data loaded (before threshold filtering):")
print(f"  Lanes detected:    {LANES}")
print(f"  Cracking files:    {len(cracking_files)} | rows={len(df_cracking_raw):,}")
print(f"  Roughness files:   {len(roughness_files)} | rows={len(df_roughness_raw):,}")
print(f"  Rutting files:     {len(rutting_files)} | rows={len(df_rutting_raw):,}")
print(f"  Ravelling files:   {len(ravelling_files)} | rows={len(df_ravelling_raw):,}")

# ── Null Check & Fill: BI m/km from Lane IRI ────────────────────────────────
# If RoughnessIndex (BI m/km) is null, derive it: BI = 630 × (Lane IRI)^1.12
if not df_roughness_raw.empty:
    _bi_col = 'BI m/km'
    _iri_col = 'IRI'
    df_roughness_raw[_bi_col] = pd.to_numeric(df_roughness_raw[_bi_col], errors='coerce')
    df_roughness_raw[_iri_col] = pd.to_numeric(df_roughness_raw[_iri_col], errors='coerce')
    _null_bi = df_roughness_raw[_bi_col].isna().sum()
    if _null_bi > 0:
        _derived_bi = 630 * (df_roughness_raw[_iri_col] ** 1.12)
        df_roughness_raw[_bi_col] = df_roughness_raw[_bi_col].fillna(_derived_bi)
        _still_null = df_roughness_raw[_bi_col].isna().sum()
        print(f"  BI null-fill: {_null_bi:,} nulls found → {_null_bi - _still_null:,} filled via 630×(IRI)^1.12")
        # Per-lane update summary
        if 'Lane' in df_roughness_raw.columns or 'Source_Lane_Folder' in df_roughness_raw.columns:
            _lane_col = 'Lane' if 'Lane' in df_roughness_raw.columns else 'Source_Lane_Folder'
            for _ln in sorted(df_roughness_raw[_lane_col].unique()):
                _ln_count = (df_roughness_raw[_lane_col] == _ln).sum()
                print(f"    Lane {_ln}: {_ln_count:,} rows updated with derived BI values")
        if _still_null > 0:
            print(f"    Remaining nulls: {_still_null:,} (IRI was also null for these rows)")
    else:
        print(f"  BI null-fill: no nulls in '{_bi_col}' — all {len(df_roughness_raw):,} rows already have values, no update needed") 

if not df_roughness_raw.empty:
    rough_iri = pd.to_numeric(df_roughness_raw.get('IRI'), errors='coerce').fillna(0)
    rough_bi = pd.to_numeric(df_roughness_raw.get('BI m/km'), errors='coerce').fillna(0)
    total = len(df_roughness_raw)
    df_roughness_raw = df_roughness_raw[(rough_iri > IRC_IRI_POOR) | (rough_bi >= CA_BI_THRESHOLD)].copy()
    print(f"\n  Roughness: {total:,} → {len(df_roughness_raw):,} defects (IRI>{IRC_IRI_POOR} or BI>={CA_BI_THRESHOLD})")
 
if not df_rutting_raw.empty:
    rut_val = pd.to_numeric(df_rutting_raw.get('Rutting'), errors='coerce').fillna(0)
    total = len(df_rutting_raw)
    df_rutting_raw = df_rutting_raw[rut_val >= CA_RUT_THRESHOLD].copy()
    print(f"  Rutting:   {total:,} → {len(df_rutting_raw):,} defects (Rutting>={CA_RUT_THRESHOLD}mm)")
 
if not df_cracking_raw.empty:
    crack_pct = pd.to_numeric(df_cracking_raw.get('Cracking'), errors='coerce').fillna(0)
    total = len(df_cracking_raw)
    df_cracking_raw = df_cracking_raw[crack_pct >= CA_CRACK_PCT].copy()
    print(f"  Cracking:  {total:,} → {len(df_cracking_raw):,} defects (Cracking>={CA_CRACK_PCT}%)")
 
if not df_ravelling_raw.empty:
    # Keep ALL ravelling rows (no threshold filtering) — breach_flag computed in Cell 10
    if RAVELLING_UNIT == 'sqm':
        rav_metric = pd.to_numeric(df_ravelling_raw.get('Ravelling_Area_sq_m', df_ravelling_raw.get('Ravelling')), errors='coerce').fillna(0)
        breach_count = (rav_metric >= RAVELLING_RATING_THRESHOLD).sum()
        print(f"  Ravelling: {len(df_ravelling_raw):,} total rows, {breach_count:,} breach threshold (Area>={RAVELLING_RATING_THRESHOLD} sq.m.)")
    else:
        rav_pct = pd.to_numeric(df_ravelling_raw.get('Ravelling'), errors='coerce').fillna(0)
        breach_count = (rav_pct >= RAVELLING_RATING_THRESHOLD).sum()
        print(f"  Ravelling: {len(df_ravelling_raw):,} total rows, {breach_count:,} breach threshold (>={RAVELLING_RATING_THRESHOLD}%)")
else:
    print("  Ravelling: no backup ravelling data")
 
print("\nThreshold-filtered defect counts:")
print(f"  Roughness: {len(df_roughness_raw):,}")
print(f"  Rutting:   {len(df_rutting_raw):,}")
print(f"  Cracking:  {len(df_cracking_raw):,}")
print(f"  Ravelling: {len(df_ravelling_raw):,}")
 

# COMMAND ----------

# DBTITLE 1,Load Tracker & Constant Clustering Intervals
# ───────────────────────────────────────────────────────────────────────────────
# STEP 2: Load Tracker & Define Constant Clustering Intervals
#
# Three-level hierarchy (FIXED intervals, NOT dynamic):
#   Parent  1000m  → always CA standard
#   Child   100m   → IRC priority, CA fallback
#   Child    10m   → CA priority, IRC fallback
# ───────────────────────────────────────────────────────────────────────────────
import pandas as pd, os, copy

TRACKER_PATH = "/Workspace/Shared/NSV Extraction/NSV_Activity_Tracker_ZoneWise_Standardized_17072026.xlsx"

def _parse_std(val):
    s = str(val).strip().upper() if pd.notna(val) else ""
    if 'IRC' in s: return 'IRC'
    if 'CA' in s: return 'CA'
    return None

def _parse_interval(val):
    try: return int(float(val))
    except: return None

def load_tracker_context(zone, phase, cycle, upc_slash):
    """Load NSV Activity Tracker and extract defect standards + survey metadata.
    Preserves raw_defect_standards (before D2 override) for interval computation."""
    sheet_map = {"A": "Zone A", "B": "Zone B", "C": "Zone C", "D": "Zone D", "E": "Zone E"}
    sheet_name = sheet_map.get(zone, f"Zone {zone}")

    def _default_ctx():
        defs = {d: {'standard': 'CA', 'interval_m': 100}
                for d in ['ravelling', 'rutting', 'cracking', 'roughness', 'pothole']}
        return {
            'survey_start_date': None, 'survey_end_date': None,
            'piu': None, 'ae_ie': None, 'contractor': None, 'state': None,
            'pavement_type': None, 'pci': None, 'threshold_label_override': None,
            'defect_standards': defs,
            'raw_defect_standards': copy.deepcopy(defs),
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
        if upc_norm not in upc_val and upc_val not in upc_norm:
            continue
        phase_val = str(row.iloc[2]).lower().replace('phase', '').strip()
        cycle_val = str(row.iloc[3]).lower().replace('cycle', '').strip()
        if phase_val == str(phase).strip() and cycle_val == str(cycle).strip():
            best_row = row
            break
        if best_row is None:
            best_row = row

    if best_row is None:
        print(f"  WARNING: UPC {upc_slash} not found in tracker. Using CA defaults.")
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

    # Save RAW standards before D2 override (for interval computation)
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

    # D2 override: prefer the actual comparison sheet naming
    d2_files = []
    if os.path.exists(D2_BASE):
        d2_files = [f for f in os.listdir(D2_BASE) if f.endswith('.parquet')]
    # Match both correct ('comparison') and misspelled ('comparision') variants,
    # and both 'as per CA' and 'with CA' naming conventions.
    _d2_lower = [f.lower() for f in d2_files]
    _has_ca = any('as per ca' in fl or 'with ca' in fl for fl in _d2_lower)
    _has_irc = any('as per irc' in fl or 'with irc' in fl for fl in _d2_lower)
    if _has_ca:
        ctx['threshold_label_override'] = 'As per CA'
        for dn in ctx['defect_standards']:
            ctx['defect_standards'][dn]['standard'] = 'CA'
            ctx['defect_standards'][dn]['interval_m'] = ctx['defect_standards'][dn].get('interval_m') or 100
    elif _has_irc:
        ctx['threshold_label_override'] = 'As per IRC'
        for dn in ctx['defect_standards']:
            ctx['defect_standards'][dn]['standard'] = 'IRC'
            ctx['defect_standards'][dn]['interval_m'] = ctx['defect_standards'][dn].get('interval_m') or 100

    return ctx


# ── Load Tracker Early ───────────────────────────────────────────────────────
TRACKER_CTX = load_tracker_context(ZONE, PHASE, CYCLE, UPC.replace('_', '/'))


# ── Helper functions (used downstream in cells 10-11) ────────────────────────
def get_threshold_name(defect_name):
    override = TRACKER_CTX.get('threshold_label_override')
    if override:
        return override
    std = TRACKER_CTX['defect_standards'].get(defect_name, {}).get('standard')
    if std == 'IRC': return 'As per IRC'
    if std == 'CA': return 'As per CA'
    return None


def get_reporting_interval(defect_name):
    return TRACKER_CTX['defect_standards'].get(defect_name, {}).get('interval_m', 100) or 100


# ── Constant Clustering Intervals ────────────────────────────────────────────
# Intervals are FIXED across all defects and all UPCs (no tracker/D2 inference).


# ══════════════════════════════════════════════════════════════════════════════
# CONSTANT intervals — same for every defect and every UPC
# ══════════════════════════════════════════════════════════════════════════════
PARENT_LENGTH_M = 1000   # Parent: always 1 km
CHILD_100M      = 100    # Child mid: always 100 m
CHILD_10M       = 10     # Child fine: always 10 m

DEFECT_INTERVALS = {
    _d: {'parent_m': PARENT_LENGTH_M, 'child_mid_m': CHILD_100M, 'child_10m_m': CHILD_10M}
    for _d in ['roughness', 'rutting', 'cracking', 'ravelling', 'pothole']
}

# Standard priority per level (first available wins, then fallback)
# These determine which threshold/condition to apply at each level.
LEVEL_STANDARDS = {
    'parent':     ['CA'],           # Parent  → always CA
    'child_100m': ['IRC', 'CA'],    # 100m    → IRC priority, CA fallback
    'child_10m':  ['CA', 'IRC'],    # 10m     → CA priority, IRC fallback
}


def get_level_standard(level):
    """Return the preferred standard label for a given level.
    E.g. 'parent' → 'As per CA', 'child_100m' → 'As per IRC' (or CA fallback)."""
    prefs = LEVEL_STANDARDS.get(level, ['CA'])
    return f'As per {prefs[0]}'


print("Constant Clustering Parameters:")
print(f"  UPC: {UPC}")
print(f"  Parent  = {PARENT_LENGTH_M}m  (standard: CA always)")
print(f"  Child   = {CHILD_100M}m  (standard: IRC priority, CA fallback)")
print(f"  Child   = {CHILD_10M}m  (standard: CA priority, IRC fallback)")
print(f"  Tracker override: {TRACKER_CTX.get('threshold_label_override', 'None')}")
print(f"  Total pothole records to process: {len(df_pothole_raw)}")

# COMMAND ----------

# DBTITLE 1,Build Parent-Child Clustering (Constant Intervals)
# ───────────────────────────────────────────────────────────────────────────────
# STEP 3: Build 3-Level Hierarchy for Pothole
#   Parent (1KM): aggregated pothole stats per 1KM segment
#   Child 100m:   aggregated pothole stats per 100m segment
#   Child 10m:    individual pothole records (raw)
# Defect_ID: D/ddmmyy/Source/UPC/Start_chainage/Pothole/xxxx
# ───────────────────────────────────────────────────────────────────────────────

df_pothole = df_pothole_raw.copy()

# ═══ Constant intervals (Parent=1000m CA, Child=100m IRC→CA, 10m CA→IRC) ═══
_POTHOLE_PARENT_M = DEFECT_INTERVALS['pothole']['parent_m']     # 1000m, CA
_POTHOLE_CHILD_MID_M = DEFECT_INTERVALS['pothole']['child_mid_m']  # 100m, IRC→CA
print(f"Pothole clustering: Parent={_POTHOLE_PARENT_M}m (CA) | Child_Mid={_POTHOLE_CHILD_MID_M}m (IRC→CA) | Child_10m={CHILD_10M}m (CA→IRC)")

# ═══ Assign grouping chainages ═══
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
    df_pothole['Start_Chainage'].astype(str) + '_' +
    df_pothole.index.astype(str)
)

# ═══ Defect_ID ═══
SOURCE_CODE = "N"
if 'Survey_Date' in df_pothole.columns:
    _survey_dt = pd.to_datetime(df_pothole['Survey_Date'], errors='coerce').dropna()
    SURVEY_DATE_STR = _survey_dt.iloc[0].strftime('%d%m%y') if len(_survey_dt) > 0 else RUN_DATE.strftime('%d%m%y')
else:
    SURVEY_DATE_STR = RUN_DATE.strftime('%d%m%y')

df_pothole['Defect_ID'] = [
    f"D/{SURVEY_DATE_STR}/{SOURCE_CODE}/{UPC}/{int(row['Start_Chainage'])}/Pothole/{i+1:04d}"
    for i, (_, row) in enumerate(df_pothole.iterrows())
]

# ═══ Metadata ═══
df_pothole['Source'] = "NSV"
df_pothole['Category'] = "POINT"
df_pothole['Defect_Type'] = "Pothole"
df_pothole['Zone'] = ZONE
df_pothole['Phase'] = PHASE
df_pothole['Cycle'] = CYCLE
df_pothole['UPC'] = UPC.replace('_', '/')
df_pothole['UPC_Code'] = UPC
df_pothole['Run_Date'] = str(RUN_DATE)
df_pothole['Project_Name'] = UPC
df_pothole['Point_Chainage'] = df_pothole['Start_Chainage']
df_pothole['Point_Lat'] = df_pothole['Latitude']
df_pothole['Point_Long'] = df_pothole['Longitude']

# ══════ CHILD 10m: Individual pothole records (raw) ══════
df_pothole_10m = df_pothole.copy()

# ══════ CHILD 100m: Aggregated per 100m segment ══════
df_pothole_100m = df_pothole.groupby(
    ['Lane', 'NH_Number', 'Child_100m_Start', 'Child_100m_End',
     'Parent_Start_Chainage', 'Parent_End_Chainage', 'Parent_ID']
).agg(
    Pothole_Count=('Child_ID', 'count'),
    Total_Area_sq_m=('Area_sq_m', 'sum'),
    Avg_Depth_mm=('Average_Depth_mm', 'mean'),
    Max_Depth_mm=('Max_Depth_mm', 'max'),
    Avg_Area_sq_m=('Area_sq_m', 'mean'),
    Point_Lat=('Latitude', 'first'),
    Point_Long=('Longitude', 'first'),
    Image_Name=('Image_Name', 'first'),
).reset_index()

df_pothole_100m.rename(columns={'Child_100m_Start': 'Start_Chainage', 'Child_100m_End': 'End_Chainage'}, inplace=True)
df_pothole_100m['Child_ID'] = (
    UPC + '_' + df_pothole_100m['Lane'].astype(str) + '_Pothole_100m_' +
    df_pothole_100m['Start_Chainage'].astype(int).astype(str) + '_' +
    df_pothole_100m['End_Chainage'].astype(int).astype(str)
)
df_pothole_100m['Defect_ID'] = [
    f"D/{SURVEY_DATE_STR}/{SOURCE_CODE}/{UPC}/{int(row['Start_Chainage'])}/Pothole_100m/{i+1:04d}"
    for i, (_, row) in enumerate(df_pothole_100m.iterrows())
]
for col in ['Source', 'Category', 'Defect_Type', 'Zone', 'Phase', 'Cycle', 'UPC_Code', 'Run_Date', 'Project_Name']:
    df_pothole_100m[col] = df_pothole[col].iloc[0]
df_pothole_100m['UPC'] = UPC.replace('_', '/')
df_pothole_100m['Point_Chainage'] = df_pothole_100m['Start_Chainage']
df_pothole_100m['Severity'] = df_pothole_100m['Avg_Depth_mm'].apply(
    lambda d: 'Critical' if d >= 50 else ('High' if d >= 30 else ('Moderate' if d >= 15 else 'Low'))
)

# ══════ PARENT: Aggregated per CA reporting interval segment ══════
df_pothole_parent = df_pothole.groupby(
    ['Lane', 'NH_Number', 'Parent_Start_Chainage', 'Parent_End_Chainage', 'Parent_ID']
).agg(
    Pothole_Count=('Child_ID', 'count'),
    Total_Area_sq_m=('Area_sq_m', 'sum'),
    Avg_Depth_mm=('Average_Depth_mm', 'mean'),
    Max_Depth_mm=('Max_Depth_mm', 'max'),
    Avg_Area_sq_m=('Area_sq_m', 'mean'),
    Point_Lat=('Latitude', 'first'),
    Point_Long=('Longitude', 'first'),
    Image_Name=('Image_Name', 'first'),
).reset_index()

df_pothole_parent.rename(columns={'Parent_Start_Chainage': 'Start_Chainage', 'Parent_End_Chainage': 'End_Chainage'}, inplace=True)
df_pothole_parent['Defect_ID'] = [
    f"D/{SURVEY_DATE_STR}/{SOURCE_CODE}/{UPC}/{int(row['Start_Chainage'])}/Pothole_Parent/{i+1:04d}"
    for i, (_, row) in enumerate(df_pothole_parent.iterrows())
]
for col in ['Source', 'Category', 'Defect_Type', 'Zone', 'Phase', 'Cycle', 'UPC_Code', 'Run_Date', 'Project_Name']:
    df_pothole_parent[col] = df_pothole[col].iloc[0]
df_pothole_parent['UPC'] = UPC.replace('_', '/')
df_pothole_parent['Point_Chainage'] = df_pothole_parent['Start_Chainage']
df_pothole_parent['Severity'] = df_pothole_parent['Avg_Depth_mm'].apply(
    lambda d: 'Critical' if d >= 50 else ('High' if d >= 30 else ('Moderate' if d >= 15 else 'Low'))
)

print(f"\n3-Level Pothole Hierarchy:")
print(f"  Parent  ({_POTHOLE_PARENT_M}m, CA always):       {len(df_pothole_parent)} groups")
print(f"  Child   ({_POTHOLE_CHILD_MID_M}m, IRC→CA):       {len(df_pothole_100m)} groups")
print(f"  Child   ({CHILD_10M}m, CA→IRC):            {len(df_pothole_10m)} records (raw)")
print(f"  Survey date:   {SURVEY_DATE_STR}")
print(f"  Defect_ID ex:  {df_pothole_10m['Defect_ID'].iloc[0]}")
print(f"  Parent_ID ex:  {df_pothole_10m['Parent_ID'].iloc[0]}")
print(f"  Image_Name:    {df_pothole_10m['Image_Name'].notna().sum()} / {len(df_pothole_10m)}")

# COMMAND ----------

# DBTITLE 1,Apply Thresholds at All Levels
# ───────────────────────────────────────────────────────────────────────────────
# STEP 4: Finalize all 3 Pothole Silver DataFrames
# ───────────────────────────────────────────────────────────────────────────────

# Column sets for each level
child_10m_cols = [
    'Defect_ID', 'Child_ID', 'Parent_ID', 'Lane', 'NH_Number', 'Side',
    'Category', 'Defect_Type', 'Type_of_Defect',
    'Start_Chainage', 'End_Chainage', 'Parent_Start_Chainage', 'Parent_End_Chainage',
    'Point_Chainage', 'Point_Lat', 'Point_Long',
    'Area_sq_m', 'Max_Depth_mm', 'Average_Depth_mm', 'Severity',
    'Image_Name', 'Source', 'Project_Name',
    'Zone', 'Phase', 'Cycle', 'UPC', 'UPC_Code', 'Run_Date',
]

agg_cols = [
    'Defect_ID', 'Child_ID', 'Parent_ID', 'Lane', 'NH_Number',
    'Category', 'Defect_Type', 'Start_Chainage', 'End_Chainage',
    'Point_Chainage', 'Point_Lat', 'Point_Long',
    'Pothole_Count', 'Total_Area_sq_m', 'Avg_Area_sq_m', 'Avg_Depth_mm', 'Max_Depth_mm',
    'Severity', 'Image_Name', 'Source', 'Project_Name',
    'Zone', 'Phase', 'Cycle', 'UPC', 'UPC_Code', 'Run_Date',
]

# Filter to available columns
df_pothole_10m_final = df_pothole_10m[[c for c in child_10m_cols if c in df_pothole_10m.columns]].copy()
df_pothole_100m_final = df_pothole_100m[[c for c in agg_cols if c in df_pothole_100m.columns]].copy()
df_pothole_parent_final = df_pothole_parent[[c for c in agg_cols if c in df_pothole_parent.columns]].copy()

print(f"Final Pothole Silver Tables:")
print(f"  pothole_parent  ({_POTHOLE_PARENT_M}m, CA):  {df_pothole_parent_final.shape}")
print(f"  pothole_child_mid ({_POTHOLE_CHILD_MID_M}m):    {df_pothole_100m_final.shape}")
print(f"  pothole_child_10m (10m, CA):   {df_pothole_10m_final.shape}")
print(f"\n--- Parent ({_POTHOLE_PARENT_M}m) Sample ---")
display(df_pothole_parent_final.head(3))
print(f"\n--- Child 100m Sample ---")
display(df_pothole_100m_final.head(3))

# COMMAND ----------

# DBTITLE 1,Schema Driven Output Design
# MAGIC %md
# MAGIC ## Schema-driven NSV defect output tables
# MAGIC
# MAGIC This section replaces the older pothole-only write logic.
# MAGIC
# MAGIC It uses `/Workspace/Shared/Medallain Arc/nsv_defect_tables_schema 2.xlsx` as the source of truth to:
# MAGIC * build the target schemas
# MAGIC * create the output Delta tables in a safe new schema: `nhai_dhms.silver_layer_nsv_schema2`
# MAGIC * map the current UPC pothole data into the workbook-defined `pothole_child_10m` structure
# MAGIC * initialize the remaining workbook-defined defect tables with their required schemas
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load Workbook Schemas and Table Definitions
# ───────────────────────────────────────────────────────────────────────────────
# STEP 6: Load workbook-driven target schemas
# Source of truth: nsv_defect_schema_master 1.xlsx (per-table schemas)
# Each table keeps ONLY its own metric columns per the Metric Matrix.
# Table names use 'master_' prefix.
# ───────────────────────────────────────────────────────────────────────────────

import json
from pyspark.sql import types as T
from pyspark.sql.window import Window

SCHEMA_WORKBOOK = "/Workspace/Users/vansh.choubey9201@softsensor.ai/(Clone) BRONZE_CODE_16_07_26/(Clone) Clone_NSV_Extraction_updated_bronze (1)/nsv_defect_schema_master 1.xlsx"
OUTPUT_SCHEMA_NAME = "silver_defect_data"
OUTPUT_NAMESPACE = f"{CATALOG}.{OUTPUT_SCHEMA_NAME}"

# Sheet-name → output table name mapping (from Overview)
SHEET_TO_TABLE = {
    "pothole":              "master_pothole",
    "cracking":             "master_cracking",
    "roughness_parent":     "master_roughness_parent",
    "roughness_child_10m":  "master_roughness_child_10m",
    "roughness_child_100m": "master_roughness_child_100m",
    "rutting_parent":       "master_rutting_parent",
    "rutting_child_10m":    "master_rutting_child_10m",
    "rutting_child_100m":   "master_rutting_child_100m",
    "ravelling_parent":     "master_ravelling_parent",
    "ravelling_child_10m":  "master_ravelling_child_10m",
    "ravelling_child_100m": "master_ravelling_child_100m",
}

POINT_TABLES = ["master_pothole", "master_cracking"]
LINEAR_TABLES = [
    "master_roughness_parent", "master_roughness_child_10m", "master_roughness_child_100m",
    "master_rutting_parent",   "master_rutting_child_10m",   "master_rutting_child_100m",
    "master_ravelling_parent", "master_ravelling_child_10m", "master_ravelling_child_100m",
]

if not os.path.exists(SCHEMA_WORKBOOK):
    raise FileNotFoundError(f"Schema workbook not found: {SCHEMA_WORKBOOK}")

try:
    import openpyxl  # noqa: F401
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError(
        "openpyxl is required to read the Excel schema workbook. "
        "Run `%pip install openpyxl` in a setup cell, then rerun this section."
    ) from exc

overview_df = pd.read_excel(SCHEMA_WORKBOOK, sheet_name="Overview")


def normalize_col_name(name):
    return str(name).strip().lower()


def pg_type_to_spark(pg_type):
    pg = str(pg_type).strip().lower()
    if "text[]" in pg:
        return T.ArrayType(T.StringType())
    if "jsonb" in pg:
        return T.StringType()
    if "timestamp" in pg:
        return T.TimestampType()
    if pg == "date":
        return T.DateType()
    if "boolean" in pg:
        return T.BooleanType()
    if pg == "uuid":
        return T.StringType()
    if "smallint" in pg or "integer" in pg:
        return T.IntegerType()
    if "numeric" in pg or "double precision" in pg or "real" in pg or "float" in pg:
        return T.DoubleType()
    return T.StringType()


def parse_sheet_schema(sheet_name):
    """Parse a single sheet from the new master into (columns_list, StructType)."""
    df = pd.read_excel(SCHEMA_WORKBOOK, sheet_name=sheet_name, header=None, skiprows=2)
    df.columns = ["num", "column", "type", "nullable", "default", "notes"][:len(df.columns)]
    df = df[pd.to_numeric(df["num"], errors="coerce").notna()].copy()
    df["column"] = df["column"].astype(str).str.strip()
    df["type"] = df["type"].astype(str).str.strip()
    df["nullable"] = df["nullable"].astype(str).str.strip()

    fields = []
    col_names = []
    for _, row in df.iterrows():
        col_name = normalize_col_name(row["column"])
        spark_type = pg_type_to_spark(row["type"])
        nullable = str(row["nullable"]).strip().upper() != "NO"
        fields.append(T.StructField(col_name, spark_type, nullable))
        col_names.append(col_name)
    return col_names, T.StructType(fields)


# ── Build per-table schemas from individual sheets ───────────────────────────
TABLE_SCHEMAS = {}   # table_name → StructType
TABLE_COLUMNS = {}   # table_name → ordered column list
TABLE_TYPE_MAPS = {} # table_name → {col: SparkType}

for sheet_name, table_name in SHEET_TO_TABLE.items():
    cols, schema = parse_sheet_schema(sheet_name)
    TABLE_SCHEMAS[table_name] = schema
    TABLE_COLUMNS[table_name] = cols
    TABLE_TYPE_MAPS[table_name] = {f.name: f.dataType for f in schema.fields}

# Convenience aliases used by the mapping cell
POINT_SCHEMA = TABLE_SCHEMAS["master_pothole"]
POINT_COLUMNS = TABLE_COLUMNS["master_pothole"]
POINT_TYPE_MAP = TABLE_TYPE_MAPS["master_pothole"]

CRACKING_SCHEMA = TABLE_SCHEMAS["master_cracking"]
CRACKING_COLUMNS = TABLE_COLUMNS["master_cracking"]
CRACKING_TYPE_MAP = TABLE_TYPE_MAPS["master_cracking"]

# For linear tables — use roughness_child_10m as the reference for roughness,
# rutting_child_10m for rutting, ravelling_child_10m for ravelling
def get_linear_schema(defect_base, level):
    table_name = f"master_{defect_base}_{level}"
    return TABLE_SCHEMAS[table_name], TABLE_COLUMNS[table_name], TABLE_TYPE_MAPS[table_name]

# ── Add Affected Length columns to all table schemas (not in workbook yet) ───
AFFECTED_LENGTH_COLS = [
    ("affected_length", T.DoubleType()),
    ("total_length", T.DoubleType()),
    ("affected_length_ca", T.DoubleType()),
    ("affected_length_irc", T.DoubleType()),
]
for _tbl_name in TABLE_SCHEMAS:
    for _col_name, _col_type in AFFECTED_LENGTH_COLS:
        if _col_name not in TABLE_COLUMNS[_tbl_name]:
            TABLE_COLUMNS[_tbl_name].append(_col_name)
            TABLE_SCHEMAS[_tbl_name] = T.StructType(
                TABLE_SCHEMAS[_tbl_name].fields + [T.StructField(_col_name, _col_type, True)]
            )
            TABLE_TYPE_MAPS[_tbl_name][_col_name] = _col_type

# Refresh convenience aliases after schema additions
POINT_SCHEMA = TABLE_SCHEMAS["master_pothole"]
POINT_COLUMNS = TABLE_COLUMNS["master_pothole"]
POINT_TYPE_MAP = TABLE_TYPE_MAPS["master_pothole"]
CRACKING_SCHEMA = TABLE_SCHEMAS["master_cracking"]
CRACKING_COLUMNS = TABLE_COLUMNS["master_cracking"]
CRACKING_TYPE_MAP = TABLE_TYPE_MAPS["master_cracking"]

print(f"Workbook loaded: {SCHEMA_WORKBOOK}")
print(f"Output schema:   {OUTPUT_NAMESPACE}")
print(f"Target tables:   {len(TABLE_SCHEMAS)}")
for tbl in sorted(TABLE_SCHEMAS):
    print(f"  {tbl:40s} {len(TABLE_COLUMNS[tbl])} columns")
display(overview_df.fillna("").astype(str))


# COMMAND ----------

# DBTITLE 1,Load Patching Parquet and Rollup to 10m Level
# ───────────────────────────────────────────────────────────────────────────────
# STEP 6C: Load Patching Data and Roll Up to 10m Level
# Source: NSV_Backup lane folders — files matching *Patching*
# The parquet is Excel-derived: row 0 = title, row 1 = actual headers,
# data from row 2 onwards.
# Logic mirrors pothole 10m clustering from Step 3 (Cell 6):
#   1. Read CHAINAGE + AREA from Patching parquets across all lanes
#   2. Assign 10m segment: Start = (Chainage // 10) * 10, End = Start + 10
#   3. Aggregate Area (sum) per 10m segment per lane
#   4. Write to Delta table: nhai_dhms.silver_layer_nsv_schema2.master_patching_child_10m
# ───────────────────────────────────────────────────────────────────────────────

import pandas as pd
import os
from pyspark.sql import functions as F


def _parse_patching_parquet(file_path):
    """Read a patching parquet (Excel-derived format).
    Row 0 = title row, Row 1 = actual column headers, Data from row 2.
    Returns a clean DataFrame with proper column names."""
    df = pd.read_parquet(file_path)
    # Detect if the parquet has Excel-derived generic column names
    named_cols = [c for c in df.columns if not c.startswith('__UNNAMED__')]
    if len(named_cols) <= 1:
        # 0–1 named columns (e.g. just 'Annexure I') — Excel-derived: row 0 = title, row 1 = headers
        actual_headers = df.iloc[1].tolist()
        actual_headers = [h if h and str(h) != 'None' else f'_col_{i}' for i, h in enumerate(actual_headers)]
        # Deduplicate column names
        seen = {}
        for i, h in enumerate(actual_headers):
            if h in seen:
                seen[h] += 1
                actual_headers[i] = f"{h}_{seen[h]}"
            else:
                seen[h] = 0
        df_clean = df.iloc[2:].copy()
        df_clean.columns = actual_headers
        df_clean = df_clean.reset_index(drop=True)
        return df_clean
    # Some columns already named (mixed format) — use as-is
    return df


# ── Flexible column resolution helper ────────────────────────────────────────
def _find_col(cols, *keywords):
    for col in cols:
        cl = col.lower()
        if all(kw.lower() in cl for kw in keywords):
            return col
    return None

# ── Load patching parquets from all backup lane folders ──────────────────────
# Normalize per-frame: extract Chainage + Area BEFORE concat so all frames
# share the same schema regardless of whether L1/L2 or R1/R2 naming is used.
patching_frames = []
patching_files = []
lanes = discover_lane_folders(BACKUP_BASE)

for lane in lanes:
    lane_path = os.path.join(BACKUP_BASE, f"Lane={lane}")
    if not os.path.exists(lane_path):
        continue
    matches = [
        f for f in sorted(os.listdir(lane_path))
        if f.endswith('.parquet') and 'patch' in f.lower()
    ]
    for pf in matches:
        part = _parse_patching_parquet(os.path.join(lane_path, pf))
        _pcols = list(part.columns)
        _ch = _find_col(_pcols, 'chainage') or _find_col(_pcols, 'chain') or _find_col(_pcols, 'start')
        _ar = _find_col(_pcols, 'area')
        if not _ch or not _ar:
            print(f"  SKIP Lane={lane}/{pf}: no chainage({_ch}) or area({_ar}) in {_pcols}")
            continue
        norm = pd.DataFrame({
            'Chainage': pd.to_numeric(part[_ch], errors='coerce'),
            'Area': pd.to_numeric(part[_ar], errors='coerce'),
            'Lane': lane,
        })
        patching_frames.append(norm)
        patching_files.append(f"Lane={lane}/{pf}")
        print(f"  Loaded patching: Lane={lane}/{pf} ({len(norm)} rows, ch='{_ch}', area='{_ar}')")

if not patching_frames:
    print("Patching sheet not available.")
    # Build chainage spine from master Delta tables (same logic as PCI spine)
    _SCHEMA_NS_PATCH = f"{CATALOG}.silver_layer_nsv_schema2"
    _patch_spine_tables = [
        'master_cracking', 'master_roughness_child_10m', 'master_rutting_child_10m',
        'master_ravelling_child_10m', 'master_pothole',
    ]
    _patch_ranges = []
    for _tbl in _patch_spine_tables:
        _full = f"{_SCHEMA_NS_PATCH}.{_tbl}"
        if spark.catalog.tableExists(_full):
            _sdf = spark.table(_full).filter(F.col("upc_code") == UPC)
            _r = (_sdf
                  .select(
                      F.col("lane").alias("Lane"),
                      F.col("start_chainage").cast("long").alias("ch"))
                  .groupBy("Lane")
                  .agg(F.min("ch").alias("min_ch"), F.max("ch").alias("max_ch"))
                  .toPandas())
            if not _r.empty:
                _patch_ranges.append(_r)

    if _patch_ranges:
        _all_ranges = pd.concat(_patch_ranges, ignore_index=True)
        _lane_bounds = _all_ranges.groupby('Lane').agg(
            min_ch=('min_ch', 'min'), max_ch=('max_ch', 'max')).reset_index()
        _spine_rows = []
        for _, _lb in _lane_bounds.iterrows():
            _lo = int((_lb['min_ch'] // CHILD_10M) * CHILD_10M)
            _hi = int((_lb['max_ch'] // CHILD_10M) * CHILD_10M)
            for _ch in range(_lo, _hi + CHILD_10M, CHILD_10M):
                _spine_rows.append({
                    'Lane': _lb['Lane'],
                    'Start_Chainage': _ch,
                    'End_Chainage': _ch + CHILD_10M,
                    'Total_Area': 0.0,
                    'Patching_Count': 0,
                })
        df_patching_10m_final = pd.DataFrame(_spine_rows)
    else:
        df_patching_10m_final = pd.DataFrame(columns=[
            'Start_Chainage', 'End_Chainage', 'Lane', 'Total_Area', 'Patching_Count'])

    # Add metadata columns
    df_patching_10m_final['Zone'] = ZONE
    df_patching_10m_final['Phase'] = PHASE
    df_patching_10m_final['Cycle'] = CYCLE
    df_patching_10m_final['UPC'] = UPC.replace('_', '/')
    df_patching_10m_final['UPC_Code'] = UPC
    df_patching_10m_final['Run_Date'] = str(RUN_DATE)
    df_patching_10m_final['Defect_Type'] = 'Patching'

    print(f"Created patching dataframe with {len(df_patching_10m_final):,} chainage segments (Total_Area=0)")
else:
    df_patch = pd.concat(patching_frames, ignore_index=True)
    print(f"\nNormalized patching records: {len(df_patch):,}")
    print(f"Columns: {list(df_patch.columns)}")
    df_patch = df_patch.dropna(subset=['Chainage'])

    # If Chainage looks like km (all values < 1000), convert to meters
    if df_patch['Chainage'].max() < 1000:
        df_patch['Chainage'] = df_patch['Chainage'] * 1000
        print("  Chainage converted from km to meters")

    print(f"Records after cleaning: {len(df_patch):,}")
    print(f"Chainage range: {df_patch['Chainage'].min()} to {df_patch['Chainage'].max()}")

    # ── Assign 10m segment (same logic as pothole in Cell 6) ─────────────────
    df_patch['Start_Chainage'] = ((df_patch['Chainage'] // CHILD_10M) * CHILD_10M).astype(int)
    df_patch['End_Chainage'] = (df_patch['Start_Chainage'] + CHILD_10M).astype(int)

    # ── Aggregate Area per 10m segment per lane ──────────────────────────────
    df_patching_10m_final = df_patch.groupby(
        ['Lane', 'Start_Chainage', 'End_Chainage']
    ).agg(
        Total_Area=('Area', 'sum'),
        Patching_Count=('Area', 'count'),
    ).reset_index()

    # ── Add metadata columns ─────────────────────────────────────────────────
    df_patching_10m_final['Zone'] = ZONE
    df_patching_10m_final['Phase'] = PHASE
    df_patching_10m_final['Cycle'] = CYCLE
    df_patching_10m_final['UPC'] = UPC.replace('_', '/')
    df_patching_10m_final['UPC_Code'] = UPC
    df_patching_10m_final['Run_Date'] = str(RUN_DATE)
    df_patching_10m_final['Defect_Type'] = 'Patching'

    print(f"\nPatching 10m rollup: {len(df_patching_10m_final):,} segments")
    print(f"Lanes: {sorted(df_patching_10m_final['Lane'].unique().tolist())}")
    print(f"Chainage range: {df_patching_10m_final['Start_Chainage'].min()} to {df_patching_10m_final['End_Chainage'].max()}")
    print(f"Total Area: {df_patching_10m_final['Total_Area'].sum():.2f}")

# ── Write to Delta at Volume path ────────────────────────────────────────────
PATCHING_DELTA_PATH = f"/Volumes/nhai_dhms/silver_layer_nsv_schema2/defect_tables/patching/Zone={ZONE}/Phase={PHASE}/Cycle={CYCLE}/"
if df_patching_10m_final.empty:
    from pyspark.sql import types as T
    _patching_schema = T.StructType([
        T.StructField("Start_Chainage", T.LongType(), True),
        T.StructField("End_Chainage", T.LongType(), True),
        T.StructField("Lane", T.StringType(), True),
        T.StructField("Total_Area", T.DoubleType(), True),
        T.StructField("Patching_Count", T.LongType(), True),
        T.StructField("Zone", T.StringType(), True),
        T.StructField("Phase", T.StringType(), True),
        T.StructField("Cycle", T.StringType(), True),
        T.StructField("UPC", T.StringType(), True),
        T.StructField("UPC_Code", T.StringType(), True),
        T.StructField("Run_Date", T.StringType(), True),
        T.StructField("Defect_Type", T.StringType(), True),
    ])
    sdf_patching = spark.createDataFrame([], schema=_patching_schema)
else:
    sdf_patching = spark.createDataFrame(df_patching_10m_final)

(
    sdf_patching.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(PATCHING_DELTA_PATH)
)

total_rows = spark.read.format("delta").load(PATCHING_DELTA_PATH).count()
print(f"\nWritten to: {PATCHING_DELTA_PATH} ({total_rows:,} rows)")
display(sdf_patching.limit(15))

# COMMAND ----------

# DBTITLE 1,Load D3 Condition Data (Tracker already loaded in Cell 5)
# ───────────────────────────────────────────────────────────────────────────────
# STEP 6B: Load NSV Activity Tracker and condition lookups
# Tracker provides survey metadata; D2 naming overrides the standard label for
# this UPC when the available comparison sheet is 'Comparison as per CA'.
# ───────────────────────────────────────────────────────────────────────────────
import pandas as pd, os

TRACKER_PATH = "/Workspace/Shared/NSV Extraction/NSV_Activity_Tracker_ZoneWise_Standardized_17072026.xlsx"


def load_tracker_context(zone, phase, cycle, upc_slash):
    sheet_map = {"A": "Zone A", "B": "Zone B", "C": "Zone C", "D": "Zone D", "E": "Zone E"}
    sheet_name = sheet_map.get(zone, f"Zone {zone}")
    df = pd.read_excel(TRACKER_PATH, sheet_name=sheet_name, header=1)

    upc_norm = upc_slash.replace('/', '_').replace('_', '').lower()
    best_row = None
    for _, row in df.iterrows():
        upc_val = str(row.iloc[12]).replace('/', '_').replace('_', '').lower()
        if upc_norm not in upc_val and upc_val not in upc_norm:
            continue
        phase_val = str(row.iloc[2]).lower().replace('phase', '').strip()
        cycle_val = str(row.iloc[3]).lower().replace('cycle', '').strip()
        if phase_val == str(phase).strip() and cycle_val == str(cycle).strip():
            best_row = row
            break
        if best_row is None:
            best_row = row

    def _default_ctx():
        return {
            'survey_start_date': None,
            'survey_end_date': None,
            'piu': None,
            'ae_ie': None,
            'contractor': None,
            'state': None,
            'pavement_type': None,
            'pci': None,
            'threshold_label_override': None,
            'defect_standards': {
                d: {'standard': 'CA', 'interval_m': 100}
                for d in ['ravelling', 'rutting', 'cracking', 'roughness', 'pothole']
            },
        }

    if best_row is None:
        print(f"  WARNING: UPC {upc_slash} not found in tracker. Using CA defaults.")
        ctx = _default_ctx()
    else:
        r = best_row
        survey_start = pd.to_datetime(r.iloc[21], errors='coerce')
        survey_end = pd.to_datetime(r.iloc[22], errors='coerce')

        def _parse_std(val):
            s = str(val).strip().upper() if pd.notna(val) else ""
            if 'IRC' in s:
                return 'IRC'
            if 'CA' in s:
                return 'CA'
            return None

        def _parse_interval(val):
            try:
                return int(float(val))
            except Exception:
                return None

        defect_standards = {
            'ravelling': {'standard': _parse_std(r.iloc[46]), 'interval_m': _parse_interval(r.iloc[47]) or 100},
            'rutting':   {'standard': _parse_std(r.iloc[48]), 'interval_m': _parse_interval(r.iloc[49]) or 100},
            'cracking':  {'standard': _parse_std(r.iloc[50]), 'interval_m': _parse_interval(r.iloc[51]) or 100},
            'roughness': {'standard': _parse_std(r.iloc[52]), 'interval_m': _parse_interval(r.iloc[53]) or 100},
        }
        defect_standards['pothole'] = defect_standards['cracking'].copy()

        ctx = {
            'survey_start_date': survey_start.date() if pd.notnull(survey_start) else None,
            'survey_end_date': survey_end.date() if pd.notnull(survey_end) else None,
            'piu': str(r.iloc[7]).strip() if pd.notna(r.iloc[7]) else None,
            'ae_ie': str(r.iloc[9]).strip() if pd.notna(r.iloc[9]) else None,
            'contractor': str(r.iloc[8]).strip() if pd.notna(r.iloc[8]) else None,
            'state': str(r.iloc[6]).strip() if pd.notna(r.iloc[6]) else None,
            'pavement_type': str(r.iloc[42]).strip() if pd.notna(r.iloc[42]) else None,
            'pci': float(r.iloc[43]) if pd.notna(r.iloc[43]) else None,
            'threshold_label_override': None,
            'defect_standards': defect_standards,
        }

    # Prefer the actual D2 comparison naming for threshold label on this UPC.
    d2_files = []
    if os.path.exists(D2_BASE):
        d2_files = [f for f in os.listdir(D2_BASE) if f.endswith('.parquet')]
    if any('comparison as per ca' in f.lower() for f in d2_files):
        ctx['threshold_label_override'] = 'As per CA'
        for defect_name in ctx['defect_standards']:
            ctx['defect_standards'][defect_name]['standard'] = 'CA'
            ctx['defect_standards'][defect_name]['interval_m'] = ctx['defect_standards'][defect_name].get('interval_m') or 100
    elif any('comparison as per irc' in f.lower() for f in d2_files):
        ctx['threshold_label_override'] = 'As per IRC'
        for defect_name in ctx['defect_standards']:
            ctx['defect_standards'][defect_name]['standard'] = 'IRC'
            ctx['defect_standards'][defect_name]['interval_m'] = ctx['defect_standards'][defect_name].get('interval_m') or 100

    return ctx


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


# TRACKER_CTX already loaded in Cell 5 with raw_defect_standards for interval computation
# TRACKER_CTX = load_tracker_context(ZONE, PHASE, CYCLE, UPC.replace('_', '/'))

D3_CONDITION = {
    'roughness': build_condition_lookup(df_roughness_raw),
    'rutting': build_condition_lookup(df_rutting_raw),
    'cracking': build_condition_lookup(df_cracking_raw),
    'ravelling': build_condition_lookup(df_ravelling_raw),
}

for defect_name, lookup_pdf in D3_CONDITION.items():
    if not lookup_pdf.empty:
        print(f"  Condition lookup loaded: {defect_name} — {len(lookup_pdf):,} rows, values: {lookup_pdf['condition'].value_counts().head(4).to_dict()}")


# get_threshold_name() and get_reporting_interval() already defined in Cell 5
# along with get_parent_interval(), get_child_mid_interval(), and DEFECT_INTERVALS


def lookup_d3_condition(defect_name, start_ch, end_ch=None, lane=None):
    df = D3_CONDITION.get(defect_name, pd.DataFrame())
    if df.empty or start_ch is None:
        return None
    probe = float(start_ch)
    matches = df[(df['start_chainage'] <= probe) & (df['end_chainage'] >= probe)]
    if not matches.empty and lane and 'lane' in matches.columns:
        lane_matches = matches[matches['lane'].astype(str) == str(lane)]
        if not lane_matches.empty:
            matches = lane_matches
    return str(matches.iloc[0]['condition']) if not matches.empty and pd.notna(matches.iloc[0]['condition']) else None


print(f"\nNSV Tracker Context for UPC {UPC}:")
print(f"  Survey: {TRACKER_CTX['survey_start_date']} to {TRACKER_CTX['survey_end_date']}")
print(f"  PIU: {TRACKER_CTX['piu']} | AE/IE: {TRACKER_CTX['ae_ie']}")
print(f"  Contractor: {TRACKER_CTX['contractor']}")
print(f"  Pavement: {TRACKER_CTX['pavement_type']} | PCI: {TRACKER_CTX['pci']}")
print(f"  Threshold label override: {TRACKER_CTX.get('threshold_label_override')}")
print("  Reporting Standards:")
for d, info in TRACKER_CTX['defect_standards'].items():
    print(f"    {d:12s}: {info['standard']} @ {info['interval_m']}m")

# COMMAND ----------

# DBTITLE 1,Map Current UPC Data to Workbook Schema
# ───────────────────────────────────────────────────────────────────────────────
# STEP 7: Map current UPC pothole data to workbook schema (88 columns)
# Populates only pothole_child_10m for this UPC; other tables are schema-only
# ───────────────────────────────────────────────────────────────────────────────

upc_slash = UPC.replace("_", "/")
source_system = "NSV"
defect_base = "pothole"
collection_name = "master_pothole"


# Pre-assign sequential row IDs in pandas to avoid Window.orderBy(monotonically_increasing_id())
# which forces all data into a single partition and kills parallelism
_pothole_pdf = df_pothole_10m_final.copy()
_pothole_pdf['_row_id'] = range(1, len(_pothole_pdf) + 1)
source_pothole_sdf = spark.createDataFrame(_pothole_pdf)
source_cols = set(source_pothole_sdf.columns)


def typed_null(dtype):
    return F.lit(None).cast(dtype)


def source_col(col_name, dtype):
    return F.col(col_name).cast(dtype) if col_name in source_cols else typed_null(dtype)


survey_ts = F.to_timestamp(F.col("Survey_Date")) if "Survey_Date" in source_cols else typed_null(T.TimestampType())
survey_dt = F.to_date(survey_ts)

nsv_payload_cols = [
    c for c in [
        "Defect_ID", "Child_ID", "Parent_ID", "Lane", "NH_Number", "Side",
        "Type_of_Defect", "Start_Chainage", "End_Chainage", "Area_sq_m",
        "Max_Depth_mm", "Average_Depth_mm", "Severity", "Image_Name",
        "Latitude", "Longitude", "Survey_Date"
    ] if c in source_cols
]

nsv_payload = F.to_json(F.struct(*[F.col(c) for c in nsv_payload_cols])) if nsv_payload_cols else typed_null(T.StringType())

measures_payload = F.to_json(
    F.struct(
        source_col("Area_sq_m", T.DoubleType()).alias("area_sqm"),
        source_col("Max_Depth_mm", T.DoubleType()).alias("max_depth_mm"),
        source_col("Average_Depth_mm", T.DoubleType()).alias("average_depth_mm")
    )
)

point_exprs = {
    "id": F.concat_ws("_", F.lit(UPC), F.lit(collection_name), F.format_string("%06d", F.col("_row_id"))),
    "collection_name": F.lit(collection_name),
    "defect_base": F.lit(defect_base),
    "level": F.lit("child_10m"),
    "standard": typed_null(POINT_TYPE_MAP["standard"]),
    "defect_id": source_col("Defect_ID", POINT_TYPE_MAP["defect_id"]),
    "display_defect_id": source_col("Defect_ID", POINT_TYPE_MAP["display_defect_id"]),
    "source_defect_id": source_col("Child_ID", POINT_TYPE_MAP["source_defect_id"]),
    "parent_id": source_col("Parent_ID", POINT_TYPE_MAP["parent_id"]),
    "record_id": source_col("Child_ID", POINT_TYPE_MAP["record_id"]),
    "upc": F.lit(upc_slash),
    "upc_code": F.lit(UPC),
    "nh_number": source_col("NH_Number", POINT_TYPE_MAP["nh_number"]),
    "section_code": typed_null(POINT_TYPE_MAP["section_code"]),
    "zone": F.lit(ZONE),
    "phase": F.lit(PHASE),
    "cycle": F.lit(int(CYCLE)).cast(POINT_TYPE_MAP["cycle"]),
    "source_system": F.lit(source_system),
    "sources": F.array(F.lit(source_system)).cast(POINT_TYPE_MAP["sources"]),
    "defect_type": F.lit("Pothole"),
    "type_of_defect": source_col("Type_of_Defect", POINT_TYPE_MAP["type_of_defect"]),
    "category": F.lit("POINT"),
    "severity": source_col("Severity", POINT_TYPE_MAP["severity"]),
    "pavement_type": typed_null(POINT_TYPE_MAP["pavement_type"]),
    "pci": typed_null(POINT_TYPE_MAP["pci"]),
    "lane": source_col("Lane", POINT_TYPE_MAP["lane"]),
    "breach_flag": F.lit(1).cast(POINT_TYPE_MAP["breach_flag"]),
    "metric_name": F.lit("area_sq_m"),
    "metric_value": source_col("Area_sq_m", POINT_TYPE_MAP["metric_value"]),
    "threshold_name": F.lit(get_threshold_name("pothole")).cast(POINT_TYPE_MAP["threshold_name"]),
    "threshold_value": F.lit(0.0).cast(POINT_TYPE_MAP["threshold_value"]),
    "total_children": F.lit(1).cast(POINT_TYPE_MAP["total_children"]),
    "length_m": F.greatest(source_col("End_Chainage", T.DoubleType()) - source_col("Start_Chainage", T.DoubleType()), F.lit(0.0)).cast(POINT_TYPE_MAP["length_m"]),
    "area_sqm": source_col("Area_sq_m", POINT_TYPE_MAP["area_sqm"]),
    "start_chainage": source_col("Start_Chainage", POINT_TYPE_MAP["start_chainage"]),
    "end_chainage": source_col("End_Chainage", POINT_TYPE_MAP["end_chainage"]),
    "defect_chainage": source_col("Point_Chainage", POINT_TYPE_MAP["defect_chainage"]),
    "start_lat": source_col("Point_Lat", POINT_TYPE_MAP["start_lat"]),
    "start_long": source_col("Point_Long", POINT_TYPE_MAP["start_long"]),
    "end_lat": source_col("Point_Lat", POINT_TYPE_MAP["end_lat"]),
    "end_long": source_col("Point_Long", POINT_TYPE_MAP["end_long"]),
    "defect_lat": source_col("Point_Lat", POINT_TYPE_MAP["defect_lat"]),
    "defect_long": source_col("Point_Long", POINT_TYPE_MAP["defect_long"]),
    "datetime": F.coalesce(survey_ts, F.lit(str(TRACKER_CTX['survey_start_date'])).cast("timestamp") if TRACKER_CTX['survey_start_date'] else F.current_timestamp()).cast(POINT_TYPE_MAP["datetime"]),
    "observed_at": F.coalesce(survey_ts, F.lit(str(TRACKER_CTX['survey_start_date'])).cast("timestamp") if TRACKER_CTX['survey_start_date'] else F.current_timestamp()).cast(POINT_TYPE_MAP["observed_at"]),
    "survey_start_date": F.lit(str(TRACKER_CTX['survey_start_date'])).cast("date").cast(POINT_TYPE_MAP["survey_start_date"]) if TRACKER_CTX['survey_start_date'] else typed_null(POINT_TYPE_MAP["survey_start_date"]),
    "survey_end_date": F.lit(str(TRACKER_CTX['survey_end_date'])).cast("date").cast(POINT_TYPE_MAP["survey_end_date"]) if TRACKER_CTX['survey_end_date'] else typed_null(POINT_TYPE_MAP["survey_end_date"]),
    "image_name": source_col("Image_Name", POINT_TYPE_MAP["image_name"]),
    "image_url": typed_null(POINT_TYPE_MAP["image_url"]),
    "image_urls": F.when(F.col("Image_Name").isNotNull(), F.array(F.col("Image_Name").cast("string"))).otherwise(typed_null(POINT_TYPE_MAP["image_urls"])) if "Image_Name" in source_cols else typed_null(POINT_TYPE_MAP["image_urls"]),
    "remarks": typed_null(POINT_TYPE_MAP["remarks"]),
    "is_duplicate": F.lit(False).cast(POINT_TYPE_MAP["is_duplicate"]),
    "nsv": nsv_payload.cast(POINT_TYPE_MAP["nsv"]),
    "dams": typed_null(POINT_TYPE_MAP["dams"]),
    "dashcam": typed_null(POINT_TYPE_MAP["dashcam"]),
    "dlm": typed_null(POINT_TYPE_MAP["dlm"]),
    "project_details": F.to_json(
        F.struct(
            F.lit(UPC).alias("project_name"),
            F.lit(ZONE).alias("zone"),
            F.lit(PHASE).alias("phase"),
            F.lit(CYCLE).alias("cycle")
        )
    ).cast(POINT_TYPE_MAP["project_details"]),
    "created_at": F.current_timestamp().cast(POINT_TYPE_MAP["created_at"]),
    "updated_at": F.current_timestamp().cast(POINT_TYPE_MAP["updated_at"]),
    "side": source_col("Side", POINT_TYPE_MAP["side"]),
    "carriageway": typed_null(POINT_TYPE_MAP["carriageway"]),
    "direction": typed_null(POINT_TYPE_MAP["direction"]),
    "parent_start_chainage": source_col("Parent_Start_Chainage", POINT_TYPE_MAP["parent_start_chainage"]),
    "parent_end_chainage": source_col("Parent_End_Chainage", POINT_TYPE_MAP["parent_end_chainage"]),
    "reporting_interval_m": F.lit(get_reporting_interval("pothole")).cast(POINT_TYPE_MAP["reporting_interval_m"]),
    "segment_count": F.lit(1).cast(POINT_TYPE_MAP["segment_count"]),
    "iri_condition": typed_null(POINT_TYPE_MAP["iri_condition"]),
    "rating_ca": source_col("Severity", POINT_TYPE_MAP["rating_ca"]) if TRACKER_CTX['defect_standards']['pothole']['standard'] == 'CA' else typed_null(POINT_TYPE_MAP["rating_ca"]),
    "rating_irc": source_col("Severity", POINT_TYPE_MAP["rating_irc"]) if TRACKER_CTX['defect_standards']['pothole']['standard'] == 'IRC' else typed_null(POINT_TYPE_MAP["rating_irc"]),
    "defect_type_filter": F.lit("Pothole"),
    "has_standard_defect": F.lit(True).cast(POINT_TYPE_MAP["has_standard_defect"]),
    "standard_flags": F.lit("{}").cast(POINT_TYPE_MAP["standard_flags"]),
    "thresholds": typed_null(POINT_TYPE_MAP["thresholds"]),
    "measures": measures_payload.cast(POINT_TYPE_MAP["measures"]),
    # Pothole: affected length = area_sqm of the pothole
    "affected_length": source_col("Area_sq_m", T.DoubleType()),
    "total_length": source_col("Area_sq_m", T.DoubleType()),
    "affected_length_ca": source_col("Area_sq_m", T.DoubleType()),
    "affected_length_irc": source_col("Area_sq_m", T.DoubleType()),
    "run_date": F.lit(RUN_DATE).cast(POINT_TYPE_MAP["run_date"]),
    "detected_on": F.lit(str(TRACKER_CTX['survey_end_date'])).cast("date").cast(POINT_TYPE_MAP["detected_on"]) if TRACKER_CTX['survey_end_date'] else F.coalesce(survey_dt, F.current_date()).cast(POINT_TYPE_MAP["detected_on"]),
    "project_id": typed_null(POINT_TYPE_MAP["project_id"]),
    "region_id": typed_null(POINT_TYPE_MAP["region_id"]),
    "piu_id": typed_null(POINT_TYPE_MAP["piu_id"]),
    "state_id": typed_null(POINT_TYPE_MAP["state_id"]),
    "zone_id": typed_null(POINT_TYPE_MAP["zone_id"]),
    "contractor_id": typed_null(POINT_TYPE_MAP["contractor_id"]),
    "ae_ie_id": typed_null(POINT_TYPE_MAP["ae_ie_id"]),
}

pothole_output_sdf = source_pothole_sdf.select(
    *[point_exprs.get(col_name, typed_null(POINT_TYPE_MAP[col_name])).alias(col_name) for col_name in POINT_COLUMNS]
)

print(f"Mapped pothole rows: {pothole_output_sdf.count():,}")
print(f"Mapped pothole cols: {len(pothole_output_sdf.columns)}")
display(pothole_output_sdf.limit(5))

# ───────────────────────────────────────────────────────────────────────────────
# Extend mapping for cracking, roughness, rutting, and ravelling from backup
# ───────────────────────────────────────────────────────────────────────────────

def pandas_to_spark_with_schema(pdf, schema_obj):
    pdf = pdf.copy()
    pdf = pdf.where(pd.notnull(pdf), None)

    def convert_value(value, dtype):
        if value is None:
            return None
        if isinstance(dtype, T.ArrayType):
            if isinstance(value, np.ndarray):
                return value.tolist()
            if isinstance(value, tuple):
                return list(value)
            if isinstance(value, list):
                return value
            return None if pd.isna(value) else [value]
        if isinstance(dtype, T.TimestampType):
            ts = pd.to_datetime(value, errors="coerce")
            return ts.to_pydatetime() if pd.notnull(ts) else None
        if isinstance(dtype, T.DateType):
            dt = pd.to_datetime(value, errors="coerce")
            return dt.date() if pd.notnull(dt) else None
        return value

    records = []
    for record in pdf.to_dict("records"):
        converted = {}
        for field in schema_obj.fields:
            converted[field.name] = convert_value(record.get(field.name), field.dataType)
        records.append(converted)

    return spark.createDataFrame(records, schema=schema_obj)


def empty_with_schema(schema_obj):
    return spark.createDataFrame([], schema_obj)


def norm_name(name):
    return "".join(ch.lower() for ch in str(name) if ch.isalnum())


def pick_col(columns, *candidates):
    normalized = {norm_name(c): c for c in columns}
    for cand in candidates:
        key = norm_name(cand)
        if key in normalized:
            return normalized[key]
    for cand in candidates:
        key = norm_name(cand)
        for col in columns:
            if key in norm_name(col):
                return col
    return None


def as_float(value):
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except Exception:
        return None


def as_int(value):
    num = as_float(value)
    return int(num) if num is not None else None


def as_chainage_m(value, is_km=False):
    num = as_float(value)
    if num is None:
        return None
    # Columns labelled '(km)' sometimes already store metres (e.g. 107020).
    # True km values for Indian NHs are < 1000; metre values are >= 1000.
    if is_km and num < 1000:
        num = num * 1000
    return int(round(num))


def as_text(value):
    if value is None or pd.isna(value):
        return None
    value = str(value).strip()
    return value if value and value != "<NA>" else None


def first_non_null(*values):
    for value in values:
        if value is None:
            continue
        if isinstance(value, float) and np.isnan(value):
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return value
    return None


def get_survey_date_str():
    if TRACKER_CTX.get("survey_start_date"):
        return pd.Timestamp(TRACKER_CTX["survey_start_date"]).strftime("%d%m%y")
    for df_candidate, date_cols in [
        (df_pothole_raw, ["Survey_Date"]),
        (df_cracking_raw, ["SurveyDate"]),
        (df_roughness_raw, ["SurveyDate"]),
        (df_rutting_raw, ["SurveyDate"]),
        (df_visual_raw, ["SurveyDate"]),
    ]:
        if df_candidate is None or len(df_candidate) == 0:
            continue
        for col_name in date_cols:
            if col_name in df_candidate.columns:
                series = pd.to_datetime(df_candidate[col_name], errors="coerce").dropna()
                if len(series) > 0:
                    return series.iloc[0].strftime("%d%m%y")
    return RUN_DATE.strftime("%d%m%y")


SURVEY_DATE_STR = get_survey_date_str()
NOW_TS = pd.Timestamp.now()
RUN_TS = pd.Timestamp(RUN_DATE)
D2_BASE = f"{UPC_BASE}/Deliverable=Deliverable - 2"

# ── Lane → Direction mapping (from D1 Roughness analysis) ──────────────────────
LANE_TO_DIRECTION = {
    'L1': 'Increasing', 'L2': 'Increasing',
    'R1': 'Decreasing', 'R2': 'Decreasing',
}
def lane_to_direction(lane_value):
    """Derive direction from lane name (L1/L2=Increasing, R1/R2=Decreasing)."""
    if lane_value is None:
        return None
    return LANE_TO_DIRECTION.get(str(lane_value).strip().upper(), None)


def compute_breach_flag(defect_base_name, metric_value, roughness_bi=None, iri_value=None):
    """Compute breach flag based on defect-specific thresholds."""
    if defect_base_name == 'roughness':
        iri_v = pd.to_numeric(pd.Series([iri_value or metric_value]), errors='coerce').iloc[0]
        bi_v = pd.to_numeric(pd.Series([roughness_bi]), errors='coerce').iloc[0]
        if pd.notna(iri_v) and iri_v > IRC_IRI_POOR:
            return 1
        if pd.notna(bi_v) and bi_v >= CA_BI_THRESHOLD:
            return 1
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
    """Breach flag per CA standard only."""
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
    """Breach flag per IRC standard: compare BI value against IRC_IRI_POOR (2400 mm/km)."""
    if defect_base_name == 'roughness':
        # IRC_IRI_POOR stores the IRC BI threshold (2400 mm/km).
        # Compare the BI value (roughness_bi or metric_value) against it.
        bi_v = pd.to_numeric(pd.Series([roughness_bi or metric_value]), errors='coerce').iloc[0]
        return 1 if pd.notna(bi_v) and bi_v >= IRC_IRI_POOR else 0
    return 0  # IRC thresholds mainly apply to roughness


def uniq(items):
    out = []
    seen = set()
    for item in items:
        if item and item not in seen:
            out.append(item)
            seen.add(item)
    return out


def prepare_lrms_lookup(pdf):
    if pdf.empty:
        return pd.DataFrame(columns=["chainage", "nh_number", "section_code", "direction"])
    cols = list(pdf.columns)
    chain_c = pick_col(cols, "Chainage")
    nh_c = pick_col(cols, "NHNumber", "New NH number", "New NH number NHNumber")
    section_c = pick_col(cols, "SectionCode", "Section")
    direction_c = pick_col(cols, "Direction")

    rows = []
    for _, row in pdf.iterrows():
        chainage_value = as_chainage_m(row[chain_c]) if chain_c else None
        if chainage_value is None:
            continue
        rows.append({
            "chainage": chainage_value,
            "nh_number": as_text(row[nh_c]) if nh_c else None,
            "section_code": as_text(row[section_c]) if section_c else None,
            "direction": as_text(row[direction_c]) if direction_c else None,
        })
    return pd.DataFrame(rows).sort_values("chainage").reset_index(drop=True) if rows else pd.DataFrame(columns=["chainage", "nh_number", "section_code", "direction"])


def prepare_interval_lookup(pdf, start_candidates, end_candidates, field_candidates, float_fields=None, date_fields=None):
    float_fields = set(float_fields or [])
    date_fields = set(date_fields or [])
    if pdf.empty:
        return pd.DataFrame(columns=["start_chainage", "end_chainage", *field_candidates.keys()])

    cols = list(pdf.columns)
    start_c = pick_col(cols, *start_candidates)
    end_c = pick_col(cols, *end_candidates)
    resolved_fields = {name: pick_col(cols, *cands) for name, cands in field_candidates.items()}

    rows = []
    for _, row in pdf.iterrows():
        start_chainage_value = as_chainage_m(row[start_c]) if start_c else None
        end_chainage_value = as_chainage_m(row[end_c]) if end_c else None
        if start_chainage_value is None and end_chainage_value is None:
            continue
        if start_chainage_value is None:
            start_chainage_value = end_chainage_value
        if end_chainage_value is None:
            end_chainage_value = start_chainage_value
        if end_chainage_value < start_chainage_value:
            start_chainage_value, end_chainage_value = end_chainage_value, start_chainage_value

        record = {
            "start_chainage": start_chainage_value,
            "end_chainage": end_chainage_value,
        }
        for field_name, col_name in resolved_fields.items():
            if not col_name:
                record[field_name] = None
            elif field_name in float_fields:
                record[field_name] = as_float(row[col_name])
            elif field_name in date_fields:
                record[field_name] = pd.to_datetime(row[col_name], errors="coerce")
            else:
                record[field_name] = as_text(row[col_name])
        rows.append(record)

    return pd.DataFrame(rows)


def _apply_optional_filter(pdf, column_name, value):
    if pdf.empty or column_name not in pdf.columns or value is None:
        return pdf
    normalized = pdf[column_name].fillna("").astype(str).str.strip().str.lower()
    filtered = pdf[normalized == str(value).strip().lower()]
    return filtered if not filtered.empty else pdf


def lookup_interval_record(pdf, chainage_value, lane=None, direction=None):
    if pdf.empty or chainage_value is None:
        return {}

    matches = pdf[(pdf["start_chainage"] <= chainage_value) & (pdf["end_chainage"] >= chainage_value)]
    if matches.empty:
        probe = pdf.copy()
        probe["_distance"] = ((probe["start_chainage"] + probe["end_chainage"]) / 2 - chainage_value).abs()
        matches = probe.sort_values("_distance")

    matches = _apply_optional_filter(matches, "direction", direction)
    matches = _apply_optional_filter(matches, "lane", lane)
    return matches.iloc[0].to_dict() if not matches.empty else {}


def lookup_lrms_record(chainage_value, direction=None):
    if LRMS_LOOKUP.empty or chainage_value is None:
        return {}
    probe = LRMS_LOOKUP.copy()
    probe = _apply_optional_filter(probe, "direction", direction)
    probe["_distance"] = (probe["chainage"] - chainage_value).abs()
    return probe.sort_values("_distance").iloc[0].to_dict() if not probe.empty else {}


df_lrms_master, _ = load_matching_parquets(D1_BASE, ["LRPMaster"])
df_pavement_master, _ = load_matching_parquets(D1_BASE, ["PavementType"])
df_carriageway_master, _ = load_matching_parquets(D1_BASE, ["CarriagewayType"])

# ── Filter out LCMS Issue / UC junk values from D1 lookups ───────────────────
_JUNK_VALUES = {'lcms issue', 'uc', 'na', 'n/a', ''}

def _clean_d1_lookup(pdf, value_col):
    """Remove rows where the key value column contains LCMS Issue or similar junk."""
    if pdf.empty or value_col not in pdf.columns:
        return pdf
    mask = pdf[value_col].fillna('').astype(str).str.strip().str.lower().isin(_JUNK_VALUES)
    cleaned = pdf[~mask].copy()
    print(f"    D1 cleanup: {value_col} — removed {mask.sum()} junk rows ({', '.join(pdf.loc[mask, value_col].dropna().unique()[:3])})")
    return cleaned

df_pavement_master = _clean_d1_lookup(df_pavement_master, 'PavementType')
df_carriageway_master = _clean_d1_lookup(df_carriageway_master, 'CarriagewayType')
df_cracking_shape, _ = load_matching_parquets(D3_BASE, ["Cracking"])
df_roughness_shape, _ = load_matching_parquets(D3_BASE, ["Roughness"])
df_rutting_shape, _ = load_matching_parquets(D3_BASE, ["Rutting"])
df_d2_raw, _ = load_matching_parquets(D2_BASE, ["Comparison as per CA", ".parquet"])

LRMS_LOOKUP = prepare_lrms_lookup(df_lrms_master)
PAVEMENT_LOOKUP = prepare_interval_lookup(
    df_pavement_master,
    ["New NH number StartChainage", "Start_Chainage"],
    ["New NH number EndChainage", "End_Chainage"],
    {
        "nh_number": ["New NH number NHNumber", "New NH number", "NHNumber"],
        "section_code": ["SectionCode", "Section"],
        "direction": ["Direction"],
        "pavement_type": ["PavementType", "Surface Type"],
    },
)
CARRIAGEWAY_LOOKUP = prepare_interval_lookup(
    df_carriageway_master,
    ["New NH number StartChainage", "Start_Chainage"],
    ["New NH number EndChainage", "End_Chainage"],
    {
        "nh_number": ["New NH number", "New NH number NHNumber", "NHNumber"],
        "section_code": ["SectionCode", "Section"],
        "direction": ["Direction"],
        "carriageway": ["CarriagewayType", "carriageway"],
    },
)
CRACKING_SHAPE_LOOKUP = prepare_interval_lookup(
    df_cracking_shape,
    ["Start_Chainage"],
    ["End_Chainage"],
    {
        "lane": ["Lane"],
        "condition": ["Condition"],
        "surface_type": ["Surface Type"],
        "cracking_pct": ["Cracking"],
        "start_lat": ["Start_Lat"],
        "start_long": ["Start_Lon"],
        "end_lat": ["End_Lat"],
        "end_long": ["End_Lon"],
    },
    float_fields={"cracking_pct", "start_lat", "start_long", "end_lat", "end_long"},
)
ROUGHNESS_SHAPE_LOOKUP = prepare_interval_lookup(
    df_roughness_shape,
    ["Start_Chainage"],
    ["End_Chainage"],
    {
        "lane": ["Lane"],
        "condition": ["Condition"],
        "surface_type": ["Surface Type"],
        "roughness_bi": ["BI m/km"],
        "start_lat": ["Start_Lat"],
        "start_long": ["Start_Lon"],
        "end_lat": ["End_Lat"],
        "end_long": ["End_Lon"],
    },
    float_fields={"roughness_bi", "start_lat", "start_long", "end_lat", "end_long"},
)
RUTTING_SHAPE_LOOKUP = prepare_interval_lookup(
    df_rutting_shape,
    ["Start_Chainage"],
    ["End_Chainage"],
    {
        "lane": ["Lane"],
        "condition": ["Condition"],
        "surface_type": ["Surface Type"],
        "rutting_avg": ["Rutting"],
        "start_lat": ["Start_Lat"],
        "start_long": ["Start_Lon"],
        "end_lat": ["End_Lat"],
        "end_long": ["End_Lon"],
    },
    float_fields={"rutting_avg", "start_lat", "start_long", "end_lat", "end_long"},
)

DEFECT_SHEET_LOOKUPS = {
    "cracking": CRACKING_SHAPE_LOOKUP,
    "roughness": ROUGHNESS_SHAPE_LOOKUP,
    "rutting": RUTTING_SHAPE_LOOKUP,
    "ravelling": pd.DataFrame(),
}


def get_road_context(start_chainage_value, end_chainage_value=None, lane=None, direction=None):
    probe_chainage = first_non_null(start_chainage_value, end_chainage_value)
    lrms_row = lookup_lrms_record(probe_chainage, direction=direction)
    pavement_row = lookup_interval_record(PAVEMENT_LOOKUP, probe_chainage, direction=direction)
    carriageway_row = lookup_interval_record(CARRIAGEWAY_LOOKUP, probe_chainage, direction=direction)
    return {
        "nh_number": first_non_null(lrms_row.get("nh_number"), pavement_row.get("nh_number"), carriageway_row.get("nh_number")),
        "section_code": first_non_null(lrms_row.get("section_code"), pavement_row.get("section_code"), carriageway_row.get("section_code")),
        "direction": first_non_null(direction, lrms_row.get("direction"), pavement_row.get("direction"), carriageway_row.get("direction")),
        "pavement_type": first_non_null(pavement_row.get("pavement_type")),
        "carriageway": first_non_null(carriageway_row.get("carriageway")),
    }


def get_defect_sheet_context(defect_base_name, start_chainage_value, end_chainage_value=None, lane=None, direction=None):
    lookup_pdf = DEFECT_SHEET_LOOKUPS.get(defect_base_name, pd.DataFrame())
    probe_chainage = first_non_null(start_chainage_value, end_chainage_value)
    return lookup_interval_record(lookup_pdf, probe_chainage, lane=lane, direction=direction)


def build_point_pdf(records, columns=None):
    cols = columns or POINT_COLUMNS
    if not records:
        return pd.DataFrame(columns=cols)
    pdf = pd.DataFrame(records)
    for col_name in cols:
        if col_name not in pdf.columns:
            pdf[col_name] = None
    return pdf[cols]


def build_linear_pdf(records, columns=None):
    cols = columns or TABLE_COLUMNS.get("master_roughness_child_10m", [])
    if not records:
        return pd.DataFrame(columns=cols)
    pdf = pd.DataFrame(records)
    for col_name in cols:
        if col_name not in pdf.columns:
            pdf[col_name] = None
    return pdf[cols]


def make_base_point_record(defect_base_name, collection_name_value, defect_id_value, record_id_value, parent_id_value, start_chainage_value, end_chainage_value, start_lat_value, start_long_value, end_lat_value, end_long_value, defect_lat_value, defect_long_value, lane_value, side_value, severity_value, metric_name_value, metric_value_value, image_name_value=None, survey_value=None, length_value=None, area_value=None, source_json=None, measures_json=None, remarks_value=None, category_value="POINT"):
    # Constant parent interval (1000m, CA)
    _parent_m = DEFECT_INTERVALS.get(defect_base_name, {}).get('parent_m', PARENT_LENGTH_M)
    survey_ts_value = pd.to_datetime(survey_value, errors="coerce") if survey_value is not None else pd.NaT
    survey_date_value = survey_ts_value.date() if pd.notnull(survey_ts_value) else None
    return {
        "id": f"{UPC}_{collection_name_value}_{record_id_value}",
        "collection_name": collection_name_value,
        "defect_base": defect_base_name,
        "level": "child_10m",
        "standard": None,
        "defect_id": defect_id_value,
        "display_defect_id": defect_id_value,
        "source_defect_id": record_id_value,
        "parent_id": parent_id_value,
        "record_id": record_id_value,
        "upc": upc_slash,
        "upc_code": UPC,
        "nh_number": None,
        "section_code": None,
        "zone": ZONE,
        "phase": PHASE,
        "cycle": int(CYCLE),
        "source_system": source_system,
        "sources": [source_system],
        "defect_type": defect_base_name.title(),
        "type_of_defect": defect_base_name.title(),
        "category": category_value,
        "severity": severity_value,
        "pavement_type": None,
        "pci": None,
        "lane": lane_value,
        "breach_flag": 1,  # Overridden per-row in caller if needed
        "metric_name": metric_name_value,
        "metric_value": metric_value_value,
        "threshold_name": None,
        "threshold_value": None,
        "total_children": 1,
        "length_m": length_value,
        "area_sqm": area_value,
        "start_chainage": start_chainage_value,
        "end_chainage": end_chainage_value,
        "defect_chainage": start_chainage_value,
        "start_lat": start_lat_value,
        "start_long": start_long_value,
        "end_lat": end_lat_value,
        "end_long": end_long_value,
        "defect_lat": defect_lat_value,
        "defect_long": defect_long_value,
        "datetime": survey_ts_value if pd.notnull(survey_ts_value) else (pd.Timestamp(TRACKER_CTX['survey_start_date']) if TRACKER_CTX['survey_start_date'] else None),
        "observed_at": survey_ts_value if pd.notnull(survey_ts_value) else (pd.Timestamp(TRACKER_CTX['survey_start_date']) if TRACKER_CTX['survey_start_date'] else None),
        "survey_start_date": TRACKER_CTX['survey_start_date'] or survey_date_value,
        "survey_end_date": TRACKER_CTX['survey_end_date'] or survey_date_value,
        "image_name": image_name_value,
        "image_url": None,
        "image_urls": [image_name_value] if image_name_value else None,
        "remarks": remarks_value,
        "is_duplicate": False,
        "nsv": source_json,
        "dams": None,
        "dashcam": None,
        "dlm": None,
        "project_details": json.dumps({"project_name": UPC, "zone": ZONE, "phase": PHASE, "cycle": CYCLE}),
        "created_at": NOW_TS,
        "updated_at": NOW_TS,
        "side": side_value,
        "carriageway": None,
        "direction": lane_to_direction(lane_value),
        "affected_length": 0.0,
        "total_length": length_value or 0.0,
        "affected_length_ca": 0.0,
        "affected_length_irc": 0.0,
        "parent_start_chainage": (start_chainage_value // _parent_m) * _parent_m if start_chainage_value is not None else None,
        "parent_end_chainage": ((start_chainage_value // _parent_m) * _parent_m + _parent_m) if start_chainage_value is not None else None,
        "reporting_interval_m": get_reporting_interval(defect_base_name),
        "segment_count": 1,
        "roughness_bi": None,
        "iri_value": None,
        "rutting_avg": None,
        "cracking_pct": None,
        "ravelling_area": None,
        "iri_condition": None,
        "rating_ca": None,
        "rating_irc": None,
        "defect_type_filter": defect_base_name.title(),
        "has_standard_defect": True,
        "standard_flags": "{}",
        "thresholds": None,
        "measures": measures_json,
        "run_date": RUN_DATE,
        "detected_on": TRACKER_CTX['survey_end_date'] or survey_date_value,
        "project_id": None,
        "region_id": None,
        "piu_id": None,
        "state_id": None,
        "zone_id": None,
        "contractor_id": None,
        "ae_ie_id": None,
    }


def build_cracking_sdf():
    if df_cracking_raw.empty:
        return empty_with_schema(POINT_SCHEMA)
    cols = list(df_cracking_raw.columns)
    start_c = pick_col(cols, "Start_Chainage")
    end_c = pick_col(cols, "End_Chainage")
    start_lat_c = pick_col(cols, "Start_Lat")
    start_lon_c = pick_col(cols, "Start_Lon")
    end_lat_c = pick_col(cols, "End_Lat")
    end_lon_c = pick_col(cols, "End_Lon")
    lane_c = pick_col(cols, "Lane")
    cond_c = pick_col(cols, "Condition")
    crack_c = pick_col(cols, "Cracking")
    surface_c = pick_col(cols, "Surface Type")
    key_c = pick_col(cols, "KEY")
    image_name_c = pick_col(cols, "Image_Name", "cracking_image_name")
    image_url_c = pick_col(cols, "Image_URL", "cracking_image_url")
    area_c = pick_col(cols, "Area_sq_m", "Area_Sum")

    records = []
    for idx, row in df_cracking_raw.iterrows():
        start_chainage_value = as_int(row[start_c]) if start_c else None
        end_chainage_value = as_int(row[end_c]) if end_c else None
        start_lat_value = as_float(row[start_lat_c]) if start_lat_c else None
        start_long_value = as_float(row[start_lon_c]) if start_lon_c else None
        end_lat_value = as_float(row[end_lat_c]) if end_lat_c else None
        end_long_value = as_float(row[end_lon_c]) if end_lon_c else None
        crack_value = as_float(row[crack_c]) if crack_c else None
        condition_value = as_text(row[cond_c]) if cond_c else None
        lane_value = as_text(row[lane_c]) if lane_c else None
        surface_value = as_text(row[surface_c]) if surface_c else None
        image_name_val = as_text(row[image_name_c]) if image_name_c else None
        image_url_val = as_text(row[image_url_c]) if image_url_c else None
        road_ctx = get_road_context(start_chainage_value, end_chainage_value, lane=lane_value)
        key_value = as_text(row[key_c]) if key_c else str(idx + 1)
        record_id_value = f"cracking_{key_value}"
        defect_id_value = f"D/{SURVEY_DATE_STR}/N/{UPC}/{start_chainage_value or 0}/Cracking/{idx + 1:04d}"
        source_json = json.dumps({
            "condition": condition_value,
            "cracking": crack_value,
            "surface_type": surface_value,
        })
        measures_json = json.dumps({"cracking_pct": crack_value})
        record = make_base_point_record(
            defect_base_name="cracking",
            collection_name_value="master_cracking",
            defect_id_value=defect_id_value,
            record_id_value=record_id_value,
            parent_id_value=f"{UPC}_{lane_value}_{(start_chainage_value // DEFECT_INTERVALS['cracking']['parent_m']) * DEFECT_INTERVALS['cracking']['parent_m']}_{(start_chainage_value // DEFECT_INTERVALS['cracking']['parent_m']) * DEFECT_INTERVALS['cracking']['parent_m'] + DEFECT_INTERVALS['cracking']['parent_m']}" if start_chainage_value is not None and lane_value is not None else None,
            start_chainage_value=start_chainage_value,
            end_chainage_value=end_chainage_value,
            start_lat_value=start_lat_value,
            start_long_value=start_long_value,
            end_lat_value=end_lat_value,
            end_long_value=end_long_value,
            defect_lat_value=start_lat_value,
            defect_long_value=start_long_value,
            lane_value=lane_value,
            side_value=None,
            severity_value=condition_value,
            metric_name_value="cracking_pct",
            metric_value_value=crack_value,
            image_name_value=image_name_val,
            survey_value=RUN_TS,
            length_value=abs(end_chainage_value - start_chainage_value) if start_chainage_value is not None and end_chainage_value is not None else None,
            area_value=as_float(row[area_c]) if area_c else None,
            source_json=source_json,
            measures_json=measures_json,
            remarks_value=condition_value,
            category_value="LENGTH",
        )
        record["nh_number"] = first_non_null(road_ctx.get("nh_number"), record.get("nh_number"))
        record["section_code"] = first_non_null(road_ctx.get("section_code"), record.get("section_code"))
        record["direction"] = first_non_null(road_ctx.get("direction"), record.get("direction"))
        record["image_url"] = image_url_val
        record["pavement_type"] = first_non_null(road_ctx.get("pavement_type"), surface_value)
        record["carriageway"] = first_non_null(road_ctx.get("carriageway"), record.get("carriageway"))
        _crack_std = TRACKER_CTX['defect_standards']['cracking']['standard']
        record["rating_ca"] = condition_value if _crack_std == 'CA' else None
        record["rating_irc"] = condition_value if _crack_std == 'IRC' else None
        record["cracking_pct"] = crack_value
        record["threshold_name"] = get_threshold_name("cracking")
        record["threshold_value"] = CA_CRACK_PCT
        # Set defect point chainage to midpoint of the 100m interval
        if start_chainage_value is not None and end_chainage_value is not None:
            record["defect_chainage"] = (start_chainage_value + end_chainage_value) / 2
        if start_lat_value is not None and end_lat_value is not None:
            record["defect_lat"] = (start_lat_value + end_lat_value) / 2
        if start_long_value is not None and end_long_value is not None:
            record["defect_long"] = (start_long_value + end_long_value) / 2
        # Affected length for cracking
        _seg = record.get("length_m") or 0.0
        _bf_crack = 1 if crack_value is not None and crack_value >= CA_CRACK_PCT else 0
        record["breach_flag"] = _bf_crack
        record["total_length"] = float(_seg)
        record["affected_length"] = round(float(_seg), 4) if _bf_crack == 1 else 0.0
        record["affected_length_ca"] = round(float(_seg), 4) if _bf_crack == 1 else 0.0
        record["affected_length_irc"] = 0.0
        records.append(record)

    return pandas_to_spark_with_schema(build_point_pdf(records, CRACKING_COLUMNS), CRACKING_SCHEMA)


# ── Pothole / Cracking count cross-reference indexes ─────────────────────────
# Sorted chainage arrays for fast interval counting via bisect
from bisect import bisect_left as _bisect_left

def _build_chainage_arr(df, ch_col):
    if df.empty or ch_col not in df.columns:
        return np.array([])
    return np.sort(pd.to_numeric(df[ch_col], errors='coerce').dropna().values.astype(float))

POTHOLE_COUNT_ARR = _build_chainage_arr(df_pothole_raw, "Start_Chainage")
CRACKING_COUNT_ARR = _build_chainage_arr(df_cracking_raw, "Start_Chainage")

def count_in_range(count_arr, start, end):
    """Count defects with chainage in [start, end)."""
    if len(count_arr) == 0 or start is None or end is None:
        return 0
    return int(_bisect_left(count_arr, float(end)) - _bisect_left(count_arr, float(start)))

print(f"Count indexes: {len(POTHOLE_COUNT_ARR)} potholes, {len(CRACKING_COUNT_ARR)} cracking entries")


def normalize_linear_source(pdf, defect_base_name):
    if pdf.empty:
        return pd.DataFrame()
    cols = list(pdf.columns)

    lane_c = pick_col(cols, "Lane", "LaneNumber")
    nh_candidates = uniq([
        pick_col(cols, "NH_Number"),
        pick_col(cols, "NHNumber"),
        pick_col(cols, "New NH number"),
        pick_col(cols, "NH\nNumber"),
    ])
    section_candidates = uniq([
        pick_col(cols, "SectionCode"),
        pick_col(cols, "Section"),
    ])
    direction_candidates = uniq([pick_col(cols, "Direction")])
    side_c = pick_col(cols, "Side")
    survey_candidates = uniq([
        pick_col(cols, "SurveyDate"),
        pick_col(cols, "NSV Survey Date [dd-mm-yyyy]"),
    ])
    start_lat_candidates = uniq([
        pick_col(cols, "Start Latitude"),
        pick_col(cols, "Start_Lat"),
        pick_col(cols, "Latitude"),
    ])
    end_lat_candidates = uniq([
        pick_col(cols, "End Latitude"),
        pick_col(cols, "End_Lat"),
        pick_col(cols, "Latitude"),
    ])
    start_long_candidates = uniq([
        pick_col(cols, "Start  Longitude"),
        pick_col(cols, "Start_Lon"),
        pick_col(cols, "Longitude"),
    ])
    end_long_candidates = uniq([
        pick_col(cols, "End  Longitude"),
        pick_col(cols, "End_Lon"),
        pick_col(cols, "Longitude"),
    ])
    project_c = pick_col(cols, "Project Name")
    piu_c = pick_col(cols, "PIU")
    ro_c = pick_col(cols, "RO")
    image_candidates = uniq([
        pick_col(cols, "Image_Name"),
        pick_col(cols, "image_name"),
        pick_col(cols, "Image_ID"),
    ])
    area_candidates = uniq([
        pick_col(cols, "Area_sq_m"),
        pick_col(cols, "area_sq_m"),
        pick_col(cols, "Area_sq_m2"),
    ])

    pavement_candidates = uniq([
        pick_col(cols, "PavementType"),
        pick_col(cols, "Surface Type"),
    ])
    carriageway_candidates = uniq([
        pick_col(cols, "carriageway"),
        pick_col(cols, "CarriagewayType"),
    ])
    condition_candidates = uniq([
        pick_col(cols, "Condition"),
        pick_col(cols, "Severity"),
        pick_col(cols, "Crack rating"),
    ])

    roughness_iri_candidates = uniq([
        pick_col(cols, "Lanelri"),
        pick_col(cols, "Lane IRI"),
        pick_col(cols, "IRI"),
        pick_col(cols, "Rwplri"),
        pick_col(cols, "Lwplri"),
        pick_col(cols, "LHS Inner IRI (m/km)"),
        pick_col(cols, "LHS Outer IRI (m/km)"),
        pick_col(cols, "RHS Inner IRI (m/km)"),
        pick_col(cols, "RHS Outer IRI (m/km)"),
    ])
    roughness_bi_candidates = uniq([
        pick_col(cols, "BI m/km"),
        pick_col(cols, "LHS Inner BI (m/km)"),
        pick_col(cols, "LHS Outer BI (m/km)"),
        pick_col(cols, "RHS Inner BI (m/km)"),
        pick_col(cols, "RHS Outer BI (m/km)"),
    ])
    rutting_candidates = uniq([
        pick_col(cols, "RuttingAvg"),
        pick_col(cols, "Rutting Avgerage (mm)"),
        pick_col(cols, "Rutting"),
        pick_col(cols, "RuttingLeft"),
        pick_col(cols, "RuttingRight"),
        pick_col(cols, "LHS Inner Rutting (mm)"),
        pick_col(cols, "LHS Outer Rutting (mm)"),
        pick_col(cols, "RHS Inner Rutting (mm)"),
        pick_col(cols, "RHS Outer Rutting (mm)"),
    ])
    ravelling_candidates = uniq([
        pick_col(cols, "Ravelling"),
        pick_col(cols, "Extent %"),
    ])

    start_candidates = [
        (pick_col(cols, "New NH number StartChainage"), False),
        (pick_col(cols, "Start_Chainage"), False),
        (pick_col(cols, "Old NH number StartChainage"), False),
        (pick_col(cols, "Chainage From (km)", "Chainage from (km)"), True),
    ]
    end_candidates = [
        (pick_col(cols, "New NH number EndChainage"), False),
        (pick_col(cols, "End_Chainage"), False),
        (pick_col(cols, "Old NH number  EndChainage", "Old NH number EndChainage"), False),
        (pick_col(cols, "Chainage To (km)"), True),
    ]

    normalized_rows = []
    for idx, row in pdf.iterrows():
        start_chainage_value = first_non_null(*[
            as_chainage_m(row[col_name], is_km=is_km) if col_name else None
            for col_name, is_km in start_candidates
        ])
        end_chainage_value = first_non_null(*[
            as_chainage_m(row[col_name], is_km=is_km) if col_name else None
            for col_name, is_km in end_candidates
        ])

        if start_chainage_value is None and end_chainage_value is None:
            continue
        if start_chainage_value is None:
            start_chainage_value = end_chainage_value
        if end_chainage_value is None:
            end_chainage_value = start_chainage_value
        if end_chainage_value < start_chainage_value:
            start_chainage_value, end_chainage_value = end_chainage_value, start_chainage_value

        iri_value = first_non_null(*[as_float(row[c]) if c else None for c in roughness_iri_candidates])
        roughness_bi_value = first_non_null(*[as_float(row[c]) if c else None for c in roughness_bi_candidates])
        rutting_avg_value = first_non_null(*[as_float(row[c]) if c else None for c in rutting_candidates])
        ravelling_area_value = first_non_null(*[as_float(row[c]) if c else None for c in ravelling_candidates])

        metric_value = {
            "roughness": first_non_null(iri_value, roughness_bi_value),
            "rutting": rutting_avg_value,
            "ravelling": ravelling_area_value,
        }[defect_base_name]

        severity_value = first_non_null(*[as_text(row[c]) if c else None for c in condition_candidates])
        # Fallback: look up D3 ShapeFile condition
        if severity_value is None:
            severity_value = lookup_d3_condition(defect_base_name, start_chainage_value, end_chainage_value)

        normalized_rows.append({
            "row_id": idx + 1,
            "lane": as_text(row[lane_c]) if lane_c else None,
            "nh_number": first_non_null(*[as_text(row[c]) if c else None for c in nh_candidates]),
            "section_code": first_non_null(*[as_text(row[c]) if c else None for c in section_candidates]),
            "direction": first_non_null(*[as_text(row[c]) if c else None for c in direction_candidates], lane_to_direction(as_text(row[lane_c]) if lane_c else None)),
            "side": as_text(row[side_c]) if side_c else None,
            "survey_date": first_non_null(*[pd.to_datetime(row[c], errors="coerce") if c else pd.NaT for c in survey_candidates]),
            "start_chainage": start_chainage_value,
            "end_chainage": end_chainage_value,
            "length_m": max(end_chainage_value - start_chainage_value, 0),
            "start_lat": first_non_null(*[as_float(row[c]) if c else None for c in start_lat_candidates]),
            "end_lat": first_non_null(*[as_float(row[c]) if c else None for c in end_lat_candidates]),
            "start_long": first_non_null(*[as_float(row[c]) if c else None for c in start_long_candidates]),
            "end_long": first_non_null(*[as_float(row[c]) if c else None for c in end_long_candidates]),
            "metric_value": metric_value,
            "severity": severity_value,
            "project_name": as_text(row[project_c]) if project_c else UPC,
            "piu": as_text(row[piu_c]) if piu_c else None,
            "ro": as_text(row[ro_c]) if ro_c else None,
            "metric_source": defect_base_name,
            "image_name": first_non_null(*[as_text(row[c]) if c else None for c in image_candidates]),
            "area_sqm": first_non_null(*[as_float(row[c]) if c else None for c in area_candidates]),
            "pavement_type": first_non_null(*[as_text(row[c]) if c else None for c in pavement_candidates]),
            "carriageway": first_non_null(*[as_text(row[c]) if c else None for c in carriageway_candidates]),
            "rating_ca": severity_value if TRACKER_CTX['defect_standards'].get(defect_base_name, {}).get('standard') == 'CA' else None,
            "rating_irc": severity_value if TRACKER_CTX['defect_standards'].get(defect_base_name, {}).get('standard') == 'IRC' else None,
            "roughness_bi": roughness_bi_value,
            "iri_value": iri_value,
            "rutting_avg": rutting_avg_value,
            "ravelling_area": ravelling_area_value,
        })

    # Add affected_length columns to normalized rows for aggregation propagation
    norm_pdf = pd.DataFrame(normalized_rows)
    if not norm_pdf.empty:
        for _idx, _row in norm_pdf.iterrows():
            _mv = _row.get('metric_value')
            _bi = _row.get('roughness_bi')
            _iri = _row.get('iri_value')
            _seg = _row.get('length_m', 0) or 0
            _bf = compute_breach_flag(defect_base_name, _mv, roughness_bi=_bi, iri_value=_iri)
            _bf_ca = compute_breach_flag_ca(defect_base_name, _mv, roughness_bi=_bi)
            _bf_irc = compute_breach_flag_irc(defect_base_name, _mv, iri_value=_iri, roughness_bi=_bi)
            norm_pdf.at[_idx, 'total_length'] = float(_seg)
            norm_pdf.at[_idx, 'affected_length'] = round(float(_seg), 4) if _bf == 1 else 0.0
            norm_pdf.at[_idx, 'affected_length_ca'] = round(float(_seg), 4) if _bf_ca == 1 else 0.0
            norm_pdf.at[_idx, 'affected_length_irc'] = round(float(_seg), 4) if _bf_irc == 1 else 0.0
    return norm_pdf


def build_linear_records_from_pdf(pdf, defect_base_name, level_name, collection_name_value, roughness_standard=None):
    # Roughness standard selection per level (constant priority):
    #   parent:     always CA
    #   child_100m: IRC priority, CA fallback
    #   child_10m:  CA priority, IRC fallback
    if defect_base_name == "roughness":
        if roughness_standard is None:
            roughness_standard = "CA"  # safe default
        use_ca = (roughness_standard == "CA")
    else:
        use_ca = True  # irrelevant for non-roughness

    metric_name_value = {
        "roughness": "roughness_bi",  # Always BI for display; standard only governs threshold
        "rutting": "rutting_avg",
        "ravelling": "ravelling_area",
    }[defect_base_name]
    threshold_numeric_info = {
        "roughness": CA_BI_THRESHOLD if use_ca else IRC_IRI_POOR,
        "rutting": CA_RUT_THRESHOLD,
        "ravelling": float(RAVELLING_RATING_THRESHOLD),
    }
    if defect_base_name == "roughness":
        threshold_name_value = "As per CA" if use_ca else "As per IRC"
    else:
        threshold_name_value = get_threshold_name(defect_base_name)
    threshold_numeric_value = threshold_numeric_info.get(defect_base_name)
    records = []
    for seq, row in pdf.reset_index(drop=True).iterrows():
        start_chainage_value = as_int(row["start_chainage"])
        end_chainage_value = as_int(row["end_chainage"])
        lane_value = as_text(row.get("lane"))
        direction_value = as_text(row.get("direction"))
        road_ctx = get_road_context(start_chainage_value, end_chainage_value, lane=lane_value, direction=direction_value)
        sheet_ctx = get_defect_sheet_context(defect_base_name, start_chainage_value, end_chainage_value, lane=lane_value, direction=direction_value)

        _parent_m = DEFECT_INTERVALS.get(defect_base_name, {}).get('parent_m', PARENT_LENGTH_M)
        parent_start = (start_chainage_value // _parent_m) * _parent_m if start_chainage_value is not None else None
        parent_end = parent_start + _parent_m if parent_start is not None else None
        defect_id_value = f"D/{SURVEY_DATE_STR}/N/{UPC}/{start_chainage_value or 0}/{defect_base_name.title()}/{seq + 1:04d}"
        record_id_value = f"{defect_base_name}_{level_name}_{seq + 1:06d}"

        severity_value = first_non_null(
            as_text(row.get("severity")),
            as_text(sheet_ctx.get("condition")),
            as_text(row.get("rating_ca")),
            as_text(row.get("rating_irc")),
        )
        roughness_bi_value = first_non_null(as_float(row.get("roughness_bi")), as_float(sheet_ctx.get("roughness_bi")))
        iri_value_value = first_non_null(as_float(row.get("iri_value")), as_float(row.get("metric_value")))
        rutting_avg_value = first_non_null(as_float(row.get("rutting_avg")), as_float(sheet_ctx.get("rutting_avg")), as_float(row.get("metric_value")))
        ravelling_area_value = first_non_null(as_float(row.get("ravelling_area")), as_float(row.get("metric_value")))
        if defect_base_name == "roughness":
            # Always BI-first for display; standard only governs threshold
            metric_value_resolved = first_non_null(roughness_bi_value, iri_value_value)
        else:
            metric_value_resolved = {
                "rutting": rutting_avg_value,
                "ravelling": ravelling_area_value,
            }[defect_base_name]

        start_lat_value = first_non_null(as_float(row.get("start_lat")), as_float(sheet_ctx.get("start_lat")))
        start_long_value = first_non_null(as_float(row.get("start_long")), as_float(sheet_ctx.get("start_long")))
        end_lat_value = first_non_null(as_float(row.get("end_lat")), as_float(sheet_ctx.get("end_lat")), start_lat_value)
        end_long_value = first_non_null(as_float(row.get("end_long")), as_float(sheet_ctx.get("end_long")), start_long_value)
        survey_value = row.get("survey_date") if pd.notnull(row.get("survey_date")) else RUN_TS

        target_columns = TABLE_COLUMNS[collection_name_value]
        record = {col_name: None for col_name in target_columns}
        record.update({
            "id": f"{UPC}_{collection_name_value}_{seq + 1:06d}",
            "collection_name": collection_name_value,
            "defect_base": defect_base_name,
            "level": level_name,
            "standard": None,
            "defect_id": defect_id_value,
            "display_defect_id": defect_id_value,
            "source_defect_id": record_id_value,
            "parent_id": f"{UPC}_{lane_value}_{parent_start}_{parent_end}" if lane_value is not None and parent_start is not None else None,
            "record_id": record_id_value,
            "upc": upc_slash,
            "upc_code": UPC,
            "nh_number": first_non_null(as_text(row.get("nh_number")), road_ctx.get("nh_number")),
            "section_code": first_non_null(as_text(row.get("section_code")), road_ctx.get("section_code")),
            "zone": ZONE,
            "phase": PHASE,
            "cycle": int(CYCLE),
            "source_system": source_system,
            "sources": [source_system],
            "defect_type": defect_base_name.title(),
            "type_of_defect": defect_base_name.title(),
            "category": "LENGTH",
            "severity": severity_value,
            "pavement_type": first_non_null(as_text(row.get("pavement_type")), road_ctx.get("pavement_type"), as_text(sheet_ctx.get("surface_type"))),
            "pci": None,
            "lane": lane_value,
            "breach_flag": (compute_breach_flag_ca(defect_base_name, metric_value_resolved, roughness_bi=roughness_bi_value) if (defect_base_name == "roughness" and use_ca) else compute_breach_flag_irc(defect_base_name, metric_value_resolved, iri_value=iri_value_value, roughness_bi=roughness_bi_value) if (defect_base_name == "roughness" and not use_ca) else compute_breach_flag(defect_base_name, metric_value_resolved, roughness_bi=roughness_bi_value, iri_value=iri_value_value)),
            "metric_name": metric_name_value,
            "metric_value": metric_value_resolved,
            "threshold_name": threshold_name_value,
            "threshold_value": threshold_numeric_value,
            "total_children": int(row.get("segment_count", 1) or 1),
            "length_m": as_float(row.get("length_m")),
            "area_sqm": as_float(row.get("area_sqm")),
            "start_chainage": start_chainage_value,
            "end_chainage": end_chainage_value,
            "defect_chainage": start_chainage_value,
            "start_lat": start_lat_value,
            "start_long": start_long_value,
            "end_lat": end_lat_value,
            "end_long": end_long_value,
            "defect_lat": start_lat_value,
            "defect_long": start_long_value,
            "datetime": survey_value if pd.notnull(survey_value) else (pd.Timestamp(TRACKER_CTX['survey_start_date']) if TRACKER_CTX['survey_start_date'] else None),
            "observed_at": survey_value if pd.notnull(survey_value) else (pd.Timestamp(TRACKER_CTX['survey_start_date']) if TRACKER_CTX['survey_start_date'] else None),
            "survey_start_date": TRACKER_CTX['survey_start_date'] or (survey_value.date() if pd.notnull(survey_value) else None),
            "survey_end_date": TRACKER_CTX['survey_end_date'] or (survey_value.date() if pd.notnull(survey_value) else None),
            "image_name": as_text(row.get("image_name")),
            "image_url": None,
            "image_urls": [as_text(row.get("image_name"))] if as_text(row.get("image_name")) else None,
            "remarks": first_non_null(as_text(row.get("severity")), severity_value),
            "is_duplicate": False,
            "nsv": json.dumps({"metric_source": as_text(row.get("metric_source")), "piu": as_text(row.get("piu")), "ro": as_text(row.get("ro"))}),
            "dams": None,
            "dashcam": None,
            "dlm": None,
            "project_details": json.dumps({"project_name": as_text(row.get("project_name")) or UPC, "zone": ZONE, "phase": PHASE, "cycle": CYCLE}),
            "created_at": NOW_TS,
            "updated_at": NOW_TS,
            "side": as_text(row.get("side")),
            "carriageway": first_non_null(as_text(row.get("carriageway")), road_ctx.get("carriageway")),
            "direction": first_non_null(direction_value, road_ctx.get("direction")),
            "parent_start_chainage": parent_start,
            "parent_end_chainage": parent_end,
            "reporting_interval_m": get_reporting_interval(defect_base_name),
            "segment_count": int(row.get("segment_count", 1) or 1),
            **({
                "roughness_bi": roughness_bi_value,
                "iri_value": iri_value_value,
            } if defect_base_name == "roughness" else {}),
            **({
                "rutting_avg": rutting_avg_value,
            } if defect_base_name == "rutting" else {}),
            **({
                "ravelling_area": ravelling_area_value,
            } if defect_base_name == "ravelling" else {}),
            "iri_condition": lookup_d3_condition("roughness", start_chainage_value, end_chainage_value, lane_value) if defect_base_name == "roughness" else None,
            "rating_ca": (first_non_null(lookup_d3_condition(defect_base_name, start_chainage_value, end_chainage_value, lane_value), severity_value) if TRACKER_CTX['defect_standards'].get(defect_base_name, {}).get('standard') == 'CA' else None),
            "rating_irc": (first_non_null(lookup_d3_condition(defect_base_name, start_chainage_value, end_chainage_value, lane_value), severity_value) if TRACKER_CTX['defect_standards'].get(defect_base_name, {}).get('standard') == 'IRC' else None),
            "defect_type_filter": defect_base_name.title(),
            "has_standard_defect": True,
            "standard_flags": "{}",
            "thresholds": None,
            "measures": json.dumps({metric_name_value: metric_value_resolved}),
            "run_date": RUN_DATE,
            "detected_on": TRACKER_CTX['survey_end_date'] or (survey_value.date() if pd.notnull(survey_value) else None),
            "project_id": None,
            "region_id": None,
            "piu_id": None,
            "state_id": None,
            "zone_id": None,
            "contractor_id": None,
            "ae_ie_id": None,
            "total_count_of_pothole": count_in_range(POTHOLE_COUNT_ARR, start_chainage_value, end_chainage_value),
            "total_count_of_cracking": count_in_range(CRACKING_COUNT_ARR, start_chainage_value, end_chainage_value),
            "count_of_others_defects": int(row.get("segment_count", 1) or 1),
        })
        # Compute affected length columns
        seg_len = as_float(row.get("length_m")) or 0.0
        _bf = record["breach_flag"]
        _bf_ca = compute_breach_flag_ca(defect_base_name, metric_value_resolved, roughness_bi=roughness_bi_value)
        _bf_irc = compute_breach_flag_irc(defect_base_name, metric_value_resolved, iri_value=iri_value_value)
        record["total_length"] = seg_len
        record["affected_length"] = round(seg_len, 4) if _bf == 1 else 0.0
        record["affected_length_ca"] = round(seg_len, 4) if _bf_ca == 1 else 0.0
        record["affected_length_irc"] = round(seg_len, 4) if _bf_irc == 1 else 0.0
        records.append(record)
    return records


def _extra_agg_cols(pdf):
    """Build extra agg dict for defect-metric cols that exist in the DataFrame."""
    agg = {}
    for c in ["roughness_bi", "iri_value", "rutting_avg", "ravelling_area"]:
        if c in pdf.columns:
            agg[c] = "mean"
    for c in ["pavement_type", "carriageway", "rating_ca", "rating_irc", "image_name", "area_sqm"]:
        if c in pdf.columns:
            agg[c] = "first"
    # Affected length columns for aggregation
    for c in ["affected_length", "total_length", "affected_length_ca", "affected_length_irc"]:
        if c in pdf.columns:
            agg[c] = "sum"
    return agg


def _threshold_filter(pdf, defect_base_name):
    """Keep only rows where aggregated metric still breaches the threshold.
    Ravelling is NOT filtered — all rows kept; breach_flag marks defects."""
    if pdf.empty:
        return pdf
    mv = pd.to_numeric(pdf["metric_value"], errors="coerce").fillna(0)
    if defect_base_name == "ravelling":
        return pdf[mv >= RAVELLING_RATING_THRESHOLD].copy()
    # return pdf  # Keep all ravelling rows
    if defect_base_name == "roughness":
        bi = pd.to_numeric(pdf["roughness_bi"], errors="coerce").fillna(0) if "roughness_bi" in pdf.columns else 0
        return pdf[(mv > IRC_IRI_POOR) | (bi >= CA_BI_THRESHOLD)].copy()
    elif defect_base_name == "rutting":
        return pdf[mv >= CA_RUT_THRESHOLD].copy()
    return pdf


def build_linear_output_set(raw_pdf, defect_base_name):
    normalized_pdf = normalize_linear_source(raw_pdf, defect_base_name)
    levels = ["parent", "child_10m", "child_100m"]
    if normalized_pdf.empty:
        return {
            f"master_{defect_base_name}_{lvl}": empty_with_schema(TABLE_SCHEMAS[f"master_{defect_base_name}_{lvl}"])
            for lvl in levels
        }

    # ── Subdivide raw ~100m segments into proper 10m sub-segments ──────────
    child_10m_rows = []
    for _, row in normalized_pdf.iterrows():
        s, e = row["start_chainage"], row["end_chainage"]
        if s is None or e is None or e <= s:
            child_10m_rows.append(row.to_dict())
            continue
        seg_len = e - s
        n_segs = max(1, int(round(seg_len / 10)))
        step = seg_len / n_segs
        base = row.to_dict()
        # ── Fix: Use 10m grid boundaries (round to nearest 10) ──
        s_aligned = int(round(s / 10)) * 10
        e_aligned = int(round(e / 10)) * 10
        if e_aligned <= s_aligned:
            e_aligned = s_aligned + 10
        _first_10m_seg = True
        for seg_start in range(s_aligned, e_aligned, 10):
            sub = base.copy()
            # First sub-segment per raw record: keep actual start
            sub["start_chainage"] = int(round(s)) if _first_10m_seg else seg_start
            sub["end_chainage"] = seg_start + 10
            sub["length_m"] = sub["end_chainage"] - sub["start_chainage"]
            child_10m_rows.append(sub)
            _first_10m_seg = False
    child_10m_pdf = pd.DataFrame(child_10m_rows)
    child_10m_pdf = _threshold_filter(child_10m_pdf, defect_base_name)

    # Constant child_mid interval (100m) — standard: IRC priority, CA fallback
    _child_mid_m = DEFECT_INTERVALS.get(defect_base_name, {}).get('child_mid_m', 100)
    child_100m_pdf = normalized_pdf.copy()
    child_100m_pdf["bin_start"] = (child_100m_pdf["start_chainage"] // _child_mid_m) * _child_mid_m
    child_100m_pdf["bin_end"] = child_100m_pdf["bin_start"] + _child_mid_m
    group_keys = ["lane", "nh_number", "section_code", "direction", "side", "project_name", "piu", "ro", "metric_source", "bin_start", "bin_end"]
    child_100m_pdf = child_100m_pdf.groupby(group_keys, dropna=False).agg({
        "survey_date": "first",
        "start_lat": "first",
        "start_long": "first",
        "end_lat": "last",
        "end_long": "last",
        "metric_value": "mean",
        "severity": "first",
        "length_m": "sum",
        "row_id": "count",
        "start_chainage": "min",
        "end_chainage": "max",
        **_extra_agg_cols(child_100m_pdf),
    }).reset_index().rename(columns={
        "row_id": "segment_count",
    })
    child_100m_pdf = _threshold_filter(child_100m_pdf, defect_base_name)

    # ── Fix: Use constant 100m grid boundaries for start/end chainage ────
    if not child_100m_pdf.empty:
        _actual_min_100 = child_100m_pdf["start_chainage"].copy()
        child_100m_pdf["start_chainage"] = child_100m_pdf["bin_start"]
        child_100m_pdf["end_chainage"] = child_100m_pdf["bin_end"]
        # First bin per lane: keep actual data start (e.g. 30→100)
        _first_bin = child_100m_pdf.groupby("lane")["bin_start"].transform("min")
        _is_first = child_100m_pdf["bin_start"] == _first_bin
        child_100m_pdf.loc[_is_first, "start_chainage"] = _actual_min_100[_is_first]

    # Constant parent interval (1000m) — standard: always CA
    _parent_m = DEFECT_INTERVALS.get(defect_base_name, {}).get('parent_m', PARENT_LENGTH_M)
    parent_pdf = normalized_pdf.copy()
    parent_pdf["parent_start_chainage"] = (parent_pdf["start_chainage"] // _parent_m) * _parent_m
    parent_pdf["parent_end_chainage"] = parent_pdf["parent_start_chainage"] + _parent_m
    parent_keys = ["lane", "nh_number", "section_code", "direction", "side", "project_name", "piu", "ro", "metric_source", "parent_start_chainage", "parent_end_chainage"]
    parent_pdf = parent_pdf.groupby(parent_keys, dropna=False).agg({
        "survey_date": "first",
        "start_lat": "first",
        "start_long": "first",
        "end_lat": "last",
        "end_long": "last",
        "metric_value": "mean",
        "severity": "first",
        "length_m": "sum",
        "row_id": "count",
        "start_chainage": "min",
        "end_chainage": "max",
        **_extra_agg_cols(parent_pdf),
    }).reset_index().rename(columns={
        "row_id": "segment_count",
    })
    parent_pdf = _threshold_filter(parent_pdf, defect_base_name)

    # ── Fix: Use constant 1km grid boundaries for start/end chainage ─────
    if not parent_pdf.empty:
        _actual_min_1k = parent_pdf["start_chainage"].copy()
        parent_pdf["start_chainage"] = parent_pdf["parent_start_chainage"]
        parent_pdf["end_chainage"] = parent_pdf["parent_end_chainage"]
        # First bin per lane: keep actual data start
        _first_bin = parent_pdf.groupby("lane")["parent_start_chainage"].transform("min")
        _is_first = parent_pdf["parent_start_chainage"] == _first_bin
        parent_pdf.loc[_is_first, "start_chainage"] = _actual_min_1k[_is_first]

    output = {}
    for lvl, pdf_data in [("child_10m", child_10m_pdf), ("child_100m", child_100m_pdf), ("parent", parent_pdf)]:
        tbl_name = f"master_{defect_base_name}_{lvl}"
        tbl_cols = TABLE_COLUMNS[tbl_name]
        tbl_schema = TABLE_SCHEMAS[tbl_name]

        if defect_base_name == "roughness" and not pdf_data.empty:
            bi_vals = pd.to_numeric(pdf_data.get("roughness_bi", pd.Series(dtype=float)), errors="coerce").fillna(0)
            mv_vals = pd.to_numeric(pdf_data.get("metric_value", pd.Series(dtype=float)), errors="coerce").fillna(0)
            has_ca = (bi_vals >= CA_BI_THRESHOLD).any()
            has_irc = "iri_value" in pdf_data.columns and pdf_data["iri_value"].notna().any()

            # Use tracker-defined standard consistently for ALL levels
            _roughness_std = TRACKER_CTX['defect_standards'].get('roughness', {}).get('standard', 'CA')
            if _roughness_std == "CA" and has_ca:
                _chosen_std = "CA"
            elif _roughness_std == "IRC" and has_irc:
                _chosen_std = "IRC"
            elif has_ca:
                _chosen_std = "CA"  # fallback
            elif has_irc:
                _chosen_std = "IRC"  # fallback
            else:
                _chosen_std = None

            if _chosen_std == "CA":
                if lvl == "child_10m":
                    records = build_linear_records_from_pdf(
                        pdf_data[bi_vals >= CA_BI_THRESHOLD].copy(), defect_base_name, lvl, tbl_name, roughness_standard="CA"
                    )
                elif lvl == "child_100m":
                    records = build_linear_records_from_pdf(
                        pdf_data, defect_base_name, lvl, tbl_name, roughness_standard="CA"
                    )
                    records = [r for r in records if r.get('breach_flag') == 1]
                else:
                    records = build_linear_records_from_pdf(pdf_data, defect_base_name, lvl, tbl_name, roughness_standard="CA")
            elif _chosen_std == "IRC":
                if lvl == "child_10m":
                    records = build_linear_records_from_pdf(
                        pdf_data[mv_vals > IRC_IRI_POOR].copy(), defect_base_name, lvl, tbl_name, roughness_standard="IRC"
                    )
                elif lvl == "child_100m":
                    records = build_linear_records_from_pdf(
                        pdf_data, defect_base_name, lvl, tbl_name, roughness_standard="IRC"
                    )
                    records = [r for r in records if r.get('breach_flag') == 1]
                else:
                    records = build_linear_records_from_pdf(pdf_data, defect_base_name, lvl, tbl_name, roughness_standard="IRC")
            else:
                records = []
        else:
            records = build_linear_records_from_pdf(pdf_data, defect_base_name, lvl, tbl_name)

        output[tbl_name] = pandas_to_spark_with_schema(
            build_linear_pdf(records, tbl_cols), tbl_schema
        )
    return output


def enrich_pothole_output(sdf):
    pothole_pdf = sdf.toPandas().copy()
    for idx, row in pothole_pdf.iterrows():
        start_chainage_value = as_int(row.get("start_chainage"))
        end_chainage_value = as_int(row.get("end_chainage"))
        lane_value = as_text(row.get("lane"))
        direction_value = as_text(row.get("direction"))
        road_ctx = get_road_context(start_chainage_value, end_chainage_value, lane=lane_value, direction=direction_value)
        pothole_pdf.at[idx, "nh_number"] = first_non_null(as_text(row.get("nh_number")), road_ctx.get("nh_number"))
        pothole_pdf.at[idx, "section_code"] = first_non_null(as_text(row.get("section_code")), road_ctx.get("section_code"))
        pothole_pdf.at[idx, "pavement_type"] = first_non_null(as_text(row.get("pavement_type")), road_ctx.get("pavement_type"))
        pothole_pdf.at[idx, "carriageway"] = first_non_null(as_text(row.get("carriageway")), road_ctx.get("carriageway"))
        pothole_pdf.at[idx, "direction"] = first_non_null(direction_value, road_ctx.get("direction"))
        _pothole_std = TRACKER_CTX['defect_standards']['pothole']['standard']
        pothole_pdf.at[idx, "rating_ca"] = as_text(row.get("severity")) if _pothole_std == 'CA' else None
        pothole_pdf.at[idx, "rating_irc"] = as_text(row.get("severity")) if _pothole_std == 'IRC' else None
    return pandas_to_spark_with_schema(pothole_pdf[POINT_COLUMNS], POINT_SCHEMA)


pothole_output_sdf = enrich_pothole_output(pothole_output_sdf)

OUTPUT_DATAFRAMES = {
    "master_pothole": pothole_output_sdf,
    "master_cracking": build_cracking_sdf(),
}
OUTPUT_DATAFRAMES.update(build_linear_output_set(df_roughness_raw, "roughness"))
OUTPUT_DATAFRAMES.update(build_linear_output_set(df_rutting_raw, "rutting"))
OUTPUT_DATAFRAMES.update(build_linear_output_set(df_ravelling_raw, "ravelling"))

# ═══ Post-process: Override display_defect_id with hierarchical format ═══════
# Formats:
#   Parent / standalone: D/{ddmmyy}/{source_code}/{sequential_counter}
#   Child 100m / 10m:   {parent_display_defect_id}/{child_seq}
# Source codes: N=NSV, D=Dashcam, R=Drone, O=Others
# RULES:
#   - Only rows with breach_flag=1 get a display_defect_id
#   - breach_flag != 1 → display_defect_id = NULL
#   - Parent rows keep the global sequential counter
#   - Child rows inherit the parent display_defect_id and append /01, /02, ...
#   - A single canonical detected date is used for the entire UPC
# ═════════════════════════════════════════════════════════════════════════════
_SRC_LETTER = "N"  # Source is always NSV
DISPLAY_DEFECT_DATE_STR = (
    pd.Timestamp(TRACKER_CTX["survey_end_date"]).strftime("%d%m%y")
    if TRACKER_CTX.get("survey_end_date")
    else SURVEY_DATE_STR
)

_PARENT_TABLES = [
    "master_roughness_parent",
    "master_rutting_parent",
    "master_ravelling_parent",
]
_CHILD_PARENT_MAP = {
    "master_roughness_child_100m": "master_roughness_parent",
    "master_roughness_child_10m": "master_roughness_parent",
    "master_rutting_child_100m": "master_rutting_parent",
    "master_rutting_child_10m": "master_rutting_parent",
    "master_ravelling_child_100m": "master_ravelling_parent",
    "master_ravelling_child_10m": "master_ravelling_parent",
}
_STANDALONE_TABLES = ["master_cracking", "master_pothole"]
_BASE_ID_TABLES = [
    "master_roughness_parent",
    "master_rutting_parent",
    "master_cracking",
    "master_ravelling_parent",
    "master_pothole",
]

# Find max existing parent/standalone counter FOR THE SAME DATE across all tables
# Counter is per-date: same date across UPCs continues; different date resets to 00001
_max_seq = 0
for _tbl_name in _BASE_ID_TABLES:
    if _tbl_name not in OUTPUT_DATAFRAMES:
        continue
    _full_tbl = f"{OUTPUT_NAMESPACE}.{_tbl_name}"
    if spark.catalog.tableExists(_full_tbl):
        _existing_max = (
            spark.table(_full_tbl)
            .filter(
                (F.col("upc_code") != UPC) &
                F.col("display_defect_id").rlike(f"^D/{DISPLAY_DEFECT_DATE_STR}/[NDRO]/\\d+$")
            )
            .withColumn("_ctr", F.element_at(F.split("display_defect_id", "/"), 4).cast("int"))
            .select(F.max("_ctr"))
            .collect()[0][0]
        )
        if _existing_max:
            _max_seq = max(_max_seq, _existing_max)

print(f"Max existing parent display_defect_id counter (other UPCs): {_max_seq}")
print(f"Canonical display defect date for UPC {UPC}: {DISPLAY_DEFECT_DATE_STR}")

# Pass 1: assign base parent / standalone display IDs
_running_seq = _max_seq
for _tbl_name in _BASE_ID_TABLES:
    if _tbl_name not in OUTPUT_DATAFRAMES:
        continue

    _sdf = OUTPUT_DATAFRAMES[_tbl_name]
    _total = _sdf.count()
    if _total == 0:
        continue

    _breach_cnt = _sdf.filter(F.col("breach_flag") == 1).count()
    # Use orderBy directly instead of partitionBy(lit(1)) to avoid single-partition shuffle
    _w = Window.orderBy(
        F.col("lane").asc_nulls_last(),
        F.col("start_chainage").asc_nulls_last(),
        F.col("end_chainage").asc_nulls_last(),
        F.col("record_id").asc_nulls_last(),
        F.col("id").asc_nulls_last(),
    )
    _sdf = _sdf.withColumn(
        "_breach_seq",
        F.when(F.col("breach_flag") == 1, F.row_number().over(_w))
    )
    _sdf = _sdf.withColumn(
        "display_defect_id",
        F.when(
            F.col("breach_flag") == 1,
            F.concat(
                F.lit(f"D/{DISPLAY_DEFECT_DATE_STR}/{_SRC_LETTER}/"),
                F.format_string("%05d", F.col("_breach_seq") + F.lit(_running_seq))
            )
        ).otherwise(F.lit(None).cast("string"))
    ).drop("_breach_seq")

    OUTPUT_DATAFRAMES[_tbl_name] = _sdf
    _running_seq += _breach_cnt
    print(f"  {_tbl_name}: {_breach_cnt:,} parent/standalone defect IDs / {_total:,} total")

# Pass 2: assign child display IDs from parent display_defect_id in chainage order
for _child_tbl, _parent_tbl in _CHILD_PARENT_MAP.items():
    if _child_tbl not in OUTPUT_DATAFRAMES:
        continue

    _child_sdf = OUTPUT_DATAFRAMES[_child_tbl]
    _total = _child_sdf.count()
    if _total == 0:
        continue

    _breach_cnt = _child_sdf.filter(F.col("breach_flag") == 1).count()
    _orphan_cnt = 0

    if _parent_tbl in OUTPUT_DATAFRAMES:
        _parent_lookup = (
            OUTPUT_DATAFRAMES[_parent_tbl]
            .filter(F.col("display_defect_id").isNotNull())
            .select(
                F.col("parent_id").alias("_parent_join_id"),
                F.col("display_defect_id").alias("_parent_display_defect_id")
            )
            .dropDuplicates(["_parent_join_id"])
        )

        _child_w = Window.partitionBy("parent_id").orderBy(
            F.col("start_chainage").asc_nulls_last(),
            F.col("end_chainage").asc_nulls_last(),
            F.col("record_id").asc_nulls_last(),
            F.col("id").asc_nulls_last(),
        )

        _child_sdf = _child_sdf.join(
            _parent_lookup,
            _child_sdf["parent_id"] == _parent_lookup["_parent_join_id"],
            "left"
        )
        _child_sdf = _child_sdf.withColumn(
            "_child_seq",
            F.when(
                (F.col("breach_flag") == 1) & F.col("_parent_display_defect_id").isNotNull(),
                F.row_number().over(_child_w)
            )
        )
        # child_10m = 3-digit suffix (/001, /002); child_100m = 2-digit suffix (/01, /02)
        _child_fmt = "%03d" if "_child_10m" in _child_tbl else "%02d"
        _child_sdf = _child_sdf.withColumn(
            "display_defect_id",
            F.when(
                (F.col("breach_flag") == 1) & F.col("_parent_display_defect_id").isNotNull(),
                F.concat(
                    F.col("_parent_display_defect_id"),
                    F.lit("/"),
                    F.format_string(_child_fmt, F.col("_child_seq"))
                )
            ).otherwise(F.lit(None).cast("string"))
        )
        _orphan_cnt = _child_sdf.filter(
            (F.col("breach_flag") == 1) & F.col("_parent_display_defect_id").isNull()
        ).count()
        _child_sdf = _child_sdf.drop("_parent_join_id", "_parent_display_defect_id", "_child_seq")
    else:
        _child_sdf = _child_sdf.withColumn("display_defect_id", F.lit(None).cast("string"))
        _orphan_cnt = _breach_cnt

    OUTPUT_DATAFRAMES[_child_tbl] = _child_sdf
    print(f"  {_child_tbl}: {_breach_cnt:,} child defect IDs / {_total:,} total | orphan children={_orphan_cnt:,}")

print(f"\nParent display ID format: D/{DISPLAY_DEFECT_DATE_STR}/{_SRC_LETTER}/00001, 00002, ... (5 digits, counter per date)")
print("Child 100m format: <parent>/01, /02 ... (2 digits)")
print("Child 10m format:  <parent>/001, /002 ... (3 digits)")
print(f"Counter logic: per-date — same date across UPCs continues, new date resets to 00001")
print("Rows with breach_flag=0 → display_defect_id = NULL")

# Consolidate .count() into a single pass to avoid 11 separate Spark actions
from functools import reduce
_count_frames = []
for table_name in sorted(OUTPUT_DATAFRAMES):
    _count_frames.append(
        OUTPUT_DATAFRAMES[table_name]
        .select(F.lit(table_name).alias("table_name"))
        .groupBy("table_name").count()
    )
if _count_frames:
    _all_counts = reduce(lambda a, b: a.unionByName(b), _count_frames).orderBy("table_name").collect()
    for r in _all_counts:
        print(f"Prepared {r['table_name']}: {r['count']:,} rows")


# COMMAND ----------

# DBTITLE 1,Validate Date Consistency and Padded Display Defect IDs
# ═══ Validate: display_defect_id date AND detected_on are consistent ═══════
print(f"Canonical display defect date: {DISPLAY_DEFECT_DATE_STR}\n")

for tbl_name in sorted(OUTPUT_DATAFRAMES.keys()):
    sdf = OUTPUT_DATAFRAMES[tbl_name]
    cnt = sdf.count()
    if cnt == 0:
        continue

    # display_defect_id date parts
    disp_dates = (
        sdf.filter(F.col("display_defect_id").isNotNull())
        .withColumn("_d", F.element_at(F.split("display_defect_id", "/"), 2))
        .select("_d").distinct().collect()
    )
    d_list = [r["_d"] for r in disp_dates]

    # detected_on date parts
    det_dates = (
        sdf.filter(F.col("detected_on").isNotNull())
        .select(F.date_format("detected_on", "ddMMyy").alias("d")).distinct().collect()
    )
    det_list = [r["d"] for r in det_dates]

    match = "MATCH" if set(d_list) == set(det_list) else "MISMATCH"
    status = "✅" if match == "MATCH" else "⚠️"
    print(f"{status} {tbl_name}: display_id_dates={d_list} | detected_on_dates={det_list} | {match}")

# ═══ Sample: parent → child hierarchical IDs ═════════════════════════════════
for defect_name, parent_tbl, child_tbl in [
    ("roughness_100m", "master_roughness_parent", "master_roughness_child_100m"),
    ("roughness_10m", "master_roughness_parent", "master_roughness_child_10m"),
    ("rutting_100m", "master_rutting_parent", "master_rutting_child_100m"),
    ("rutting_10m", "master_rutting_parent", "master_rutting_child_10m"),
]:
    parent_sdf = OUTPUT_DATAFRAMES.get(parent_tbl)
    child_sdf = OUTPUT_DATAFRAMES.get(child_tbl)
    if parent_sdf is None or child_sdf is None or parent_sdf.count() == 0:
        continue
    print(f"\n=== {defect_name.upper()} parent → child sample ===")
    parent_lookup = parent_sdf.select(
        F.col("parent_id").alias("_pjoin"),
        F.col("display_defect_id").alias("parent_display_defect_id")
    )
    preview = (
        child_sdf.join(parent_lookup, child_sdf.parent_id == parent_lookup._pjoin, "left")
        .select("parent_display_defect_id", F.col("display_defect_id").alias("child_display_defect_id"), "start_chainage")
        .filter(F.col("parent_display_defect_id").isNotNull())
        .orderBy("parent_display_defect_id", "start_chainage")
        .limit(10)
    )
    display(preview)

# COMMAND ----------

# DBTITLE 1,PCI Weighted Defect Score — Read from Master Tables
# ───────────────────────────────────────────────────────────────────────────────
# STEP 7B: PCI — Weighted Defect Score at 10m / 100m / 1km
#
# Reads metrics DIRECTLY from the master Delta tables:
#   Cracking pct (%)      → Crack Index     × 0.12
#   Raveling_Area (sq.m)  → Ravel Index     × 0.10
#   Pothole Area (sq.m)   → Pothole Index   × 0.16
#   Patching Area (sq.m)  → Patch Index     × 0.08
#   Rutting Average (mm)  → Rut Index       × 0.14
#   IRI roughness (m/km)  → Roughness Index × 0.40
#
# Weighted_Defect_Score = sum of (Index × Weight)
# PCI_10m = 100 − Weighted_Defect_Score
#
# Each Index is normalized to 0–100 via max-observed-value scaling.
# Result rolled up to 100m and 1km by averaging constituent 10m PCI scores.
# Produces pci_lookup_10m / _100m / _1km + fill_pci_column() helper.
# ───────────────────────────────────────────────────────────────────────────────
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

SCHEMA_NS = f"{CATALOG}.silver_layer_nsv_schema2"

# ════ Weights (must sum to 1.0) ═══════════════════════════════════════════════
W_CRACK  = 0.12
W_RAVEL  = 0.10
W_POTHOLE = 0.16
W_PATCH  = 0.08
W_RUT    = 0.14
W_ROUGH  = 0.40
assert abs(W_CRACK + W_RAVEL + W_POTHOLE + W_PATCH + W_RUT + W_ROUGH - 1.0) < 1e-9

# ════ Helper: resolve table — prefer in-memory OUTPUT_DATAFRAMES, fallback Delta ═══
def _resolve_table(table_name):
    """Return a Spark DF for this UPC from OUTPUT_DATAFRAMES (cell 12) or Delta."""
    try:
        if OUTPUT_DATAFRAMES and table_name in OUTPUT_DATAFRAMES:
            sdf = OUTPUT_DATAFRAMES[table_name]
            if 'upc_code' in sdf.columns:
                return sdf.filter(F.col("upc_code") == UPC)
            return sdf
    except NameError:
        pass
    _full = f"{SCHEMA_NS}.{table_name}"
    if spark.catalog.tableExists(_full):
        return spark.table(_full).filter(F.col("upc_code") == UPC)
    return None

# ════ Step 1: Build 10m chainage spine from UPC's full chainage range ═════
# Scan ALL master defect tables + patching to find the true min/max chainage
# per lane, so the spine covers every defect (including potholes outside the
# patching range).
_spine_tables = [
    'master_cracking', 'master_roughness_child_10m', 'master_rutting_child_10m',
    'master_ravelling_child_10m', 'master_pothole',
]
_spine_ranges = []
for _tbl in _spine_tables:
    _sdf = _resolve_table(_tbl)
    if _sdf is None:
        continue
    _r = (_sdf
          .select(
              F.col("lane").alias("Lane"),
              F.col("start_chainage").cast("long").alias("ch"))
          .groupBy("Lane")
          .agg(F.min("ch").alias("min_ch"), F.max("ch").alias("max_ch"))
          .toPandas())
    if not _r.empty:
        _spine_ranges.append(_r)
# Also include patching (in-memory)
_patch_range = df_patching_10m_final.groupby('Lane').agg(
    min_ch=('Start_Chainage', 'min'), max_ch=('Start_Chainage', 'max')).reset_index()
_spine_ranges.append(_patch_range)

_all_ranges = pd.concat(_spine_ranges, ignore_index=True)
_lane_bounds = _all_ranges.groupby('Lane').agg(min_ch=('min_ch', 'min'), max_ch=('max_ch', 'max')).reset_index()

# Generate 10m bins for each lane from min to max chainage
_spine_rows = []
for _, _lb in _lane_bounds.iterrows():
    _lo = int((_lb['min_ch'] // CHILD_10M) * CHILD_10M)
    _hi = int((_lb['max_ch'] // CHILD_10M) * CHILD_10M)
    for _ch in range(_lo, _hi + CHILD_10M, CHILD_10M):
        _spine_rows.append({'Lane': _lb['Lane'], 'Start_Chainage': _ch, 'End_Chainage': _ch + CHILD_10M})
pci_spine = pd.DataFrame(_spine_rows)
print(f"PCI spine: {len(pci_spine):,} segments, lanes {sorted(pci_spine['Lane'].unique().tolist())}")
print(f"Chainage: {pci_spine['Start_Chainage'].min()} – {pci_spine['End_Chainage'].max()}")

# ════ Step 2: Read metrics from master Delta tables for this UPC ════════════

def _read_master_metric(table_name, value_col, alias, agg_func='sum'):
    """Read (lane, start_chainage, value) from OUTPUT_DATAFRAMES or Delta, snap to 10m grid."""
    sdf = _resolve_table(table_name)
    if sdf is None:
        return pd.DataFrame(columns=['Lane', 'Start_Chainage', alias])
    pdf = sdf.select(
        F.col("lane").alias("Lane"),
        F.col("start_chainage").cast("long").alias("raw_ch"),
        F.col(value_col).alias("val")
    ).toPandas()
    if pdf.empty:
        return pd.DataFrame(columns=['Lane', 'Start_Chainage', alias])
    pdf['Start_Chainage'] = (pdf['raw_ch'] // CHILD_10M) * CHILD_10M
    pdf['val'] = pd.to_numeric(pdf['val'], errors='coerce').fillna(0.0)
    agg_fn = 'sum' if agg_func == 'sum' else 'max'
    out = pdf.groupby(['Lane', 'Start_Chainage']).agg(val=('val', agg_fn)).reset_index()
    out.rename(columns={'val': alias}, inplace=True)
    return out

# Cracking pct (%): stored in metric_value in master_cracking
cracking_metric = _read_master_metric('master_cracking', 'metric_value', 'cracking_pct', agg_func='max')

# Raveling Area (sq.m): stored in area_sqm in master_ravelling_child_10m
ravelling_metric = _read_master_metric('master_ravelling_child_10m', 'area_sqm', 'ravelling_area', agg_func='sum')

# Pothole Area (sq.m): stored in area_sqm in master_pothole
pothole_metric = _read_master_metric('master_pothole', 'area_sqm', 'pothole_area', agg_func='sum')

# Rutting Average (mm): stored in metric_value in master_rutting_child_10m
rutting_metric = _read_master_metric('master_rutting_child_10m', 'metric_value', 'rutting_mm', agg_func='max')

# IRI roughness (m/km): stored in iri_value column (NOT metric_value, which holds BI m/km)
roughness_metric = _read_master_metric('master_roughness_child_10m', 'iri_value', 'roughness_iri', agg_func='max')

# Patching Area (sq.m): from df_patching_10m_final (already in memory)
patching_metric = df_patching_10m_final[['Lane', 'Start_Chainage', 'Total_Area']].rename(
    columns={'Total_Area': 'patching_area'})

for label, pdf in [('Cracking pct', cracking_metric), ('Ravelling area', ravelling_metric),
                   ('Pothole area', pothole_metric), ('Rutting mm', rutting_metric),
                   ('Roughness IRI', roughness_metric), ('Patching area', patching_metric)]:
    print(f"  {label:16s}: {len(pdf):>6,} rows with data")

# ════ Step 3: Merge all metrics onto the spine ════════════════════════════
pci_df = pci_spine.copy()
for mdf in [cracking_metric, ravelling_metric, pothole_metric,
            rutting_metric, roughness_metric, patching_metric]:
    pci_df = pci_df.merge(mdf, on=['Lane', 'Start_Chainage'], how='left')

raw_cols = ['cracking_pct', 'ravelling_area', 'pothole_area',
            'rutting_mm', 'roughness_iri', 'patching_area']
for col in raw_cols:
    pci_df[col] = pci_df[col].fillna(0.0)

# ════ Step 4: Normalize each to 0–100 Index (max-observed scaling) ═════════
max_vals = {c: pci_df[c].max() for c in raw_cols}

print(f"\nNormalization ceilings (max observed):")
for c, mx in max_vals.items():
    print(f"  {c:20s}: {mx:.4f}")

index_map = {
    'cracking_pct':   'crack_index',
    'ravelling_area':  'ravel_index',
    'pothole_area':    'pothole_index',
    'patching_area':   'patch_index',
    'rutting_mm':      'rut_index',
    'roughness_iri':   'roughness_index',
}
for raw_col, idx_col in index_map.items():
    mx = max_vals[raw_col]
    pci_df[idx_col] = (pci_df[raw_col] / mx * 100.0).clip(0, 100) if mx > 0 else 0.0

# ════ Step 5: Weighted Defect Score & PCI ═════════════════════════════════
pci_df['weighted_defect_score'] = (
    pci_df['crack_index']     * W_CRACK  +
    pci_df['ravel_index']     * W_RAVEL  +
    pci_df['pothole_index']   * W_POTHOLE +
    pci_df['patch_index']     * W_PATCH  +
    pci_df['rut_index']       * W_RUT    +
    pci_df['roughness_index'] * W_ROUGH
).round(4)

pci_df['pci_10m'] = (100.0 - pci_df['weighted_defect_score']).clip(0, 100).round(4)

# ════ Step 6: Metadata ═══════════════════════════════════════════════════
for col_name, val in [('Zone', ZONE), ('Phase', PHASE), ('Cycle', CYCLE),
                      ('UPC', UPC.replace('_', '/')), ('UPC_Code', UPC),
                      ('Run_Date', str(RUN_DATE))]:
    pci_df[col_name] = val

print(f"\n{'='*60}")
print(f"PCI 10m: {len(pci_df):,} segments")
print(f"  PCI range: {pci_df['pci_10m'].min():.2f} – {pci_df['pci_10m'].max():.2f}")
print(f"  Mean PCI:  {pci_df['pci_10m'].mean():.2f}")
print(f"  Segments with defects (PCI < 100): {(pci_df['pci_10m'] < 100).sum():,}")

# ════ Step 7: Roll up to 100m ═════════════════════════════════════════════
pci_df['_ch_100m'] = (pci_df['Start_Chainage'] // 100) * 100
pci_100m = pci_df.groupby(['Lane', '_ch_100m']).agg(
    pci_score=('pci_10m', 'mean'), segment_count=('Start_Chainage', 'count'),
    cracking_pct=('cracking_pct', 'max'), ravelling_area=('ravelling_area', 'sum'),
    pothole_area=('pothole_area', 'sum'), patching_area=('patching_area', 'sum'),
    rutting_mm=('rutting_mm', 'max'), roughness_iri=('roughness_iri', 'max'),
).reset_index()
pci_100m.rename(columns={'_ch_100m': 'Start_Chainage'}, inplace=True)
pci_100m['End_Chainage'] = pci_100m['Start_Chainage'] + 100
pci_100m['pci_score'] = pci_100m['pci_score'].round(4)
for col_name, val in [('Zone', ZONE), ('Phase', PHASE), ('Cycle', CYCLE),
                      ('UPC', UPC.replace('_', '/')), ('UPC_Code', UPC),
                      ('Run_Date', str(RUN_DATE))]:
    pci_100m[col_name] = val
print(f"PCI 100m: {len(pci_100m):,} segments, mean PCI={pci_100m['pci_score'].mean():.2f}")

# ════ Step 8: Roll up to 1km ══════════════════════════════════════════════
pci_df['_ch_1km'] = (pci_df['Start_Chainage'] // 1000) * 1000
pci_1km = pci_df.groupby(['Lane', '_ch_1km']).agg(
    pci_score=('pci_10m', 'mean'), segment_count=('Start_Chainage', 'count'),
    cracking_pct=('cracking_pct', 'max'), ravelling_area=('ravelling_area', 'sum'),
    pothole_area=('pothole_area', 'sum'), patching_area=('patching_area', 'sum'),
    rutting_mm=('rutting_mm', 'max'), roughness_iri=('roughness_iri', 'max'),
).reset_index()
pci_1km.rename(columns={'_ch_1km': 'Start_Chainage'}, inplace=True)
pci_1km['End_Chainage'] = pci_1km['Start_Chainage'] + 1000
pci_1km['pci_score'] = pci_1km['pci_score'].round(4)
for col_name, val in [('Zone', ZONE), ('Phase', PHASE), ('Cycle', CYCLE),
                      ('UPC', UPC.replace('_', '/')), ('UPC_Code', UPC),
                      ('Run_Date', str(RUN_DATE))]:
    pci_1km[col_name] = val
print(f"PCI 1km:  {len(pci_1km):,} segments, mean PCI={pci_1km['pci_score'].mean():.2f}")

# ════ Step 9: PCI lookups + fill_pci_column() helper ═══════════════════════
pci_lookup_10m  = pci_df[['Lane', 'Start_Chainage', 'pci_10m']].rename(columns={'pci_10m': 'pci_score'})
pci_lookup_100m = pci_100m[['Lane', 'Start_Chainage', 'pci_score']].copy()
pci_lookup_1km  = pci_1km[['Lane', 'Start_Chainage', 'pci_score']].copy()

_pci_sdf_10m = spark.createDataFrame(
    pci_lookup_10m.rename(columns={'Lane': '_pci_lane', 'Start_Chainage': '_pci_ch', 'pci_score': '_pci_val'}))
_pci_sdf_100m = spark.createDataFrame(
    pci_lookup_100m.rename(columns={'Lane': '_pci_lane', 'Start_Chainage': '_pci_ch', 'pci_score': '_pci_val'}))
_pci_sdf_1km = spark.createDataFrame(
    pci_lookup_1km.rename(columns={'Lane': '_pci_lane', 'Start_Chainage': '_pci_ch', 'pci_score': '_pci_val'}))
PCI_LOOKUPS_SPARK = {'10m': _pci_sdf_10m, '100m': _pci_sdf_100m, '1km': _pci_sdf_1km}


def fill_pci_column(sdf, level='10m'):
    """Join PCI lookup onto a defect Spark DataFrame, filling its `pci` column."""
    lookup = PCI_LOOKUPS_SPARK[level]
    cols = set(sdf.columns)
    lane_col = 'lane' if 'lane' in cols else 'Lane'
    ch_col = 'start_chainage' if 'start_chainage' in cols else 'Start_Chainage'
    grid = {'10m': CHILD_10M, '100m': 100, '1km': 1000}[level]
    sdf2 = sdf.withColumn('_join_ch', (F.col(ch_col).cast('long') / grid).cast('long') * grid)
    joined = sdf2.join(
        lookup,
        (sdf2[lane_col] == lookup['_pci_lane']) & (sdf2['_join_ch'] == lookup['_pci_ch'].cast('long')),
        'left'
    )
    return joined.withColumn('pci', F.coalesce(F.col('_pci_val'), F.lit(None)).cast('double')) \
                 .drop('_pci_lane', '_pci_ch', '_pci_val', '_join_ch')


print(f"\nfill_pci_column() ready. Lookups: 10m={len(pci_lookup_10m):,}, 100m={len(pci_lookup_100m):,}, 1km={len(pci_lookup_1km):,}")

# ════ Step 10: Backfill pci into OUTPUT_DATAFRAMES if already created ═══════
try:
    if 'OUTPUT_DATAFRAMES' in dir() and OUTPUT_DATAFRAMES:
        updated = []
        for tbl_name, sdf in OUTPUT_DATAFRAMES.items():
            if 'pci' in sdf.columns:
                lvl = '1km' if ('parent' in tbl_name) else ('100m' if '100m' in tbl_name else '10m')
                OUTPUT_DATAFRAMES[tbl_name] = fill_pci_column(sdf, level=lvl)
                updated.append(f"{tbl_name} ({lvl})")
        if updated:
            print(f"Backfilled pci in: {', '.join(updated)}")
        else:
            print("No OUTPUT_DATAFRAMES with pci column found yet.")
    else:
        print("OUTPUT_DATAFRAMES not yet available — fill_pci_column() will be used after cell 12 runs.")
except Exception as e:
    print(f"Note: Could not backfill yet ({e}).")

# ════ Display samples ═══════════════════════════════════════════════════
show_cols = ['Lane', 'Start_Chainage', 'End_Chainage',
             'cracking_pct', 'ravelling_area', 'pothole_area',
             'patching_area', 'rutting_mm', 'roughness_iri',
             'weighted_defect_score', 'pci_10m']
print("\nPCI 10m — worst segments (lowest PCI):")
display(pci_df[show_cols].nsmallest(15, 'pci_10m'))

# ════ Step 11: Write PCI tables to Volume as CSV ══════════════════════════════
PCI_BASE_PATH = f"/Volumes/nhai_dhms/silver_layer_nsv_schema2/defect_tables/pci/Zone={ZONE}/Phase={PHASE}/Cycle={CYCLE}/UPC={UPC}"
import os
os.makedirs(PCI_BASE_PATH, exist_ok=True)

for level_name, level_df in [('10m', pci_df), ('100m', pci_100m), ('1km', pci_1km)]:
    _csv_path = f"{PCI_BASE_PATH}/pci_{level_name}.csv"
    level_df.to_csv(_csv_path, index=False)
    print(f"PCI {level_name}: {len(level_df):,} rows written to {_csv_path}")

# COMMAND ----------

# DBTITLE 1,Preview Defect Tables with PCI Filled
# ───────────────────────────────────────────────────────────────────────────────
# Preview: Read from OUTPUT_DATAFRAMES (cell 12) with PCI filled (cell 13),
# so we validate in-memory data before writing to Delta.
# ───────────────────────────────────────────────────────────────────────────────

TABLES_TO_CHECK = {
    # table_name                       → level for PCI lookup
    'master_pothole':                   '10m',
    'master_cracking':                  '10m',
    'master_roughness_child_10m':       '10m',
    'master_rutting_child_10m':         '10m',
    'master_ravelling_child_10m':       '10m',
    'master_roughness_child_100m':      '100m',
    'master_rutting_child_100m':        '100m',
    'master_ravelling_child_100m':      '100m',
    'master_roughness_parent':          '1km',
    'master_rutting_parent':            '1km',
    'master_ravelling_parent':          '1km',
}

for tbl, lvl in TABLES_TO_CHECK.items():
    if tbl not in OUTPUT_DATAFRAMES:
        print(f"\n⚠ {tbl} — not in OUTPUT_DATAFRAMES, skipping")
        continue

    sdf = OUTPUT_DATAFRAMES[tbl]
    row_count = sdf.count()
    if row_count == 0:
        print(f"\n{tbl} — 0 rows, skipping")
        continue

    print(f"\n{'='*70}")
    print(f"{tbl}  ({row_count:,} rows, PCI level={lvl})")
    display(sdf)

# COMMAND ----------

# DBTITLE 0,Create Workbook Defined Delta Tables
# ───────────────────────────────────────────────────────────────────────────────
# STEP 8: Create workbook-defined Delta tables and write current UPC data
# Safe target schema: nhai_dhms.silver_defect_data
# ───────────────────────────────────────────────────────────────────────────────

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {OUTPUT_NAMESPACE}")


def empty_df(schema_obj):
    return spark.createDataFrame([], schema_obj)


OUTPUT_TABLES = sorted(TABLE_SCHEMAS.keys())

# Guard: skip silver write when orchestrator runs in master-only mode
_skip_silver_write = False
try:
    _skip_silver_write = dbutils.widgets.get("MASTER_RUN_MODE").strip().lower() == "single_upc_write"
except Exception:
    _skip_silver_write = False

if _skip_silver_write:
    print("⏭ Skipping silver_defect_data write — running in master NSV mode only")

for table_name in ([] if _skip_silver_write else OUTPUT_TABLES):
    full_name = f"{OUTPUT_NAMESPACE}.{table_name}"
    if table_name in OUTPUT_DATAFRAMES:
        write_df = OUTPUT_DATAFRAMES[table_name]
    else:
        write_df = empty_df(TABLE_SCHEMAS[table_name])

    table_exists = spark.catalog.tableExists(full_name)

    if table_exists:
        # Replace only the current UPC's data, preserving other UPCs
        (
            write_df.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"upc_code = '{UPC}' AND phase = '{PHASE}' AND cycle = '{CYCLE}'")
            .saveAsTable(full_name)
        )
    else:
        # First run — create the table
        (
            write_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(full_name)
        )

    total_rows = spark.read.table(full_name).count()
    upc_rows = write_df.count()
    print(f"{'Updated' if table_exists else 'Created'}: {full_name} ({upc_rows:,} rows for {UPC}, {total_rows:,} total)")
    if upc_rows > 0:
        display(write_df.limit(10))


# COMMAND ----------

# DBTITLE 1,Validate Workbook Based Output Tables
# ───────────────────────────────────────────────────────────────────────────────
# STEP 9: Validate workbook-based output tables
# ───────────────────────────────────────────────────────────────────────────────

validation_rows = []
for table_name in OUTPUT_TABLES:
    full_name = f"{OUTPUT_NAMESPACE}.{table_name}"
    sdf = spark.table(full_name)
    expected_cols = len(TABLE_COLUMNS[table_name])
    actual_cols = len(sdf.columns)
    row_count = sdf.count()
    match = "OK" if actual_cols == expected_cols else f"MISMATCH (expected {expected_cols})"
    validation_rows.append((table_name, row_count, actual_cols, expected_cols, match))

validation_df = spark.createDataFrame(
    validation_rows,
    ["table_name", "row_count", "actual_cols", "expected_cols", "status"]
).orderBy("table_name")

print(f"Validation namespace: {OUTPUT_NAMESPACE}")
display(validation_df)

print("Sample rows from master_pothole:")
display(spark.table(f"{OUTPUT_NAMESPACE}.master_pothole").limit(5))

print("Schema for master_pothole:")
spark.table(f"{OUTPUT_NAMESPACE}.master_pothole").printSchema()


# COMMAND ----------

# DBTITLE 1,Null Column Analysis Across All Tables
# ───────────────────────────────────────────────────────────────────────────────
# NULL COLUMN ANALYSIS: Find columns that are 100% NULL in each output table
# This helps identify which columns need data from other sources/sheets.
# ───────────────────────────────────────────────────────────────────────────────

from pyspark.sql import functions as F

null_report = []

for table_name in sorted(TABLE_SCHEMAS.keys()):
    full_name = f"{OUTPUT_NAMESPACE}.{table_name}"
    sdf = spark.table(full_name)
    total_rows = sdf.count()

    if total_rows == 0:
        # All columns are null if table is empty
        for col_name in sdf.columns:
            null_report.append((table_name, col_name, 0, 0, "EMPTY TABLE"))
        continue

    # Count nulls per column in one pass
    null_exprs = [
        F.sum(F.when(F.col(f"`{c}`").isNull(), 1).otherwise(0)).alias(c)
        for c in sdf.columns
    ]
    null_counts = sdf.select(*null_exprs).collect()[0]

    for col_name in sdf.columns:
        null_count = null_counts[col_name]
        if null_count == total_rows:
            null_report.append((table_name, col_name, total_rows, null_count, "ALL NULL"))

if null_report:
    null_df = spark.createDataFrame(
        null_report,
        ["table_name", "column_name", "total_rows", "null_count", "status"]
    ).orderBy("table_name", "column_name")

    # Summary: count of fully-null columns per table
    print("FULLY NULL COLUMNS PER TABLE:")
    print("=" * 60)
    summary = null_df.groupBy("table_name").count().orderBy("table_name")
    display(summary)

    print("\nDETAILED NULL COLUMNS:")
    print("=" * 60)
    display(null_df)
else:
    print("No fully-null columns found in any table.")

# COMMAND ----------

# DBTITLE 1,Export All Tables to CSV on Volume
# # ───────────────────────────────────────────────────────────────────────────────
# # STEP 10: Export all 11 master tables to CSV on UC Volume
# # ───────────────────────────────────────────────────────────────────────────────

# CSV_VOLUME = f"/Volumes/nhai_dhms/silver_layer_nsv_schema2/csv_exports/_1/{UPC}"

# # Create the volume if it doesn't exist
# spark.sql("""
#     CREATE VOLUME IF NOT EXISTS nhai_dhms.silver_layer_nsv_schema2.csv_exports
# """)

# import os
# os.makedirs(CSV_VOLUME, exist_ok=True)

# for table_name in sorted(OUTPUT_TABLES):
#     full_table = f"{OUTPUT_NAMESPACE}.{table_name}"
#     csv_path = f"{CSV_VOLUME}/{table_name}.csv"
#     pdf = spark.table(full_table).toPandas()
#     pdf.to_csv(csv_path, index=False)
#     print(f"Exported: {csv_path}  ({len(pdf):,} rows, {len(pdf.columns)} cols)")

# print(f"\nAll {len(OUTPUT_TABLES)} tables exported to {CSV_VOLUME}/")
# print("Files:")
# for f in sorted(os.listdir(CSV_VOLUME)):
#     size_kb = os.path.getsize(f"{CSV_VOLUME}/{f}") / 1024
#     print(f"  {f:45s} {size_kb:>8.1f} KB")

# COMMAND ----------

# DEFECT_TABLES_VOLUME = "nhai_dhms.silver_layer_nsv_schema2.defect_tables"

# spark.sql(f"CREATE VOLUME IF NOT EXISTS {DEFECT_TABLES_VOLUME}")

# DEFECT_TABLES_BASE = (
#     f"/Volumes/nhai_dhms/silver_layer_nsv_schema2/defect_tables/"
#     f"Zone={ZONE}/Phase={PHASE}/Cycle={CYCLE}/UPC={UPC}"
# )
# os.makedirs(DEFECT_TABLES_BASE, exist_ok=True)

# for table_name in OUTPUT_TABLES:
#     table_path = f"{DEFECT_TABLES_BASE}/{table_name}"
#     write_df = OUTPUT_DATAFRAMES.get(table_name, empty_df(TABLE_SCHEMAS[table_name]))

#     (
#         write_df.write
#         .format("delta")
#         .mode("overwrite")
#         .option("overwriteSchema", "true")
#         .save(table_path)
#     )

#     print(f"Written: {table_path}  ({write_df.count():,} rows)")

# print(f"\nAll {len(OUTPUT_TABLES)} tables written under {DEFECT_TABLES_BASE}/")

# COMMAND ----------

# DBTITLE 1,Verify Delta Table Contents by UPC
# # ───────────────────────────────────────────────────────────────────────────────
# # Verify Delta table contents: row counts per UPC across all tables
# # ───────────────────────────────────────────────────────────────────────────────

# from pyspark.sql import functions as F

# verify_rows = []
# for table_name in sorted(TABLE_SCHEMAS.keys()):
#     full_name = f"{OUTPUT_NAMESPACE}.{table_name}"
#     sdf = spark.table(full_name)
#     total = sdf.count()
#     # Per-UPC breakdown
#     upc_counts = (
#         sdf.groupBy("upc_code")
#         .count()
#         .orderBy("upc_code")
#         .collect()
#     )
#     if upc_counts:
#         for row in upc_counts:
#             verify_rows.append((table_name, row["upc_code"], row["count"], total))
#     else:
#         verify_rows.append((table_name, None, 0, 0))

# verify_df = spark.createDataFrame(
#     verify_rows,
#     ["table_name", "upc_code", "upc_rows", "total_rows"]
# ).orderBy("table_name", "upc_code")

# print(f"Delta tables in: {OUTPUT_NAMESPACE}")
# print(f"Current UPC: {UPC}")
# display(verify_df)

# COMMAND ----------

# DBTITLE 1,Migrate Volume Parquet Data to Schema Tables (Deduped)
# # ═══════════════════════════════════════════════════════════════════════════════
# # Migrate defect data from Volume parquets → schema Delta tables
# # - Reads Delta tables from /Volumes/nhai_dhms/silver_layer_nsv_schema2/defect_tables
# # - Deduplicates each UPC on the `id` column
# # - Uses replaceWhere per UPC to avoid cross-UPC data loss
# # ═══════════════════════════════════════════════════════════════════════════════

# import os
# from pyspark.sql import functions as F

# VOLUME_BASE = "/Volumes/nhai_dhms/silver_layer_nsv_schema2/defect_tables"
# TARGET_SCHEMA = "nhai_dhms.silver_layer_nsv_schema2"

# TABLE_NAMES = [
#     "master_cracking", "master_pothole",
#     "master_ravelling_child_100m", "master_ravelling_child_10m", "master_ravelling_parent",
#     "master_roughness_child_100m", "master_roughness_child_10m", "master_roughness_parent",
#     "master_rutting_child_100m", "master_rutting_child_10m", "master_rutting_parent",
# ]

# # ── Discover all UPC paths in the volume ──────────────────────────────────────
# upc_paths = []  # list of (upc_code, full_path_to_upc_folder)
# for root, dirs, files in os.walk(VOLUME_BASE):
#     rel_parts = os.path.relpath(root, VOLUME_BASE).split(os.sep)
#     upc_part = [p for p in rel_parts if p.startswith("UPC=")]
#     if upc_part and any(d in TABLE_NAMES for d in dirs):
#         upc_code = upc_part[0].replace("UPC=", "")
#         upc_paths.append((upc_code, root))

# print(f"Discovered {len(upc_paths)} UPCs to migrate\n")

# # ── Migrate each UPC × table ─────────────────────────────────────────────────
# summary = []

# # for upc_code, upc_folder in sorted(upc_paths):
#     print(f"\n{'═'*70}")
#     print(f"UPC: {upc_code}")
#     print(f"{'═'*70}")

#     for table_name in TABLE_NAMES:
#         table_path = os.path.join(upc_folder, table_name)
#         full_target = f"{TARGET_SCHEMA}.{table_name}"

#         # Skip if table folder doesn't exist in volume
#         if not os.path.exists(table_path):
#             print(f"  ⏭  {table_name}: not found in volume, skipping")
#             summary.append((upc_code, table_name, 0, 0, "skipped"))
#             continue

#         # Read from volume (Delta format — has _delta_log)
#         try:
#             src_sdf = spark.read.format("delta").load(table_path)
#         except Exception:
#             # Fall back to plain parquet if delta log is incomplete
#             try:
#                 src_sdf = spark.read.parquet(table_path)
#             except Exception as e:
#                 print(f"  ❌  {table_name}: read failed — {e}")
#                 summary.append((upc_code, table_name, 0, 0, "read_error"))
#                 continue

#         raw_count = src_sdf.count()
#         if raw_count == 0:
#             print(f"  ⏭  {table_name}: 0 rows in volume, skipping")
#             summary.append((upc_code, table_name, 0, 0, "empty"))
#             continue

#         # Deduplicate on `id` column (keep first occurrence)
#         if "id" in src_sdf.columns:
#             deduped_sdf = src_sdf.dropDuplicates(["id"])
#         else:
#             deduped_sdf = src_sdf.dropDuplicates()
#         deduped_count = deduped_sdf.count()
#         dups_removed = raw_count - deduped_count

#         # Check if target table exists
#         target_exists = spark.catalog.tableExists(full_target)

#         if target_exists:
#             # Align source schema to target schema (handle column order / missing cols)
#             target_schema = spark.table(full_target).schema
#             target_cols = [f.name for f in target_schema.fields]
#             # Add missing columns as nulls, select in target order
#             for col_name in target_cols:
#                 if col_name not in deduped_sdf.columns:
#                     target_field = [f for f in target_schema.fields if f.name == col_name][0]
#                     deduped_sdf = deduped_sdf.withColumn(col_name, F.lit(None).cast(target_field.dataType))
#             deduped_sdf = deduped_sdf.select(*target_cols)

#             # replaceWhere: overwrite only this UPC's data
#             (
#                 deduped_sdf.write
#                 .format("delta")
#                 .mode("overwrite")
#                 .option("replaceWhere", f"upc_code = '{upc_code}'")
#                 .saveAsTable(full_target)
#             )
#             status = f"updated (–{dups_removed} dups)" if dups_removed else "updated"
#         else:
#             # First time — create the table
#             (
#                 deduped_sdf.write
#                 .format("delta")
#                 .mode("overwrite")
#                 .option("overwriteSchema", "true")
#                 .saveAsTable(full_target)
#             )
#             status = f"created (–{dups_removed} dups)" if dups_removed else "created"

#         print(f"  ✅  {table_name}: {raw_count:,} raw → {deduped_count:,} deduped | {status}")
#         summary.append((upc_code, table_name, raw_count, deduped_count, status))

# # ── Final summary ────────────────────────────────────────────────────────────
# print(f"\n\n{'═'*70}")
# print("MIGRATION SUMMARY")
# print(f"{'═'*70}")
# summary_df = spark.createDataFrame(summary, ["upc_code", "table_name", "raw_rows", "deduped_rows", "status"])
# display(summary_df.orderBy("upc_code", "table_name"))

# COMMAND ----------

# DBTITLE 1,Query All Rows for UPC N_02001_11001_GJ
# # Query ALL rows for UPC N_02001_11001_GJ across all schema tables
# TARGET_UPC = "N_04003_14001_GJ"
# SCHEMA = "nhai_dhms.silver_layer_nsv_schema2"

# tables = [
#     "master_cracking", "master_pothole",
#     "master_ravelling_child_100m", "master_ravelling_child_10m", "master_ravelling_parent",
#     "master_roughness_child_100m", "master_roughness_child_10m", "master_roughness_parent",
#     "master_rutting_child_100m", "master_rutting_child_10m", "master_rutting_parent",
# ]

# for tbl in tables:
#     full_name = f"{SCHEMA}.{tbl}"
#     upc_df = spark.table(full_name).filter(F.col("upc_code") == TARGET_UPC)
#     cnt = upc_df.count()
#     print(f"\n{'='*60}")
#     print(f"{tbl}: {cnt:,} rows for {TARGET_UPC}")
#     print(f"{'='*60}")
#     if cnt > 0:
#         display(upc_df)

# COMMAND ----------

# DBTITLE 1,UPC Counts Per Table in silver_defect_data
from pyspark.sql import functions as F

SCHEMA = "nhai_dhms.silver_defect_data"

tables = [
    "master_cracking", "master_pothole",
    "master_ravelling_child_100m", "master_ravelling_child_10m", "master_ravelling_parent",
    "master_roughness_child_100m", "master_roughness_child_10m", "master_roughness_parent",
    "master_rutting_child_100m", "master_rutting_child_10m", "master_rutting_parent",
]

rows = []
for tbl in tables:
    full_name = f"{SCHEMA}.{tbl}"
    if not spark.catalog.tableExists(full_name):
        rows.append((tbl, 0, 0))
        continue
    sdf = spark.table(full_name)
    total = sdf.count()
    upc_count = sdf.select("upc_code").distinct().count()
    rows.append((tbl, total, upc_count))
    print(f"{tbl}: {total:,} total rows, {upc_count} distinct UPCs")

print(f"\n{'='*60}")
summary_df = spark.createDataFrame(rows, ["table_name", "total_rows", "distinct_upcs"]).orderBy("table_name")
display(summary_df)

# COMMAND ----------

# DBTITLE 1,Unique UPCs with Zone Phase Cycle + Table Counts
# ── Unique UPCs with Zone, Phase, Cycle from both schemas ────────────────────
from pyspark.sql import functions as F

SILVER_SCHEMA = "nhai_dhms.silver_defect_data"
MASTER_SCHEMA = "nhai_dhms.master_nsv_data"

df_silver_upcs = spark.sql(f"""
    SELECT DISTINCT upc_code, zone, phase, CAST(cycle AS STRING) AS cycle
    FROM {SILVER_SCHEMA}.master_pothole
    ORDER BY zone, phase, cycle, upc_code
""")

df_master_upcs = spark.sql(f"""
    SELECT DISTINCT upc_code, zone, phase, CAST(cycle AS STRING) AS cycle
    FROM {MASTER_SCHEMA}.master_pothole
    ORDER BY zone, phase, cycle, upc_code
""")

# Side-by-side comparison
silver_pdf = df_silver_upcs.toPandas().assign(in_silver='YES')
master_pdf = df_master_upcs.toPandas().assign(in_master='YES')

merged = (
    silver_pdf[['upc_code', 'zone', 'phase', 'cycle', 'in_silver']]
    .merge(master_pdf[['upc_code', 'in_master']], on='upc_code', how='outer')
)
merged['in_silver'] = merged['in_silver'].fillna('NO')
merged['in_master'] = merged['in_master'].fillna('NO')

for col in ['zone', 'phase', 'cycle']:
    mask = merged[col].isna()
    if mask.any():
        fill = master_pdf.set_index('upc_code')[col]
        merged.loc[mask, col] = merged.loc[mask, 'upc_code'].map(fill)

merged = merged.sort_values(['zone', 'phase', 'cycle', 'upc_code']).reset_index(drop=True)
merged.index += 1
merged.index.name = '#'

print(f"Silver UPCs: {(merged['in_silver']=='YES').sum()}  |  Master UPCs: {(merged['in_master']=='YES').sum()}  |  Total unique: {len(merged)}")
print(f"Silver only: {((merged['in_silver']=='YES') & (merged['in_master']=='NO')).sum()}  |  Master only: {((merged['in_silver']=='NO') & (merged['in_master']=='YES')).sum()}")
print()
display(spark.createDataFrame(merged.reset_index()))

# ── UPC counts per table in silver schema ────────────────────────────────────
print(f"\n{'='*70}")
print("UPC COUNTS PER TABLE IN silver_layer_nsv_schema2")
print(f"{'='*70}")

tables = [
    "master_pothole", "master_cracking",
    "master_roughness_child_10m", "master_roughness_child_100m", "master_roughness_parent",
    "master_rutting_child_10m", "master_rutting_child_100m", "master_rutting_parent",
    "master_ravelling_child_10m", "master_ravelling_child_100m", "master_ravelling_parent",
]

count_rows = []
for tbl in tables:
    full_name = f"{SILVER_SCHEMA}.{tbl}"
    if not spark.catalog.tableExists(full_name):
        count_rows.append((tbl, 0, 0))
        continue
    sdf = spark.table(full_name)
    total = sdf.count()
    upc_count = sdf.select("upc_code").distinct().count()
    count_rows.append((tbl, total, upc_count))
    print(f"  {tbl:40s}: {total:>8,} rows, {upc_count:>3} UPCs")

print()
display(spark.createDataFrame(count_rows, ["table_name", "total_rows", "distinct_upcs"]).orderBy("table_name"))

# COMMAND ----------

# DBTITLE 1,Multi-UPC Master NSV Orchestrator
# ───────────────────────────────────────────────────────────────────────────────
# STEP 11: Multi-UPC orchestrator for full-data master NSV tables
# Writes the same master_* collections to nhai_dhms.master_nsv_data without the
# silver-layer breach-only filtering. Use preview mode first, then enable batch.
# ───────────────────────────────────────────────────────────────────────────────

import os
import json
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.window import Window

THIS_NOTEBOOK_PATH = "/Shared/Medallain Arc/Silver Layer Delta Tables Pipeline Clone Update_5"
MASTER_NAMESPACE = "nhai_dhms.master_nsv_data"


def _safe_widget(name, default=""):
    try:
        value = dbutils.widgets.get(name)
        return value if value is not None and value != "" else default
    except Exception:
        return default


CURRENT_ZONE = _safe_widget("ZONE", "D")
CURRENT_PHASE = _safe_widget("PHASE", "2")
CURRENT_CYCLE = _safe_widget("CYCLE", "1")
CURRENT_UPC = _safe_widget("UPC", "")
MASTER_RUN_MODE = _safe_widget("MASTER_RUN_MODE", "interactive").strip().lower()
MASTER_WRITE_ENABLED = _safe_widget("MASTER_WRITE_ENABLED", "0").strip() == "1"

TARGET_TABLES = [
    "master_pothole", "master_cracking",
    "master_ravelling_child_100m", "master_ravelling_child_10m", "master_ravelling_parent",
    "master_roughness_child_100m", "master_roughness_child_10m", "master_roughness_parent",
    "master_rutting_child_100m", "master_rutting_child_10m", "master_rutting_parent",
]

PCI_LEVEL_MAP = {
    "master_pothole": "10m",
    "master_cracking": "10m",
    "master_roughness_child_10m": "10m",
    "master_rutting_child_10m": "10m",
    "master_ravelling_child_10m": "10m",
    "master_roughness_child_100m": "100m",
    "master_rutting_child_100m": "100m",
    "master_ravelling_child_100m": "100m",
    "master_roughness_parent": "1km",
    "master_rutting_parent": "1km",
    "master_ravelling_parent": "1km",
}

# Dynamically find UPCs present in silver but missing from master NSV
SILVER_SCHEMA_NAME = "nhai_dhms.silver_defect_data"
_silver_upcs = spark.sql(f"""
    SELECT DISTINCT upc_code, zone, phase, CAST(cycle AS STRING) AS cycle
    FROM {SILVER_SCHEMA_NAME}.master_pothole
""")
if spark.catalog.tableExists(f"{MASTER_NAMESPACE}.master_pothole"):
    _master_upcs = spark.sql(f"SELECT DISTINCT upc_code FROM {MASTER_NAMESPACE}.master_pothole")
    _missing = _silver_upcs.join(_master_upcs, on='upc_code', how='left_anti')
else:
    _missing = _silver_upcs

ORCHESTRATOR_UPCS = [
    {"ZONE": row.zone, "PHASE": row.phase, "CYCLE": row.cycle, "UPC": row.upc_code}
    for row in _missing.orderBy("zone", "phase", "cycle", "upc_code").collect()
]
print(f"UPCs missing from master_nsv_data: {len(ORCHESTRATOR_UPCS)}")
for cfg in ORCHESTRATOR_UPCS:
    print(f"  {cfg['UPC']}  Zone={cfg['ZONE']} Phase={cfg['PHASE']} Cycle={cfg['CYCLE']}")

RUN_MASTER_UPC_BATCH = True
PREVIEW_CURRENT_UPC = False
PARALLEL_WORKERS = 2
NOTEBOOK_RUN_TIMEOUT_SECONDS = 7200


def _require_globals(names):
    missing = [name for name in names if name not in globals()]
    if missing:
        raise RuntimeError(
            "This step depends on the notebook setup cells. Missing globals: "
            + ", ".join(missing)
            + ". Run the notebook from the top, or trigger this cell in orchestrator mode so the child run builds the setup first."
        )


REQUIRED_GLOBALS = [
    "BACKUP_BASE", "UPC", "ZONE", "PHASE", "CYCLE", "RUN_DATE",
    "pothole_output_sdf", "build_cracking_sdf", "build_linear_output_set",
    "load_backup_lane_parquets", "discover_lane_folders", "empty_with_schema",
    "TABLE_SCHEMAS", "fill_pci_column", "OUTPUT_DATAFRAMES", "TRACKER_CTX",
    "pick_col", "CA_BI_THRESHOLD", "IRC_IRI_POOR", "CA_RUT_THRESHOLD",
    "CA_CRACK_PCT", "RAVELLING_RATING_THRESHOLD"
]


def _load_unfiltered_backup_frames():
    cracking_pdf, _ = load_backup_lane_parquets(BACKUP_BASE, '_Cracking.parquet')
    roughness_pdf, _ = load_backup_lane_parquets(BACKUP_BASE, '_Roughness_IRI.parquet')
    rutting_pdf, _ = load_backup_lane_parquets(BACKUP_BASE, '_Rutting.parquet')
    ravelling_pdf, _ = load_backup_lane_parquets(BACKUP_BASE, '_Raveling.parquet')

    if not cracking_pdf.empty:
        cracking_pdf = cracking_pdf.copy()
        cracking_pdf.rename(columns={
            'LANE': 'Lane',
            'Percent_Area': 'Cracking',
            'cracking_image_name': 'Image_Name',
            'cracking_image_url': 'Image_URL',
            'Start_Latitude': 'Start_Lat',
            'Start_Longitude': 'Start_Lon',
            'End_Latitude': 'End_Lat',
            'End_Longitude': 'End_Lon',
            'survey_date_crack': 'SurveyDate',
            'Area_Sum': 'Area_sq_m'
        }, inplace=True)
        if 'Lane' not in cracking_pdf.columns and 'Source_Lane_Folder' in cracking_pdf.columns:
            cracking_pdf['Lane'] = cracking_pdf['Source_Lane_Folder']
        if 'Cracking' in cracking_pdf.columns:
            crack_pct = pd.to_numeric(cracking_pdf['Cracking'], errors='coerce').fillna(0)
            cracking_pdf['Condition'] = crack_pct.apply(crack_condition_from_pct)
        cracking_pdf['Type_of_Defect'] = 'Cracking'

    if not roughness_pdf.empty:
        roughness_pdf = roughness_pdf.copy()
        roughness_pdf.rename(columns={
            'RoughnessIndex': 'BI m/km',
            'LaneIri': 'IRI',
            'Start_Latitude': 'Start_Lat',
            'Start_Longitude': 'Start_Lon',
            'End_Latitude': 'End_Lat',
            'End_Longitude': 'End_Lon',
            'ConditionOfRoadSurface': 'Condition',
        }, inplace=True)
        if 'SurveyDate' not in roughness_pdf.columns:
            roughness_pdf['SurveyDate'] = pd.NaT
        if 'BI m/km' not in roughness_pdf.columns:
            roughness_pdf['BI m/km'] = np.nan
        if 'IRI' not in roughness_pdf.columns:
            roughness_pdf['IRI'] = np.nan
        roughness_pdf['BI m/km'] = pd.to_numeric(roughness_pdf['BI m/km'], errors='coerce')
        roughness_pdf['IRI'] = pd.to_numeric(roughness_pdf['IRI'], errors='coerce')
        null_bi = roughness_pdf['BI m/km'].isna().sum()
        if null_bi > 0:
            roughness_pdf['BI m/km'] = roughness_pdf['BI m/km'].fillna(630 * (roughness_pdf['IRI'] ** 1.12))
        derived_condition = [
            roughness_condition_from_metrics(i, b)
            for i, b in zip(roughness_pdf['IRI'], roughness_pdf['BI m/km'])
        ]
        existing_condition = roughness_pdf['Condition'] if 'Condition' in roughness_pdf.columns else pd.Series([None] * len(roughness_pdf))
        roughness_pdf['Condition'] = existing_condition.fillna(pd.Series(derived_condition))
        roughness_pdf['Type_of_Defect'] = 'Roughness'

    if not rutting_pdf.empty:
        rutting_pdf = rutting_pdf.copy()
        rutting_pdf.rename(columns={
            'LANE': 'Lane',
            'Rutting Average': 'Rutting',
            'Start_Latitude': 'Start_Lat',
            'Start_Longitude': 'Start_Lon',
            'End_Latitude': 'End_Lat',
            'End_Longitude': 'End_Lon',
            'survey_date_rutting': 'SurveyDate',
        }, inplace=True)
        if 'Lane' not in rutting_pdf.columns and 'Source_Lane_Folder' in rutting_pdf.columns:
            rutting_pdf['Lane'] = rutting_pdf['Source_Lane_Folder']
        if 'Rutting' in rutting_pdf.columns:
            rut_val = pd.to_numeric(rutting_pdf['Rutting'], errors='coerce')
            rutting_pdf['Condition'] = rut_val.apply(rutting_condition_from_mm)
        rutting_pdf['Type_of_Defect'] = 'Rutting'

    if not ravelling_pdf.empty:
        ravelling_pdf = ravelling_pdf.copy()
        ravelling_pdf.rename(columns={
            'Raveling_Area_m2': 'Ravelling_Area_sq_m',
            'Start_Latitude': 'Start_Lat',
            'Start_Longitude': 'Start_Lon',
            'End_Latitude': 'End_Lat',
            'End_Longitude': 'End_Lon',
            'survey_date_raveling': 'SurveyDate',
        }, inplace=True)
        if 'Ravelling_Area_sq_m' in ravelling_pdf.columns:
            rav_pct = pd.to_numeric(ravelling_pdf['Ravelling_Area_sq_m'], errors='coerce').fillna(0) * 100.0 / 35.0
            ravelling_pdf['Ravelling'] = rav_pct
            ravelling_pdf['Condition'] = rav_pct.apply(ravelling_condition_from_pct)
        ravelling_pdf['Type_of_Defect'] = 'Ravelling'

    return {
        'cracking': cracking_pdf,
        'roughness': roughness_pdf,
        'rutting': rutting_pdf,
        'ravelling': ravelling_pdf,
    }


def _build_unfiltered_master_outputs():
    _require_globals(REQUIRED_GLOBALS)

    unfiltered_pdfs = _load_unfiltered_backup_frames()
    master_outputs = {
        'master_pothole': pothole_output_sdf
    }

    old_threshold_filter = globals().get('_threshold_filter')
    old_output_dataframes = globals().get('OUTPUT_DATAFRAMES')
    old_raw_frames = {
        'df_cracking_raw': globals().get('df_cracking_raw'),
        'df_roughness_raw': globals().get('df_roughness_raw'),
        'df_rutting_raw': globals().get('df_rutting_raw'),
        'df_ravelling_raw': globals().get('df_ravelling_raw'),
        'df_visual_raw': globals().get('df_visual_raw'),
    }

    try:
        globals()['df_cracking_raw'] = unfiltered_pdfs['cracking']
        globals()['df_roughness_raw'] = unfiltered_pdfs['roughness']
        globals()['df_rutting_raw'] = unfiltered_pdfs['rutting']
        globals()['df_ravelling_raw'] = unfiltered_pdfs['ravelling']
        globals()['df_visual_raw'] = unfiltered_pdfs['ravelling'].copy() if not unfiltered_pdfs['ravelling'].empty else pd.DataFrame()
        globals()['_threshold_filter'] = lambda pdf, defect_base_name: pdf.copy()

        master_outputs['master_cracking'] = build_cracking_sdf()
        master_outputs.update(build_linear_output_set(globals()['df_roughness_raw'], 'roughness'))
        master_outputs.update(build_linear_output_set(globals()['df_rutting_raw'], 'rutting'))
        master_outputs.update(build_linear_output_set(globals()['df_ravelling_raw'], 'ravelling'))

        globals()['OUTPUT_DATAFRAMES'] = master_outputs
        for tbl_name, lvl in PCI_LEVEL_MAP.items():
            if tbl_name in master_outputs and 'pci' in master_outputs[tbl_name].columns:
                master_outputs[tbl_name] = fill_pci_column(master_outputs[tbl_name], level=lvl)
    finally:
        globals()['_threshold_filter'] = old_threshold_filter
        globals()['OUTPUT_DATAFRAMES'] = old_output_dataframes
        for key, value in old_raw_frames.items():
            globals()[key] = value

    return master_outputs


def _assign_master_display_ids(output_dataframes):
    src_letter = 'N'
    defect_order = [
        'master_roughness_parent', 'master_roughness_child_100m', 'master_roughness_child_10m',
        'master_rutting_parent', 'master_rutting_child_100m', 'master_rutting_child_10m',
        'master_cracking',
        'master_ravelling_parent', 'master_ravelling_child_100m', 'master_ravelling_child_10m',
        'master_pothole',
    ]

    max_seq = 0
    for tbl_name in defect_order:
        if tbl_name not in output_dataframes:
            continue
        full_name = f"{MASTER_NAMESPACE}.{tbl_name}"
        if spark.catalog.tableExists(full_name):
            existing_max = (
                spark.table(full_name)
                .filter(
                    (F.col('upc_code') != UPC) &
                    F.col('display_defect_id').rlike(r'^D/\d{6}/[NDRO]/\d+$')
                )
                .withColumn('_ctr', F.element_at(F.split('display_defect_id', '/'), 4).cast('int'))
                .select(F.max('_ctr'))
                .collect()[0][0]
            )
            if existing_max:
                max_seq = max(max_seq, existing_max)

    running_seq = max_seq
    for tbl_name in defect_order:
        if tbl_name not in output_dataframes:
            continue
        sdf = output_dataframes[tbl_name]
        total_rows = sdf.count()
        if total_rows == 0:
            continue
        breach_rows = sdf.filter(F.col('breach_flag') == 1).count()
        window_spec = Window.partitionBy(F.lit(1)).orderBy(F.monotonically_increasing_id())
        sdf = sdf.withColumn(
            '_breach_seq',
            F.when(F.col('breach_flag') == 1, F.row_number().over(window_spec))
        ).withColumn(
            'display_defect_id',
            F.when(
                F.col('breach_flag') == 1,
                F.concat(
                    F.lit('D/'),
                    F.coalesce(F.date_format('survey_end_date', 'ddMMyy'), F.date_format('survey_start_date', 'ddMMyy'), F.lit(SURVEY_DATE_STR)),
                    F.lit(f'/{src_letter}/'),
                    (F.col('_breach_seq') + F.lit(running_seq)).cast('string')
                )
            )
        ).drop('_breach_seq')
        output_dataframes[tbl_name] = sdf
        running_seq += breach_rows

    return output_dataframes


def _write_master_outputs(output_dataframes, write_enabled):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MASTER_NAMESPACE}")

    summary_rows = []
    for table_name in TARGET_TABLES:
        full_name = f"{MASTER_NAMESPACE}.{table_name}"
        write_df = output_dataframes.get(table_name, empty_with_schema(TABLE_SCHEMAS[table_name]))
        row_count = write_df.count()
        breach_count = write_df.filter(F.col('breach_flag') == 1).count() if 'breach_flag' in write_df.columns else None

        if write_enabled:
            table_exists = spark.catalog.tableExists(full_name)
            if table_exists:
                (
                    write_df.write
                    .format('delta')
                    .mode('overwrite')
                    .option('replaceWhere', f"upc_code = '{UPC}' AND phase = '{PHASE}' AND cycle = '{CYCLE}'")
                    .saveAsTable(full_name)
                )
                action = 'updated'
            else:
                (
                    write_df.write
                    .format('delta')
                    .mode('overwrite')
                    .option('overwriteSchema', 'true')
                    .saveAsTable(full_name)
                )
                action = 'created'
        else:
            action = 'preview'

        summary_rows.append((table_name, row_count, breach_count, action))

    summary_df = spark.createDataFrame(
        summary_rows,
        ['table_name', 'row_count', 'breach_rows', 'action']
    ).orderBy('table_name')

    display(summary_df)
    return summary_df


def _run_child_notebook(upc_cfg, write_enabled):
    args = {
        'ZONE': str(upc_cfg['ZONE']),
        'PHASE': str(upc_cfg['PHASE']),
        'CYCLE': str(upc_cfg['CYCLE']),
        'UPC': str(upc_cfg['UPC']),
        'MASTER_RUN_MODE': 'single_upc_write',
        'MASTER_WRITE_ENABLED': '1' if write_enabled else '0',
    }
    raw_result = dbutils.notebook.run(THIS_NOTEBOOK_PATH, NOTEBOOK_RUN_TIMEOUT_SECONDS, args)
    try:
        return json.loads(raw_result)
    except Exception:
        return {
            'status': 'unknown',
            'mode': 'single_upc_write',
            'write_enabled': write_enabled,
            'upc': upc_cfg['UPC'],
            'raw_result': raw_result,
        }


if MASTER_RUN_MODE == 'single_upc_write':
    master_outputs = _build_unfiltered_master_outputs()
    master_outputs = _assign_master_display_ids(master_outputs)
    summary_df = _write_master_outputs(master_outputs, MASTER_WRITE_ENABLED)

    summary_rows = [row.asDict() for row in summary_df.collect()]
    totals = {
        row['table_name']: {
            'row_count': row['row_count'],
            'breach_rows': row['breach_rows'],
            'action': row['action'],
        }
        for row in summary_rows
    }

    dbutils.notebook.exit(json.dumps({
        'status': 'ok',
        'mode': MASTER_RUN_MODE,
        'write_enabled': MASTER_WRITE_ENABLED,
        'zone': str(ZONE),
        'phase': str(PHASE),
        'cycle': str(CYCLE),
        'upc': UPC,
        'target_namespace': MASTER_NAMESPACE,
        'table_summary': totals,
    }))

elif RUN_MASTER_UPC_BATCH:
    from concurrent.futures import ThreadPoolExecutor, as_completed

    batch_items = [cfg for cfg in ORCHESTRATOR_UPCS if cfg.get('UPC')]
    print(f"\nRunning master NSV batch for {len(batch_items)} UPC(s) \u2192 {MASTER_NAMESPACE}  (parallelism={PARALLEL_WORKERS})")
    print("=" * 70)

    batch_results = []
    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as pool:
        futures = {
            pool.submit(_run_child_notebook, cfg, True): cfg
            for cfg in batch_items
        }
        for i, future in enumerate(as_completed(futures), 1):
            cfg = futures[future]
            try:
                result = future.result()
                result['_upc'] = cfg['UPC']
                batch_results.append(result)
                status = result.get('status', 'unknown')
                rows_total = sum(v.get('row_count', 0) for v in result.get('table_summary', {}).values())
                print(f"  [{i}/{len(batch_items)}] \u2713 {cfg['UPC']} \u2014 {status} ({rows_total:,} rows)")
            except Exception as exc:
                batch_results.append({'_upc': cfg['UPC'], 'status': 'error', 'error': str(exc)})
                print(f"  [{i}/{len(batch_items)}] \u2717 {cfg['UPC']} \u2014 FAILED: {exc}")

    print(f"\n{'=' * 70}")
    ok_count = sum(1 for r in batch_results if r.get('status') == 'ok')
    fail_count = len(batch_results) - ok_count
    print(f"Batch complete: {ok_count} succeeded, {fail_count} failed out of {len(batch_results)}")
    display(spark.createDataFrame(pd.DataFrame(batch_results)))

elif PREVIEW_CURRENT_UPC and CURRENT_UPC:
    preview_cfg = {
        'ZONE': CURRENT_ZONE,
        'PHASE': CURRENT_PHASE,
        'CYCLE': str(CURRENT_CYCLE),
        'UPC': CURRENT_UPC,
    }
    print(
        f"Previewing full-data master build for {CURRENT_UPC} "
        f"(Zone={CURRENT_ZONE}, Phase={CURRENT_PHASE}, Cycle={CURRENT_CYCLE})"
    )
    preview_result = _run_child_notebook(preview_cfg, write_enabled=False)
    print(json.dumps(preview_result, indent=2))

else:
    print('Configure ORCHESTRATOR_UPCS, then set RUN_MASTER_UPC_BATCH=True to write master NSV tables.')
    print(f'Current target namespace: {MASTER_NAMESPACE}')

# COMMAND ----------

# DBTITLE 1,Performance Analysis — Pipeline Bottlenecks
# MAGIC %md
# MAGIC ## Performance Analysis — Pipeline Bottlenecks
# MAGIC
# MAGIC **Execution timeline (last run):**
# MAGIC
# MAGIC | Cell | Step | Duration | Notes |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 2 | Config/Imports | \~5s | Fine |
# MAGIC | 4 | Load Pothole | \~21s | D4 parquet reads |
# MAGIC | 5 | Load Defect Sources | \~17s | Backup parquets |
# MAGIC | 10 | Load Workbook Schemas | \~32s | Reads Excel workbook every run |
# MAGIC | **13** | **Map to Schema (88 cols × 5 defects)** | **\~2.5 min** | **BIGGEST BOTTLENECK** |
# MAGIC | 14 | Validate Dates | \~26s | Multiple .count() actions |
# MAGIC | 15 | PCI Calculation | \~15s | OK |
# MAGIC | 17 | Write Delta Tables | \~51s | 11 tables, replaceWhere |
# MAGIC
# MAGIC **Root causes in Cell 13 (\~77K chars, 2.5 min):**
# MAGIC
# MAGIC 1. **`Window.orderBy(monotonically_increasing_id())` — CRITICAL**: Appears multiple times. Spark warns: *"Moving all data to a single partition"*. This forces ALL rows to one executor for row numbering, killing parallelism. **FIX: Assign sequential IDs in pandas before `spark.createDataFrame()`.** 
# MAGIC 2. **Multiple pandas→Spark round-trips**: Each defect type does `spark.createDataFrame(df.copy())` — 5 conversions. Consolidate where possible.
# MAGIC 3. **88-column `select()` per defect**: Each defect builds 88 column expressions with type casts, `to_json(struct(...))`, etc. These are lazy but the plan compilation is slow.
# MAGIC 4. **Multiple `.count()` in Cell 14**: Each `.count()` triggers a separate Spark action.
# MAGIC
# MAGIC **Other optimizations:**
# MAGIC * Cell 10: Cache the Excel workbook schema as a JSON file instead of reading `.xlsx` every run
# MAGIC * Cell 17: Use `mergeSchema` option to avoid full table scans during write
# MAGIC * Cells 19-26: Validation/analysis cells — skip during child notebook runs

# COMMAND ----------

# DBTITLE 1,Manual Run — 2 UPCs Ingest to Master NSV Data
# ───────────────────────────────────────────────────────────────────────────────
# MANUAL RUN: Ingest 2 UPCs from silver_defect_data into master_nsv_data
# Edit the UPC list below, then run this cell.
# Runs the child notebook sequentially for each UPC.
# ───────────────────────────────────────────────────────────────────────────────

import json
import pandas as pd

# Guard: skip this cell when running as a child notebook (single_upc_write mode)
try:
    _run_mode = dbutils.widgets.get("MASTER_RUN_MODE").strip().lower()
except Exception:
    _run_mode = "interactive"

if _run_mode == "single_upc_write":
    print("\u26a0\ufe0f  Skipping manual-run cell (child notebook mode)")
    dbutils.notebook.exit("skipped_manual_cell")

# ── EDIT THIS LIST ── Add/remove UPCs as needed ────────────────────────────
MANUAL_UPCS = [
    {"UPC": "N_07024_01001_GJ", "ZONE": "B", "PHASE": "2", "CYCLE": "1"},
    {"UPC": "N_02001_10003_GJ", "ZONE": "B", "PHASE": "2", "CYCLE": "1"},
]
WRITE_ENABLED = True   # False = preview only, True = actually write to Delta
# ──────────────────────────────────────────────────────────────────────────────

NOTEBOOK_PATH = "/Shared/Medallain Arc/Silver Layer Delta Tables Pipeline Clone Update_5"
MASTER_NAMESPACE = "nhai_dhms.master_nsv_data"
TIMEOUT_SECONDS = 14400

print(f"{'='*70}")
print(f"MANUAL RUN — {len(MANUAL_UPCS)} UPC(s) → {MASTER_NAMESPACE}")
print(f"Write: {'ENABLED' if WRITE_ENABLED else 'PREVIEW ONLY'}")
print(f"{'='*70}")
for i, cfg in enumerate(MANUAL_UPCS, 1):
    print(f"  [{i}] {cfg['UPC']}  Zone={cfg['ZONE']}  Phase={cfg['PHASE']}  Cycle={cfg['CYCLE']}")
print()

all_results = []

for i, cfg in enumerate(MANUAL_UPCS, 1):
    upc = cfg["UPC"]
    print(f"\n{'='*70}")
    print(f"[{i}/{len(MANUAL_UPCS)}] Running: {upc}  (Zone={cfg['ZONE']}, Phase={cfg['PHASE']}, Cycle={cfg['CYCLE']})")
    print(f"{'='*70}")

    args = {
        "ZONE":                 cfg["ZONE"],
        "PHASE":                cfg["PHASE"],
        "CYCLE":                cfg["CYCLE"],
        "UPC":                  cfg["UPC"],
        "MASTER_RUN_MODE":      "single_upc_write",
        "MASTER_WRITE_ENABLED": "1" if WRITE_ENABLED else "0",
    }

    try:
        raw_result = dbutils.notebook.run(NOTEBOOK_PATH, TIMEOUT_SECONDS, args)
        try:
            result = json.loads(raw_result)
        except Exception:
            result = {"status": "unknown", "raw_result": raw_result}

        status = result.get("status", "unknown")
        table_summary = result.get("table_summary", {})
        total_rows = sum(v.get("row_count", 0) for v in table_summary.values())
        print(f"\n  \u2705 {upc} — {status} ({total_rows:,} rows)")

        all_results.append({
            "upc": upc, "zone": cfg["ZONE"], "phase": cfg["PHASE"], "cycle": cfg["CYCLE"],
            "status": status, "total_rows": total_rows,
        })

    except Exception as e:
        print(f"\n  \u274c {upc} — FAILED: {e}")
        all_results.append({
            "upc": upc, "zone": cfg["ZONE"], "phase": cfg["PHASE"], "cycle": cfg["CYCLE"],
            "status": "FAILED", "total_rows": 0,
        })

# Final summary
print(f"\n{'='*70}")
print("OVERALL SUMMARY")
print(f"{'='*70}")
summary_df = pd.DataFrame(all_results)
display(summary_df)
ok = sum(1 for r in all_results if r["status"] == "ok")
failed = len(all_results) - ok
print(f"\n\u2705 {ok} succeeded, \u274c {failed} failed out of {len(all_results)}")

# COMMAND ----------

