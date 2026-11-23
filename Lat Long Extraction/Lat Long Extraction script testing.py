# Databricks notebook source
# MAGIC %pip install openpyxl

# COMMAND ----------

# DBTITLE 1,UPC Processing Configuration
# ══════════════════════════════════════════════════════════════════════
# UPC PROCESSING CONFIGURATION
# ══════════════════════════════════════════════════════════════════════
# Change ONLY these two values to control how many UPCs are processed.
#
# OPTION 1 — Process a specific number of UPCs:
#   RUN_ALL = False
#   NUMBER_OF_UPCS = 5
#
# OPTION 2 — Process ALL available UPCs:
#   RUN_ALL = True
#   NUMBER_OF_UPCS = None
# ══════════════════════════════════════════════════════════════════════

RUN_ALL = False
NUMBER_OF_UPCS = 20


# ── Automatic UPC Discovery & Selection ──────────────────────────────────────
# Uses the same directory structure as the existing pipeline:
#   Zone= / Phase= / Cycle= / UPC=
import os

_NSV_BACKUP_PATH = os.path.join(
    "/Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output",
    "Source=NSV_Backup",
)

def _discover_all_upc_names(base_path):
    """Lightweight enumeration of unique UPC names from folder structure."""
    upc_names = set()
    if not os.path.isdir(base_path):
        print(f"ERROR: Base path does not exist: {base_path}")
        return sorted(upc_names)
    for zone_entry in os.listdir(base_path):
        zone_path = os.path.join(base_path, zone_entry)
        if not os.path.isdir(zone_path) or not zone_entry.startswith("Zone="):
            continue
        for phase_entry in os.listdir(zone_path):
            phase_path = os.path.join(zone_path, phase_entry)
            if not os.path.isdir(phase_path) or not phase_entry.startswith("Phase="):
                continue
            for cycle_entry in os.listdir(phase_path):
                cycle_path = os.path.join(phase_path, cycle_entry)
                if not os.path.isdir(cycle_path) or not cycle_entry.startswith("Cycle="):
                    continue
                for upc_entry in os.listdir(cycle_path):
                    upc_path = os.path.join(cycle_path, upc_entry)
                    if os.path.isdir(upc_path) and upc_entry.startswith("UPC="):
                        upc_names.add(upc_entry.split("=", 1)[1])
    return sorted(upc_names)


# 1. Discover all unique UPC names
_all_upc_names = _discover_all_upc_names(_NSV_BACKUP_PATH)
_total_available = len(_all_upc_names)

# 2. Validate configuration & select
if RUN_ALL:
    _selected_upc_names = set(_all_upc_names)
    _mode = "ALL UPCs"
    _requested = _total_available
else:
    if NUMBER_OF_UPCS is None or NUMBER_OF_UPCS <= 0:
        raise ValueError(
            "NUMBER_OF_UPCS must be greater than 0 when RUN_ALL is False."
        )
    _requested = NUMBER_OF_UPCS
    if NUMBER_OF_UPCS > _total_available:
        print(
            f"WARNING: Requested {NUMBER_OF_UPCS} UPCs, but only "
            f"{_total_available} are available. Processing will use "
            f"the {_total_available} available UPCs."
        )
        _selected_upc_names = set(_all_upc_names)
    else:
        _selected_upc_names = set(_all_upc_names[:NUMBER_OF_UPCS])
    _mode = "Selected Number of UPCs"

# 3. Display selection summary
print("=" * 70)
print("UPC PROCESSING CONFIGURATION")
print("=" * 70)
print(f"Processing Mode   : {_mode}")
if not RUN_ALL:
    print(f"Requested UPCs    : {_requested}")
print(f"Available UPCs    : {_total_available}")
print(f"Selected UPCs     : {len(_selected_upc_names)}")
print("=" * 70)

print(f"\nSelected UPC names ({len(_selected_upc_names)}):")
for i, upc_name in enumerate(sorted(_selected_upc_names), 1):
    print(f"  {i:>4d}. {upc_name}")
    if i >= 25 and len(_selected_upc_names) > 25:
        print(f"       ... and {len(_selected_upc_names) - 25} more")
        break

# COMMAND ----------

# DBTITLE 1,Step 1: Imports, Configuration & Validation Thresholds
"""
L1 GPS Coordinate Extraction — Production Pipeline with Validation
Author : Shivam Raj | v2.0 | 2026-09-24

Extracts Start/End Chainage, Lat/Long, and NH Number from L1 GPS Parquet
files with comprehensive validation, anomaly detection, and reconciliation.
"""

import os
import re
import math
import uuid
import traceback
from datetime import datetime
from collections import defaultdict, Counter

import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from openpyxl.styles import PatternFill

# ── FOLDER PATHS ─────────────────────────────────────────────────────────────
BRONZE_BASE_PATH = "/Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output"
NSV_BACKUP_PATH = os.path.join(BRONZE_BASE_PATH, "Source=NSV_Backup")
OUTPUT_DIR = "/Workspace/Users/shivam.raj9226@softsensor.ai/Lat Long Extraction"
OUTPUT_EXCEL_PATH = os.path.join(OUTPUT_DIR, "Final_Lat_Long_Output.xlsx")
ANOMALY_LOG_PATH = os.path.join(OUTPUT_DIR, "Anomaly_Log.xlsx")
PROJECT_NAME_REF_PATH = "/Workspace/Users/shivam.raj9226@softsensor.ai/Lat Long Extraction/20260519_Consolidated Master_Ver7.xlsx"

# ── PROCESSING PARAMETERS ────────────────────────────────────────────────────
MAX_GPS_SEARCH_DEPTH = 10
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]

# ── OUTPUT COLUMNS ───────────────────────────────────────────────────────────
BASE_OUTPUT_COLUMNS = [
    "UPC", "Zone", "Phase", "Cycle", "Lane", "File Name", "NH Number",
    "Start Chainage", "End Chainage", "Start Lat", "Start Long",
    "End Lat", "End Long", "Active Flag",
]
GPS_COLUMNS = ["Start Lat", "Start Long", "End Lat", "End Long"]

FINAL_OUTPUT_COLUMNS = [
    "S.No", "Zone", "Phase", "Cycle", "UPC", "Unnamed: 5", "Survey_UPC",
    "Project Name", "Start Chainage", "End Chainage",
    "Start Lat", "Start Long", "End Lat", "End Long", "Lane",
]
ASPHALT_KEYWORD = "asphalt"
CC_KEYWORD = "cement concrete"

# ── VALIDATION THRESHOLDS (configurable) ─────────────────────────────────────
VALIDATION_CONFIG = {
    # India bounding box for GPS
    "lat_min": 6.0, "lat_max": 38.0,
    "lon_min": 68.0, "lon_max": 98.0,
    # Chainage
    "max_chainage_gap_meters": 5000,
    "min_chainage_increment_meters": 1,
    # GPS movement
    "max_gps_jump_km": 10.0,
    "min_gps_movement_km_per_km_chainage": 0.001,
    "max_gps_movement_km_per_km_chainage": 50.0,
    # Duplicate thresholds
    "max_identical_gps_consecutive": 100,
    # Chainage-GPS consistency
    "chainage_gps_ratio_min": 0.01,
    "chainage_gps_ratio_max": 100.0,
}

# ── EXPECTED PARTITION PATTERNS ──────────────────────────────────────────────
EXPECTED_PARTITIONS = {
    "zone_prefix": "Zone=",
    "phase_prefix": "Phase=",
    "cycle_prefix": "Cycle=",
    "upc_prefix": "UPC=",
    "year_prefix": "year=",
    "month_prefix": "month=",
    "day_prefix": "day=",
    "valid_phase_values": {"1", "2", "3"},
    "valid_cycle_values": {"1", "2", "3"},
}

print("=" * 70)
print(f"PIPELINE CONFIGURATION LOADED — Run ID: {RUN_ID}")
print("=" * 70)
print(f"NSV Backup Path   : {NSV_BACKUP_PATH}")
print(f"Output Directory  : {OUTPUT_DIR}")
print(f"Output Excel      : {OUTPUT_EXCEL_PATH}")
print(f"Anomaly Log       : {ANOMALY_LOG_PATH}")
print(f"Path exists       : {os.path.isdir(NSV_BACKUP_PATH)}")
print(f"GPS range (India) : Lat [{VALIDATION_CONFIG['lat_min']},{VALIDATION_CONFIG['lat_max']}] "
      f"Lon [{VALIDATION_CONFIG['lon_min']},{VALIDATION_CONFIG['lon_max']}]")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Step 2: AnomalyLogger Class
class AnomalyLogger:
    """Structured anomaly logging framework.
    
    Every anomaly is recorded with standardized fields so the log can
    be filtered by zone, UPC, severity, category, or source file.
    """

    CATEGORIES = {
        "FILE_ERROR", "SCHEMA_ERROR", "METADATA_ERROR", "NULL_VALUE",
        "INVALID_VALUE", "DUPLICATE", "CHAINAGE_ERROR", "GPS_ERROR",
        "GPS_CHAINAGE_MISMATCH", "CONTINUITY_ERROR", "JOIN_ERROR",
        "BUSINESS_RULE_ERROR", "OUTPUT_VALIDATION_ERROR",
    }
    SEVERITIES = {"CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"}

    def __init__(self, run_id):
        self.run_id = run_id
        self.records = []
        self._counters = Counter()

    def log(self, *, category, severity, description,
            source_file=None, source_path=None, zone=None, upc=None,
            phase=None, cycle=None, lane=None, record_id=None,
            row_number=None, column_name=None, previous_value=None,
            current_value=None, next_value=None, expected_value=None,
            actual_value=None, chainage_start=None, chainage_end=None,
            latitude=None, longitude=None, previous_latitude=None,
            previous_longitude=None, distance_meters=None,
            chainage_difference=None, recommended_action=None,
            processing_status="FLAGGED"):
        """Record a single anomaly with full context."""
        self.records.append({
            "run_id": self.run_id,
            "timestamp": datetime.now().isoformat(),
            "source_file": source_file,
            "source_path": source_path,
            "zone": zone, "upc": upc, "phase": phase,
            "cycle": cycle, "lane": lane,
            "record_id": record_id, "row_number": row_number,
            "anomaly_type": f"{category}:{severity}",
            "anomaly_category": category,
            "severity": severity,
            "column_name": column_name,
            "previous_value": str(previous_value) if previous_value is not None else None,
            "current_value": str(current_value) if current_value is not None else None,
            "next_value": str(next_value) if next_value is not None else None,
            "expected_value": str(expected_value) if expected_value is not None else None,
            "actual_value": str(actual_value) if actual_value is not None else None,
            "chainage_start": chainage_start, "chainage_end": chainage_end,
            "latitude": latitude, "longitude": longitude,
            "previous_latitude": previous_latitude,
            "previous_longitude": previous_longitude,
            "distance_meters": distance_meters,
            "chainage_difference": chainage_difference,
            "description": description,
            "recommended_action": recommended_action,
            "processing_status": processing_status,
        })
        self._counters[(category, severity)] += 1

    def to_dataframe(self):
        return pd.DataFrame(self.records) if self.records else pd.DataFrame()

    def summary(self):
        """Return summary counts by category and severity."""
        df = self.to_dataframe()
        if df.empty:
            return {"total": 0}
        return {
            "total": len(df),
            "by_severity": df["severity"].value_counts().to_dict(),
            "by_category": df["anomaly_category"].value_counts().to_dict(),
        }

    def print_summary(self):
        s = self.summary()
        print(f"\nTotal anomalies: {s['total']}")
        if s["total"] > 0:
            print("By severity:")
            for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"]:
                cnt = s["by_severity"].get(sev, 0)
                if cnt: print(f"  {sev:<10s}: {cnt}")
            print("By category:")
            for cat, cnt in sorted(s["by_category"].items(), key=lambda x: -x[1]):
                print(f"  {cat:<30s}: {cnt}")


# ── INITIALIZE GLOBAL LOGGER ─────────────────────────────────────────────────
alog = AnomalyLogger(RUN_ID)
print(f"AnomalyLogger initialized for run {RUN_ID}")
print(f"Categories: {len(AnomalyLogger.CATEGORIES)} | Severities: {len(AnomalyLogger.SEVERITIES)}")

# COMMAND ----------

# DBTITLE 1,Step 3: Helper Functions & Utilities
# ── CORE HELPERS (preserved from original) ───────────────────────────────────

def get_subdirectories(directory_path):
    """Return all immediate subdirectories, sorted."""
    if not os.path.isdir(directory_path):
        return []
    return sorted(
        os.path.join(directory_path, d)
        for d in os.listdir(directory_path)
        if os.path.isdir(os.path.join(directory_path, d))
    )


def find_matching_column(column_names, *candidate_names):
    """Find a source column using normalized candidate names."""
    normalized_columns = {
        re.sub(r"[^a-z0-9]", "", str(c).lower()): c
        for c in column_names
    }
    for candidate in candidate_names:
        normalized = re.sub(r"[^a-z0-9]", "", candidate.lower())
        if normalized in normalized_columns:
            return normalized_columns[normalized]
    return None


def get_safe_value(data_record, column_name):
    """Return value from a row; missing/null → 'Not Present in Source Parquet'."""
    if column_name is None:
        return "Not Present in Source Parquet"
    value = data_record.get(column_name)
    if pd.isna(value):
        return "Not Present in Source Parquet"
    return value


# ── NEW VALIDATION HELPERS ───────────────────────────────────────────────────

def haversine_km(lat1, lon1, lat2, lon2):
    """Haversine distance between two GPS points in kilometres."""
    try:
        R = 6371.0
        rlat1, rlon1, rlat2, rlon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        dlat = rlat2 - rlat1
        dlon = rlon2 - rlon1
        a = math.sin(dlat/2)**2 + math.cos(rlat1)*math.cos(rlat2)*math.sin(dlon/2)**2
        return R * 2 * math.asin(math.sqrt(a))
    except (ValueError, TypeError):
        return None


def safe_to_numeric(series, col_name=""):
    """Convert series to numeric, returning (converted_series, error_count)."""
    result = pd.to_numeric(series, errors="coerce")
    error_count = result.isna().sum() - series.isna().sum()
    return result, max(0, error_count)


def is_valid_india_coordinate(lat, lon):
    """Check if coordinates fall within India bounding box."""
    cfg = VALIDATION_CONFIG
    try:
        return (cfg["lat_min"] <= float(lat) <= cfg["lat_max"] and
                cfg["lon_min"] <= float(lon) <= cfg["lon_max"])
    except (TypeError, ValueError):
        return False


def extract_partition_value(dir_name, prefix):
    """Safely extract partition value from directory name."""
    if dir_name.startswith(prefix):
        return dir_name.split("=", 1)[1]
    return None


print("Helpers defined: get_subdirectories, find_matching_column, get_safe_value,")
print("  haversine_km, safe_to_numeric, is_valid_india_coordinate, extract_partition_value")

# COMMAND ----------

# DBTITLE 1,Step 4: Directory Inspection & Partition Metadata Validation
def inspect_directory_structure(base_path):
    """Recursively inspect the full directory tree, logging every anomaly."""
    inventory = {"zones": [], "total_upcs": 0, "total_files": 0,
                 "empty_dirs": 0, "stray_files": [], "malformed_partitions": []}
    if not os.path.isdir(base_path):
        alog.log(category="FILE_ERROR", severity="CRITICAL",
                 description=f"Base path does not exist: {base_path}",
                 source_path=base_path, recommended_action="Verify path")
        return inventory

    for entry in sorted(os.listdir(base_path)):
        entry_path = os.path.join(base_path, entry)
        if os.path.isfile(entry_path):
            inventory["stray_files"].append(entry_path)
            alog.log(category="METADATA_ERROR", severity="LOW",
                     description=f"Stray file at zone level: {entry}",
                     source_file=entry, source_path=base_path)
            continue
        if not entry.startswith("Zone="):
            alog.log(category="METADATA_ERROR", severity="MEDIUM",
                     description=f"Non-Zone directory at top level: {entry}",
                     source_path=entry_path)
            continue
        zone_name = entry.split("=", 1)[1]
        inventory["zones"].append(zone_name)

        for phase_entry in sorted(os.listdir(entry_path)):
            phase_path = os.path.join(entry_path, phase_entry)
            if os.path.isfile(phase_path):
                alog.log(category="METADATA_ERROR", severity="LOW",
                         description=f"Stray file in Zone={zone_name}: {phase_entry}",
                         zone=zone_name, source_file=phase_entry)
                continue
            if not phase_entry.startswith("Phase="):
                alog.log(category="METADATA_ERROR", severity="MEDIUM",
                         description=f"Non-Phase directory in Zone={zone_name}: {phase_entry}",
                         zone=zone_name, source_path=phase_path)
                continue
            phase_val = phase_entry.split("=", 1)[1]
            if phase_val not in EXPECTED_PARTITIONS["valid_phase_values"]:
                inventory["malformed_partitions"].append(f"Phase={phase_val}")
                alog.log(category="METADATA_ERROR", severity="HIGH",
                         description=f"Malformed Phase value: '{phase_val}' (expected numeric like 1,2,3)",
                         zone=zone_name, phase=phase_val,
                         expected_value="1, 2, or 3", actual_value=phase_val)

            for cycle_entry in sorted(os.listdir(phase_path)):
                cycle_path = os.path.join(phase_path, cycle_entry)
                if os.path.isfile(cycle_path):
                    alog.log(category="METADATA_ERROR", severity="LOW",
                             description=f"Stray file in Phase={phase_val}: {cycle_entry}",
                             zone=zone_name, phase=phase_val, source_file=cycle_entry)
                    continue
                if not cycle_entry.startswith("Cycle="):
                    alog.log(category="METADATA_ERROR", severity="MEDIUM",
                             description=f"Non-Cycle directory: {cycle_entry}",
                             zone=zone_name, phase=phase_val, source_path=cycle_path)
                    continue
                cycle_val = cycle_entry.split("=", 1)[1]
                if cycle_val not in EXPECTED_PARTITIONS["valid_cycle_values"]:
                    inventory["malformed_partitions"].append(f"Cycle={cycle_val}")
                    alog.log(category="METADATA_ERROR", severity="HIGH",
                             description=f"Malformed Cycle value: '{cycle_val}' (expected numeric)",
                             zone=zone_name, phase=phase_val, cycle=cycle_val,
                             expected_value="1, 2, or 3", actual_value=cycle_val)

                upc_entries = sorted(os.listdir(cycle_path))
                for upc_entry in upc_entries:
                    upc_path = os.path.join(cycle_path, upc_entry)
                    if os.path.isfile(upc_path):
                        alog.log(category="METADATA_ERROR", severity="LOW",
                                 description=f"Stray file in Cycle={cycle_val}: {upc_entry}",
                                 zone=zone_name, phase=phase_val, cycle=cycle_val)
                        continue
                    if not upc_entry.startswith("UPC="):
                        alog.log(category="METADATA_ERROR", severity="MEDIUM",
                                 description=f"Non-UPC directory: {upc_entry}",
                                 zone=zone_name, phase=phase_val, cycle=cycle_val)
                        continue
                    inventory["total_upcs"] += 1
    return inventory


# ── RUN DIRECTORY INSPECTION ────────────────────────────────────────────────
dir_inventory = inspect_directory_structure(NSV_BACKUP_PATH)
print(f"Zones: {dir_inventory['zones']}")
print(f"Total UPC directories: {dir_inventory['total_upcs']}")
print(f"Malformed partitions: {dir_inventory['malformed_partitions']}")
print(f"Stray files: {len(dir_inventory['stray_files'])}")
alog.print_summary()

# COMMAND ----------

# DBTITLE 1,Step 5: UPC Discovery with Validation
def has_partition_data(upc_directory):
    """Check whether a UPC contains at least one valid year/month/day partition."""
    for year_dir in get_subdirectories(upc_directory):
        if not os.path.basename(year_dir).startswith("year="): continue
        for month_dir in get_subdirectories(year_dir):
            if not os.path.basename(month_dir).startswith("month="): continue
            for day_dir in get_subdirectories(month_dir):
                if os.path.basename(day_dir).startswith("day="):
                    return True
    return False


def discover_extracted_upcs(bronze_backup_path):
    """Discover all Zone/Phase/Cycle/UPC combinations with validation."""
    records = []
    if not os.path.isdir(bronze_backup_path):
        alog.log(category="FILE_ERROR", severity="CRITICAL",
                 description=f"Backup path not found: {bronze_backup_path}",
                 source_path=bronze_backup_path)
        return records

    for zone_dir in get_subdirectories(bronze_backup_path):
        zone_dn = os.path.basename(zone_dir)
        if not zone_dn.startswith("Zone="): continue
        zone_name = zone_dn.split("=", 1)[1]
        for phase_dir in get_subdirectories(zone_dir):
            phase_dn = os.path.basename(phase_dir)
            if not phase_dn.startswith("Phase="): continue
            phase_name = phase_dn.split("=", 1)[1]
            for cycle_dir in get_subdirectories(phase_dir):
                cycle_dn = os.path.basename(cycle_dir)
                if not cycle_dn.startswith("Cycle="): continue
                cycle_name = cycle_dn.split("=", 1)[1]
                for upc_dir in get_subdirectories(cycle_dir):
                    upc_dn = os.path.basename(upc_dir)
                    if not upc_dn.startswith("UPC="): continue
                    upc_name = upc_dn.split("=", 1)[1]
                    if not has_partition_data(upc_dir):
                        alog.log(category="METADATA_ERROR", severity="MEDIUM",
                                 description=f"UPC has no valid date partitions",
                                 zone=zone_name, upc=upc_name, phase=phase_name,
                                 cycle=cycle_name, source_path=upc_dir)
                        continue
                    records.append({
                        "UPC": upc_name, "Zone": zone_name,
                        "Phase": phase_name, "Cycle": cycle_name,
                        "upc_path": upc_dir,
                    })
    return records


# ── RUN UPC DISCOVERY ───────────────────────────────────────────────────────
extracted_upc_records = discover_extracted_upcs(NSV_BACKUP_PATH)
upc_df = pd.DataFrame(extracted_upc_records)
print(f"Total UPCs discovered: {len(extracted_upc_records)}")
print(f"Zone distribution: {upc_df['Zone'].value_counts().to_dict() if not upc_df.empty else {}}")
print(f"Phase distribution: {upc_df['Phase'].value_counts().to_dict() if not upc_df.empty else {}}")
print(f"Cycle distribution: {upc_df['Cycle'].value_counts().to_dict() if not upc_df.empty else {}}")

# Check for duplicate UPCs across Phase/Cycle
if not upc_df.empty:
    dup_upcs = upc_df.groupby("UPC").size()
    multi_upcs = dup_upcs[dup_upcs > 1]
    if len(multi_upcs) > 0:
        print(f"\nUPCs appearing in multiple Phase/Cycle combos: {len(multi_upcs)}")
        for upc_name, cnt in multi_upcs.head(5).items():
            print(f"  {upc_name}: {cnt} occurrences")
    display(upc_df.drop(columns=["upc_path"]).head(10))

# ── APPLY UPC SELECTION FROM CONFIGURATION ─────────────────────────────────
if not RUN_ALL:
    extracted_upc_records = [
        r for r in extracted_upc_records if r["UPC"] in _selected_upc_names
    ]
    print(f"\nFiltered to {len(extracted_upc_records)} records "
          f"matching {len(_selected_upc_names)} selected UPCs")

selected_upcs = extracted_upc_records
print(f"\nselected_upcs: {len(selected_upcs)} records ready for processing")

# COMMAND ----------

# DBTITLE 1,Step 6: File Discovery, Read & Schema Validation Functions
def find_latest_date_directory(upc_directory):
    """Find the latest available year/month/day partition for a UPC."""
    latest_partition = None
    for year_dir in get_subdirectories(upc_directory):
        year_name = os.path.basename(year_dir)
        if not year_name.startswith("year="): continue
        for month_dir in get_subdirectories(year_dir):
            month_name = os.path.basename(month_dir)
            if not month_name.startswith("month="): continue
            for day_dir in get_subdirectories(month_dir):
                day_name = os.path.basename(day_dir)
                if not day_name.startswith("day="): continue
                try:
                    dt = (int(year_name.split("=",1)[1]),
                          int(month_name.split("=",1)[1]),
                          int(day_name.split("=",1)[1]))
                except (ValueError, IndexError): continue
                if latest_partition is None or dt > latest_partition[0]:
                    latest_partition = (dt, day_dir)
    return latest_partition[1] if latest_partition else None


def find_l1_gps_parquet_files(day_directory, max_depth=MAX_GPS_SEARCH_DEPTH):
    """Locate L1 GPS/geometry Parquet files using Lane= partition directories.
    Walks the tree from the day partition, tracking Lane= context.
    Returns only GPS files under Lane=L1 (or with no Lane partition).
    [Adopted from working reference script]"""
    l1_files = []
    def scan(current_dir, lane_ctx=None, depth=0):
        if depth > max_depth: return
        try:
            entries = sorted(os.listdir(current_dir))
        except Exception:
            return
        for entry in entries:
            entry_path = os.path.join(current_dir, entry)
            if os.path.isdir(entry_path):
                # Track Lane= partition context as we descend
                if "=" in entry:
                    key, val = entry.split("=", 1)
                    if key.strip().lower() == "lane":
                        scan(entry_path, val, depth + 1)
                        continue
                scan(entry_path, lane_ctx, depth + 1)
                continue
            if not entry.lower().endswith(".parquet"): continue
            fn = entry.lower()
            if "unified_segments" in fn: continue
            if "gps" not in fn and "geometry" not in fn: continue
            # Keep only if Lane context is L1 or no Lane partition exists
            if lane_ctx is not None and lane_ctx.strip().upper() not in ("L1", "1"):
                continue
            l1_files.append(entry_path)
    scan(day_directory)
    return sorted(l1_files)


def read_l1_parquet_safe(parquet_file_path, upc_ctx=None):
    """Read a Parquet file with selective column loading.
    Reads only chainage + GPS + NH Number columns.
    Excludes Lane column — it is dictionary-encoded in some row groups
    and plain string in others, causing ArrowTypeError on merge.
    [Fix adopted from working reference script]
    Returns (DataFrame or None, schema_info dict)."""
    ctx = upc_ctx or {}
    schema_info = {"columns": [], "num_rows": 0, "read_ok": False}
    try:
        pf = pq.ParquetFile(parquet_file_path)
        schema_info["num_rows"] = pf.metadata.num_rows
        all_names = pf.schema_arrow.names
        schema_info["columns"] = [(f.name, str(f.type)) for f in pf.schema_arrow]

        # Select only columns we need — avoids ArrowTypeError from
        # mixed dictionary-encoded columns (e.g. Lane)
        want = []
        for cands in [
            ("Start_Chainage", "StartChainage", "start chainage"),
            ("End_Chainage", "EndChainage", "end chainage"),
            ("Start_Latitude", "StartLatitude", "StartLat", "start latitude", "St Latitude"),
            ("Start_Longitude", "StartLongitude", "StartLong", "start longitude", "St Longitude"),
            ("End_Latitude", "EndLatitude", "EndLat", "end latitude"),
            ("End_Longitude", "EndLongitude", "EndLong", "end longitude"),
            ("NH_Number", "NHNumber", "NH Number", "NH_No", "NHNo"),
        ]:
            matched = find_matching_column(all_names, *cands)
            if matched and matched not in want:
                want.append(matched)

        df = pf.read(columns=want).to_pandas(timestamp_as_object=True) if want \
            else pf.read().to_pandas(timestamp_as_object=True)

        schema_info["read_ok"] = True
        if df.empty:
            alog.log(category="FILE_ERROR", severity="MEDIUM",
                     description=f"Parquet file is empty (0 rows)",
                     source_file=os.path.basename(parquet_file_path),
                     source_path=parquet_file_path, **ctx)
        return df, schema_info
    except Exception as e:
        alog.log(category="FILE_ERROR", severity="CRITICAL",
                 description=f"Failed to read Parquet: {type(e).__name__}: {str(e)[:200]}",
                 source_file=os.path.basename(parquet_file_path),
                 source_path=parquet_file_path, **ctx,
                 recommended_action="Inspect file manually; may be corrupt")
        return None, schema_info


def validate_file_schema(df, file_path, upc_ctx=None):
    """Validate schema against expected columns for the existing business logic.
    Returns dict of identified columns."""
    ctx = upc_ctx or {}
    fname = os.path.basename(file_path)
    col_map = identify_l1_columns(df)

    # Check for required columns
    if col_map["start_chainage"] is None:
        alog.log(category="SCHEMA_ERROR", severity="HIGH",
                 description=f"Missing Start Chainage column",
                 source_file=fname, column_name="Start_Chainage", **ctx)
    if col_map["end_chainage"] is None:
        alog.log(category="SCHEMA_ERROR", severity="HIGH",
                 description=f"Missing End Chainage column",
                 source_file=fname, column_name="End_Chainage", **ctx)

    # Log missing optional columns at INFO level
    for key, label in [("start_latitude", "Start Latitude"), ("start_longitude", "Start Longitude"),
                        ("end_latitude", "End Latitude"), ("end_longitude", "End Longitude")]:
        if col_map[key] is None:
            alog.log(category="SCHEMA_ERROR", severity="MEDIUM",
                     description=f"Missing GPS column: {label}",
                     source_file=fname, column_name=label, **ctx)
    if col_map["nh_number"] is None:
        alog.log(category="SCHEMA_ERROR", severity="LOW",
                 description=f"Missing NH Number column",
                 source_file=fname, column_name="NH_Number", **ctx)
    if col_map["lane"] is None:
        alog.log(category="SCHEMA_ERROR", severity="LOW",
                 description=f"Missing Lane column",
                 source_file=fname, column_name="Lane", **ctx)

    # Check for __UNNAMED__ columns (sign of corrupted/shifted schema)
    unnamed_cols = [c for c in df.columns if str(c).startswith("__UNNAMED__")]
    if unnamed_cols:
        alog.log(category="SCHEMA_ERROR", severity="INFO",
                 description=f"File has {len(unnamed_cols)} unnamed columns (possible schema shift)",
                 source_file=fname, actual_value=str(unnamed_cols[:5]), **ctx)
    return col_map


print("File discovery & schema validation functions defined.")

# COMMAND ----------

# DBTITLE 1,Step 7: Data Validation Functions (Chainage, GPS, Consistency, Duplicates)
def identify_l1_columns(gps_data):
    """Identify chainage, coordinate and NH Number fields in the source Parquet.
    [PRESERVED from original business logic]"""
    cols = gps_data.columns
    return {
        "start_chainage": find_matching_column(cols, "Start_Chainage", "StartChainage", "start chainage"),
        "end_chainage": find_matching_column(cols, "End_Chainage", "EndChainage", "end chainage"),
        "start_latitude": find_matching_column(cols, "Start_Latitude", "StartLatitude", "StartLat", "start latitude", "St Latitude"),
        "start_longitude": find_matching_column(cols, "Start_Longitude", "StartLongitude", "StartLong", "start longitude", "St Longitude"),
        "end_latitude": find_matching_column(cols, "End_Latitude", "EndLatitude", "EndLat", "end latitude"),
        "end_longitude": find_matching_column(cols, "End_Longitude", "EndLongitude", "EndLong", "end longitude"),
        "nh_number": find_matching_column(cols, "NH_Number", "NHNumber", "NH Number", "NH_No", "NHNo", "National Highway Number"),
        "lane": find_matching_column(cols, "Lane", "Lane_Number", "LaneNumber"),
    }


def validate_chainage(df, col_map, file_path, upc_ctx):
    """Validate chainage integrity: nulls, types, ordering, gaps, overlaps, duplicates."""
    fname = os.path.basename(file_path)
    c_sc, c_ec = col_map["start_chainage"], col_map["end_chainage"]
    if c_sc is None or c_ec is None:
        return  # Already logged by schema validation

    sc, sc_errors = safe_to_numeric(df[c_sc], c_sc)
    ec, ec_errors = safe_to_numeric(df[c_ec], c_ec)
    if sc_errors > 0:
        alog.log(category="INVALID_VALUE", severity="HIGH",
                 description=f"{sc_errors} non-numeric Start Chainage values",
                 source_file=fname, column_name=c_sc, actual_value=sc_errors, **upc_ctx)
    if ec_errors > 0:
        alog.log(category="INVALID_VALUE", severity="HIGH",
                 description=f"{ec_errors} non-numeric End Chainage values",
                 source_file=fname, column_name=c_ec, actual_value=ec_errors, **upc_ctx)

    # Null chainage
    sc_nulls = sc.isna().sum()
    ec_nulls = ec.isna().sum()
    if sc_nulls > 0:
        alog.log(category="NULL_VALUE", severity="HIGH",
                 description=f"{sc_nulls} null Start Chainage values",
                 source_file=fname, column_name=c_sc, **upc_ctx)
    if ec_nulls > 0:
        alog.log(category="NULL_VALUE", severity="HIGH",
                 description=f"{ec_nulls} null End Chainage values",
                 source_file=fname, column_name=c_ec, **upc_ctx)

    valid = df.dropna(subset=[c_sc, c_ec]).copy() if c_sc in df.columns else pd.DataFrame()
    if valid.empty: return
    valid["_sc"] = pd.to_numeric(valid[c_sc], errors="coerce")
    valid["_ec"] = pd.to_numeric(valid[c_ec], errors="coerce")
    valid = valid.dropna(subset=["_sc", "_ec"])
    if valid.empty: return

    # Negative chainage
    neg_sc = (valid["_sc"] < 0).sum()
    neg_ec = (valid["_ec"] < 0).sum()
    if neg_sc > 0:
        alog.log(category="CHAINAGE_ERROR", severity="HIGH",
                 description=f"{neg_sc} negative Start Chainage values",
                 source_file=fname, column_name=c_sc, **upc_ctx)

    # Start >= End within a row
    bad_order = (valid["_sc"] >= valid["_ec"]).sum()
    if bad_order > 0:
        alog.log(category="CHAINAGE_ERROR", severity="HIGH",
                 description=f"{bad_order} rows where Start Chainage >= End Chainage",
                 source_file=fname, **upc_ctx)

    # Monotonic check (expected: increasing)
    if not valid["_sc"].is_monotonic_increasing:
        alog.log(category="CHAINAGE_ERROR", severity="MEDIUM",
                 description="Start Chainage is NOT monotonically increasing",
                 source_file=fname, **upc_ctx)

    # Duplicate chainage
    dup_sc = valid["_sc"].duplicated().sum()
    if dup_sc > 0:
        alog.log(category="DUPLICATE", severity="MEDIUM",
                 description=f"{dup_sc} duplicate Start Chainage values",
                 source_file=fname, column_name=c_sc, **upc_ctx)

    # Gaps and overlaps between consecutive rows
    sorted_v = valid.sort_values("_sc")
    prev_ec = sorted_v["_ec"].shift(1)
    curr_sc = sorted_v["_sc"]
    gaps = curr_sc - prev_ec
    gaps = gaps.dropna()
    large_gaps = gaps[gaps > VALIDATION_CONFIG["max_chainage_gap_meters"]]
    if len(large_gaps) > 0:
        alog.log(category="CONTINUITY_ERROR", severity="HIGH",
                 description=f"{len(large_gaps)} chainage continuity gaps > {VALIDATION_CONFIG['max_chainage_gap_meters']}m",
                 source_file=fname, actual_value=f"max gap={large_gaps.max():.0f}m", **upc_ctx)
    overlaps = gaps[gaps < 0]
    if len(overlaps) > 0:
        alog.log(category="CONTINUITY_ERROR", severity="MEDIUM",
                 description=f"{len(overlaps)} overlapping chainage segments",
                 source_file=fname, actual_value=f"max overlap={abs(overlaps.min()):.0f}m", **upc_ctx)


def validate_gps(df, col_map, file_path, upc_ctx):
    """Validate GPS coordinates: nulls, range, zero, repeated, jumps."""
    fname = os.path.basename(file_path)
    cfg = VALIDATION_CONFIG
    gps_pairs = [
        ("start_latitude", "start_longitude", "Start"),
        ("end_latitude", "end_longitude", "End"),
    ]
    for lat_key, lon_key, prefix in gps_pairs:
        lat_col, lon_col = col_map.get(lat_key), col_map.get(lon_key)
        if lat_col is None or lon_col is None:
            continue
        lat_s, lat_errs = safe_to_numeric(df[lat_col])
        lon_s, lon_errs = safe_to_numeric(df[lon_col])
        if lat_errs > 0:
            alog.log(category="INVALID_VALUE", severity="HIGH",
                     description=f"{lat_errs} non-numeric {prefix} Latitude values",
                     source_file=fname, column_name=lat_col, **upc_ctx)
        # Null GPS
        lat_nulls = lat_s.isna().sum()
        if lat_nulls > 0:
            alog.log(category="NULL_VALUE", severity="HIGH",
                     description=f"{lat_nulls} null {prefix} Latitude values",
                     source_file=fname, column_name=lat_col, **upc_ctx)
        # Zero coordinates
        zero_mask = (lat_s == 0) & (lon_s == 0)
        if zero_mask.sum() > 0:
            alog.log(category="GPS_ERROR", severity="HIGH",
                     description=f"{zero_mask.sum()} rows with zero/zero {prefix} coordinates",
                     source_file=fname, **upc_ctx)
        # Out of India range
        valid_mask = lat_s.notna() & lon_s.notna()
        if valid_mask.sum() > 0:
            out_range = ((lat_s < cfg["lat_min"]) | (lat_s > cfg["lat_max"]) |
                         (lon_s < cfg["lon_min"]) | (lon_s > cfg["lon_max"])) & valid_mask
            if out_range.sum() > 0:
                alog.log(category="GPS_ERROR", severity="HIGH",
                         description=f"{out_range.sum()} {prefix} coordinates outside India range",
                         source_file=fname, expected_value=f"Lat[{cfg['lat_min']},{cfg['lat_max']}] Lon[{cfg['lon_min']},{cfg['lon_max']}]",
                         **upc_ctx)
        # Repeated consecutive GPS
        if valid_mask.sum() > 1:
            combined = lat_s.astype(str) + "," + lon_s.astype(str)
            runs = (combined != combined.shift()).cumsum()
            run_lengths = combined.groupby(runs).transform("count")
            max_run = run_lengths.max()
            if max_run > cfg["max_identical_gps_consecutive"]:
                alog.log(category="GPS_ERROR", severity="HIGH",
                         description=f"{prefix} GPS has {max_run} consecutive identical coordinates",
                         source_file=fname, actual_value=max_run, **upc_ctx)


def _vectorized_haversine_km(lat1, lon1, lat2, lon2):
    """Vectorized haversine distance in km using numpy arrays."""
    R = 6371.0
    rlat1 = np.radians(lat1); rlon1 = np.radians(lon1)
    rlat2 = np.radians(lat2); rlon2 = np.radians(lon2)
    dlat = rlat2 - rlat1; dlon = rlon2 - rlon1
    a = np.sin(dlat/2)**2 + np.cos(rlat1)*np.cos(rlat2)*np.sin(dlon/2)**2
    return R * 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


def validate_chainage_gps_consistency(df, col_map, file_path, upc_ctx):
    """Validate chainage movement is consistent with GPS movement (vectorized)."""
    fname = os.path.basename(file_path)
    c_sc = col_map.get("start_chainage")
    slat = col_map.get("start_latitude")
    slon = col_map.get("start_longitude")
    if not all([c_sc, slat, slon]):
        return
    work = df[[c_sc, slat, slon]].copy()
    work.columns = ["ch", "lat", "lon"]
    work["ch"] = pd.to_numeric(work["ch"], errors="coerce")
    work["lat"] = pd.to_numeric(work["lat"], errors="coerce")
    work["lon"] = pd.to_numeric(work["lon"], errors="coerce")
    work = work.dropna().sort_values("ch").reset_index(drop=True)
    if len(work) < 2:
        return
    # Vectorized distance computation
    lat1 = work["lat"].values[:-1]; lon1 = work["lon"].values[:-1]
    lat2 = work["lat"].values[1:];  lon2 = work["lon"].values[1:]
    dist_km = _vectorized_haversine_km(lat1, lon1, lat2, lon2)
    d_ch = np.diff(work["ch"].values)
    # Chainage increases but GPS doesn't move
    ch_moves = d_ch > 100
    gps_still = (dist_km < 0.001) & ch_moves
    if gps_still.sum() > 10:
        alog.log(category="GPS_CHAINAGE_MISMATCH", severity="HIGH",
                 description=f"{gps_still.sum()} segments: chainage increases >100m but GPS stationary",
                 source_file=fname, **upc_ctx)
    # GPS jumps
    gps_jumps = dist_km > VALIDATION_CONFIG["max_gps_jump_km"]
    if gps_jumps.sum() > 0:
        alog.log(category="GPS_CHAINAGE_MISMATCH", severity="HIGH",
                 description=f"{gps_jumps.sum()} GPS jumps > {VALIDATION_CONFIG['max_gps_jump_km']}km between consecutive records",
                 source_file=fname, **upc_ctx)


def validate_duplicates(df, col_map, file_path, upc_ctx):
    """Check for exact duplicate rows and duplicate chainage values."""
    fname = os.path.basename(file_path)
    exact_dups = df.duplicated().sum()
    if exact_dups > 0:
        alog.log(category="DUPLICATE", severity="MEDIUM",
                 description=f"{exact_dups} exact duplicate rows",
                 source_file=fname, **upc_ctx)
    # Duplicate business keys (chainage + lane if available)
    c_sc = col_map.get("start_chainage")
    c_lane = col_map.get("lane")
    if c_sc:
        key_cols = [c_sc]
        if c_lane: key_cols.append(c_lane)
        bk_dups = df.duplicated(subset=key_cols).sum()
        if bk_dups > 0:
            alog.log(category="DUPLICATE", severity="MEDIUM",
                     description=f"{bk_dups} duplicate business key rows ({'+'.join(key_cols)})",
                     source_file=fname, **upc_ctx)


def validate_null_values(df, col_map, file_path, upc_ctx):
    """Check null/empty/whitespace in all mapped columns."""
    fname = os.path.basename(file_path)
    for key, col_name in col_map.items():
        if col_name is None: continue
        if col_name not in df.columns: continue
        series = df[col_name]
        nulls = series.isna().sum()
        # Check empty strings and whitespace-only
        if series.dtype == object:
            empty_str = (series == "").sum()
            ws_only = series.str.strip().eq("").sum() - empty_str if hasattr(series.str, "strip") else 0
            if empty_str > 0:
                sev = "HIGH" if key in ("start_chainage", "end_chainage") else "MEDIUM"
                alog.log(category="NULL_VALUE", severity=sev,
                         description=f"{empty_str} empty-string values in {col_name}",
                         source_file=fname, column_name=col_name, **upc_ctx)


print("Data validation functions defined: validate_chainage, validate_gps,")
print("  validate_chainage_gps_consistency, validate_duplicates, validate_null_values")

# COMMAND ----------

# DBTITLE 1,Step 8: Enhanced Business Logic (Coordinate Extraction with Validation)
def identify_material_type(file_path):
    """Identify whether the source file represents Asphalt or Cement Concrete.
    [PRESERVED from original]"""
    norm = file_path.lower()
    if ASPHALT_KEYWORD in norm: return "Asphalt"
    if CC_KEYWORD in norm: return "CC"
    if "/cc/" in norm: return "CC"
    return ""


def extract_chainage_and_coordinates(gps_data, col_map=None):
    """Extract GPS extent using the REQUIRED same-row logic.
    [PRESERVED from original] with added defensive handling.

    BUSINESS RULE:
      first = row with min(Start Chainage) → Start Lat, Start Long, NH Number
      last  = row with max(End Chainage)   → End Lat, End Long
    """
    if col_map is None:
        col_map = identify_l1_columns(gps_data)
    c_sc = col_map["start_chainage"]
    c_ec = col_map["end_chainage"]
    if c_sc is None or c_ec is None:
        return None

    # Defensive: convert to numeric
    gps_data = gps_data.copy()
    gps_data[c_sc] = pd.to_numeric(gps_data[c_sc], errors="coerce")
    gps_data[c_ec] = pd.to_numeric(gps_data[c_ec], errors="coerce")
    gps_data = gps_data.dropna(subset=[c_sc, c_ec])

    if gps_data.empty:
        return None

    # Edge case: single row
    if len(gps_data) == 1:
        row = gps_data.iloc[0]
        return {
            "Lane":           get_safe_value(row, col_map["lane"]),
            "NH Number":      get_safe_value(row, col_map["nh_number"]),
            "Start Chainage": row[c_sc],
            "End Chainage":   row[c_ec],
            "Start Lat":      get_safe_value(row, col_map["start_latitude"]),
            "Start Long":     get_safe_value(row, col_map["start_longitude"]),
            "End Lat":        get_safe_value(row, col_map["end_latitude"]),
            "End Long":       get_safe_value(row, col_map["end_longitude"]),
        }

    first = gps_data.loc[gps_data[c_sc].idxmin()]  # min Start Chainage row
    last  = gps_data.loc[gps_data[c_ec].idxmax()]  # max End Chainage row

    return {
        "Lane":           get_safe_value(first, col_map["lane"]),
        "NH Number":      get_safe_value(first, col_map["nh_number"]),
        "Start Chainage": first[c_sc],
        "End Chainage":   last[c_ec],
        "Start Lat":      get_safe_value(first, col_map["start_latitude"]),
        "Start Long":     get_safe_value(first, col_map["start_longitude"]),
        "End Lat":        get_safe_value(last, col_map["end_latitude"]),
        "End Long":       get_safe_value(last, col_map["end_longitude"]),
    }


print("Business logic functions defined: identify_material_type, extract_chainage_and_coordinates")

# COMMAND ----------

# DBTITLE 1,Step 9: Comprehensive Single-UPC Processing with All Validations
# Processing stats tracker
processing_stats = {
    "files_discovered": 0, "files_processed": 0, "files_failed": 0,
    "files_skipped": 0, "files_no_gps": 0,
    "input_records": 0, "valid_records": 0, "rejected_records": 0,
    "upcs_processed": 0, "upcs_no_files": 0, "upcs_no_day": 0,
}


def process_single_file_with_validation(upc_record, parquet_file_path):
    """Process one L1 source Parquet with full validation pipeline.
    Returns extracted record dict or None."""
    upc_ctx = {"zone": upc_record["Zone"], "upc": upc_record["UPC"],
               "phase": upc_record["Phase"], "cycle": upc_record["Cycle"]}
    fname = os.path.basename(parquet_file_path)

    # 1. Read file (Lane filtering now handled by directory partition discovery)
    df, schema_info = read_l1_parquet_safe(parquet_file_path, upc_ctx)
    if df is None:
        processing_stats["files_failed"] += 1
        return None
    if df.empty:
        processing_stats["files_skipped"] += 1
        return None

    processing_stats["input_records"] += len(df)

    # 2. Schema validation + column identification
    col_map = validate_file_schema(df, parquet_file_path, upc_ctx)

    # 2b. Sort by Start Chainage ascending for correct ordering
    sc_col = col_map.get("start_chainage")
    if sc_col is not None and sc_col in df.columns:
        df["_sort_ch"] = pd.to_numeric(df[sc_col], errors="coerce")
        df = df.sort_values("_sort_ch", na_position="last").drop(columns=["_sort_ch"]).reset_index(drop=True)

    # 3. Null value validation
    validate_null_values(df, col_map, parquet_file_path, upc_ctx)

    # 4. Chainage validation
    validate_chainage(df, col_map, parquet_file_path, upc_ctx)

    # 5. GPS validation
    validate_gps(df, col_map, parquet_file_path, upc_ctx)

    # 6. Chainage-GPS consistency
    validate_chainage_gps_consistency(df, col_map, parquet_file_path, upc_ctx)

    # 7. Duplicate validation
    validate_duplicates(df, col_map, parquet_file_path, upc_ctx)

    # 8. Extract coordinates using ORIGINAL business logic
    coords = extract_chainage_and_coordinates(df, col_map)
    if coords is None:
        alog.log(category="BUSINESS_RULE_ERROR", severity="HIGH",
                 description="Coordinate extraction returned None (no valid chainage data after cleaning)",
                 source_file=fname, **upc_ctx,
                 recommended_action="Check if file has valid Start/End Chainage")
        processing_stats["rejected_records"] += len(df)
        processing_stats["files_failed"] += 1
        return None

    processing_stats["valid_records"] += 1
    processing_stats["files_processed"] += 1

    # 9. Validate extracted output coordinates
    for coord_key, coord_val in [("Start Lat", coords["Start Lat"]),
                                  ("Start Long", coords["Start Long"]),
                                  ("End Lat", coords["End Lat"]),
                                  ("End Long", coords["End Long"])]:
        if coord_val == "Not Present in Source Parquet":
            alog.log(category="GPS_ERROR", severity="MEDIUM",
                     description=f"{coord_key} not present in extracted output",
                     source_file=fname, column_name=coord_key, **upc_ctx)
        else:
            try:
                v = float(coord_val)
                if "Lat" in coord_key and not (VALIDATION_CONFIG["lat_min"] <= v <= VALIDATION_CONFIG["lat_max"]):
                    alog.log(category="GPS_ERROR", severity="HIGH",
                             description=f"Extracted {coord_key}={v} outside India range",
                             source_file=fname, column_name=coord_key,
                             actual_value=v, latitude=v if "Lat" in coord_key else None, **upc_ctx)
                if "Long" in coord_key and not (VALIDATION_CONFIG["lon_min"] <= v <= VALIDATION_CONFIG["lon_max"]):
                    alog.log(category="GPS_ERROR", severity="HIGH",
                             description=f"Extracted {coord_key}={v} outside India range",
                             source_file=fname, column_name=coord_key,
                             actual_value=v, longitude=v if "Long" in coord_key else None, **upc_ctx)
            except (ValueError, TypeError):
                pass

    # Build output record
    material = identify_material_type(parquet_file_path)
    display_name = f"{fname} [{material}]" if material else fname
    return {
        "UPC": upc_record["UPC"], "Zone": upc_record["Zone"],
        "Phase": upc_record["Phase"], "Cycle": upc_record["Cycle"],
        "Lane": "L1", "File Name": display_name,
        "NH Number": coords["NH Number"],
        "Start Chainage": coords["Start Chainage"],
        "End Chainage": coords["End Chainage"],
        "Start Lat": coords["Start Lat"], "Start Long": coords["Start Long"],
        "End Lat": coords["End Lat"], "End Long": coords["End Long"],
    }


def process_single_upc(upc_record):
    """Process every L1 GPS file for one UPC with full validation."""
    upc_ctx = {"zone": upc_record["Zone"], "upc": upc_record["UPC"],
               "phase": upc_record["Phase"], "cycle": upc_record["Cycle"]}
    latest_day = find_latest_date_directory(upc_record["upc_path"])
    if not latest_day:
        processing_stats["upcs_no_day"] += 1
        alog.log(category="FILE_ERROR", severity="MEDIUM",
                 description="No valid date partition found for UPC",
                 source_path=upc_record["upc_path"], **upc_ctx)
        return []

    l1_files = find_l1_gps_parquet_files(latest_day)
    processing_stats["files_discovered"] += len(l1_files)
    if not l1_files:
        processing_stats["upcs_no_files"] += 1
        alog.log(category="FILE_ERROR", severity="MEDIUM",
                 description="No L1 GPS Parquet files found under latest date partition",
                 source_path=latest_day, **upc_ctx)
        return []

    results = []
    for fp in l1_files:
        try:
            rec = process_single_file_with_validation(upc_record, fp)
            if rec is not None:
                results.append(rec)
        except Exception as e:
            processing_stats["files_failed"] += 1
            # Skip silently — technical processing errors are not data anomalies

    processing_stats["upcs_processed"] += 1
    return results


print("Processing functions defined: process_single_file_with_validation, process_single_upc")

# COMMAND ----------

# DBTITLE 1,Step 10: Batch Processing - All UPCs
def process_all_upcs(upc_records):
    """Process every discovered UPC with progress reporting."""
    all_results = []
    total = len(upc_records)
    for i, upc_rec in enumerate(upc_records, 1):
        all_results.extend(process_single_upc(upc_rec))
        if i % 100 == 0 or i == total:
            print(f"  [{i}/{total}] Output rows: {len(all_results)} | "
                  f"Files OK: {processing_stats['files_processed']} | "
                  f"Failed: {processing_stats['files_failed']} | "
                  f"Anomalies: {len(alog.records)}")
    return pd.DataFrame(all_results)


# Reset stats and anomaly log (idempotent re-run)
for k in processing_stats:
    processing_stats[k] = 0
alog.records.clear()
alog._counters.clear()

# Re-run directory + metadata anomalies
_ = inspect_directory_structure(NSV_BACKUP_PATH)

print(f"Processing {len(selected_upcs)} UPC records...")
print("=" * 70)
results_dataframe = process_all_upcs(selected_upcs)
print(f"\n{'='*70}")
print(f"Total output records: {len(results_dataframe)}")
print(f"Processing stats:")
for k, v in processing_stats.items():
    print(f"  {k:<25s}: {v}")
if not results_dataframe.empty:
    display(results_dataframe.head(10))

# COMMAND ----------

# DBTITLE 1,Step 11: Active Flag, Project Name Lookup & Final Output
def identify_active_records(df):
    """Mark the latest Phase+Cycle for each UPC as Active Flag=1.
    [PRESERVED from original business logic]"""
    if df.empty:
        return df
    df = df.copy()
    df["_phase_num"] = pd.to_numeric(df["Phase"], errors="coerce").fillna(0).astype(int)
    df["_cycle_num"] = pd.to_numeric(df["Cycle"], errors="coerce").fillna(0).astype(int)

    latest = (
        df.groupby("UPC")[["_phase_num", "_cycle_num"]]
        .apply(lambda g: (g["_phase_num"].max(),
                          g.loc[g["_phase_num"] == g["_phase_num"].max(), "_cycle_num"].max()))
    )
    df["Active Flag"] = 0
    for upc, (lp, lc) in latest.items():
        mask = (df["UPC"] == upc) & (df["_phase_num"] == lp) & (df["_cycle_num"] == lc)
        df.loc[mask, "Active Flag"] = 1

    df.drop(columns=["_phase_num", "_cycle_num"], inplace=True)
    return df


def _load_project_name_lookup():
    """Load UPC → Project Name mapping from Consolidated Master Excel.
    Reads Zone A–E sheets; converts UPC from '/' to '_' format for matching."""
    upc_to_project = {}
    zone_configs = [
        ("Zone A", 1), ("Zone B", 1), ("Zone C", 1),
        ("Zone D", 2), ("Zone E", 1),  # Zone D header is one row lower
    ]
    for sheet, header_row in zone_configs:
        try:
            df = pd.read_excel(PROJECT_NAME_REF_PATH, sheet_name=sheet, header=header_row)
            upc_col = next((c for c in df.columns if str(c).strip().lower() == "upc"), None)
            proj_col = next((c for c in df.columns if "project name" in str(c).strip().lower()), None)
            if upc_col is None or proj_col is None:
                continue
            for _, row in df[[upc_col, proj_col]].dropna(subset=[upc_col]).iterrows():
                raw_upc = str(row[upc_col]).strip()
                upc_key = raw_upc.replace("/", "_")  # normalise to underscore format
                proj = str(row[proj_col]).strip() if pd.notna(row[proj_col]) else ""
                if upc_key and proj and proj != "nan":
                    upc_to_project[upc_key] = proj
        except Exception as e:
            print(f"  Warning: Could not read sheet '{sheet}': {e}")
    return upc_to_project


# Load lookup once
print("Loading Project Name reference data...")
_PROJECT_LOOKUP = _load_project_name_lookup()
print(f"  Loaded {len(_PROJECT_LOOKUP)} UPC → Project Name mappings")


def prepare_final_output(df):
    """Prepare the final output:
    1. Keep only Active Flag=1 (latest Phase+Cycle per UPC)
    2. Derive S.No, Unnamed: 5, Survey_UPC, Project Name
    3. Select FINAL_OUTPUT_COLUMNS
    """
    if df.empty:
        return pd.DataFrame(columns=FINAL_OUTPUT_COLUMNS)

    df = df.copy()

    # ── Keep only latest Phase+Cycle per UPC (Active Flag = 1) ────────
    df = identify_active_records(df)
    df = df[df["Active Flag"] == 1].copy()
    df.drop(columns=["Active Flag"], inplace=True, errors="ignore")

    if df.empty:
        return pd.DataFrame(columns=FINAL_OUTPUT_COLUMNS)

    # Numeric GPS conversion
    for col in GPS_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: pd.to_numeric(v, errors="coerce")
                if v != "Not Present in Source Parquet" else v
            )

    # ── Derived columns ──────────────────────────────────────────────────
    df["S.No"] = range(1, len(df) + 1)
    df["Unnamed: 5"] = df["UPC"].str.replace("_", "/")
    df["Survey_UPC"] = df["UPC"].apply(
        lambda x: "/".join(x.rsplit("_", 1)) if isinstance(x, str) and "_" in x else x
    )
    # Project Name from Consolidated Master Excel (matched on UPC)
    df["Project Name"] = df["UPC"].map(_PROJECT_LOOKUP).fillna("")

    # Ensure all expected columns exist
    for col in FINAL_OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[FINAL_OUTPUT_COLUMNS]


final_results_dataframe = prepare_final_output(results_dataframe.copy())
print(f"\nFinal output shape: {final_results_dataframe.shape}")
print(f"Unique UPCs: {final_results_dataframe['UPC'].nunique() if not final_results_dataframe.empty else 0}")
matched = (final_results_dataframe["Project Name"] != "").sum() if not final_results_dataframe.empty else 0
print(f"Project Names matched: {matched}/{len(final_results_dataframe)}")
if not final_results_dataframe.empty:
    display(final_results_dataframe.astype(str))

# COMMAND ----------

# DBTITLE 1,Step 12: Output Validation
def validate_final_output(df):
    """Comprehensive validation of the final output dataset."""
    if df.empty:
        alog.log(category="OUTPUT_VALIDATION_ERROR", severity="CRITICAL",
                 description="Final output is empty (0 records)")
        return

    # Null primary keys
    for col in ["UPC", "Zone", "Phase", "Cycle"]:
        nulls = df[col].isna().sum()
        if nulls > 0:
            alog.log(category="OUTPUT_VALIDATION_ERROR", severity="CRITICAL",
                     description=f"{nulls} null values in primary key column '{col}'",
                     column_name=col)

    # Duplicate output rows
    exact_dups = df.duplicated().sum()
    if exact_dups > 0:
        alog.log(category="OUTPUT_VALIDATION_ERROR", severity="HIGH",
                 description=f"{exact_dups} exact duplicate rows in final output")

    # Duplicate business keys
    bk_cols = ["UPC", "Phase", "Cycle"]
    bk_dups = df.duplicated(subset=bk_cols).sum()
    if bk_dups > 0:
        alog.log(category="OUTPUT_VALIDATION_ERROR", severity="HIGH",
                 description=f"{bk_dups} duplicate business key rows (UPC+Phase+Cycle+FileName)")

    # Invalid GPS in output
    for col in GPS_COLUMNS:
        if col not in df.columns: continue
        not_present = (df[col] == "Not Present in Source Parquet").sum()
        if not_present > 0:
            alog.log(category="OUTPUT_VALIDATION_ERROR", severity="MEDIUM",
                     description=f"{not_present} rows with '{col}' = 'Not Present in Source Parquet'",
                     column_name=col)
        numeric_vals = pd.to_numeric(df[col], errors="coerce")
        valid_numeric = numeric_vals.dropna()
        if "Lat" in col:
            out_range = ((valid_numeric < VALIDATION_CONFIG["lat_min"]) |
                         (valid_numeric > VALIDATION_CONFIG["lat_max"])).sum()
        else:
            out_range = ((valid_numeric < VALIDATION_CONFIG["lon_min"]) |
                         (valid_numeric > VALIDATION_CONFIG["lon_max"])).sum()
        if out_range > 0:
            alog.log(category="OUTPUT_VALIDATION_ERROR", severity="HIGH",
                     description=f"{out_range} rows with '{col}' outside valid range in final output",
                     column_name=col)

    # Active Flag validation
    if "Active Flag" in df.columns:
        invalid_flags = ~df["Active Flag"].isin([0, 1])
        if invalid_flags.sum() > 0:
            alog.log(category="OUTPUT_VALIDATION_ERROR", severity="HIGH",
                     description=f"{invalid_flags.sum()} rows with invalid Active Flag value")
        # Every UPC should have at least one active record
        upcs_no_active = df.groupby("UPC")["Active Flag"].max()
        no_active = (upcs_no_active == 0).sum()
        if no_active > 0:
            alog.log(category="OUTPUT_VALIDATION_ERROR", severity="MEDIUM",
                     description=f"{no_active} UPCs have no active records")

    # Chainage validation in output
    sc_num = pd.to_numeric(df["Start Chainage"], errors="coerce")
    ec_num = pd.to_numeric(df["End Chainage"], errors="coerce")
    bad_order = (sc_num >= ec_num).sum()
    if bad_order > 0:
        alog.log(category="OUTPUT_VALIDATION_ERROR", severity="HIGH",
                 description=f"{bad_order} output rows where Start Chainage >= End Chainage")

    print(f"Output validation complete. Rows: {len(df)}, New anomalies logged.")


validate_final_output(final_results_dataframe)
alog.print_summary()

# COMMAND ----------

# DBTITLE 1,Step 13: Save Final Output + Simple Anomaly Log
def save_results_to_excel(df, output_path):
    """Save final extraction output to Excel."""
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    try:
        df.to_excel(output_path, index=False)
        print(f"Results saved to: {output_path}")
    except Exception as e:
        print(f"ERROR saving results: {e}")


# ── SIMPLE ANOMALY LOG BUILDER ───────────────────────────────────────────────

def _friendly_description(category, raw_desc):
    """Convert internal anomaly to plain-English problem description."""
    d = (raw_desc or "").lower()

    if category == "FILE_ERROR":
        if "empty" in d: return "Input file is empty (no records)."
        if "no l1 gps" in d: return "No GPS data files found."
        if "no valid date" in d: return "No valid date partition found."
        if "failed to read" in d or "could not read" in d:
            return "Input file could not be read (possibly corrupt)."
        if "unhandled exception" in d:
            return "Input file caused an unexpected error during processing."
        return "Input file could not be processed."

    if category == "SCHEMA_ERROR":
        if "chainage" in d: return "Required chainage column is missing from input file."
        if "lat" in d or "long" in d or "gps" in d:
            return "Latitude/Longitude column is missing from input file."
        if "nh" in d: return "NH Number column is missing from input file."
        if "lane" in d: return "Lane column is missing from input file."
        if "unnamed" in d: return "Input file has unexpected column structure."
        return "Input file has missing or unexpected columns."

    if category == "NULL_VALUE":
        if "chainage" in d: return "Chainage values are missing for some records."
        if "lat" in d: return "Latitude is missing for some records."
        if "long" in d: return "Longitude is missing for some records."
        if "empty" in d: return "Some required fields contain empty values."
        return "Required data values are missing for some records."

    if category == "INVALID_VALUE":
        if "chainage" in d: return "Some chainage values are not valid numbers."
        if "lat" in d: return "Some latitude values are not valid numbers."
        if "long" in d: return "Some longitude values are not valid numbers."
        return "Data contains invalid or non-numeric values."

    if category == "DUPLICATE":
        if "exact" in d: return "Exact duplicate records found."
        return "Duplicate records found for the same UPC, Phase and Cycle."

    if category == "CHAINAGE_ERROR":
        if "negative" in d: return "Chainage contains negative values."
        if ">=" in d: return "Start chainage is greater than or equal to end chainage."
        if "monotonic" in d: return "Chainage is decreasing unexpectedly."
        return "Chainage data has quality issues."

    if category == "GPS_ERROR":
        if "zero" in d: return "GPS coordinates are zero (0, 0) for some records."
        if "outside" in d: return "GPS coordinates are outside the valid geographic range."
        if "consecutive identical" in d:
            return "Same latitude and longitude repeated for a long sequence while chainage increases."
        if "not present" in d: return "Latitude/Longitude is missing in the extracted output."
        return "GPS coordinates have quality issues."

    if category == "GPS_CHAINAGE_MISMATCH":
        if "stationary" in d:
            return "Same GPS coordinates repeated while chainage keeps increasing."
        if "jump" in d:
            return "GPS location changes significantly between consecutive records."
        return "GPS movement is inconsistent with chainage progression."

    if category == "CONTINUITY_ERROR":
        if "gap" in d: return "Start and end chainage have a gap between consecutive records."
        if "overlap" in d: return "Chainage ranges overlap between consecutive records."
        return "Chainage continuity issues found."

    if category == "BUSINESS_RULE_ERROR":
        return "No valid GPS records could be extracted for this UPC."

    if category == "OUTPUT_VALIDATION_ERROR":
        if "not present" in d: return "GPS coordinates are missing in the final output."
        if "duplicate" in d: return "Duplicate records found in the final output."
        if "active" in d: return "No active Phase/Cycle could be determined."
        if "chainage" in d: return "Start chainage >= End chainage in the output."
        return "Final output has validation issues."

    if category == "METADATA_ERROR":
        if "phase" in d: return "Phase folder has a non-standard naming pattern."
        if "cycle" in d: return "Cycle folder has a non-standard naming pattern."
        return "Folder structure has an unexpected naming pattern."

    return raw_desc or "Unknown issue."


def build_simple_anomaly_log(alog_obj):
    """Build a simple, grouped anomaly log: UPC | Phase | Cycle | Problem Description."""
    full_df = alog_obj.to_dataframe()

    if full_df.empty:
        return pd.DataFrame([{
            "UPC": "-", "Phase": "-", "Cycle": "-",
            "Problem Description": "No anomalies detected."
        }])

    # ── Only keep DATA-QUALITY anomalies ─────────────────────────────
    # Exclude: folder/metadata issues, technical processing errors
    _exclude_cats = {"METADATA_ERROR"}
    full_df = full_df[~full_df["anomaly_category"].isin(_exclude_cats)].copy()

    # Within FILE_ERROR keep only missing-data issues; drop read/processing failures
    _fe_mask = full_df["anomaly_category"] == "FILE_ERROR"
    _keep_fe = full_df["description"].str.lower().str.contains(
        "empty|no l1 gps|no valid date|not found", na=False)
    full_df = full_df[~_fe_mask | _keep_fe].copy()

    if full_df.empty:
        return pd.DataFrame([{
            "UPC": "-", "Phase": "-", "Cycle": "-",
            "Problem Description": "No anomalies detected."
        }])

    full_df["_friendly"] = full_df.apply(
        lambda r: _friendly_description(
            r.get("anomaly_category", ""), r.get("description", "")), axis=1
    )
    full_df["_upc"]   = full_df["upc"].fillna("-")
    full_df["_phase"] = full_df["phase"].fillna("-")
    full_df["_cycle"] = full_df["cycle"].fillna("-")

    # Group identical (UPC, Phase, Cycle, description) into one row
    simple = (
        full_df
        .groupby(["_upc", "_phase", "_cycle", "_friendly"], sort=False)
        .size()
        .reset_index(name="_n")
    )
    simple = simple.rename(columns={
        "_upc": "UPC", "_phase": "Phase",
        "_cycle": "Cycle", "_friendly": "Problem Description"
    }).drop(columns=["_n"])

    return simple.sort_values(["UPC", "Phase", "Cycle"]).reset_index(drop=True)


def save_simple_anomaly_log(alog_obj, output_path):
    """Save the simple anomaly log to Excel."""
    simple_df = build_simple_anomaly_log(alog_obj)
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    try:
        simple_df.to_excel(output_path, index=False)
        print(f"Anomaly log saved to: {output_path} ({len(simple_df)} entries)")
    except Exception as e:
        print(f"ERROR saving anomaly log: {e}")


# ── SAVE BOTH ────────────────────────────────────────────────────────────────
save_results_to_excel(final_results_dataframe, OUTPUT_EXCEL_PATH)
save_simple_anomaly_log(alog, ANOMALY_LOG_PATH)

# ── PREVIEW ANOMALY LOG ─────────────────────────────────────────────────────
simple_anomaly_df = build_simple_anomaly_log(alog)
print(f"\nSimple Anomaly Log ({len(simple_anomaly_df)} entries):")
display(simple_anomaly_df)

# COMMAND ----------

# DBTITLE 1,Step 14: Validation Summary & Reconciliation
# ── RECONCILIATION SUMMARY ───────────────────────────────────────────────────
alog_df = alog.to_dataframe()

print("=" * 79)
print(f"PIPELINE VALIDATION SUMMARY — Run ID: {RUN_ID}")
print("=" * 79)

print(f"\n--- Processing Metrics ---")
for k, v in processing_stats.items():
    print(f"  {k:<25s}: {v}")

print(f"\n--- Output Metrics ---")
total_out = len(final_results_dataframe)
print(f"  Total output rows       : {total_out}")
print(f"  Unique UPCs in output   : {final_results_dataframe['UPC'].nunique() if not final_results_dataframe.empty else 0}")
print(f"  Output file             : {OUTPUT_EXCEL_PATH}")

print(f"\n--- Anomaly Metrics ---")
print(f"  Total anomalies         : {len(alog_df)}")
if not alog_df.empty:
    print(f"  \n  By Severity:")
    for sev in ["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"]:
        cnt = (alog_df["severity"] == sev).sum()
        if cnt:
            marker = "***" if sev == "CRITICAL" else "**" if sev == "HIGH" else ""
            print(f"    {sev:<10s}: {cnt:>6d} {marker}")
    print(f"\n  By Category:")
    for cat, cnt in alog_df["anomaly_category"].value_counts().items():
        print(f"    {cat:<30s}: {cnt:>6d}")
    print(f"\n  By Zone:")
    zone_counts = alog_df[alog_df["zone"].notna()].groupby("zone").size()
    for zone, cnt in zone_counts.items():
        print(f"    Zone={zone:<5s}: {cnt:>6d}")
    print(f"\n  Top 10 UPCs by anomaly count:")
    upc_counts = alog_df[alog_df["upc"].notna()].groupby("upc").size().nlargest(10)
    for upc, cnt in upc_counts.items():
        print(f"    {upc:<25s}: {cnt:>4d}")

print(f"\n--- Reconciliation ---")
print(f"  UPCs discovered         : {len(extracted_upc_records)}")
print(f"  UPCs processed          : {processing_stats['upcs_processed']}")
print(f"  UPCs no date partition   : {processing_stats['upcs_no_day']}")
print(f"  UPCs no GPS files        : {processing_stats['upcs_no_files']}")
print(f"  Files discovered        : {processing_stats['files_discovered']}")
print(f"  Files processed OK      : {processing_stats['files_processed']}")
print(f"  Files failed            : {processing_stats['files_failed']}")
print(f"  Files skipped (empty)   : {processing_stats['files_skipped']}")
print(f"  Input records (total)   : {processing_stats['input_records']}")
print(f"  Valid extraction records: {processing_stats['valid_records']}")
print(f"  Anomaly log file        : {ANOMALY_LOG_PATH}")
print("=" * 79)

# Display final results
if not final_results_dataframe.empty:
    display(final_results_dataframe.astype(str))