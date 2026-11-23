# Databricks notebook source
# MAGIC %md
# MAGIC ## 0. Setup

# COMMAND ----------

# MAGIC %pip install -r "/Workspace/Users/manu.jain9244@softsensor.ai/Requirement/requirements (1).txt"
# MAGIC %pip install openpyxl
# MAGIC %pip install pillow
# MAGIC # pillow's EXIF reading is used to extract GPS Lat/Lon embedded in
# MAGIC # pothole/cracking photos -- per Prathap's instruction, OCR is NOT used
# MAGIC # for this at all.

# COMMAND ----------

import bisect
import datetime as _dt
import fnmatch
import glob
import os
import re
import shutil
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import openpyxl
import pandas as pd
from openpyxl.utils import get_column_letter
from rapidfuzz import fuzz
import math
from PIL import Image as PILImage

# COMMAND ----------

# MAGIC %md
# MAGIC Load all the helper files 
# MAGIC
# MAGIC bronze_common
# MAGIC
# MAGIC zone_a_backup_extract
# MAGIC
# MAGIC  nsv_backup_unified
# MAGIC
# MAGIC  process_backup 
# MAGIC
# MAGIC  NSV_Extraction_Final

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This cell sets all input and output paths used by the pipeline.
# MAGIC
# MAGIC It also:
# MAGIC 1. Stores the deliverable, backup, Dashcam, DAMS, schema, and output locations.
# MAGIC 2. Saves today's date in `RUN_DATE`.
# MAGIC 3. Reads the deliverable and backup path structure to extract:
# MAGIC    - Zone
# MAGIC    - Phase
# MAGIC    - Cycle
# MAGIC    - UPC/project code
# MAGIC 4. Checks that the paths follow the expected folder structure.
# MAGIC 5. Verifies that the deliverable and backup folders belong to the same project.
# MAGIC 6. Stores the extracted values in `ZONE`, `PHASE`, `CYCLE`, and `UPC`.
# MAGIC 7. Prints these values for confirmation.
# MAGIC
# MAGIC **Note:** This cell only stores and validates path information. It does not read or process any Excel, CSV, deliverable, or backup files.

# COMMAND ----------

def _long_path(path: str) -> str:
    if os.name != 'nt':
        return path
    ap = os.path.abspath(path)
    if ap.startswith('\\\\?\\'):
        return ap
    if ap.startswith('\\\\'):
        return '\\\\?\\UNC\\' + ap[2:]
    return '\\\\?\\' + ap

# COMMAND ----------


# ── Paths ──────────────────────────────────────────────────────────────────────
DELIVERABLE_ROOT = (
    r"/Volumes/nhai_dhms/nsv/nsv_downloads/Zone_E/Phase_2/Cycle_1/N_07015_01002_BR/deliverable/"
)

BACKUP_ROOT = (
    r"/Volumes/nhai_dhms/nsv/nsv_downloads/Zone_E/Phase_2/Cycle_1/N_07015_01002_BR/backup/"
)
DASHCAM_FILE      = r"/Workspace/Users/manu.jain9244@softsensor.ai/Excel_file/DAHSCAM_Data (1).xlsx"       # AI dashcam defect detections
DAMS_FILE         =  r"/Workspace/Users/manu.jain9244@softsensor.ai/Excel_file/DAMS_Data (1).csv"        # field-reported defects (RSA/TSP1/TSP2)
OUTPUT_ROOT = r"/Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output/"

SCHEMA_FILE = (
    r"/Workspace/Users/manu.jain9244@softsensor.ai/Bronze/schemas_with_patching (3).xlsx"
)

BACKUP_INDEX_FILE = (
    r"/Workspace/Users/manu.jain9244@softsensor.ai/Bronze/Zone_E_updated.xlsx"
)
BACKUP_INDEX_SHEET = "Zone E"

RUN_DATE = datetime.today().date()


# ── Input path structure ───────────────────────────────────────────────────────
_VOLUME_ANCHOR = "nsv_downloads"

# Support both your new and old ending-folder names.
_FILES_FOLDER_NAMES = {
    "deliverable",
    "backup",
    "deliverable files",
    "backup files",
}


def parse_nsv_root(root_path: str) -> tuple[str, str, str, str]:
    """
    Extract Zone, Phase, Cycle and UPC from a path having this structure:

    .../nsv_downloads/
        Zone_A/
        Phase_1/
        Cycle_1/
        N_02007_07001_PB/
        deliverable/

    Returns:
        zone, phase, cycle, upc

    Example:
        ("A", "1", "1", "N_02007_07001_PB")
    """
    parts = list(Path(root_path).parts)
    lower_parts = [part.casefold() for part in parts]

    # Find the nsv_downloads folder.
    try:
        anchor_index = lower_parts.index(_VOLUME_ANCHOR.casefold())
    except ValueError as exc:
        raise ValueError(
            f"'{_VOLUME_ANCHOR}' was not found in path: {root_path}"
        ) from exc

    # Find the deliverable/backup folder.
    files_folder_index = next(
        (
            index
            for index in range(anchor_index + 1, len(parts))
            if lower_parts[index] in _FILES_FOLDER_NAMES
        ),
        None,
    )

    if files_folder_index is None:
        raise ValueError(
            "No deliverable or backup folder was found in path: "
            f"{root_path}"
        )

    # Everything between nsv_downloads and deliverable/backup.
    hierarchy_parts = parts[anchor_index + 1 : files_folder_index]

    expected_example = (
        "Zone_A/Phase_1/Cycle_1/N_02007_07001_PB"
    )

    if len(hierarchy_parts) != 4:
        raise ValueError(
            "Expected exactly four folders between 'nsv_downloads' and "
            f"the deliverable/backup folder, like {expected_example}. "
            f"Found: {hierarchy_parts}"
        )

    zone_folder, phase_folder, cycle_folder, upc = hierarchy_parts

    def remove_prefix(folder_name: str, prefix: str) -> str:
        marker = f"{prefix}_"

        if not folder_name.casefold().startswith(marker.casefold()):
            raise ValueError(
                f"Expected folder beginning with '{marker}', "
                f"but found '{folder_name}' in path: {root_path}"
            )

        value = folder_name[len(marker) :]

        if not value:
            raise ValueError(
                f"No value found after '{marker}' in '{folder_name}'"
            )

        return value

    zone = remove_prefix(zone_folder, "Zone")
    phase = remove_prefix(phase_folder, "Phase")
    cycle = remove_prefix(cycle_folder, "Cycle")

    if not upc:
        raise ValueError(f"UPC folder is empty in path: {root_path}")

    return zone, phase, cycle, upc


# Parse both roots.
deliverable_metadata = parse_nsv_root(DELIVERABLE_ROOT)
backup_metadata = parse_nsv_root(BACKUP_ROOT)

# Make sure deliverable and backup belong to the same project.
if deliverable_metadata != backup_metadata:
    raise ValueError(
        "Deliverable and backup paths contain different metadata.\n"
        f"Deliverable: {deliverable_metadata}\n"
        f"Backup     : {backup_metadata}"
    )
LIMIT_DELIVERABLE_PROJECTS = None
LIMIT_LANE_FILES           = None
ZONE, PHASE, CYCLE, UPC = deliverable_metadata

print("Zone :", ZONE)
print("Phase:", PHASE)
print("Cycle:", CYCLE)
print("UPC  :", UPC)

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION & LOGGING SETUP
# ─────────────────────────────────────────────────────────────────────────────
# Writes a run log INSIDE OUTPUT_ROOT (the same folder the bronze parquet
# output goes to). By design:
#   - The .log file on disk holds CRITICAL entries ONLY (kept short/human
#     readable). WARNING/ERROR/INFO issues are still tracked in memory and
#     written to the .csv for full detail, but they don't clutter the log.
#   - Nothing is printed to the console for individual issues -- the
#     pipeline never surfaces raw tracebacks. Only the short summary prints
#     already in each stage cell (e.g. "SUCCESS: N segment(s)") show.
#   - The pipeline NEVER stops on CRITICAL issues. raise_if_critical() logs
#     a one-line summary and lets execution continue to the next stage.

import logging
import csv
import re


class PipelineValidationError(Exception):
    """Kept for backwards compatibility; no longer raised anywhere -- the
    pipeline logs CRITICAL issues and keeps running instead of stopping."""
    pass


def concise_exc(exc: BaseException, limit: int = 150) -> str:
    """
    Turn any exception into a short, single-line, human-readable message,
    e.g. "ValueError: cannot convert float NaN to integer". Never raises.
    """
    try:
        msg = re.sub(r"\s+", " ", str(exc).strip())
        name = type(exc).__name__
        text = f"{name}: {msg}" if msg else name
        if len(text) > limit:
            text = text[: limit - 1].rstrip() + "…"
        return text
    except Exception:
        return "Unknown error"


# Moved up from Cell 18 (still defined there too, harmlessly re-defined
# when that cell runs -- kept in both places rather than restructuring
# more cells right now). Needed here because log_issue() below calls
# normalize_deliverable(), and log_issue() can itself be called as early
# as Cell 17 (e.g. its "no project folders found" error path) -- BEFORE
# Cell 18 would otherwise have run yet, which would have raised
# NameError: normalize_deliverable is not defined the first time that
# happened.
# Kept in sync with the fuller version in Cell 18 (which re-defines these
# same two names and wins once it runs -- this copy only matters for the
# narrow window before Cell 18 has executed yet, see note above).
_DELIV_BARE_RE = re.compile(r"(?<![A-Za-z0-9])D[-_\s]?([1-4])(?![A-Za-z0-9])", re.IGNORECASE)
_DELIVERABLE_WORD_FUZZY_THRESHOLD = 75


def normalize_deliverable(text: str) -> str:
    """Normalise any spelling/format variant of a 'Deliverable N' name ->
    'deliverable-N'. See the full docstring on this function in Cell 18."""
    text = str(text)
    words = [w for w in re.split(r"[^A-Za-z]+", text) if w]
    has_deliverable_word = any(
        fuzz.ratio(w.lower(), "deliverable") >= _DELIVERABLE_WORD_FUZZY_THRESHOLD
        or fuzz.ratio(w.lower(), "deliverables") >= _DELIVERABLE_WORD_FUZZY_THRESHOLD
        for w in words
    )
    if not has_deliverable_word:
        m = _DELIV_BARE_RE.search(text)
        return f"deliverable-{m.group(1)}" if m else ""
    digits = {d for d in re.findall(r"\d", text) if d in "1234"}
    return f"deliverable-{digits.pop()}" if len(digits) == 1 else ""


LOG_DIR = Path(OUTPUT_ROOT) / "_logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

_RUN_TAG  = f"Zone{ZONE}_Phase{PHASE}_Cycle{CYCLE}_{UPC}_{RUN_DATE}"
LOG_FILE  = LOG_DIR / f"pipeline_validation_{_RUN_TAG}.log"
ISSUE_CSV = LOG_DIR / f"pipeline_validation_{_RUN_TAG}.csv"

validation_logger = logging.getLogger(f"nsv_bronze_validation_{_RUN_TAG}")
validation_logger.setLevel(logging.INFO)
validation_logger.handlers.clear()  # avoid duplicate handlers if this cell re-runs
validation_logger.propagate = False  # never let records bubble up to the root/console

# .log file: CRITICAL only, so it stays short and easy to scan.
_file_handler = logging.FileHandler(_long_path(str(LOG_FILE)), mode="w", encoding="utf-8")
_file_handler.setLevel(logging.CRITICAL)
_file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s"))
validation_logger.addHandler(_file_handler)

# No console/stream handler -- errors are recorded, never printed raw.

VALIDATION_ISSUES = []  # list[dict]: timestamp, severity, stage, file, table, column, issue -- ALL severities, for the CSV


def log_issue(severity: str, stage: str, issue: str, file: str = "", table: str = "", column: str = "", deliverable: str = None):
    """
    Record one validation issue. Always added to VALIDATION_ISSUES (so the
    CSV has full detail across every severity); only written to the .log
    file when severity == CRITICAL. Never raises, never stops the run.

    Adds a "deliverable" field to every record, derived from `file`/`table`
    via normalize_deliverable() (Cell 18/51's own helper -- same logic that
    tags each Deliverable row "Deliverable - 1".."Deliverable - 4"). This
    is computed here, not passed in by every call site, so existing
    log_issue(...) calls all over the notebook don't need to change --
    it's blank ("") for Backup/Dashcam/DAMS entries, where "Deliverable"
    doesn't apply.
    """
    if deliverable is not None:
        _deliverable = deliverable
    else:
        _deliv_nd = normalize_deliverable(file) or normalize_deliverable(table)
        _deliverable = f"Deliverable - {_deliv_nd.rsplit('-', 1)[-1]}" if _deliv_nd else ""
    record = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "severity": severity,
        "stage": stage,
        "deliverable": _deliverable,
        "file": file,
        "table": table,
        "column": column,
        "issue": issue,
    }
    VALIDATION_ISSUES.append(record)
    msg = f"[{stage}] file='{file}' table='{table}' column='{column}' -> {issue}"
    level = {
        "CRITICAL": logging.CRITICAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
    }.get(severity, logging.INFO)
    try:
        validation_logger.log(level, msg)
    except Exception:
        pass  # logging itself must never break the pipeline


def write_validation_csv():
    """Flush VALIDATION_ISSUES (all severities) to a CSV in the _logs folder."""
    if not VALIDATION_ISSUES:
        return
    try:
        with open(_long_path(str(ISSUE_CSV)), "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(VALIDATION_ISSUES[0].keys()))
            writer.writeheader()
            writer.writerows(VALIDATION_ISSUES)
    except Exception as exc:
        print(f"  (could not write validation CSV: {concise_exc(exc)})")


def raise_if_critical(stage: str):
    """
    Call at the end of each pipeline stage. Flushes the CSV and prints a
    short one-line notice if any CRITICAL issue was logged so far -- but
    NEVER stops the run. Kept under its old name so existing call sites
    don't need to change.
    """
    write_validation_csv()
    criticals = [r for r in VALIDATION_ISSUES if r["severity"] == "CRITICAL"]
    if criticals:
        print(f"  {stage}: {len(criticals)} critical issue(s) logged -> {LOG_FILE.name} (continuing)")


print(f"Validation log : {LOG_FILE}")
print(f"Validation csv : {ISSUE_CSV}")

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────
# _GPS_ALIASES
# ──────────────────────────────────────────────────────────────────────
_GPS_ALIASES = {
    'new nh number': 'NH_Number',
    'new nh\nnumber': 'NH_Number',
    'nh number': 'NH_Number',

    'chainage': 'CHAINAGE',
    'chainage(m)': 'CHAINAGE',
    'chainage (m)': 'CHAINAGE',
    'start chainage': 'CHAINAGE',
    'start chainage(m)': 'CHAINAGE',
    'distance': 'CHAINAGE',
    'distance (m)': 'CHAINAGE',
    'dist': 'CHAINAGE',

    'section code': 'Section_Code',
    'section_code': 'Section_Code',

    'lane number': 'Lane_Number',
    'lane no': 'Lane_Number',
    'lane no.': 'Lane_Number',

    'latitude': 'Latitude',
    'lat': 'Latitude',
    'gps latitude': 'Latitude',
    'latitude(deg)': 'Latitude',
    'latitude (deg)': 'Latitude',

    'longitude': 'Longitude',
    'lon': 'Longitude',
    'long': 'Longitude',
    'gps longitude': 'Longitude',
    'longitude(deg)': 'Longitude',
    'longitude (deg)': 'Longitude',

    'direction': 'Direction',
    'dir': 'Direction',
}

# ──────────────────────────────────────────────────────────────────────
# _CRACK_ALIASES
# ──────────────────────────────────────────────────────────────────────
_CRACK_ALIASES = {
    'chainage': 'CHAINAGE',
    'chainage(m)': 'CHAINAGE',
    'chainage (m)': 'CHAINAGE',

    'length': 'LENGTH',
    'length(mm)': 'LENGTH',
    'length (mm)': 'LENGTH',
    'length mm': 'LENGTH',   # e.g. L1/L2 Zone E: header literally "LENGTH MM" (space, no brackets)

    'width': 'WIDTH',
    'width(mm)': 'WIDTH',
    'width (mm)': 'WIDTH',
    'width mm': 'WIDTH',     # same "<NAME> MM" style as LENGTH MM above

    'depth': 'DEPTH',
    'depth(mm)': 'DEPTH',
    'depth (mm)': 'DEPTH',
    'depth mm': 'DEPTH',     # same "<NAME> MM" style as LENGTH MM above

}

# ──────────────────────────────────────────────────────────────────────
# _ROUGH_ALIASES
# ──────────────────────────────────────────────────────────────────────
_ROUGH_ALIASES = {
    'new nh number': 'NH_Number',
    'new nh\nnumber': 'NH_Number',

    'lwp iri': 'LWP IRI (m/km)',
    'lwp_iri(m/km)': 'LWP IRI (m/km)',
    'lwp_iri': 'LWP IRI (m/km)',

    'rwp iri': 'RWP IRI (m/km)',
    'rwp_iri(m/km)': 'RWP IRI (m/km)',
    'rwp_iri': 'RWP IRI (m/km)',
  
    'lane iri': 'Lane IRI (m/km)',
    'lane_iri(m/km)': 'Lane IRI (m/km)',
    'lane_iri': 'Lane IRI (m/km)',
    'avg iri': 'Lane IRI (m/km)',

    # camelCase / no-separator variant (e.g. Zone E L1/R1 IRI sheets:
    # headers literally "LwpIri", "RwpIri", "LaneIri" -- no space or
    # underscore at all, so none of the keys above ever matched, and
    # these columns silently stayed unrenamed -> "no usable block found".
    'lwpiri': 'LWP IRI (m/km)',
    'rwpiri': 'RWP IRI (m/km)',
    'laneiri': 'Lane IRI (m/km)',

    'roughness index': 'Roughness Index (mm/km)',

    'speed(kmph)': 'Average Speed of NSV',
    'speed': 'Average Speed of NSV',

    # LCMS aggregated block
    'lwp': 'LWP IRI (m/km)',
    'rwp': 'RWP IRI (m/km)',
    'average': 'Lane IRI (m/km)',

    'condition of road surface(as per irc:sp:16-2019':
        'Condition of Road Surface (As per IRC:SP:16-2019)',
    'condition of road surface (as per irc:sp:16-2019':
        'Condition of Road Surface (As per IRC:SP:16-2019)',
    'condition of road surface(as per irc:sp:16-2019)':
        'Condition of Road Surface (As per IRC:SP:16-2019)',
}

# ──────────────────────────────────────────────────────────────────────
# _RUT_ALIASES
# ──────────────────────────────────────────────────────────────────────
_RUT_ALIASES = {
    'new nh number': 'NH_Number',
    'new nh\nnumber': 'NH_Number',

    'lwp_rut_depth(mm)': 'Rutting Left',
    'lwp rut depth(mm)': 'Rutting Left',

    'rwp_rut_depth(mm)': 'Rutting Right',
    'rwp rut depth(mm)': 'Rutting Right',

    # Average
    'average lane rutting (mm)': 'Rutting Average',
    'rutting average': 'Rutting Average',
    'average rutting (mm)': 'Rutting Average',
    'average rut depth (mm)': 'Rutting Average',
    'avg rutting (mm)': 'Rutting Average',
    'avg rut depth (mm)': 'Rutting Average',
    'rut average (mm)': 'Rutting Average',
    'rut avg (mm)': 'Rutting Average',
    'mean rutting (mm)': 'Rutting Average',
    'mean rut depth (mm)': 'Rutting Average',
    'lane average (mm)': 'Rutting Average',
    'lane avg (mm)': 'Rutting Average',

    'average (mm)': 'Rutting Average',
    'avg (mm)': 'Rutting Average',
    'average': 'Rutting Average',
    'avg': 'Rutting Average',

    # Left / Right
    'rutting depth in left wheel path (mm)': 'Rutting Left',
    'rutting depth in  right wheel path (mm)': 'Rutting Right',
    'rutting depth in right wheel path (mm)': 'Rutting Right',

    # LCMS columns
    'lwp': 'Rutting Left',
    'rwp': 'Rutting Right',

    # Raw point sample columns
    'left_depth': 'Rutting Left',
    'right_depth': 'Rutting Right',

    # camelCase / no-separator variant (e.g. Zone E L1/R1 RUT sheets:
    # headers literally "RuttingLeft", "RuttingRight", "RuttingAvg" --
    # same root cause as the LwpIri/RwpIri fix above in _ROUGH_ALIASES).
    'ruttingleft': 'Rutting Left',
    'ruttingright': 'Rutting Right',
    'ruttingavg': 'Rutting Average',
    'ruttingaverage': 'Rutting Average',
}

# ──────────────────────────────────────────────────────────────────────
# _RAVEL_ALIASES
# ──────────────────────────────────────────────────────────────────────
_RAVEL_ALIASES = {
    'chainage': 'Start_Chainage',
    'chainage(m)': 'Start_Chainage',
    'start chainage': 'Start_Chainage',
    'start chainage(m)': 'Start_Chainage',

    'ravelling index (ri)': 'Raveling_RI',
    'raveling index (ri)': 'Raveling_RI',
    'raveling index': 'Raveling_RI',
    'ravelling index': 'Raveling_RI',
    'ri': 'Raveling_RI',

    'area ravelling (sq m)': 'Raveling_Area_m2',
    'area raveling (sq m)': 'Raveling_Area_m2',
    'area of ravelling (m2)': 'Raveling_Area_m2',
    'area of raveling (m2)': 'Raveling_Area_m2',
    'area of raveling': 'Raveling_Area_m2',
    'total area': 'Raveling_Area_m2',

    'survey date': 'survey_date_raveling',
}

# COMMAND ----------

def norm_key(raw) -> str:
    """Normalise a column/table name for matching: strip spaces/underscores, lowercase."""
    return re.sub(r'[\s_]+', '', str(raw)).lower()

# COMMAND ----------

_SCHEMA_HEADER_ALIASES = {
    'tablename': 'Table_Name', 'table': 'Table_Name',
    'sheetname': 'Table_Name', 'sheet': 'Table_Name',
    'columnname': 'Column_Name', 'column': 'Column_Name', 'col': 'Column_Name',
    'datatype': 'Data_Type', 'type': 'Data_Type', 'dtype': 'Data_Type',
}

# COMMAND ----------

def load_schema(schema_file: str) -> pd.DataFrame:
    """
    Load a schema file (.csv or .xlsx/.xls) with columns Table_Name,
    Column_Name, Data_Type (header spelling/case/spacing tolerant — see
    _SCHEMA_HEADER_ALIASES). Adds a normalised _table_key lookup column.
    """
    ext = Path(schema_file).suffix.lower()
    df = pd.read_excel(_long_path(schema_file), dtype=str) if ext in {'.xlsx', '.xls'} \
        else pd.read_csv(_long_path(schema_file), dtype=str)

    rename_map = {c: _SCHEMA_HEADER_ALIASES[norm_key(c)] for c in df.columns if norm_key(c) in _SCHEMA_HEADER_ALIASES}
    df = df.rename(columns=rename_map)
    df.columns = [str(c).strip() for c in df.columns]

    required = ['Table_Name', 'Column_Name', 'Data_Type']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Schema file missing required columns: {missing}. Found: {df.columns.tolist()}")

    for col in required:
        df[col] = df[col].astype(str).str.strip()
    df = df[(df['Table_Name'].str.len() > 0) & (df['Column_Name'].str.len() > 0)].copy()
    df['_table_key'] = df['Table_Name'].apply(norm_key)
    return df

# COMMAND ----------

schema_df = load_schema(SCHEMA_FILE)

# COMMAND ----------

def extract_folder(bridge_dir: str) -> dict:
    """
    Walk an entire bridge folder and extract every Excel and CSV file.

    Routing
    -------
    Summary sheets ('summary' in name)
        → run_d2()        Multiple tables, full edge-case handling.
          Value in dict : list of DataFrames.
    All other xlsx sheets
        → extract_sheet() Auto D1/D3/D4, multi-row headers.
          Value in dict : single DataFrame.
    CSV files
        → extract_sheet() Read exact strings.
          Value in dict : {"data": DataFrame}.

    Returns
    -------
    dict  {
        "Subfolder/file.xlsx": {
            "SheetName" : pd.DataFrame or list[pd.DataFrame],
            ...
        },
        "Subfolder/file.csv": {"data": pd.DataFrame},
        ...
    }
    """
    all_results = {}

    for root, dirs, files in os.walk(bridge_dir):
        dirs.sort()
        xlsx_files = sorted(
            f for f in files
            if f.lower().endswith(".xlsx") and not f.startswith("~$")
        )
        csv_files = sorted(f for f in files if f.lower().endswith(".csv"))

        if not xlsx_files and not csv_files:
            continue

        rel_folder = os.path.relpath(root, bridge_dir)
        print(f"\n{'─'*60}")
        print(f"  Folder : {rel_folder}")

        # ── xlsx ──────────────────────────────────────────────────────────
        for fname in xlsx_files:
            fpath = os.path.join(root, fname)
            key   = os.path.join(rel_folder, fname)
            print(f"\n  File   : {fname}")

            try:
                wb_tmp = openpyxl.load_workbook(_long_path(fpath), read_only=True, data_only=True)
                sheets = wb_tmp.sheetnames
                wb_tmp.close()
            except Exception as e:
                short = concise_exc(e)
                print(f"    ERROR opening workbook: {short}")
                log_issue("WARNING", "Deliverable", f"Could not open workbook: {short}", file=fpath)
                continue

            file_results = {}
            for sheet in sheets:
                print(f"    Sheet : {sheet}")
                try:
                    if "summary" in sheet.strip().lower():
                        # D2: multiple tables, full edge-case handling
                        tables = run_d2(fpath, sheet, verbose=True)
                        file_results[sheet] = tables
                        print(f"      → {len(tables)} table(s) extracted (D2)")
                    else:
                        # D1/D3/D4: single table, multi-row headers
                        df = extract_sheet(fpath, sheet)
                        file_results[sheet] = df
                        print(f"      → {df.shape[0]} rows × {df.shape[1]} cols")
                except Exception as e:
                    short = concise_exc(e)
                    print(f"      ERROR: {short}")
                    log_issue("WARNING", "Deliverable", f"Sheet extraction failed: {short}", file=fpath, table=sheet)
                    file_results[sheet] = pd.DataFrame()

            all_results[key] = file_results

        # ── csv ───────────────────────────────────────────────────────────
        for fname in csv_files:
            fpath = os.path.join(root, fname)
            key   = os.path.join(rel_folder, fname)
            print(f"\n  File   : {fname}  [CSV]")
            try:
                df = extract_sheet(fpath)
                all_results[key] = {"data": df}
                print(f"    → {df.shape[0]} rows × {df.shape[1]} cols")
            except Exception as e:
                short = concise_exc(e)
                print(f"    ERROR: {short}")
                log_issue("WARNING", "Deliverable", f"CSV extraction failed: {short}", file=fpath)
                all_results[key] = {"data": pd.DataFrame()}

    return all_results

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────────────────
# Required files / sheets for this run. Edit the "patterns" lists if a
# project spells these filenames differently.
# ─────────────────────────────────────────────────────────────────────────────

# Deliverable side: file must exist under the given Deliverable-N folder and
# its filename must contain at least one of "patterns" (case-insensitive).
REQUIRED_DELIVERABLE_FILES = {
    # TEMPORARILY DISABLED -- LRMS strict enforcement commented out for now.
    # "D1-LRMS":    {"deliverable": "D1", "patterns": ["lrms"]},

    "D1-RIS":     {"deliverable": "D1", "patterns": ["ris_datatemplate", "ris data", "ris_"]},
    "D4-Pothole": {"deliverable": "D4", "patterns": ["pothole"]},
}

# "Potholes" is NOT in this set -- only these ORIGINAL 5 sheets
# (GPS/Cracking/Roughness/Rutting/Raveling) trigger a CRITICAL when
# entirely missing for a lane. Potholes is still built and written
# every time (D4 when present, D2+D3 fallback otherwise), just not
# held to "missing sheet = CRITICAL" the way these 5 are.
REQUIRED_BACKUP_STAGES = {"GPS_Geometry", "Cracking", "Roughness_IRI", "Rutting", "Raveling"}

# These columns are known to be genuinely absent from many real projects'
# RIS sheets (providers don't consistently supply them) -- demoted to
# WARNING-only whenever a caller opts in via lenient_columns=LENIENT_COLUMNS.
# IMPORTANT: this is only ever passed in from the Deliverable/RIS call site
# (Cell 53) -- the Backup call site (Cell 96) never passes it, so
# End_Latitude/End_Longitude etc. stay fully strict inside Backup, where
# GPS_Geometry/Cracking/Rutting/Raveling genuinely need them.
LENIENT_COLUMNS = {
    # NEW: Source_Subfolder is a data column added by us to
    # every extracted table (records which subfolder/package a
    # row came from) -- not present in any schema entry, so it
    # would otherwise show as "not in schema" even for sheets
    # with require_schema=True.
    "Source_Subfolder",
    "Nhnumber_old",
    "Old NH number StartChainage",
    "Old NH number EndChainage",
    "Old NH number  EndChainage",
    "OldStartChainage",
    "OldEndChainage",
    "Old StartChainage",
    "Old EndChainage",
    "As per Old NH number > Chainage",  # e.g. CarriagewayFurniture -- some
                                         # projects label this raw column
                                         # "...> Chainage" instead of the
                                         # schema's "...> StartChainage",
                                         # so it never matches and shows as
                                         # an unmapped "old chainage" column
    "End Latitude",
    "End Longitude",
    "EndLatitude",
    "EndLongitude",
    "Remarks",  # e.g. CarriagewayFurniture -- not every project fills this in
    "to_ch_km",
    "to_ch_m",  # D4 Potholes -- recurring across many projects' Pothole
                # sheets: a pothole is a spot (point) defect, so the "To"
                # chainage genuinely doesn't exist as a column at all in
                # many source sheets (not just empty values -- the whole
                # column is simply absent). Confirmed on multiple
                # projects' raw files; there's no raw column to alias to,
                # so this can only be resolved by leniency, not a schema row.
}

# Backup-only leniency (passed just to the Backup call sites in Cell 93 --
# LENIENT_COLUMNS above stays Deliverable/RIS-only exactly as before, so
# Cracking/Rutting/Raveling's genuine strictness on End_Latitude/
# End_Longitude/etc. is untouched).
#
# Root cause of the survey-date entries: the code renames each stage's own
# raw 'Survey Date'/'Ravel_Survey_Date' column to a unique output name
# ('survey_date_crack'/'survey_date_rutting'/'survey_date_raveling') so
# they can never collide under norm_key() when merged into
# Unified_Segments -- but your schema file was never updated to match, so
# EVERY run now double-flags this as CRITICAL: the OLD name shows up as
# "missing from source" (schema still expects it, code no longer produces
# it) and the NEW name shows up as "not in schema" (code produces it,
# schema doesn't know it) -- for Rutting, Raveling, Cracking, AND
# Unified_Segments all at once. Listing every old+new name variant here
# stops that noise regardless of which one the schema currently expects.
# avg_corrected_width_mm is included too -- it's a real, intentional
# Cracking/Unified_Segments column that was simply never added to the
# schema file at all, so it always shows as "not in schema".
#
# NOTE: this does NOT fix Roughness_IRI's OWN separate issue -- its
# Table_Name entry in the schema has only its 6 measurement columns and
# was never given the structural/join columns every stage table carries
# (Start_Chainage, End_Chainage, Lane, Start/End_Latitude, Start/
# End_Longitude), so those always show as "not in schema" for Roughness_IRI
# specifically. Adding them to a column-name-only lenient set here would
# also silently weaken Cracking/Rutting/Raveling's genuine strictness on
# those same column names -- the correct fix is adding those 7 columns to
# Roughness_IRI's own row in the schema file, not a code-side workaround.
BACKUP_LENIENT_COLUMNS = {
    "Survey Date", "survey_date_crack", "survey_date_rutting", "survey_date_raveling",
    "Ravel_Survey_Date",
    "avg_corrected_width_mm",
    # Roughness_IRI/Rutting's schema entry expects these 4 columns
    # (inherited from a broader raw-sheet schema), but the ACTUAL stage
    # tables built here never carry them by design -- they never flow
    # into Unified_Segments from these two stages, so they should never
    # be CRITICAL here -- only the genuine measurement columns that DO
    # reach Unified_Segments stay fully strict.
    "NH_Number", "Section_Code", "Direction", "Lane_Number_Original",
    # cracking_image_name/cracking_image_url are NEW columns (D3 image
    # matching onto Cracking's own segments) not present in the existing
    # Cracking schema entry at all -- lenient, same reasoning as
    # avg_corrected_width_mm above.
    "cracking_image_name", "cracking_image_url",
}



def log_schema_mapping_result(summary: dict, stage: str,
                               require_data: bool = False,
                               require_schema: bool = False,
                               lenient_columns: set = None) -> bool:
    """
    Inspect one write_bronze_table() summary dict and log any mapping issue.

    lenient_columns: an optional set of column names that should always be
    logged as WARNING (never CRITICAL), even when require_schema=True.
    Only pass this from call sites where those specific columns are known
    to be genuinely, consistently absent -- e.g. Deliverable/RIS. Leave it
    None (the default) anywhere those same column names must stay fully
    strict, e.g. Backup.
    """
    file_  = summary.get("source_file", "")
    table_ = summary.get("table_name", "")
    had_critical = False
    # NOTE: match lenient_columns with whitespace collapsed/stripped, not
    # exact string equality. updated_schema_MERGED.xlsx has real entries
    # like Column_Name == "End  Longitude" (double space, e.g. under
    # 'Carriageway Type') that don't exact-match a single-spaced
    # "End Longitude" in LENIENT_COLUMNS below -- silently staying
    # CRITICAL instead of the intended WARNING. Collapsing whitespace on
    # both sides fixes this for every current and future entry instead of
    # hand-adding a double-space duplicate of each one (a few of which
    # were already worked around that way further down).
    _norm_ws = lambda s: re.sub(r"\s+", " ", str(s).strip())
    _lenient = {_norm_ws(c) for c in (lenient_columns or set())}

    if summary["status"] == "ERROR":
        # Downgraded to WARNING: a raw extraction/cast crash isn't one of
        # the 6 defined CRITICAL checks (folder presence D1-D4, D1-RIS
        # Carriageway/PavementType columns, Backup GPS/Cracking/
        # Roughness/Rutting/Raveling presence+columns) -- only those stay
        # CRITICAL now, everything else logs as WARNING instead.
        log_issue("WARNING", stage, f"Extraction/cast failed: {summary.get('error')}",
                   file=file_, table=table_)
        return False

    if summary["status"] == "SKIPPED_EMPTY":
        sev = "CRITICAL" if require_data else "WARNING"
        log_issue(sev, stage, "File/sheet found but contains no data.", file=file_, table=table_)
        had_critical = had_critical or (sev == "CRITICAL")

    if not summary.get("schema_matched", False):
        sev = "CRITICAL" if require_schema else "WARNING"
        note = (" -- data was NOT mapped to the schema."
                 if require_schema else
                 " (expected -- this table has no Schema.xlsx entry by design; "
                 "kept via auto dtype-inference instead).")
        log_issue(sev, stage, f"No matching Table_Name for '{table_}' in Schema.xlsx{note}",
                   file=file_, table=table_)
        had_critical = had_critical or (sev == "CRITICAL")
        return had_critical

    for col in summary.get("col_info", []):
      for w in col.get("warnings", []):
        sev = "WARNING"
        # Both "missing from source" (a schema column absent from this
        # file's data) and "not in schema" (a column present in the data
        # that ISN'T in Schema.xlsx -- i.e. a brand new/unexpected column)
        # now follow the SAME severity rule: lenient_columns always stay
        # WARNING (a known, accepted gap); otherwise CRITICAL exactly
        # where require_schema=True (Backup; D4-Pothole; D1-RIS sheets
        # matching STRICT_RIS_SHEET_KEYWORDS, i.e. Carriage*/PavementType),
        # WARNING everywhere else. This makes a newly-appeared column just
        # as visible/loud as a disappeared one in those strict tables --
        # previously "not in schema" was unconditionally WARNING even in
        # Backup/D4-Pothole/strict-RIS, so a brand new column could slip
        # in there unnoticed.
        if "missing from source" in w or "not in schema" in w:
            if _norm_ws(col["column_name"]) in _lenient:
                sev = "WARNING"
            else:
                sev = "CRITICAL" if require_schema else "WARNING"
        _src = col.get("source_ref", "")
        msg = f"{w} [{_src}]" if _src else w
        log_issue(sev, stage, msg, file=file_, table=table_, column=col["column_name"])
        had_critical = had_critical or (sev == "CRITICAL")

    return had_critical

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Deliverable Bronze (D1–D4)
# MAGIC
# MAGIC Reuses `NSV_Extraction_Final.extract_folder()` (multi-row header detection, D2 summary-table handling) per project, tags each sheet with its Deliverable number from the folder path, casts dtypes automatically, and writes partitioned parquet.

# COMMAND ----------

# MAGIC %md
# MAGIC This code finds all the project folders inside the DELIVERABLE_ROOT directory, optionally limits how many projects to process, and prints their names.

# COMMAND ----------

# Detect which of two real-world layouts DELIVERABLE_ROOT uses, since both
# occur in practice and were being treated identically before:
#
#   Layout A (package layer):   DELIVERABLE_ROOT/DME Pkg-1/Deliverable - 1/...
#                                DELIVERABLE_ROOT/DME Pkg-2/Deliverable - 1/...
#     -> each immediate child of DELIVERABLE_ROOT is its own project
#        (package), with "Deliverable - N" folders nested one level inside
#        each. deliverable_projects = the package folders themselves.
#
#   Layout B (no package layer): DELIVERABLE_ROOT/Deliverable - 1/...
#                                 DELIVERABLE_ROOT/Deliverable - 2/...
#     -> there is only ONE project overall, and "Deliverable - N" folders
#        sit directly under DELIVERABLE_ROOT with no package folder in
#        between. deliverable_projects = [DELIVERABLE_ROOT] itself.
#
# FIX: previously always assumed Layout A (every immediate child treated
# as its own project). Under Layout B this treated each "Deliverable - N"
# folder as its OWN separate project -- 3 separate projects instead of 1
# -- which then broke the D1-D4 completeness check further below in this
# notebook: walking e.g. "Deliverable - 1"'s own subtree for a nested
# folder NAMED "Deliverable - 1" obviously finds nothing (that name IS
# the folder itself, not a descendant of it), so every one of the 3
# mis-identified "projects" got a false CRITICAL for all 4 deliverables --
# exactly the pattern seen in a real run: "No 'Deliverable - 1/2/3/4'
# folder found" logged three times over, once per Deliverable-N folder.
#
# Detected via normalize_deliverable() (Cell 18's fuzzy/typo-tolerant
# matcher) on each immediate child's OWN name -- if ANY of them already
# normalizes to "Deliverable - N" by itself, Layout B is in play for the
# whole root (a real project doesn't mix the two conventions).
_root_children = sorted(p for p in Path(DELIVERABLE_ROOT).iterdir() if p.is_dir())
_direct_deliverable_children = [p for p in _root_children if normalize_deliverable(p.name)]

# HAS_PACKAGE_LAYER: True only for Layout A (real package subfolders
# genuinely exist). Used everywhere a "Project=" partition might get
# added -- per instruction, "Project" should ONLY ever appear when a
# package layer genuinely exists; Layout B (single project, no package
# subfolder at all) must never get a "Project=<generic root name>"
# wrapper, since there's no real package identity to record there.
if _direct_deliverable_children:
    deliverable_projects = [Path(DELIVERABLE_ROOT)]
    HAS_PACKAGE_LAYER = False
    print(f"Layout B detected: 'Deliverable - N' folders sit directly under "
          f"DELIVERABLE_ROOT (no package layer) -- treating DELIVERABLE_ROOT "
          f"itself as one project. No 'Project=' partition will be created.")
else:
    deliverable_projects = _root_children
    HAS_PACKAGE_LAYER = True
    print(f"Layout A detected: DELIVERABLE_ROOT's immediate children are "
          f"package folders, each with its own nested 'Deliverable - N' "
          f"subfolders. 'Project=' partition will be created per package.")

if LIMIT_DELIVERABLE_PROJECTS:
    deliverable_projects = deliverable_projects[:LIMIT_DELIVERABLE_PROJECTS]

print(f"{len(deliverable_projects)} project folder(s) under Deliverable root:")
for p in deliverable_projects:
    print(" ", p.name)

if not deliverable_projects:
    log_issue("WARNING", "Deliverable",
               "No project folders found under DELIVERABLE_ROOT.", file=str(DELIVERABLE_ROOT))
    raise_if_critical("Deliverable Bronze - project discovery")


# COMMAND ----------

# Bare "D1".."D4" as a standalone token (e.g. "D3", "D-3", "D_3") --
# fallback ONLY for names that don't contain any word resembling
# "deliverable"/"deliverables" at all (see normalize_deliverable below).
_DELIV_BARE_RE = re.compile(r"(?<![A-Za-z0-9])D[-_\s]?([1-4])(?![A-Za-z0-9])", re.IGNORECASE)
_DELIVERABLE_WORD_FUZZY_THRESHOLD = 75


def normalize_deliverable(text: str) -> str:
    """
    Normalise any spelling/format variant of a 'Deliverable N' folder or
    file name -> 'deliverable-N'. '' if it doesn't look like one at all.

    A single rigid regex (the word "deliverable", then optional
    separators, then a digit right after) only matches the tidiest
    cases -- real project folder trees have included, among others:
    "Deliverable - 3" (tidy), "1.Deliverable" (digit BEFORE the word,
    not after), "3. Deliverables -3 - GIS Mapping" (plural "Deliverables"
    with other characters before the digit), "4 .Deliverbales - 4 - NH
    ONE Platform" (typo: "Deliverbales", not "Deliverables" -- the
    literal substring "deliverable" isn't even present). A previous
    version of this function returned '' for every single one of these
    except the first, which cascaded into "whole deliverable missing"
    AND "required file not found" both firing incorrectly for
    deliverables that were actually present.

    This instead: (a) fuzzy-checks whether ANY alphabetic chunk of the
    name resembles "deliverable"/"deliverables" closely enough, regardless
    of exact spelling or where it sits relative to the digit, and (b) if
    so, pulls out the single distinct digit 1-4 found anywhere in the
    name (folders legitimately repeat the same digit more than once, e.g.
    "3. ...-3 - ...", which is fine -- only genuinely AMBIGUOUS names with
    2+ different candidate digits are left unresolved, returning '').
    Falls back to bare "D1".."D4" matching only when no deliverable-like
    word is present at all.
    """
    text = str(text)
    words = [w for w in re.split(r"[^A-Za-z]+", text) if w]
    has_deliverable_word = any(
        fuzz.ratio(w.lower(), "deliverable") >= _DELIVERABLE_WORD_FUZZY_THRESHOLD
        or fuzz.ratio(w.lower(), "deliverables") >= _DELIVERABLE_WORD_FUZZY_THRESHOLD
        for w in words
    )
    if not has_deliverable_word:
        m = _DELIV_BARE_RE.search(text)
        return f"deliverable-{m.group(1)}" if m else ""

    digits = {d for d in re.findall(r"\d", text) if d in "1234"}
    return f"deliverable-{digits.pop()}" if len(digits) == 1 else ""


def extract_deliverable_from_path(file_path: str) -> str:
    """Walk path parts and return the normalised deliverable string, or ''."""
    for part in Path(file_path).parts:
        nd = normalize_deliverable(part)
        if nd:
            return nd
    return ""

# COMMAND ----------

def run_d4(file_path: str, sheet_name: str, min_fill_ratio: float = 0.02) -> pd.DataFrame:
    """
    Extract a non-summary Excel sheet into a single DataFrame.

    Enhancements vs. basic D4:
      • Auto-detects 1/2/3 header rows (multi-row header support)
      • Compound column names when parent headers exist
      • '+' sign format preserved from Excel number formats
      • D1 fallback when fill-ratio filter yields empty result

    Returns a single pd.DataFrame.
    """
    wb         = _load_workbook_cached(file_path)
    ws         = wb[sheet_name]
    merged_map = _build_merged_map(ws)
    # Pre-load all cells into a fast lookup cache (3–5× speedup for large sheets)
    cache      = _preload_sheet(ws, merged_map)
    # ws.max_column/max_row are NOT cached properties on a non-read-only
    # openpyxl Worksheet — each access rescans the sheet's cells (~20ms on a
    # 16k-row sheet). Hoisted once here; the loops below used to call
    # ws.max_column fresh on every row, turning an O(rows) pass into an
    # O(rows × cells) one (minutes instead of milliseconds on large sheets).
    max_col    = ws.max_column
    max_row    = ws.max_row

    header_rows  = _detect_header_block(ws, merged_map)
    last_header  = header_rows[-1]
    first_header = header_rows[0]

    # ── Column detection: use FIRST header row, not LAST ─────────────────────
    # Some sheets have a "description/legend" row as the final detected header
    # (e.g. NHAI Potholes: row 1 = column names, row 2 = yellow explanation).
    # That description row is blank for cols UPC / Datetime / NH No. / Chainage,
    # so _find_data_groups(header_row=last_header) silently drops those columns.
    # Using first_header (the real column-name row) captures every named column.
    # Data extraction still starts at last_header + 1 → description row excluded.
    # ─────────────────────────────────────────────────────────────────────────

    # For RIS_DataTemplate: pass all detected header rows so that columns
    # labelled only in the leaf header row (row 3) — with rows 1 & 2 blank —
    # are not silently dropped.  Has no effect on any other file type.
    _fname       = os.path.basename(file_path)
    _ris_headers = header_rows if "RIS_DataTemplate" in _fname else None

    # Attempt 1 — D2/D3/D4: fill-ratio group filter
    groups   = _find_data_groups(ws, merged_map,
                                 header_row=first_header,
                                 all_header_rows=_ris_headers,
                                 min_fill_ratio=min_fill_ratio,
                                 cell_cache=cache)
    all_cols = [c for s, e in groups for c in range(s, e + 1)]
    headers  = _build_column_names(ws, merged_map, header_rows, all_cols)
    rows = [
        [cache.get((r, c)) for c in all_cols]
        for r in range(last_header + 1, max_row + 1)
        if any(cache.get((r, c)) is not None for c in all_cols)
        and not _is_trailing_note_row(ws, merged_map, r, all_cols,
                                      max_col, cache)
        and not _is_section_break_row(ws, merged_map, r, all_cols, cache)
    ]
    df = pd.DataFrame(rows, columns=headers)
    if not df.empty:
        return df

    # Fallback — D1: no fill-ratio gate
    # Union columns from ALL detected header rows so that zero-data template
    # sheets with multi-level headers (e.g. SkidResistance rows [1,2,3])
    # produce the full expected column set, not just the leaf header row.
    all_cols_set: set = set()
    for hr in header_rows:
        for c in range(1, max_col + 1):
            if cache.get((hr, c)) is not None:
                all_cols_set.add(c)
    all_cols = sorted(all_cols_set)
    if not all_cols:
        return pd.DataFrame()

    headers = _build_column_names(ws, merged_map, header_rows, all_cols)
    rows = [
        [cache.get((r, c)) for c in all_cols]
        for r in range(last_header + 1, max_row + 1)
        if any(cache.get((r, c)) is not None for c in all_cols)
        and not _is_trailing_note_row(ws, merged_map, r, all_cols,
                                      max_col, cache)
        and not _is_section_break_row(ws, merged_map, r, all_cols, cache)
    ]
    return pd.DataFrame(rows, columns=headers)

# COMMAND ----------

def extract_sheet(file_path: str, sheet_name: str = None,
                  min_fill_ratio: float = 0.02) -> pd.DataFrame:
    """
    Extract one sheet (xlsx) or file (csv) into a DataFrame.
    Supports D1, D2, D3, D4 — no manual routing needed.

    Multi-row header support:
      Auto-detects 1, 2, or 3 header rows.
      Merged parent headers become a prefix in the column name:
        'As per Old NH number > StartChainage'

    xlsx — two attempts:
      Attempt 1 : detect header block → fill-ratio group filter (D2/D3/D4)
      Fallback  : same header block   → no fill-ratio gate          (D1)

    All values exact — no type coercion, '+' formats preserved.
    csv — read with dtype=str for exact values.
    """
    ext = os.path.splitext(file_path)[1].lower()

    # ── CSV ────────────────────────────────────────────────────────────────
    if ext == ".csv":
        df = pd.read_csv(_long_path(file_path), dtype=str)
        print(f"      [CSV] {df.shape[0]} rows × {df.shape[1]} cols")
        return df

    # ── xlsx ───────────────────────────────────────────────────────────────
    wb = _load_workbook_cached(file_path)
    if sheet_name not in wb.sheetnames:
        raise ValueError(
            f"Sheet '{sheet_name}' not found. Available: {wb.sheetnames}"
        )
    ws         = wb[sheet_name]
    merged_map = _build_merged_map(ws)
    # Pre-load all cells into a fast lookup cache (3–5× speedup for large sheets)
    cache      = _preload_sheet(ws, merged_map)
    # Hoisted once — see the comment in run_d4() above for why.
    max_col    = ws.max_column
    max_row    = ws.max_row

    header_rows  = _detect_header_block(ws, merged_map)
    last_header  = header_rows[-1]
    first_header = header_rows[0]
    hdr_lbl      = (f"rows {header_rows}" if len(header_rows) > 1
                    else f"row {last_header}")

    # Pass all detected header rows (not just the first) so that columns
    # labelled only in the LEAF header row -- with the row(s) above them
    # blank for that column -- are not silently dropped. Per this
    # function's own docstring, this "has no effect on any other file
    # type" (single-header-row sheets only ever have one row to draw
    # labels from anyway), so it's applied unconditionally rather than
    # gated on the file's own name.
    #
    # BUG HISTORY: this used to be gated on `"RIS_DataTemplate" in
    # filename` (then later on a normalised substring match to tolerate
    # spacing/underscore variants like "RIS_Data_Template_v1_01.xlsx") --
    # but real D1 source files are frequently named after the ROAD, not
    # the template (e.g. "RIS Dhankuni to Kharaghpur.xlsx" has no
    # resemblance to "RIS_DataTemplate" at all), so any filename-based
    # check was always going to miss some real file eventually. Any
    # column whose text lives ONLY in the leaf header row (blank above it
    # -- e.g. bare "SurveyDate", "SectionCode", "CarriagewayType",
    # "Latitude"/"Longitude"/"Altitude" in CarriagewayFurniture) was
    # silently dropped from extraction entirely -- not just null,
    # structurally ABSENT -- which is what was actually causing those
    # "missing from source" CRITICAL errors (a real extraction bug, not a
    # schema gap). Making this unconditional removes the filename-pattern
    # guessing game entirely.
    _ris_hdrs2 = header_rows

    # Attempt 1 — D2/D3/D4
    # Use first_header for column detection (same reason as run_d4 above).
    groups   = _find_data_groups(ws, merged_map,
                                 header_row=first_header,
                                 all_header_rows=_ris_hdrs2,
                                 min_fill_ratio=min_fill_ratio,
                                 cell_cache=cache)
    all_cols = [c for s, e in groups for c in range(s, e + 1)]
    headers  = _build_column_names(ws, merged_map, header_rows, all_cols)
    rows = [
        [cache.get((r, c)) for c in all_cols]
        for r in range(last_header + 1, max_row + 1)
        if any(cache.get((r, c)) is not None for c in all_cols)
        and not _is_trailing_note_row(ws, merged_map, r, all_cols,
                                      max_col, cache)
        and not _is_section_break_row(ws, merged_map, r, all_cols, cache)
    ]
    df = pd.DataFrame(rows, columns=headers)

    if not df.empty:
        print(f"      [D2/D3/D4] header {hdr_lbl} | "
              f"{df.shape[0]} rows × {df.shape[1]} cols")
        return df

    # Fallback — D1: no fill-ratio gate
    # Union columns from ALL detected header rows (same fix as run_d4).
    all_cols_set2: set = set()
    for hr in header_rows:
        for c in range(1, max_col + 1):
            if cache.get((hr, c)) is not None:
                all_cols_set2.add(c)
    all_cols = sorted(all_cols_set2)
    if not all_cols:
        return pd.DataFrame()

    headers = _build_column_names(ws, merged_map, header_rows, all_cols)
    rows = [
        [cache.get((r, c)) for c in all_cols]
        for r in range(last_header + 1, max_row + 1)
        if any(cache.get((r, c)) is not None for c in all_cols)
        and not _is_trailing_note_row(ws, merged_map, r, all_cols,
                                      max_col, cache)
        and not _is_section_break_row(ws, merged_map, r, all_cols, cache)
    ]
    df = pd.DataFrame(rows, columns=headers)
    print(f"      [D1 fallback] header {hdr_lbl} | "
          f"{df.shape[0]} rows × {df.shape[1]} cols")
    return df

# COMMAND ----------

def build_output_dir(source: str, run_date: _dt.date = None,
                     zone: str = None, phase: str = None,
                     cycle: str = None, upc: str = None,
                     extra_partitions: dict = None) -> Path:
    """
    Data sources (NSV, NSV_Backup, Dashcam, DAMS):
      <OUTPUT_ROOT>/Source=<src>/Zone=/Phase=/Cycle=/UPC=/year=/month=/day=/[extras...]
    Meta outputs (source starting with '_', e.g. _summary, _manifest, _excel_export):
      <OUTPUT_ROOT>/General/<_name>/Zone=/Phase=/Cycle=/UPC=/year=/month=/day=/

    Zone/Phase/Cycle/UPC default to the globals parsed by parse_nsv_root(),
    so a new Phase/Cycle/UPC in the input roots automatically lands in the
    matching output partition. extra_partitions (insertion-ordered) are
    appended AFTER the date — e.g. Deliverable= for NSV, or
    Project=/Pavement=/Variant=/Lane= for NSV_Backup.
    """
    run_date = run_date or RUN_DATE
    root = Path(OUTPUT_ROOT)
    folder = (root / "General" / source) if source.startswith("_") else (root / f"Source={source}")
    folder = (folder
              / f"Zone={zone or ZONE}"
              / f"Phase={phase or PHASE}"
              / f"Cycle={cycle or CYCLE}"
              / f"UPC={upc or UPC}"
              / f"year={run_date.year}"
              / f"month={run_date.month:02d}"
              / f"day={run_date.day:02d}")
    for k, v in (extra_partitions or {}).items():
        folder = folder / f"{k}={slugify(v)}"
    os.makedirs(_long_path(str(folder)), exist_ok=True)
    return folder


def build_bronze_path(output_root: str, source: str, project: str,
                       extra_partitions: dict, table_name: str, run_date: _dt.date,
                       zone: str = None, upc: str = None, phase: str = None,
                       cycle: str = None) -> Path:
    """
    New layout (see build_output_dir). `project` is no longer a path level —
    it is kept in the signature only for the pipeline summary rows.
    The filename is prefixed with Z<zone>_P<phase>_C<cycle>_<UPC>_.
    """
    folder = build_output_dir(source, run_date, zone=zone, phase=phase,
                              cycle=cycle, upc=upc, extra_partitions=extra_partitions)
    prefix = f"Z{zone or ZONE}_P{phase or PHASE}_C{cycle or CYCLE}_{upc or UPC}"
    return folder / f"{prefix}_{table_name}.parquet"

# COMMAND ----------

def parse_chainage(val):
    """Parse '0+000' notation to float km; pass through plain numerics; else None-safe."""
    if val is None:
        return None
    if isinstance(val, str):
        val_s = val.strip()
        if '+' in val_s:
            try:
                whole, frac = val_s.split('+', 1)
                return float(whole) + float(frac) / 1000.0
            except (ValueError, TypeError):
                return val
        try:
            return float(val_s)
        except (ValueError, TypeError):
            return val
    return val

# COMMAND ----------




_CHAINAGE_KEYWORDS = frozenset({
    'chainage', 'ch.', 'from km', 'to km',
    'start ch', 'end ch', 'ch from', 'ch to',
    'from chainage', 'to chainage',
})

_BOOL_MAP = {
    'yes': True, 'no': False,
    'true': True, 'false': False,
    'y': True, 'n': False,
    '1': True, '0': False,
}


def infer_and_cast(col_name: str, series: pd.Series):
    """
    Infer the best dtype for one column and return (dtype_label, cast_series).
    dtype_label in {'chainage','datetime','integer','float','boolean','text','empty'}.
    """
    non_null = series.dropna()
    if len(non_null) == 0:
        return 'empty', series.astype(object)

    col_lower = str(col_name).lower()

    if any(kw in col_lower for kw in _CHAINAGE_KEYWORDS):
        cast = series.apply(lambda x: parse_chainage(x) if x is not None else None)
        try:
            return 'chainage', cast.astype('Float64')
        except (TypeError, ValueError):
            pass

    if any(isinstance(v, (_dt.datetime, _dt.date)) for v in non_null):
        return 'datetime', pd.to_datetime(series, errors='coerce')

    native_num = [v for v in non_null if isinstance(v, (int, float)) and not isinstance(v, bool)]
    if len(native_num) == len(non_null):
        all_int = all(isinstance(v, int) or (isinstance(v, float) and v == int(v)) for v in native_num)
        try:
            if all_int:
                arr = [int(v) if v is not None else pd.NA for v in series]
                return 'integer', pd.Series(pd.array(arr, dtype='Int64'), index=series.index, name=series.name)
            arr = [float(v) if v is not None else pd.NA for v in series]
            return 'float', pd.Series(pd.array(arr, dtype='Float64'), index=series.index, name=series.name)
        except (TypeError, ValueError, OverflowError):
            pass

    try:
        numeric = pd.to_numeric(series, errors='raise')
        if (numeric.notna() == series.notna()).all():
            if (numeric.dropna() % 1 == 0).all():
                return 'integer', numeric.astype('Int64')
            return 'float', numeric.astype('Float64')
    except (TypeError, ValueError):
        pass

    str_lowers = [str(v).strip().lower() for v in non_null]
    if all(v in _BOOL_MAP for v in str_lowers):
        cast = series.apply(lambda x: _BOOL_MAP.get(str(x).strip().lower()) if x is not None else None)
        try:
            return 'boolean', cast.astype('boolean')
        except (TypeError, ValueError):
            pass

    cast = series.apply(lambda x: str(x).strip() if x is not None else None)
    return 'text', cast


def cast_dataframe(df: pd.DataFrame):
    """Apply infer_and_cast to every column. Returns (typed_df, col_info: list[dict])."""
    # A handful of source sheets (e.g. "Table List", wide CA-comparison sheets)
    # repeat a column label more than once. df[col] on a duplicate label
    # returns a DataFrame instead of a Series, which breaks infer_and_cast.
    # De-duplicate by position first so every column is addressed uniquely.
    if df.columns.duplicated().any():
        seen: dict = {}
        new_cols = []
        for c in df.columns:
            seen[c] = seen.get(c, 0) + 1
            new_cols.append(c if seen[c] == 1 else f"{c}_{seen[c]}")
        df = df.copy()
        df.columns = new_cols

    typed_cols = {}
    col_info = []
    for i, col in enumerate(df.columns):
        series = df.iloc[:, i]
        dtype_label, cast_series = infer_and_cast(col, series)
        typed_cols[col] = cast_series
        samples = series.dropna().head(3).tolist()
        col_info.append({
            'column_name': col,
            'inferred_dtype': dtype_label,
            'null_count': int(series.isna().sum()),
            'non_null_count': int(series.notna().sum()),
            'sample_values': ' | '.join(str(v) for v in samples),
        })
    typed_df = pd.DataFrame(typed_cols, index=df.index)
    return typed_df, col_info


# ── Schema-driven casting (explicit Table_Name/Column_Name/Data_Type) ──────────
# Ported from nsv_backup_bronze_layer.py's schema engine. Used instead of
# infer_and_cast()/cast_dataframe() (auto dtype-inference) when a schema
# file is supplied — matched columns are cast to the declared type, missing
# schema columns are added as typed nulls, and columns present in the data
# but absent from the schema are kept as string (flagged, never dropped).

_SCHEMA_STRING_TYPES = {'string', 'str', 'varchar', 'text', 'object'}
_SCHEMA_INT_TYPES    = {'int', 'int64', 'integer', 'long', 'bigint', 'smallint'}
_SCHEMA_FLOAT_TYPES  = {'float', 'float64', 'double', 'decimal', 'numeric', 'real'}
_SCHEMA_BOOL_TYPES   = {'bool', 'boolean'}
_SCHEMA_DATE_TYPES   = {'date'}
_SCHEMA_TS_TYPES     = {'timestamp', 'datetime'}

_SCHEMA_BOOL_MAP = {
    'true': True, 'false': False, '1': True, '0': False,
    'yes': True, 'no': False, 'y': True, 'n': False, 't': True, 'f': False,
}




# COMMAND ----------

def target_pandas_dtype(dtype: str) -> str:
    """Pandas dtype string an empty/missing schema column should use."""
    d = str(dtype).lower().strip()
    if d in _SCHEMA_INT_TYPES:
        return 'Int64'
    if d in _SCHEMA_FLOAT_TYPES:
        return 'Float64'
    if d in _SCHEMA_BOOL_TYPES:
        return 'boolean'
    if d in _SCHEMA_DATE_TYPES or d in _SCHEMA_TS_TYPES:
        return 'datetime64[ns]'
    return 'string'


def cast_series_with_schema(series: pd.Series, dtype: str):
    """Cast a Series to a schema Data_Type. Returns (cast_series, warnings). Never raises."""
    d = str(dtype).lower().strip()
    warnings = []
    original_nulls = series.isna().sum()

    try:
        if d in _SCHEMA_STRING_TYPES:
            return series.astype('string'), warnings

        if d in _SCHEMA_INT_TYPES:
            casted = pd.to_numeric(series, errors='coerce')
            new_nulls = casted.isna().sum() - original_nulls
            if new_nulls > 0:
                warnings.append(f"{new_nulls} value(s) not parseable as {dtype} -> null")
            return casted.round().astype('Int64'), warnings

        if d in _SCHEMA_FLOAT_TYPES:
            casted = pd.to_numeric(series, errors='coerce').astype('Float64')
            new_nulls = casted.isna().sum() - original_nulls
            if new_nulls > 0:
                warnings.append(f"{new_nulls} value(s) not parseable as {dtype} -> null")
            return casted, warnings

        if d in _SCHEMA_BOOL_TYPES:
            casted = series.astype('string').str.strip().str.lower().map(_SCHEMA_BOOL_MAP).astype('boolean')
            unmapped = casted.isna().sum() - original_nulls
            if unmapped > 0:
                warnings.append(f"{unmapped} value(s) not mappable as boolean -> null")
            return casted, warnings

        if d in _SCHEMA_DATE_TYPES or d in _SCHEMA_TS_TYPES:
            casted = pd.to_datetime(series, errors='coerce', dayfirst=True)
            new_nulls = casted.isna().sum() - original_nulls
            if new_nulls > 0:
                warnings.append(f"{new_nulls} value(s) not parseable as {dtype} -> null")
            return casted, warnings

        warnings.append(f"Unknown Data_Type '{dtype}' -> kept as string")
        return series.astype('string'), warnings

    except Exception as exc:
        warnings.append(f"Cast error: {concise_exc(exc)} -> kept as string")
        return series.astype('string'), warnings

# COMMAND ----------

def cast_dataframe_with_schema(df: pd.DataFrame, table_key: str, schema_df: pd.DataFrame):
    """
    Schema-driven counterpart to cast_dataframe(). Looks up table_key in
    schema_df (normalised match); matched columns are cast to their
    declared Data_Type (schema column order), missing schema columns are
    added as typed nulls, extra data columns are kept as string.

    Returns (typed_df, col_info, matched: bool). When matched is False (no
    schema rows for this table_key), typed_df/col_info are None — callers
    should fall back to cast_dataframe() (auto-inference).
    """
    tk = norm_key(table_key)
    table_schema = schema_df[schema_df['_table_key'] == tk]
    if table_schema.empty:
        return None, None, False

    source_ref = ""
    _r = table_schema.iloc[0]
    _parts = [str(_r.get('Source', '') or ''), str(_r.get('File_Name', '') or '')]
    source_ref = " | ".join(p for p in _parts if p)

    if df.columns.duplicated().any():
        seen = {}
        new_cols = []
        for c in df.columns:
            seen[c] = seen.get(c, 0) + 1
            new_cols.append(c if seen[c] == 1 else f"{c}_{seen[c]}")
        df = df.copy()
        df.columns = new_cols

    schema_map = {}
    for _, row in table_schema.iterrows():
        # Match on the RAW/extracted header text; Column_Name is only the
        # desired OUTPUT name -- never used for matching.
        schema_map[norm_key(row['Extracted_Column_Name'])] = (str(row['Column_Name']).strip(), str(row['Data_Type']).strip())

    # Build norm_key -> original-column-name map WITHOUT silently letting a
    # later column overwrite an earlier one when two differently-named data
    # columns normalise to the same key (e.g. 'survey_date' and 'Survey
    # Date' both -> 'surveydate'). Keep the FIRST (leftmost) occurrence
    # deterministically and log a WARNING for every collision so it's
    # visible in the run log instead of silently dropping a column's data
    # -- this bit the Backup pipeline for Cracking/Rutting's differently-
    # cased survey-date columns inside Unified_Segments (now renamed to
    # 'survey_date_crack'/'survey_date_rutting'/'survey_date_raveling' precisely to avoid
    # this), but the guard is kept generic so it protects any future
    # same-normalised-key collision across ANY two source columns too.
    data_col_norm = {}
    for c in df.columns:
        k = norm_key(c)
        if k in data_col_norm:
            log_issue("WARNING", "Schema",
                       f"Column name collision on normalised key '{k}': "
                       f"'{data_col_norm[k]}' and '{c}' both map to it -- "
                       f"keeping '{data_col_norm[k]}', '{c}' excluded from "
                       f"schema-driven casting to avoid silently overwriting it.",
                       table=table_key)
            continue
        data_col_norm[k] = c
    data_cols   = set(data_col_norm.keys())
    schema_cols = set(schema_map.keys())
    matched_norm = data_cols & schema_cols
    missing_cols = schema_cols - data_cols
    extra_norm   = data_cols - schema_cols

    out_frames = {}
    col_info = []
    # FIX 1: iterate schema rows directly and match on the RAW key (not on
    # norm_key(Column_Name), which almost never matches the raw data key).
    # FIX 2: track which canonical output columns already got matched via
    # ANY of their raw-name variants, so an unused alternate variant isn't
    # separately flagged as "missing" once another variant matched fine.
    seen_canon = set()
    for _, row in table_schema.iterrows():
        raw_nk = norm_key(row['Extracted_Column_Name'])
        canon_col = str(row['Column_Name']).strip()
        if canon_col in seen_canon:
            continue
        if raw_nk not in matched_norm:
            continue
        orig_col = data_col_norm[raw_nk]
        _, dtype = schema_map[raw_nk]
        casted, warns = cast_series_with_schema(df[orig_col], dtype)
        out_frames[canon_col] = casted.reset_index(drop=True)
        samples = df[orig_col].dropna().head(3).tolist()
        col_info.append({
            'column_name': canon_col, 'inferred_dtype': dtype.lower(),
            'null_count': int(df[orig_col].isna().sum()), 'non_null_count': int(df[orig_col].notna().sum()),
            'sample_values': ' | '.join(str(v) for v in samples), 'warnings': warns,
            'source_ref': source_ref,
        })
        seen_canon.add(canon_col)

    for nk in missing_cols:
        canon_col, dtype = schema_map[nk]
        if canon_col in seen_canon:
            # already matched via a different raw-name variant -- not
            # actually missing, just this particular variant wasn't present.
            continue
        out_frames[canon_col] = pd.Series([pd.NA] * len(df), dtype=target_pandas_dtype(dtype), name=canon_col)
        col_info.append({
            'column_name': canon_col, 'inferred_dtype': dtype.lower(),
            'null_count': len(df), 'non_null_count': 0,
            'sample_values': '', 'warnings': [f"missing from source -> null column ({dtype})"],
            'source_ref': source_ref,
        })
        seen_canon.add(canon_col)

    for nk in sorted(extra_norm):
        orig_col = data_col_norm[nk]
        out_frames[orig_col] = df[orig_col].astype('string').reset_index(drop=True)
        samples = df[orig_col].dropna().head(3).tolist()
        col_info.append({
            'column_name': orig_col, 'inferred_dtype': 'text',
            'null_count': int(df[orig_col].isna().sum()), 'non_null_count': int(df[orig_col].notna().sum()),
            'sample_values': ' | '.join(str(v) for v in samples), 'warnings': ['not in schema -> string'],
            'source_ref': source_ref,
        })

    typed_df = pd.DataFrame(out_frames)
    return typed_df, col_info, True

# COMMAND ----------


def write_bronze_table(df: pd.DataFrame, output_root: str, source: str, project: str,
                        extra_partitions: dict, table_name: str, run_date: _dt.date,
                        source_file: str = "", schema_df: pd.DataFrame = None,
                        table_key: str = None, zone: str = None, upc: str = None,
                        phase: str = None, cycle: str = None) -> dict:
    """
    Cast df and write it to the bronze partition layout.

    If schema_df is supplied (see load_schema()) and table_key (defaults to
    table_name) has rows in it, casting is schema-driven — explicit
    Table_Name/Column_Name/Data_Type, matching nsv_backup_bronze_layer.py's
    engine. Otherwise falls back to auto dtype-inference (cast_dataframe()),
    so tables not yet in the schema don't break the run.

    table_key vs table_name: table_name still drives the output *filename*
    (kept unique per source file, e.g. 'LRMS_DataTemplate v1.01_GPS'), while
    table_key is the schema *lookup* key (e.g. bare sheet name 'GPS') — the
    same logical sheet is written by many differently-named source files
    across projects, and the schema should match all of them.

    zone/upc/phase/cycle are optional partition levels inserted between
    Source= and Project= (see build_bronze_path). Leave them unset (None)
    for sources that don't use the Zone/UPC/Phase/Cycle hierarchy.

    Returns a summary record dict (one row for the pipeline_summary table).
    """
    summary = {
        'source_file': source_file,
        'source': source,
        'zone': zone,
        'upc': upc,
        'phase': phase,
        'cycle': cycle,
        'project': project,
        **extra_partitions,
        'table_name': table_name,
        'raw_rows': len(df),
        'raw_columns': len(df.columns),
        'status': 'SKIPPED_EMPTY',
        'output_path': '',
        'schema_matched': False,
        'error': '',
    }
    if df is None or df.empty:
        return summary

    try:
        typed_df = None
        if schema_df is not None:
            typed_df, col_info, matched = cast_dataframe_with_schema(df, table_key or table_name, schema_df)
            summary['schema_matched'] = matched
        if typed_df is None:
            typed_df, col_info = cast_dataframe(df)

        # No fillna(0) here -- removed. Nulls are left exactly as they
        # come out of casting/auto-inference. (An earlier zero-fill of
        # certain measurement columns was also the root cause of Cracking/
        # Unified_Segments silently failing to write for some phase/cycle
        # projects: when a column's source data didn't cleanly coerce to
        # numeric, casting fell back to 'string' dtype, and .fillna(0) on
        # a string column silently produced a mixed-type 'object' column
        # that pyarrow's to_parquet() then refused to write at all.)
        typed_df = typed_df.where(pd.notnull(typed_df), None)
        out_path = build_bronze_path(output_root, source, project, extra_partitions, table_name, run_date,
                                      zone=zone, upc=upc, phase=phase, cycle=cycle)
        typed_df.to_parquet(_long_path(str(out_path)), engine='pyarrow', index=False,
                             coerce_timestamps='us', allow_truncated_timestamps=True)
        summary.update({
            'status': 'SUCCESS',
            'output_path': str(out_path),
            'output_rows': len(typed_df),
            'output_columns': len(typed_df.columns),
            'dtype_counts': ' | '.join(
                f"{lbl}:{sum(1 for c in col_info if c['inferred_dtype'] == lbl)}"
                for lbl in sorted({c['inferred_dtype'] for c in col_info})
            ),
        })
        summary['col_info'] = col_info
    except Exception as exc:
        summary.update({'status': 'ERROR', 'error': concise_exc(exc)})
    return summary

# COMMAND ----------

def slugify(name: str, max_len: int = 80) -> str:
    """Sanitise an arbitrary folder/label into a safe, readable partition value."""
    s = str(name).strip()
    s = _INVALID_CHARS.sub('_', s)
    s = re.sub(r'\s+', ' ', s)
    s = s.strip(' .')
    s = _REPEAT_UNDERSCORE.sub('_', s).strip('_ ')
    if not s:
        return 'Unnamed'
    return s[:max_len].rstrip(' ._') if len(s) > max_len else s

# COMMAND ----------

_INVALID_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_REPEAT_UNDERSCORE = re.compile(r'_{2,}')

# COMMAND ----------

def _load_workbook_cached(file_path: str):
    """Load a full openpyxl Workbook, reusing it if file_path is unchanged."""
    global _LAST_WB_PATH, _LAST_WB
    if _LAST_WB_PATH == file_path and _LAST_WB is not None:
        return _LAST_WB
    if _LAST_WB is not None:
        try:
            _LAST_WB.close()
        except Exception:
            pass
    _LAST_WB = openpyxl.load_workbook(_long_path(file_path), data_only=True)
    _LAST_WB_PATH = file_path
    return _LAST_WB

# COMMAND ----------

def _build_merged_map(ws):
    """
    Return {(row, col): (value, number_format)} for every cell
    inside a merged range.  Storing number_format lets _get()
    reproduce the exact displayed value (e.g. '+3.5').
    """
    merged = {}
    for rng in ws.merged_cells.ranges:
        cell = ws.cell(rng.min_row, rng.min_col)
        val  = cell.value
        fmt  = cell.number_format or "@"
        for r in range(rng.min_row, rng.max_row + 1):
            for c in range(rng.min_col, rng.max_col + 1):
                merged[(r, c)] = (val, fmt)
    return merged

# COMMAND ----------


def _preload_sheet(ws, merged_map):
    """
    Pre-load ALL worksheet cell values into a fast {(row, col): value} cache.

    Why this speeds things up (3–5× for 20 k+ row sheets)
    -------------------------------------------------------
    openpyxl's ws.cell(r, c) issues a separate dictionary lookup per call.
    iter_rows() walks the internal row data in one sequential sweep and is
    significantly faster when reading thousands of rows.

    The cache is consulted instead of calling _get() inside every hot loop:
      _find_data_groups  (col_fill: max_col × data_rows calls)
      run_d4 / extract_sheet  (data extraction: rows × cols calls)

    Only non-None values are stored so the dict stays lean for sparse sheets.
    Merged-cell resolution is applied during the build so callers get the
    same values they would have received from _get().

    Returns
    -------
    dict { (row, col) : value }   — 1-based row/col, same as ws.cell()
    """
    cache   = {}
    max_col = ws.max_column or 1
    for row_cells in ws.iter_rows(min_row=1, max_row=ws.max_row,
                                   min_col=1, max_col=max_col):
        for cell in row_cells:
            r, c = cell.row, cell.column
            key  = (r, c)
            if key in merged_map:
                val, fmt = merged_map[key]
            else:
                val = cell.value
                fmt = cell.number_format or "@"
            result = _exact_value(val, fmt)
            if result is not None:
                cache[key] = result
    return cache

# COMMAND ----------

def _exact_value(val, number_format):
    """
    Return the exact cell value with no type coercion.
      None                         → None
      Excel-error string           → None  (filtered out)
      str ('+', '-', 'NIL', text) → as-is
      numeric with '+' in format  → signed string '+3.5' / '-2.0'
      all other numeric/date/bool → returned unchanged
    """
    if val is None:
        return None
    if isinstance(val, str):
        return None if val.strip() in _EXCEL_ERRORS else val
    if isinstance(val, (int, float)) and number_format and "+" in number_format:
        return f"+{val}" if val >= 0 else str(val)
    return val

# COMMAND ----------

def _detect_header_block(ws, merged_map, max_scan=20, min_data_cols=3):
    """
    Auto-detect the full block of header rows (1-based list).

    Strategy
    --------
    1. Find the first row where ≥30 % of filled cells are numeric/datetime
       → that is the first data row.
    2. Walk back to find the leaf header row (last all-text row above data).
    3. Walk further back collecting parent header rows:
       all-text rows with ≥2 DISTINCT values.
       (Excludes full-width title merges like "NSV Data Lake Format 2024"
        that expand to the same value across all columns.)

    Returns
    -------
    e.g.  [1]       — single header row
          [1, 2]    — group row + column-name row
          [1, 2, 3] — three-level header
    """
    max_col = ws.max_column

    def _row_stats(r):
        vals     = [_get(ws, merged_map, r, c) for c in range(1, max_col + 1)]
        non_none = [v for v in vals if v is not None]
        # `distinct` ignores stray junk values (e.g. a leftover "`", "-",
        # "." with no letters/digits at all) -- a single accidental
        # punctuation character sitting next to a real title (e.g. "`" in
        # one cell, "Carriageway Furniture" in another, on an otherwise
        # title-only row) was enough to push that row's distinct count to
        # 2, wrongly qualifying it as a genuine 2-column PARENT HEADER row
        # in Step 3 below instead of being skipped as a plain title --
        # which then merged the title text into every field name beneath
        # it (e.g. "Carriageway Furniture > SurveyDate" instead of just
        # "SurveyDate"). `filled`/`nums` are untouched to avoid changing
        # Step 1's data-row detection.
        _real = [v for v in non_none if not (isinstance(v, str) and not re.search(r'[A-Za-z0-9]', v))]
        distinct = len({str(v) for v in _real}) if _real else 0
        # NOTE: chainage-style columns (StartChainage/EndChainage) are
        # sometimes stored with a "+" Excel number format (e.g. "+279800.0"),
        # which _exact_value() deliberately turns into a STRING to preserve
        # the "+" sign -- but that meant this numeric-ratio check never
        # counted them as numeric, undercounting real data rows (seen on
        # RIS_DataTemplate PavementType/CarriagewayFurniture: true ratio was
        # 4/14 = 28.6%, just under the 30% cutoff, so header detection
        # silently fell back to row 1 -- a single merged title cell --
        # instead of the real 2/3-row header). Counting "+3.5"/"-3.5"/"3.5"
        # -style numeric strings here (in addition to real int/float/date
        # values) fixes that without loosening detection elsewhere: a
        # genuine header cell (plain text like "StartChainage") never
        # matches this numeric pattern, so header rows are unaffected --
        # only true numeric-looking values gain an extra count.
        nums     = sum(
            1 for v in non_none
            if isinstance(v, (int, float)) or hasattr(v, 'year')
            or (isinstance(v, str) and re.fullmatch(r'[+-]?\d+(\.\d+)?', v.strip()))
        )
        return len(non_none), distinct, nums

    # Step 1 — first data row
    first_data_row = None
    for r in range(1, min(max_scan + 1, ws.max_row + 1)):
        filled, distinct, nums = _row_stats(r)
        if filled >= min_data_cols and nums / filled >= 0.3:
            first_data_row = r
            break

    if first_data_row is None or first_data_row <= 1:
        # For zero-data template sheets (no numeric data found), collect the
        # consecutive non-empty rows from the top as header rows.
        # We count actual non-empty rows (up to max_scan) instead of checking
        # ws.max_row, because sheets like ServiceRoad have formatting/borders
        # that push ws.max_row far beyond the 3 real content rows.
        #
        # Threshold: if ≤ 6 non-empty rows exist in the scan window, treat
        # them all as header rows (template-only or header-only sheet).
        # Sheets with actual data (e.g. Table List with 30+ rows) exceed this
        # threshold and fall through to return [1] unchanged.
        if first_data_row is None:
            non_empty_rows = []
            for r in range(1, min(max_scan + 1, ws.max_row + 1)):
                filled, _, _ = _row_stats(r)
                if filled >= 1:
                    non_empty_rows.append(r)

            if len(non_empty_rows) <= 6:
                # Collect only the leading consecutive block
                header_rows_found = []
                for r in non_empty_rows:
                    if not header_rows_found or r == header_rows_found[-1] + 1:
                        header_rows_found.append(r)
                    else:
                        break       # gap in rows → stop
                return header_rows_found if header_rows_found else [1]

        return [1]

    # Step 2 — leaf header
    leaf = first_data_row - 1
    while leaf >= 1:
        filled, distinct, nums = _row_stats(leaf)
        if filled > 0 and nums == 0:
            break
        leaf -= 1

    if leaf < 1:
        return [1]

    # Step 3 — parent header rows
    header_rows = [leaf]
    r = leaf - 1
    while r >= 1:
        filled, distinct, nums = _row_stats(r)
        if filled >= 2 and distinct >= 2 and nums == 0:
            header_rows.insert(0, r)
            r -= 1
        else:
            break

    return header_rows

# COMMAND ----------

def _get(ws, merged_map, row, col):
    """
    Resolve a cell value honoring merged ranges, display format,
    and Excel error filtering.
    """
    if (row, col) in merged_map:
        val, fmt = merged_map[(row, col)]
        return _exact_value(val, fmt)
    cell = ws.cell(row, col)
    return _exact_value(cell.value, cell.number_format or "@")

# COMMAND ----------

def _find_data_groups(ws, merged_map, header_row=1,
                      all_header_rows=None, min_fill_ratio=0.02,
                      cell_cache=None):
    """
    Find contiguous groups of columns that are real data sections.

    Stage 1 — Header check (per column):
      Only columns with a non-None value at header_row are candidates.
      Chart/graph regions have no headers so they are excluded.

      Optional – all_header_rows (list of int):
        When provided (RIS_DataTemplate use case), any column that has a
        non-None value in ANY of those rows AND has fill > 0 is also
        included, even if it is blank in header_row.
        This covers columns whose label lives in row 3 (the leaf header)
        while rows 1 and 2 are empty for that column.
        Has no effect on sheets that don't pass this parameter.

    Stage 2 — Fill ratio check (per GROUP):
      Group-level average fill ratio must meet min_fill_ratio.
      Keeps sparse-but-valid columns (e.g. 'Remark') inside data-rich
      groups; rejects stray label-only groups.

    cell_cache : optional pre-loaded {(row, col): value} dict from
      _preload_sheet().  When supplied, col_fill is derived by iterating
      the cache keys once (O(non-None cells)) instead of issuing
      max_col × data_rows individual _get() calls — ~5× faster for
      large sheets.

    Returns list of (start_col, end_col) 1-based inclusive tuples.
    """
    max_col        = ws.max_column
    data_row_count = max(ws.max_row - header_row, 1)

    # ── col_fill computation ───────────────────────────────────────────────────
    if cell_cache is not None:
        # Fast path: iterate cache keys once instead of max_col × data_rows calls
        col_counts: dict = {}
        for (r, c) in cell_cache:
            if r > header_row:
                col_counts[c] = col_counts.get(c, 0) + 1
        col_fill = {col: col_counts.get(col, 0) / data_row_count
                    for col in range(1, max_col + 1)}
        def _gc(r, c): return cell_cache.get((r, c))
    else:
        col_fill = {
            col: sum(
                1 for r in range(header_row + 1, ws.max_row + 1)
                if _get(ws, merged_map, r, col) is not None
            ) / data_row_count
            for col in range(1, max_col + 1)
        }
        def _gc(r, c): return _get(ws, merged_map, r, c)

    # ── Stage 1a: columns labelled in header_row ──────────────────────────────
    header_cols = [
        col for col in range(1, max_col + 1)
        if _gc(header_row, col) is not None
    ]

    # ── Stage 1b (RIS_DataTemplate only): union columns from other header rows ─
    # Handles the pattern where rows 1 & 2 are blank for a column but row 3
    # carries the actual label (e.g. a standalone column not under any group).
    if all_header_rows:
        header_set = set(header_cols)
        for hr in all_header_rows:
            if hr == header_row:
                continue
            for col in range(1, max_col + 1):
                if col in header_set:
                    continue
                if _gc(hr, col) is None:
                    continue
                # Only include if the column actually has data
                if col_fill.get(col, 0) > 0:
                    header_set.add(col)
        header_cols = sorted(header_set)

    if not header_cols:
        return []

    raw_groups = []
    start = prev = header_cols[0]
    for col in header_cols[1:]:
        if col == prev + 1:
            prev = col
        else:
            raw_groups.append((start, prev))
            start = prev = col
    raw_groups.append((start, prev))

    final_groups = []
    for s, e in raw_groups:
        cols_in_group = list(range(s, e + 1))
        avg_fill = sum(col_fill[c] for c in cols_in_group) / len(cols_in_group)
        if avg_fill >= min_fill_ratio:
            final_groups.append((s, e))

    return final_groups

# COMMAND ----------

def _build_column_names(ws, merged_map, header_rows, all_cols):
    """
    Build compound column names from one or more header rows.

    For each column:
      • Collect non-None values from each header row, top-to-bottom.
      • Skip consecutive duplicates (same merged-cell value repeated).
      • Join with ' > ' when multiple distinct levels exist.
      • Single level → plain name, no separator.
      • All-None → fallback 'Col_N'.

    Example (2-row header with merged parent):
      row 1: 'As per Old NH number'  (merged across two cols)
      row 2: 'StartChainage'
      → 'As per Old NH number > StartChainage'
    """
    col_names = []
    for c in all_cols:
        parts = []
        prev  = None
        for r in header_rows:
            val = _get(ws, merged_map, r, c)
            if val is not None:
                s = str(val).strip()
                if s and s != prev:
                    parts.append(s)
                    prev = s
        col_names.append(" > ".join(parts) if parts else f"Col_{c}")
    return col_names

# COMMAND ----------

_NOTE_SEEDS           = {"note", "remark", "remarks", "footnote", "disclaimer"}
_NOTE_FUZZY_THRESHOLD = 80

def _is_note_label(text: str) -> bool:
    cleaned    = text.lower().strip().lstrip("*#-").strip()
    first_word = cleaned.split()[0] if cleaned.split() else ""
    if any(cleaned.startswith(seed) for seed in _NOTE_SEEDS):
        return True
    return any(
        fuzz.ratio(first_word, seed) >= _NOTE_FUZZY_THRESHOLD
        for seed in _NOTE_SEEDS
    )

# COMMAND ----------

def _is_trailing_note_row(ws, merged_map, row, all_cols, max_col,
                          cell_cache=None):
    """
    Return True if this data row is a trailing note/remark annotation
    that should be excluded from the extracted DataFrame.

    Checks the first non-None cell in columns 1-4 (plus the first 3
    columns in all_cols) against the note-label pattern:
    'Note:', 'Remark', '* ...', 'Disclaimer', 'Footnote', etc.

    cell_cache : pre-loaded sheet cache from _preload_sheet() (optional).
    """
    scan_cols = sorted(set(list(range(1, min(5, max_col + 1)))
                           + list(sorted(all_cols))[:3]))
    _gc = (lambda c: cell_cache.get((row, c))) if cell_cache is not None \
          else (lambda c: _get(ws, merged_map, row, c))
    for c in scan_cols:
        v = _gc(c)
        if v is not None:
            return _is_note_label(str(v))
    return False

# COMMAND ----------

def _is_section_break_row(ws, merged_map, row, all_cols, cell_cache=None):
    """
    Return True if this row looks like a section-break / sub-header row
    rather than an actual data row.

    Criteria:
    • Exactly one non-None value among the extracted columns, AND
    • That value is a short text string (≤ 80 chars, no digit characters), AND
    • The single filled cell sits in the LEFT half of the column list.

    Catches rows like a merged "Visual Condition Observations" heading that
    openpyxl reads as a single wide cell — they are sub-header rows, not data.

    cell_cache : pre-loaded sheet cache from _preload_sheet() (optional).
    """
    if cell_cache is not None:
        values = [cell_cache.get((row, c)) for c in all_cols]
    else:
        values = [_get(ws, merged_map, row, c) for c in all_cols]
    non_none = [v for v in values if v is not None]
    if len(non_none) != 1:
        return False
    val = str(non_none[0]).strip()
    if len(val) > 80 or any(ch.isdigit() for ch in val):
        return False
    first_filled_idx = next(
        (i for i, v in enumerate(values) if v is not None), len(values)
    )
    return first_filled_idx < len(all_cols) // 2

# COMMAND ----------

def _read_matrix(ws, merged_map):
    """
    Load the entire sheet into a 2D Python list.
    Uses enhanced _get() so Excel errors are None and '+' formats are exact.
    """
    max_r = ws.max_row    or 0
    max_c = ws.max_column or 0
    return [
        [_get(ws, merged_map, r, c) for c in range(1, max_c + 1)]
        for r in range(1, max_r + 1)
    ]

# COMMAND ----------

def _find_regions(matrix):
    """
    Detect separate table blocks by finding fully-blank row gaps.
    Within each row band, also splits on column gaps to handle
    side-by-side tables on the same rows.
    """
    if not matrix:
        return []

    nrows      = len(matrix)
    ncols      = max(len(row) for row in matrix)
    blank_rows = {r for r in range(nrows) if all(v is None for v in matrix[r])}

    row_bands, start = [], None
    for r in range(nrows):
        if r not in blank_rows:
            if start is None:
                start = r
        else:
            if start is not None:
                row_bands.append((start, r - 1))
                start = None
    if start is not None:
        row_bands.append((start, nrows - 1))

    regions = []
    for r_start, r_end in row_bands:
        filled_cols = {
            c
            for r in range(r_start, r_end + 1)
            for c in range(ncols)
            if c < len(matrix[r]) and matrix[r][c] is not None
        }
        if not filled_cols:
            continue

        sorted_cols = sorted(filled_cols)
        cs = cp = sorted_cols[0]
        for c in sorted_cols[1:]:
            if c == cp + 1:
                cp = c
            else:
                regions.append((r_start, r_end, cs, cp))
                cs = cp = c
        regions.append((r_start, r_end, cs, cp))

    return regions

# COMMAND ----------

def _find_header_row(matrix, r_start, r_end, c_start, c_end, min_cells=2):
    """
    Find the first row in a region that looks like a header
    (≥min_cells non-None values AND more than 1 distinct value).
    Rows before it are collected as title candidates.
    """
    title_candidates = []
    for r in range(r_start, r_end + 1):
        row_slice = [
            matrix[r][c]
            for c in range(c_start, min(c_end + 1, len(matrix[r])))
        ]
        non_none = [v for v in row_slice if v is not None]
        distinct  = set(str(v) for v in non_none)
        if len(non_none) >= min_cells and len(distinct) > 1:
            return r, title_candidates
        if non_none:
            title_candidates.append(str(non_none[0]))
    return None, title_candidates

# COMMAND ----------

def _is_notes_region(data_rows, c_start, c_end, title_candidates, header_first_val):
    """Return True if the region looks like a notes/remarks block."""
    n_cols = c_end - c_start + 1
    if n_cols == 1 and len(data_rows) > 3:
        return True
    if any(_is_note_label(str(t)) for t in title_candidates):
        return True
    if header_first_val and _is_note_label(header_first_val):
        return True
    return False

# COMMAND ----------

_LANE_SEEDS            = {"chainage from", "chainage to", "ch from", "ch to"}
_LANE_FUZZY_THRESHOLD  = 85
_MAX_LANE_HEADER_WORDS = 4
_LANE_LABEL_RE         = re.compile(r'^[LR]\d+$', re.IGNORECASE)

# COMMAND ----------

def _is_lane_header(header: str) -> bool:
    h = header.lower().strip()
    if len(h.split()) > _MAX_LANE_HEADER_WORDS:
        return False
    return any(
        fuzz.partial_ratio(seed, h) >= _LANE_FUZZY_THRESHOLD
        for seed in _LANE_SEEDS
    )

# COMMAND ----------

def _is_lane_table(raw_headers) -> bool:
    non_none = [str(h).strip() for h in raw_headers if h is not None]
    if not non_none:
        return False
    if any(_is_lane_header(h) for h in non_none):
        return True
    return all(_LANE_LABEL_RE.match(h) for h in non_none)

# COMMAND ----------

def _infer_table_name(matrix, r_start, title_candidates, max_lookahead=3):
    """Guess a table name from title rows above the header."""
    filtered = [t for t in title_candidates if not _is_note_label(t)]
    if filtered:
        return filtered[-1]
    for offset in range(1, max_lookahead + 1):
        r = r_start - offset
        if r < 0:
            break
        for v in matrix[r]:
            if v is not None:
                s = str(v)
                if not _is_note_label(s):
                    return s
                break
    return None

# COMMAND ----------

def _extract_survey_month(title_candidates):
    """Pull the survey-month label from title candidate strings."""
    for t in title_candidates:
        if "survey month" in t.lower():
            return re.sub(r"(?i)survey\s+month\s*:?\s*", "", t).strip()
    return None

# COMMAND ----------

def _deduplicate_headers(raw_headers):
    """Append _2, _3, … to repeated column names."""
    seen, out = {}, []
    for i, h in enumerate(raw_headers):
        key = str(h).strip() if h is not None else f"Col_{i + 1}"
        if key in seen:
            seen[key] += 1
            out.append(f"{key}_{seen[key]}")
        else:
            seen[key] = 0
            out.append(key)
    return out

# COMMAND ----------

def run_d2(
    file_path: str,
    sheet_name: str,
    min_data_rows: int = 1,
    min_header_cells: int = 2,
    verbose: bool = True,
) -> list:
    """
    Extract all tables from one Summary sheet.

    Returns a list of DataFrames (one per detected table).
    Each DataFrame has four metadata columns prepended:
        source_file, sheet_name, survey_month, table_name

    Edge cases handled
    ------------------
    • Merged cells (resolved via merged map + exact value)
    • Multiple side-by-side and stacked tables in one sheet
    • Notes/remarks blocks → skipped
    • Lane/chainage breakdown tables → skipped
    • All-empty data regions → skipped
    • Duplicate column names → suffixed (_2, _3, …)
    • '+' sign numeric formats → preserved as strings
    """
    wb    = _load_workbook_cached(file_path)
    ws    = wb[sheet_name]
    fname = os.path.basename(file_path)

    merged_map = _build_merged_map(ws)
    matrix     = _read_matrix(ws, merged_map)
    regions    = _find_regions(matrix)
    tables     = []

    for r_start, r_end, c_start, c_end in regions:
        header_row_idx, title_candidates = _find_header_row(
            matrix, r_start, r_end, c_start, c_end,
            min_cells=min_header_cells,
        )
        if header_row_idx is None:
            continue

        raw_headers = [
            matrix[header_row_idx][c]
            for c in range(c_start, min(c_end + 1, len(matrix[header_row_idx])))
        ]
        header_first = next((str(h) for h in raw_headers if h is not None), "")

        data_rows = [
            [matrix[r][c] for c in range(c_start, c_end + 1)]
            for r in range(header_row_idx + 1, r_end + 1)
            if any(matrix[r][c] is not None for c in range(c_start, c_end + 1))
        ]

        # Skip regions where every non-first column is blank
        if len(raw_headers) > 1 and data_rows:
            has_real_data = any(
                v is not None for row in data_rows for v in row[1:]
            )
            if not has_real_data:
                if verbose:
                    print(
                        f"  [{sheet_name}] SKIP all-empty region "
                        f"rows {r_start+1}–{r_end+1} "
                        f"(table structure present but no data)"
                    )
                continue

        if _is_notes_region(data_rows, c_start, c_end,
                             title_candidates, header_first):
            if verbose:
                print(
                    f"  [{sheet_name}] SKIP notes region "
                    f"rows {r_start+1}–{r_end+1}"
                )
            continue

        if _is_lane_table(raw_headers):
            if verbose:
                print(
                    f"  [{sheet_name}] SKIP lane table "
                    f"rows {r_start+1}–{r_end+1}"
                )
            continue

        if len(data_rows) < min_data_rows:
            continue

        table_name   = (
            _infer_table_name(matrix, r_start, title_candidates)
            or f"Table_{len(tables) + 1}"
        )
        survey_month = _extract_survey_month(title_candidates)
        headers      = _deduplicate_headers(raw_headers)

        df = pd.DataFrame(data_rows, columns=headers)
        df.insert(0, "table_name",   table_name)
        df.insert(0, "survey_month", survey_month)
        df.insert(0, "sheet_name",   sheet_name)
        df.insert(0, "source_file",  fname)

        tables.append(df)

        if verbose:
            print(
                f"  [{sheet_name}] Table {len(tables):>2}: \"{table_name}\""
                f"  rows {r_start+1}–{r_end+1},"
                f"  cols {get_column_letter(c_start+1)}–{get_column_letter(c_end+1)}"
                f"  →  {df.shape[0]} data rows × {df.shape[1]-4} data cols"
            )

    return tables

# COMMAND ----------

deliverable_summary_rows = []
# Keyed by project name -- lets the Backup stage (Cell 95) reuse the
# SAME extract_folder() results for the D4-missing Potholes fallback
# (D2 measurements + D3 image resolution) rather than re-parsing every
# Excel file in the project a second time.
deliverable_extraction_results = {}
_EXCEL_ERRORS = frozenset({
    '#REF!', '#ERROR!', '#N/A', '#VALUE!',
    '#DIV/0!', '#NAME?', '#NUM!', '#NULL!'
})
_LAST_WB_PATH = None
_LAST_WB = None

# Only these sheet-name keywords get strict schema enforcement within a
# matched RIS file -- match is case-insensitive, substring-based on the
# sheet's own name. Any "*Carriage*" variant (CarriagewayType,
# CarriagewayFurniture, etc.) and PavementType are strict; every other RIS
# sheet is WARNING-only for now.
STRICT_RIS_SHEET_KEYWORDS = ["carriage", "pavementtype"]

# write_bronze_table()'s table_key defaults to the raw Excel sheet name
# (table_tag below), which works fine for files whose sheets are named
# meaningfully (GPS/Cracking/Rutting/...) -- but ad-hoc Deliverable
# uploads (e.g. a standalone Pothole workbook) often keep Excel's own
# generic default tab name ('Sheet1'). Under norm_key(), that generic name
# can collide with a COMPLETELY unrelated, much larger Table_Name entry
# elsewhere in updated_schema.xlsx that happens to be keyed the same way
# (e.g. a full RIS condition/defect master table with Rutting/Cracking/
# IRI/Bleeding/... columns) -- which is exactly what pulled ~48 foreign
# columns into the 21-column D4 Pothole sheet. For files already
# identified as one of our REQUIRED_DELIVERABLE_FILES, use a dedicated,
# unambiguous schema table_key instead of the raw sheet name, so the
# lookup can only ever match that file's own intended schema entry.
#
# IMPORTANT: the value here must exactly match (by norm_key) the
# Table_Name your updated_schema.xlsx actually uses for that 21-column
# Pothole schema -- adjust if your schema file spells it differently.
REQUIRED_LABEL_SCHEMA_KEY = {
    "D4-Pothole": "Potholes",  # confirmed against updated_schema_MERGED.xlsx -- Table_Name is plural 'Potholes'
}


# Module-level (not nested in the loop below) so Cell 93's
# load_d4_pothole_points() can reuse the exact same typo-tolerant
# matching for finding D4 pothole bronze files by name -- moved out here
# after finding that a second, separate plain-substring check in that
# function had exactly the same "Photholes" (not "Potholes") typo bug
# this was originally written to fix, just in a different place.
#
# Exact substring match first (cheap, and the common case); fuzzy
# fallback catches real-world filename typos that break plain substring
# matching entirely -- e.g. "Silkboard_to_Hosur_MCW_Photholes_sheet.xlsx"
# (an actual source filename: "Photholes", not "Potholes") never contains
# the substring "pothole", so a plain check silently never matches it at
# all. fuzz.partial_ratio against each filename "word" catches this and
# similar typos without loosening the match so much that unrelated files
# start matching.
def _filename_matches_required_patterns(path_lower: str, patterns) -> bool:
    if any(p in path_lower for p in patterns):
        return True
    words = re.split(r'[^a-z0-9]+', path_lower)
    return any(
        fuzz.partial_ratio(p, w) >= 82
        for p in patterns for w in words if w
    )


# PERFORMANCE (no logic change -- schema_df never changes during this
# loop, so this set is always identical every time it's built): hoisted
# out of the innermost loop below, where it used to be rebuilt from
# schema_df's ~900+ row '_table_key' column once per table/sheet/file/
# project (potentially hundreds of times per run) instead of once total.
_known_table_keys = set(schema_df['_table_key']) if schema_df is not None else set()


for project_dir in deliverable_projects:
    project = project_dir.name
    deliverable_files_dir = project_dir 
    if not deliverable_files_dir.is_dir():
        print(f"[SKIP] no 'Deliverable Files' subfolder under: {project}")
        continue

    print(f"\n{'='*70}\nProject: {project}\n{'='*70}")
    t0 = time.time()
    results = extract_folder(str(deliverable_files_dir))
    print(f"  extract_folder took {round(time.time() - t0, 1)}s, {len(results)} file(s)")
    deliverable_extraction_results[project] = results

    for rel_path, sheets in results.items():
        # Uses normalize_deliverable() (Cell 18) -- fuzzy-tolerant, not the
        # old raw _DELIV_RE regex search directly -- so real-world folder
        # naming like "1.Deliverable", "3. Deliverables -3 - GIS Mapping",
        # or "4 .Deliverbales - 4 - NH ONE Platform" (typo) all resolve
        # correctly instead of falling through to "Unknown", which used to
        # also break the required-file-present and whole-deliverable-
        # missing checks further below (both compare against this exact
        # tag) even when the deliverable was genuinely there.
        # BUG FIXED: normalize_deliverable(str(rel_path)) checked the FULL
        # relative path (folder + filename) as ONE string -- a real file
        # like "Deliverable - 1 - .../....DVM Pkg-1.xlsx" has TWO distinct
        # digits once the filename is included ('1' from the folder, '1'
        # from "Pkg-1" in the filename -- or '2'/'1' for a D2 file, etc.),
        # and normalize_deliverable() requires EXACTLY ONE distinct digit
        # to resolve -- so it silently returned "" and deliv_label became
        # "Unknown" for genuinely real D1-D4 files, which then never
        # combined into the correct "Deliverable=Deliverable - N" bucket
        # at all -- they got written to "Deliverable=Unknown" instead.
        #
        # Fixed by using extract_deliverable_from_path() (already defined
        # above, previously unused) -- it walks each path PART separately,
        # so the filename's own digits are checked in isolation and never
        # combine with the folder's digit to create a false rejection.
        _nd = normalize_deliverable(project) or extract_deliverable_from_path(str(rel_path))
        deliv_label = f"Deliverable - {_nd.rsplit('-', 1)[-1]}" if _nd else "Unknown"
        # NOTE: excel_stem used to be just Path(rel_path).stem (filename
        # only, e.g. "Cracking_data"). When a Deliverable folder has more
        # than one subfolder containing a same-named file (e.g. D3's
        # "Fly Over/Cracking_data.xlsx" AND "MCW/Cracking_data.xlsx"),
        # both produced the IDENTICAL table_name -> identical output
        # bronze path -- so the second one written (MCW, since os.walk's
        # dirs.sort() visits "Fly Over" before "MCW" alphabetically)
        # silently overwrote the first (Fly Over) on disk, and only one
        # subfolder's data ever survived to the final parquet. Prefixing
        # the immediate subfolder name (when there is one) makes every
        # subfolder's output land in its own distinct file instead.
        _rel_subfolder = os.path.dirname(rel_path)
        _subfolder_tag = slugify(_rel_subfolder) if _rel_subfolder and _rel_subfolder != "." else ""
        excel_stem = f"{_subfolder_tag}_{Path(rel_path).stem}" if _subfolder_tag else Path(rel_path).stem

        _lbl_digit_m = re.search(r'(\d)', deliv_label)
        _lbl_digit = _lbl_digit_m.group(1) if _lbl_digit_m else None
        _file_req_label = next(
            (lbl for lbl, cfg in REQUIRED_DELIVERABLE_FILES.items()
             if cfg["deliverable"].lstrip("Dd") == _lbl_digit
             and _filename_matches_required_patterns(rel_path.lower(), cfg["patterns"])),
            None,
        )

        for sheet_name, val in sheets.items():
            tables = val if isinstance(val, list) else [val]
            for i, df in enumerate(tables):
                if not isinstance(df, pd.DataFrame) or df.empty:
                    continue
                table_tag = f"{sheet_name}_{i + 1}" if len(tables) > 1 else sheet_name
                # Use the dedicated schema key from REQUIRED_LABEL_SCHEMA_KEY
                # ONLY when this sheet's own name doesn't already correspond
                # to a real Table_Name in the schema -- otherwise trust the
                # sheet's own name over the file-level override.
                #
                # Verified bug this fixes: a file matched to "D4-Pothole"
                # purely because its FILENAME contains "pothole" (e.g.
                # "...NHAI POTHOLE Data...xlsx") can still contain OTHER
                # sheets that are not pothole point-data at all -- e.g. a
                # sheet literally named "Roughness,Rut,Crack,Raveling",
                # which already has its own complete, correct 82-column
                # schema entry under that exact Table_Name. Forcing
                # table_key="Potholes" onto it regardless made every one of
                # its real columns show as "not in schema" and every one of
                # the (irrelevant, unmatched) Potholes columns show as
                # "missing from source" -- ~90 false CRITICAL entries from
                # a single sheet that was never a Potholes table to begin
                # with. Checking whether table_tag's own norm_key already
                # exists in the schema lets a sheet with a real, specific
                # name (like this one) win over the file-level override,
                # while still-generic names (e.g. "Sheet1", which has no
                # schema entry of its own) still fall through to it.
                if norm_key(table_tag) in _known_table_keys:
                    _schema_table_key = table_tag
                else:
                    _override_key = REQUIRED_LABEL_SCHEMA_KEY.get(_file_req_label)
                    if _override_key and schema_df is not None:
                        # Only apply the file-level override if this SHEET's
                        # own raw columns actually resemble that schema's
                        # columns by name -- otherwise a genuinely different,
                        # non-matching sheet inside the same file (verified
                        # real case: a "Potholes"-named file's own extra
                        # "Sheet2" is a raw LCMS point-export duplicating the
                        # same data with totally different columns --
                        # CHAINAGE/LRP_NUMBER/AREA/MAX_DEPTH/... instead of
                        # UPC/Datetime/NH No./Chainage/... -- while the
                        # file's OTHER, real "Potholes" sheet already has the
                        # correct standard layout) gets force-fit into that
                        # schema and flagged CRITICAL on nearly every column.
                        # Match by DISTINCT CANONICAL Column_Name (not raw
                        # Extracted_Column_Name rows -- several aliases can
                        # map to the same canonical field, which would
                        # double-count) and require a strong majority of
                        # the override schema's own fields to be present,
                        # not just any 3: verified real case where a raw
                        # LCMS point-export "Sheet2" coincidentally shared
                        # only 4 generic words (chainage/severity/date/
                        # nh_no) with Potholes' 21 canonical fields --
                        # nowhere near enough to actually BE a Potholes
                        # table, but enough to slip past a low absolute
                        # threshold.
                        _override_rows = schema_df[schema_df['_table_key'] == norm_key(_override_key)]
                        _extracted_to_canon = dict(zip(
                            _override_rows['Extracted_Column_Name'].apply(norm_key),
                            _override_rows['Column_Name'].apply(norm_key),
                        ))
                        _sheet_cols = set(norm_key(c) for c in df.columns)
                        _matched_canon = {canon for ext, canon in _extracted_to_canon.items() if ext in _sheet_cols}
                        _total_canon = set(_extracted_to_canon.values())
                        _overlap_frac = len(_matched_canon) / len(_total_canon) if _total_canon else 0
                        _schema_table_key = _override_key if _overlap_frac >= 0.5 else table_tag
                    else:
                        _schema_table_key = table_tag
                # "Project" removed from extra_partitions on purpose: files
                # from EVERY package/subfolder that map to the same deliverable
                # number (e.g. every package's own D1 files) must land in the
                # SAME "Deliverable=Deliverable - 1" bucket, combined together
                # -- not kept in separate per-package "Project=Pkg-N" folders.
                # project=project is still passed through for the summary
                # table's own bookkeeping (which package a given row came
                # from), it just no longer creates its own partition folder.
                # NEW: records the EXACT subfolder/package name this row's
                # data came from -- "project" is that folder's own real name
                # (the package folder under Layout A, e.g. "DME Pkg-1"; or
                # DELIVERABLE_ROOT's own name under Layout B, where there's
                # no package level at all). This is a DATA column only --
                # the output FOLDER structure is unchanged (still combines
                # into "Deliverable=Deliverable - N" regardless of which
                # subfolder/package a row came from), so multiple packages'
                # D1 files still land in the same bucket; this column is
                # what makes each row's own source traceable afterwards.
                df = df.copy()
                df['Source_Subfolder'] = project

                rec = write_bronze_table(
                    df, OUTPUT_ROOT, source="NSV", project=project,
                    # "Project" restored -- per instruction: the OUTPUT bronze structure
                    # should mirror the INPUT structure exactly (each package's own
                    # D1/D2/D3 wrapped separately, e.g. "DME Pkg-1/Deliverable - 1",
                    # "DME Pkg-2/Deliverable - 1"), NOT combined into one shared
                    # bucket. The extraction/parsing/schema-casting logic itself
                    # (extract_folder/run_d2/extract_sheet, everything that decides
                    # WHAT gets read from a sheet) is completely unchanged -- only
                    # this partition-naming choice is reverted back.
                    extra_partitions=({"Project": project, "Deliverable": deliv_label}
                                    if HAS_PACKAGE_LAYER else {"Deliverable": deliv_label}),  # "Project"
                    # ONLY when a real package layer exists (Layout A) -- Layout B
                    # (single project, no package subfolder) never gets one.
                    table_name=slugify(f"{excel_stem}_{table_tag}", 120),
                    table_key=_schema_table_key,
                    schema_df=schema_df,
                    run_date=RUN_DATE, source_file=rel_path,
                    zone=ZONE, upc=UPC, phase=PHASE, cycle=CYCLE,
                )
                deliverable_summary_rows.append(rec)

                # Per-sheet fallback: _file_req_label only ever looked at
                # the FILE's own name (rel_path). A file like
                # "NSV_NHAONE_Format.xlsx" has zero resemblance to
                # "pothole" itself, but genuinely contains a sheet named
                # "Potholes" -- which _schema_table_key already resolved
                # correctly above (via the "sheet's own name matches a
                # real schema Table_Name" check), just not tagged as
                # "D4-Pothole" here since that used the filename alone.
                # Re-derive it from the sheet name too so this case is
                # ALSO held to Potholes' normal strictness, not silently
                # downgraded to ordinary (non-required) WARNING-only
                # Deliverable severity.
                # Also gated on _lbl_digit (this file's own resolved
                # Deliverable number) being "4" or unknown -- NOT any other
                # KNOWN digit. Verified real case: a sheet literally named
                # "Potholes" sitting under a D2 folder was getting treated
                # exactly like a genuine D4 Potholes table (CRITICAL on any
                # mismatch) purely because its own sheet name matched the
                # schema's "Potholes" Table_Name -- per your instruction, a
                # Potholes-named sheet confirmed to be under D2 (or D1/D3)
                # should stay ordinary WARNING-only Deliverable severity,
                # not be held to D4's strictness just because of its name.
                _req_label = _file_req_label or (
                    "D4-Pothole" if _schema_table_key == "Potholes"
                    and _lbl_digit in (None, "4") else None
                )

                if _req_label == "D4-Pothole" and _schema_table_key == "Potholes":
                    # D4-Pothole's own column-schema strictness is not one
                    # of the 6 defined CRITICAL checks -- only D1-RIS's
                    # Carriageway/PavementType columns (below) stay
                    # CRITICAL on a missing/unexpected column. Downgraded
                    # to WARNING-only, same as any other non-required D4
                    # sheet.
                    _require_schema_now = False
                elif _req_label == "D1-RIS":
                    # RIS: strict ONLY on sheets whose name contains a
                    # STRICT_RIS_SHEET_KEYWORDS match; everything else WARNING.
                    _sheet_nk = sheet_name.lower()
                    _require_schema_now = any(kw in _sheet_nk for kw in STRICT_RIS_SHEET_KEYWORDS)
                else:
                    # D1-LRMS is disabled in REQUIRED_DELIVERABLE_FILES, so
                    # _req_label is None for LRMS files -- falls through to
                    # WARNING-only here, same as any unmatched file.
                    _require_schema_now = False

                log_schema_mapping_result(
                    rec, stage="Deliverable",
                    # "File/sheet found but empty" isn't one of the 6
                    # defined CRITICAL checks -- always WARNING now.
                    require_data=False,
                    require_schema=_require_schema_now,
                    lenient_columns=LENIENT_COLUMNS,   # Deliverable/RIS only -- Backup never passes this
                )

print(f"\nDeliverable bronze tables written: "
      f"{sum(1 for r in deliverable_summary_rows if r['status'] == 'SUCCESS')} / {len(deliverable_summary_rows)}")
print(f"Schema-matched   : {sum(1 for r in deliverable_summary_rows if r.get('schema_matched'))} / {len(deliverable_summary_rows)}")

# No exceptions: a missing required file for ANY of D1/D2/D3/D4 is
# always CRITICAL (an earlier SKIP_IF_MISSING = {"D4-Pothole"} demoted
# that one specific case to WARNING -- removed per your instruction that
# ANY deliverable not being available should be CRITICAL, no exceptions).
for _label, _cfg in REQUIRED_DELIVERABLE_FILES.items():
    _cfg_digit = _cfg["deliverable"].lstrip("Dd")
    def _digit(v):
        m = re.search(r'(\d)', str(v))
        return m.group(1) if m else None
    # Same fuzzy/typo-tolerant matcher used when files were first scanned
    # above -- a plain substring check here would disagree with what
    # actually got processed for a typo'd filename (e.g. "Photholes"
    # instead of "Potholes"): the file WAS found and bronzed correctly,
    # but this check would still wrongly call it "missing".
    # Checks the SHEET/output table_name too, not just the source
    # filename -- verified real case: "NSV_NHAONE_Format.xlsx" has zero
    # resemblance to "pothole" in its own name, but genuinely contains a
    # sheet named "Potholes" inside it, and gets bronzed as table_name
    # "NSV_NHAONE_Format_Potholes" (excel_stem + sheet name). Filename-only
    # matching missed this entirely -- correctly-processed pothole data
    # (schema_matched=True, real rows written) was still flagged CRITICAL
    # "Required file not found", because nothing about the FILE's own name
    # said "pothole"; only its SHEET did.
    _present = any(
        _digit(r.get("Deliverable", "")) == _cfg_digit
        and (_filename_matches_required_patterns(r["source_file"].lower(), _cfg["patterns"])
             or _filename_matches_required_patterns(str(r.get("table_name", "")).lower(), _cfg["patterns"]))
        for r in deliverable_summary_rows
    )
    if not _present:
        # D4-Pothole missing is one of the 6 defined CRITICAL rules;
        # D1-RIS file-missing on its own is WARNING (only its
        # Carriageway/PavementType COLUMN-level check is CRITICAL,
        # via STRICT_RIS_SHEET_KEYWORDS further below).
        _sev = "CRITICAL" if _label == "D4-Pothole" else "WARNING"
        log_issue(_sev, "Deliverable",
                   f"Required file not found under {_cfg['deliverable']} "
                   f"(expected filename to contain one of {_cfg['patterns']}).",
                   table=_label)

# Whole-deliverable-folder-missing check, D1-D4 -- PER PROJECT, walking
# each project's FULL subtree (arbitrary depth), not just its own
# top-level folder name.
#
# FIX: `deliverable_projects` lists DELIVERABLE_ROOT's immediate children
# -- for real project layouts that insert an extra "package" folder
# between DELIVERABLE_ROOT and the actual "Deliverable - N" folders (e.g.
# .../deliverable/DME Pkg-1/Deliverable - 1 - Data Lake Upload Format/...,
# .../deliverable/DME Pkg-2/Deliverable - 1 - .../, .../DME Pkg-4/...),
# each project_dir.name is "DME Pkg-1"/"DME Pkg-2"/"DME Pkg-4" -- none of
# which contain the word "Deliverable" at all. The old code called
# normalize_deliverable() on ONLY that top-level name, got '' for every
# single project, and concluded NONE of Deliverable 1-4 existed anywhere
# -- 4 false CRITICALs per run, even though every folder was genuinely
# present just one level deeper. Fixed by walking each project's own
# subtree at every depth via os.walk() instead of inspecting only its own
# folder name.
#
# Also changed from a single flat set combined across ALL projects to a
# PER-PROJECT check: each package (DME Pkg-1, DME Pkg-2, DME Pkg-4, ...)
# is its own independent deliverable set and should be judged on its own
# completeness -- the old flat-set version would have called D3 "found"
# globally as long as ANY one project had it, even if a DIFFERENT project
# was completely missing it.
for project_dir in deliverable_projects:
    _found_digits_this_project = set()
    for _root, _dirs, _files in os.walk(project_dir):
        for _d_name in _dirs:
            _nd = normalize_deliverable(_d_name)
            if _nd:
                _found_digits_this_project.add(_nd.rsplit('-', 1)[-1])
    for _d in ('1', '2', '3', '4'):
        if _d not in _found_digits_this_project:
            # BUG FIXED: no explicit deliverable= override meant log_issue()
            # guessed it from file=str(project_dir) -- which contains the
            # word "deliverable" (the folder's own name) plus whatever
            # stray digits happen to sit in Phase_N/Cycle_N/the UPC itself.
            # For a UPC like "N_01001_01001_HR" every one of those stray
            # digits happens to be '1', so the guess wrongly resolved to
            # "Deliverable - 1" even when this message was genuinely about
            # D4. For a DIFFERENT UPC with mixed digits (e.g.
            # "N_01001_02002_HR") the same guess happened to come back
            # empty instead -- same bug, just not visible that time by
            # coincidence. Passing deliverable= explicitly removes the
            # guess entirely.
            log_issue("CRITICAL", "Deliverable",
                       f"No 'Deliverable - {_d}' folder found anywhere under this "
                       f"project's deliverable tree.",
                       file=str(project_dir),
                       deliverable=f"Deliverable - {_d}")

def _relocate_defect_gps_reference_files(deliverable_output_dir: str, defect_types: tuple,
                                          summary_rows: list = None) -> dict:
    """
    Moves the raw per-lane GPS reference bronze parquet files
    ('..._{DefectType}_GPS_{lane}_...parquet') out of Deliverable-3's
    top-level listing and into their own '{DefectType}_GPS_Reference'
    subfolder -- for ALL given defect_types in a SINGLE pass.

    Moving the file on disk without also updating its corresponding
    entry's 'output_path' in deliverable_summary_rows would leave that
    summary row pointing at a path that no longer exists --
    FileNotFoundError for anything that reads the file back via
    all_summary/deliverable_summary_rows afterwards. summary_rows is
    searched for any row whose OWN output_path matches the file just
    moved, and that row's output_path is updated in-place to the new
    location -- so deliverable_summary_rows/all_summary always reflects
    reality.

    Returns {defect_type: files_moved_count}.
    """
    counts = {d: 0 for d in defect_types}
    if not os.path.isdir(_long_path(deliverable_output_dir)):
        return counts

    patterns = {d: f"_{d.lower()}_gps_" for d in defect_types}
    dest_dirs = {d: os.path.join(deliverable_output_dir, f"{d}_GPS_Reference") for d in defect_types}
    dest_dirs_created = set()

    for fname in os.listdir(_long_path(deliverable_output_dir)):
        if not fname.lower().endswith('.parquet'):
            continue
        fname_lower = fname.lower()
        for d in defect_types:
            if patterns[d] in fname_lower:
                if d not in dest_dirs_created:
                    os.makedirs(_long_path(dest_dirs[d]), exist_ok=True)
                    dest_dirs_created.add(d)
                src_path = os.path.join(deliverable_output_dir, fname)
                dst_path = os.path.join(dest_dirs[d], fname)
                try:
                    shutil.move(_long_path(src_path), _long_path(dst_path))
                    counts[d] += 1
                    if summary_rows:
                        for _row in summary_rows:
                            if _row.get('output_path') == src_path:
                                _row['output_path'] = dst_path
                except Exception as exc:
                    log_issue("WARNING", "Deliverable",
                               f"Could not relocate {d} GPS reference file into "
                               f"subfolder: {concise_exc(exc)}", file=src_path)
                break  # a filename matches at most one defect type
    return counts


# Relocate BOTH defect types' raw GPS reference files in ONE pass -- keeps
# Deliverable-3's own bronze output directory clean (no loose per-sheet
# reference files sitting alongside the consolidated Images/
# Images_Metadata folders the Backup stage builds).
# BUG FIXED: this always looked at the Layout-B-style path (no
# "Project" partition) -- once the main write became conditional
# (Project= only under Layout A), the REAL D3 files for a Layout A
# project live nested inside EACH package's own "Project=<pkg>/
# Deliverable=Deliverable - 3/", not at this top-level path at all, so
# this relocation silently found nothing to move for every multi-
# package project. Now runs once per package under Layout A (each
# package's own D3 folder), or once at the Layout-B path otherwise --
# matching exactly where the files actually are in each case.
if HAS_PACKAGE_LAYER:
    for _pkg_dir in deliverable_projects:
        _d3_output_dir = str(build_output_dir(
            "NSV", extra_partitions={"Project": _pkg_dir.name, "Deliverable": "Deliverable - 3"}))
        _relocate_counts = _relocate_defect_gps_reference_files(
            _d3_output_dir, ("Potholes", "Cracking"), summary_rows=deliverable_summary_rows)
        for _defect, _n_moved in _relocate_counts.items():
            if _n_moved:
                print(f"  Relocated {_n_moved} {_defect} GPS reference file(s) into "
                      f"{_pkg_dir.name}/{_defect}_GPS_Reference/")
else:
    _d3_output_dir = str(build_output_dir("NSV", extra_partitions={"Deliverable": "Deliverable - 3"}))
    _relocate_counts = _relocate_defect_gps_reference_files(
        _d3_output_dir, ("Potholes", "Cracking"), summary_rows=deliverable_summary_rows)
    for _defect, _n_moved in _relocate_counts.items():
        if _n_moved:
            print(f"  Relocated {_n_moved} {_defect} GPS reference file(s) into {_defect}_GPS_Reference/")

raise_if_critical("Deliverable Bronze (D1-D4)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Backup Bronze (raw LCMS lane workbooks)
# MAGIC

# COMMAND ----------

def find_mdb_files(backup_root: str) -> list:
    """Manifest raw .mdb LCMS Access-DB exports (not read — proprietary format)."""
    records = []
    root = Path(backup_root)
    for project_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        project = project_dir.name
        for dirpath, _dirnames, filenames in os.walk(project_dir):
            for fname in filenames:
                if fname.lower().endswith('.mdb'):
                    fp = Path(dirpath) / fname
                    records.append({
                        'project': project,
                        'file_path': str(fp),
                        'size_mb': round(os.stat(_long_path(str(fp))).st_size / 1024 ** 2, 1),
                    })
    return records

# COMMAND ----------

mdb_manifest = find_mdb_files(BACKUP_ROOT)
mdb_df = pd.DataFrame(mdb_manifest)

manifest_path = build_output_dir("_manifest") / "mdb_files_skipped.csv"
mdb_df.to_csv(manifest_path, index=False)

print(f"{len(mdb_df)} raw .mdb file(s) logged (not read) -> {manifest_path}")
mdb_df.head()

# COMMAND ----------

def lane_from_filename(path: str) -> str:
    stem = os.path.splitext(os.path.basename(path))[0]
    m = LANE_RE.search(stem)
    return m.group(1).upper() if m else stem.split('_')[0]

# COMMAND ----------

LANE_XLSX_RE = re.compile(r'.*(?<![A-Za-z])([LR]\d+)\.xlsx$', re.IGNORECASE)
_PAVEMENT_KEYWORDS = [
    ('cement concrete', 'Rigid'), ('rigid', 'Rigid'),
    ('flexible', 'Flexible'), ('asphalt', 'Flexible'),
]

# COMMAND ----------

# DBTITLE 1,Cell 61
def find_lane_files(backup_root: str) -> list:
    """
    Discover lane workbooks (L1/L2/L3/R1/R2/R3) for the UPC parsed from
    backup_root, using the Backup_Path.xlsx index instead of walking the
    filesystem.

    In the index sheet (BACKUP_INDEX_SHEET), the row whose Zone, Phase,
    Cycle and UPC match backup_root lists up to three package folders in Column1/
    Column2/Column3, each written as 'ROOT/<sub>/<...>'. 'ROOT' maps to
    backup_root itself; the rest becomes a subfolder path underneath it
    (e.g. 'ROOT/Backups/Package-1' -> <backup_root>/Backups/Package-1).
    Every listed package is scanned; a workbook counts as lane <X> if its
    filename contains that lane code (L1/L2/L3/R1/R2/R3) anywhere.

    Raises if the Zone/Phase/Cycle/UPC combination is absent from the index,
    or if a package folder the
    index lists does not exist on the volume.

    Returns one record per lane workbook:
    {project, file_path, lane, variant, pavement, size_mb} — 'project'
    carries the package identity (e.g. 'Backups/Package-1') so lane files
    from different packages of the same UPC never collide in the output.
    """
    zone, phase, cycle, upc = parse_nsv_root(backup_root)

    idx = pd.read_excel(_long_path(BACKUP_INDEX_FILE),
                        sheet_name=BACKUP_INDEX_SHEET, dtype=str)
    idx.columns = [str(c).strip() for c in idx.columns]

    def _normalise_index_value(value, prefix: str = "") -> str:
        text = str(value).strip()
        if text.casefold() in ("", "nan", "none"):
            return ""
        if prefix:
            text = re.sub(
                rf"^{re.escape(prefix)}[\s_-]*", "", text,
                flags=re.IGNORECASE,
            )
        return text.strip().casefold()

    required_columns = {'Zone', 'Phase', 'Cycle', 'UPC'}
    missing_columns = required_columns.difference(idx.columns)
    if missing_columns:
        raise ValueError(
            "Backup index is missing required column(s): "
            + ", ".join(sorted(missing_columns))
        )

    matches = idx[
        (idx['Zone'].apply(lambda value: _normalise_index_value(value, 'Zone'))
         == _normalise_index_value(zone, 'Zone'))
        & (idx['Phase'].apply(lambda value: _normalise_index_value(value, 'Phase'))
           == _normalise_index_value(phase, 'Phase'))
        & (idx['Cycle'].apply(lambda value: _normalise_index_value(value, 'Cycle'))
           == _normalise_index_value(cycle, 'Cycle'))
        & (idx['UPC'].astype(str).str.strip().str.casefold()
           == str(upc).strip().casefold())
    ]
    if matches.empty:
        raise ValueError(
            f"Zone={zone!r}, Phase={phase!r}, Cycle={cycle!r}, UPC={upc!r} "
            f"not found in backup index {BACKUP_INDEX_FILE} "
            f"(sheet {BACKUP_INDEX_SHEET!r})."
        )
    row = matches.iloc[0]

    # Expanded from the original ('L1','L2','L3','R1','R2','R3') to cover
    # L4-L8/R4-R8 (some projects have more than 3 lanes per side) and
    # hyphenated variants (e.g. "R-1 Backup..xlsx", an actual filename
    # that "R1" alone -- with no hyphen -- doesn't match as a substring).
    _LANE_CODES = (
        'L1', 'L2', 'L3', 'L4', 'L5', 'L6', 'L7', 'L8',
        'R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7', 'R8',
        'L-1', 'L-2', 'L-3', 'L-4', 'L-5', 'L-6', 'L-7', 'L-8',
        'R-1', 'R-2', 'R-3', 'R-4', 'R-5', 'R-6', 'R-7', 'R-8',
    )

    def _lane_of(fname: str) -> str:
        up = fname.upper()
        return next((code for code in _LANE_CODES if code in up), None)

    records = []
    for col in ('Column1', 'Column2', 'Column3', 'Column4', 'Column5', 'Column6'):
        raw = row.get(col)
        if raw is None or str(raw).strip().lower() in ('', 'nan', 'none'):
            continue
        rel = str(raw).strip().replace('\\', '/')
        parts = [p for p in rel.split('/') if p]
        if parts and parts[0].upper() == 'ROOT':
            parts = parts[1:]
        package_dir = Path(backup_root).joinpath(*parts) if parts else Path(backup_root)
        package_label = '/'.join(parts) if parts else 'ROOT'

        if not Path(_long_path(str(package_dir))).is_dir():
            raise FileNotFoundError(
                f"Package folder listed in backup index for UPC {upc} "
                f"({col}={raw!r}) does not exist on volume: {package_dir}"
            )

        for fname in sorted(os.listdir(_long_path(str(package_dir)))):
            if not fname.lower().endswith('.xlsx'):
                continue
            # Skip Excel's own temporary lock file (created as "~$<name>.xlsx"
            # in the same folder while someone has the real file open) --
            # it's a tiny placeholder, not a real workbook, and reading it
            # throws "File is not a zip file". It was matching the lane
            # regex too (e.g. "~$LCMS L1.xlsx" contains "L1"), so it got
            # treated as a genuine lane workbook and crashed on read.
            if fname.startswith('~$'):
                continue
            lane = _lane_of(fname)
            if lane is None:
                continue
            file_path = package_dir / fname
            pavement = next(
                (tag for kw, tag in _PAVEMENT_KEYWORDS
                 if kw in (package_label + ' ' + fname).lower()),
                'Unknown',
            )
            records.append({
                'project': package_label,
                'file_path': str(file_path),
                'lane': lane,
                'variant': 'Default',
                'pavement': pavement,
                'size_mb': round(os.stat(_long_path(str(file_path))).st_size / 1024 ** 2, 1),
            })

    if not records:
        raise ValueError(
            f"No lane workbooks (L1-R3) found in any package folder "
            f"listed for UPC {upc} in {BACKUP_INDEX_FILE}."
        )
    return records


# COMMAND ----------

LANE_RE = re.compile(r'\b([LR]\d+)\b', re.IGNORECASE)

lane_files = find_lane_files(BACKUP_ROOT)

if LIMIT_LANE_FILES:
    lane_files = lane_files[:LIMIT_LANE_FILES]

# BACKUP_HAS_PACKAGE_LAYER: same "Project only when a real package layer
# exists" principle as the Deliverable stage's HAS_PACKAGE_LAYER --
# find_lane_files() tags every lane file with rec['project'] = "ROOT"
# when Backup_Path.xlsx lists only one entry for this UPC (no real
# package distinction), or the package's own real name when it lists
# several. "Project=" should only ever appear in the Backup-stage bronze
# output when at least one lane file genuinely has a non-"ROOT" package
# identity -- never a "Project=ROOT" wrapper for the plain single-package
# case.
BACKUP_HAS_PACKAGE_LAYER = any(f.get('project', 'ROOT') != 'ROOT' for f in lane_files)
print(f"Backup package layer detected: {BACKUP_HAS_PACKAGE_LAYER}")

print(f"{len(lane_files)} lane workbook(s) to process"
      f"{' (LIMITED for a test run)' if LIMIT_LANE_FILES else ''}")
if lane_files:
    display(pd.DataFrame(lane_files))
else:
    print("No lane workbooks found.")

if not lane_files:
    log_issue("WARNING", "Backup",
               "No lane workbooks (L*/R*.xlsx) found under BACKUP_ROOT.", file=str(BACKUP_ROOT))
    raise_if_critical("Backup Bronze - file discovery")

# COMMAND ----------

STAGE_TABLE_NAMES = (
    'GPS_Geometry', 'Cracking', 'Roughness_IRI', 'Rutting', 'Raveling',
    'Unified_Segments',
)

# COMMAND ----------

def clear_wb_cache():
    """Call between projects to free memory."""
    _WB_CACHE.clear()
def _resolve_sheet(file_path: str, requested: str) -> str:
    """Find real sheet name matching `requested`, handling numeric prefixes and all aliases."""
    wb = _get_wb(file_path)
    available = wb.sheet_names
    req_norm = _norm_sheet(requested)

    # 1. Exact normalised match
    for actual in available:
        if _norm_sheet(actual) == req_norm:
            return actual

    # 2. Alias lookup
    all_aliases = None
    for canon, al in _SHEET_ALIASES.items():
        if any(_norm_sheet(x) == req_norm for x in [canon] + al):
            all_aliases = [canon] + al
            break
    if all_aliases:
        for a in all_aliases:
            for actual in available:
                if _norm_sheet(actual) == _norm_sheet(a):
                    return actual

    # 3. Substring fallback
    for actual in available:
        if req_norm in _norm_sheet(actual) or _norm_sheet(actual) in req_norm:
            return actual

    return requested


# COMMAND ----------

def read_sheet_fast(file_path: str, sheet_name: str, header: int = 0) -> pd.DataFrame:
    """Read via cached fastexcel workbook; auto-resolves sheet name."""
    real = _resolve_sheet(file_path, sheet_name)
    wb = _get_wb(file_path)
    _devnull = open(os.devnull, 'w'); _old = sys.stderr; sys.stderr = _devnull
    try:
        ws = wb.load_sheet(real, header_row=header)
        df = ws.to_pandas()
    finally:
        sys.stderr = _old; _devnull.close()
    df.columns = [str(c).strip() for c in df.columns]
    return df

# COMMAND ----------

# ── Lane ──────────────────────────────────────────────────────────────────────

# ── Optional: hardcode NH/Section for projects missing those GPS columns ──────
OPTIONAL_NH_NUMBER    = None   # e.g. "NH-53"
OPTIONAL_SECTION_CODE = None   # e.g. "OD/53/1"

# COMMAND ----------

def load_gps(file_path: str) -> pd.DataFrame:
    """Load GPS sheet; handles all known sheet/column name variants and GPS Fix filtering."""
    gps = None
    for _hdr in [0, 1, 2, 3]:
        try:
            _cand = read_sheet_fast(file_path, 'GPS', header=_hdr)
            _cand = _apply_aliases(_cand, _GPS_ALIASES)
            if 'CHAINAGE' in _cand.columns:
                gps = _cand; break
        except Exception:
            continue
    if gps is None:
        try:
            gps = read_sheet_fast(file_path, 'GPS', header=2)
            gps = _apply_aliases(gps, _GPS_ALIASES)
        except Exception as _e:
            raise RuntimeError(f"GPS sheet unreadable in {os.path.basename(file_path)}: {_e}") from _e

    if OPTIONAL_NH_NUMBER    is not None: gps['NH_Number']    = OPTIONAL_NH_NUMBER
    if OPTIONAL_SECTION_CODE is not None: gps['Section_Code'] = OPTIONAL_SECTION_CODE

    _fix_col = next((a for a in _GPS_FIX_ALIASES if a in gps.columns), None)
    keep = [c for c in ['NH_Number', 'Section_Code', 'CHAINAGE', 'Direction',
                         'Lane_Number', 'Latitude', 'Longitude'] if c in gps.columns]
    if _fix_col: keep.append(_fix_col)
    gps = gps[keep].copy()

    for c in ['CHAINAGE', 'Latitude', 'Longitude']:
        if c in gps.columns:
            gps[c] = pd.to_numeric(gps[c], errors='coerce')

    if _fix_col:
        _no_fix = _is_no_fix(gps[_fix_col])
        if _no_fix.any():
            gps.loc[_no_fix, 'Latitude']  = float('nan')
            gps.loc[_no_fix, 'Longitude'] = float('nan')
            _msg = f"GPS Fix: {int(_no_fix.sum())}/{len(gps)} No-Fix rows cleared — gaps bridged ≤ max_gap_m."
            print(f"  {_msg}")
            log_issue("WARNING", "Backup", _msg, table="GPS_Geometry")
    else:
        # Logged (not just printed) -- a GPS sheet with no Fix column at
        # all means fix-quality filtering never actually runs for this
        # project (every row is treated as valid regardless of real GPS
        # quality) -- worth having in the persistent log, not just
        # console output that disappears once the cell finishes running.
        _msg = f"GPS Fix column not found — all {len(gps)} rows treated as valid."
        print(f"  {_msg}")
        log_issue("WARNING", "Backup", _msg, table="GPS_Geometry")

    miss = [c for c in ['CHAINAGE', 'Latitude', 'Longitude'] if c not in gps.columns]
    if miss:
        raise KeyError(f"GPS sheet missing required columns {miss}. Present: {list(gps.columns)}")

    # Infer Direction if absent or all-null
    if 'Direction' not in gps.columns or gps['Direction'].isna().all():
        ch = gps['CHAINAGE'].dropna().to_numpy(dtype=float)
        gps['Direction'] = 'Decreasing' if (len(ch) > 1 and ch[-1] < ch[0]) else 'Increasing'

    gps = gps.dropna(subset=['CHAINAGE', 'Latitude', 'Longitude'])
    return gps.drop_duplicates(subset=['CHAINAGE'], keep='first')

# COMMAND ----------

def build_lookup(gps: pd.DataFrame) -> dict:
    """Null-safe: rows with a non-numeric/missing CHAINAGE can't become a
    dict key, so they're dropped here instead of raising on .astype(int)."""
    ch = pd.to_numeric(gps['CHAINAGE'], errors='coerce')
    mask = ch.notna()
    if not mask.all():
        log_issue("WARNING", "Backup", f"{(~mask).sum()} row(s) with null/non-numeric CHAINAGE dropped in build_lookup.")
    keys = ch[mask].round().astype(int).tolist()
    return dict(zip(keys, zip(gps.loc[mask, 'Latitude'].tolist(), gps.loc[mask, 'Longitude'].tolist())))

# COMMAND ----------

def _aggregate_raw_cracks(file_path: str, geom: pd.DataFrame) -> pd.DataFrame:
    crack_sheet = _resolve_sheet(file_path, 'Cracking')
    df = None
    for hdr in range(0, 5):
        try:
            cand = read_sheet_fast(file_path, crack_sheet, hdr)
            cand = _apply_aliases(cand, _CRACK_ALIASES)
            if 'CHAINAGE' in cand.columns: df = cand; break
        except Exception:
            continue
    # NOTE: both early-return branches below (no Cracking sheet found /
    # sheet found but zero valid crack rows) used to set ONLY crack_count
    # and return -- meaning a lane with genuinely zero recorded cracks
    # (a perfectly valid, real result -- not an error) never got
    # avg_length_mm/avg_width_mm/avg_depth_mm/avg_corrected_width_mm/
    # survey_date_crack columns AT ALL. Schema-casting then saw those
    # columns as fully ABSENT (not just null-valued), logging "missing
    # from source" CRITICAL even though nothing was actually wrong --
    # the lane just had no cracks to average. Both branches now emit the
    # full expected column set as null/0, so a crack-free lane reports
    # "0 cracks, no average to report" instead of looking like broken data.
    _EMPTY_CRACK_COLS = {
        'avg_length_mm': np.nan, 'avg_width_mm': np.nan, 'avg_depth_mm': np.nan,
        'avg_corrected_width_mm': np.nan, 'survey_date_crack': None,
    }
    out = geom.copy()
    if df is None:
        out['crack_count'] = 0
        for c, v in _EMPTY_CRACK_COLS.items():
            out[c] = v
        return out

    for col in ['CHAINAGE', 'LENGTH', 'WIDTH', 'DEPTH', 'Corrected Width', 'AreaConsidered']:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.dropna(subset=['CHAINAGE'])
    if df.empty:
        out['crack_count'] = 0
        for c, v in _EMPTY_CRACK_COLS.items():
            out[c] = v
        return out

    cracks = df.sort_values('CHAINAGE').reset_index(drop=True)
    ch_arr = cracks['CHAINAGE'].to_numpy(dtype=float)
    lo     = out[['Start_Chainage', 'End_Chainage']].min(axis=1).to_numpy(dtype=float)
    hi     = out[['Start_Chainage', 'End_Chainage']].max(axis=1).to_numpy(dtype=float)
    idx_lo = np.searchsorted(ch_arr, lo, side='left')
    idx_hi = np.searchsorted(ch_arr, hi, side='right')
    out['crack_count'] = (idx_hi - idx_lo).astype(int)

    NCOLS = {'LENGTH': 'avg_length_mm', 'WIDTH': 'avg_width_mm', 'DEPTH': 'avg_depth_mm',
             'Corrected Width': 'avg_corrected_width_mm'}
    for orig, new in NCOLS.items():
        if orig not in cracks.columns: continue
        arr   = cracks[orig].to_numpy(dtype=float); valid = ~np.isnan(arr)
        ps    = np.concatenate([[0.0], np.cumsum(np.where(valid, arr, 0.0))])
        pc    = np.concatenate([[0.0], np.cumsum(valid.astype(float))])
        with np.errstate(invalid='ignore', divide='ignore'):
            out[new] = np.where((idx_hi - idx_lo) > 0,
                                np.round((ps[idx_hi] - ps[idx_lo]) / np.maximum(pc[idx_hi] - pc[idx_lo], 1), 4),
                                np.nan)
    # NOTE: output column is 'survey_date_crack' (not 'survey_date') so it can
    # never collide with Rutting's 'survey_date_rutting' / Raveling's
    # 'survey_date_raveling' during schema casting (see cast_dataframe_with_schema).
    if 'Survey Date' in cracks.columns:
        dates = cracks['Survey Date'].astype(str).tolist()
        out['survey_date_crack'] = [dates[int(l)] if int(h) > int(l) else None
                              for l, h in zip(idx_lo, idx_hi)]
    return out

# COMMAND ----------

def build_unified_lane_segments(file_path: str, lane: str) -> pd.DataFrame:
    """
    One row per 10m road segment for one lane workbook, with GPS-derived
    geometry, cracking, roughness (IRI), rutting, and raveling merged
    together — see module docstring for what this does and doesn't cover.

    Returns an empty DataFrame if the GPS sheet's chainage range is too
    short to form even one 10m segment (build_geometry_10m returns empty
    in that case) -- seen on Zone A's Kharar-Kurali RIGID L1/L2 files,
    whose GPS sheet only spans 7m of chainage (8 rows total).

    Kept as a thin wrapper around build_lane_stage_tables() for backward
    compatibility with any caller that only wants the final merged table.
    """
    return build_lane_stage_tables(file_path, lane)['Unified_Segments']

# COMMAND ----------

# ── Roughness ─────────────────────────────────────────────────────────────────
_ROUGH_KEEP = ['NH_Number', 'Section_Code', 'Start_Chainage', 'End_Chainage',
               'Direction', 'Lane_Number_Original', 'LWP IRI (m/km)',
               'RWP IRI (m/km)', 'Lane IRI (m/km)', 'Roughness Index (mm/km)',
               'Average Speed of NSV', 'Condition of Road Surface (As per IRC:SP:16-2019)']

# COMMAND ----------

def _roughness_from_df(df: pd.DataFrame, sc: str, ec: str, lane: str, lookup, sorted_keys) -> pd.DataFrame:
    """Build roughness output from a dataframe where sc/ec are the chainage columns."""
    df = df.rename(columns={sc: 'Start_Chainage', ec: 'End_Chainage',
                             'New NH\nNumber': 'NH_Number', 'Section Code': 'Section_Code',
                             'Lane Number': 'Lane_Number_Original'})
    df = _apply_aliases(df, _ROUGH_ALIASES)
    keep = [c for c in _ROUGH_KEEP if c in df.columns]
    out  = df[keep].copy()
    out['Start_Chainage'] = pd.to_numeric(out['Start_Chainage'], errors='coerce')
    out['End_Chainage']   = pd.to_numeric(out['End_Chainage'],   errors='coerce')
    out = out.dropna(subset=['Start_Chainage', 'End_Chainage'])
    if out.empty: return pd.DataFrame()
    # Auto-resample if interval is not 10m
    info = detect_chainage_interval(out)
    if info['interval_m'] and info['interval_m'] != 10:
        print(f"  [{lane}] Roughness: {info['interval_m']}m interval — resampling to 10m.")
        _num = ['LWP IRI (m/km)', 'RWP IRI (m/km)', 'Lane IRI (m/km)',
                'Roughness Index (mm/km)', 'Average Speed of NSV']
        _str = ['NH_Number', 'Section_Code', 'Direction', 'Lane_Number_Original',
                'Condition of Road Surface (As per IRC:SP:16-2019)']
        out = _resample_to_10m(out, [c for c in _num if c in out.columns],
                                    [c for c in _str if c in out.columns])
    slats, slons, elats, elons = [], [], [], []
    for s, e in zip(out['Start_Chainage'], out['End_Chainage']):
        sl, slo = nearest_lat_lon(lookup, sorted_keys, s)
        el, elo = nearest_lat_lon(lookup, sorted_keys, e)
        slats.append(sl); slons.append(slo); elats.append(el); elons.append(elo)
    out['Start_Latitude']  = slats; out['Start_Longitude'] = slons
    out['End_Latitude']    = elats; out['End_Longitude']   = elons
    out['Lane'] = lane
    return out

# COMMAND ----------

def build_roughness(file_path: str, lookup: dict, sorted_keys, lane: str) -> pd.DataFrame:
    """
    Build roughness aligned to 10m grid.
    Strategy: raw per-metre point samples (LCMS-style, LWP_IRI/RWP_IRI/
    LANE_IRI averaged into 10m buckets) → 10m-block detection → fall back
    to Start/End Chainage columns.
    """
    sheet = _resolve_sheet(file_path, 'Roughness')
    for hdr in [2, 1, 0, 3]:
        try:
            d = read_sheet_fast(file_path, sheet, hdr)
            d = _apply_aliases(d, _ROUGH_ALIASES)
            d.columns = [c.strip() for c in d.columns]

            # Raw per-metre point-sample block (LCMS-style) — preferred when
            # present: it's the only source with real 1m granularity and
            # doesn't carry the -1 CWP_IRI sentinel.
            res = _roughness_from_raw_points(d, lane, lookup, sorted_keys)
            if not res.empty:
                return res

            # Try 10m-block detection first
            idx = _locate_10m_block(d)
            if idx is not None:
                sc, ec = list(d.columns)[idx], list(d.columns)[idx + 1]
                res = _roughness_from_df(d, sc, ec, lane, lookup, sorted_keys)
                if not res.empty and any(c in res.columns for c in ['Lane IRI (m/km)', 'LWP IRI (m/km)']):
                    return res

            # Standard second-occurrence Start/End Chainage. Checks BOTH
            # the spaced ("Start Chainage") and no-space ("StartChainage")
            # naming conventions -- e.g. Zone E's L1/R1 IRI sheets use the
            # no-space camelCase style, which this check used to miss
            # entirely (c.startswith('Start Chainage') is False for
            # "StartChainage"), falling all the way through to "no usable
            # block found" even though the data was perfectly usable.
            _sc_base = ('Start Chainage' if any(c.startswith('Start Chainage') for c in d.columns)
                        else 'StartChainage' if any(c.startswith('StartChainage') for c in d.columns)
                        else None)
            _ec_base = ('End Chainage' if any(c.startswith('End Chainage') for c in d.columns)
                        else 'EndChainage' if any(c.startswith('EndChainage') for c in d.columns)
                        else None)
            if _sc_base and _ec_base:
                sc = _new_nh_ch_col(d, _sc_base)
                ec = _new_nh_ch_col(d, _ec_base)
                res = _roughness_from_df(d, sc, ec, lane, lookup, sorted_keys)
                if not res.empty: return res

        except Exception:
            continue

    # By this point _resolve_sheet(file_path, 'Roughness') has ALREADY
    # tried every known alias for this table (Roughness, IRI, RAW IRI --
    # see _SHEET_ALIASES), every header row (0-3) has been tried, AND every
    # extraction strategy (raw per-metre points, 10m-block, explicit
    # Start/End Chainage in both spaced and no-space naming) has failed --
    # so reaching here means Roughness genuinely isn't recoverable from
    # this file, not that it's just named differently somewhere.
    log_issue("CRITICAL", "Backup",
               "No usable Roughness data found in this lane workbook (tried "
               "every known Roughness sheet-name alias, header row, and "
               "extraction strategy) -- Roughness_IRI will be written with "
               "all measurement columns null.",
               file=file_path, table="Roughness_IRI")
    print(f"  [{lane}] Roughness: no usable block found.")
    return pd.DataFrame()

# COMMAND ----------

# Each entry is (column_key, mm2_scale_factor). build_cracking() sums the
# returned Series and divides by 1_000_000 to get m^2 -- so this function's
# contract is "always return the area already expressed in mm^2", and every
# entry must supply its own scale factor to satisfy that, whatever unit the
# raw source column is actually in.
#
# FIX (bug 1 -- column not recognised at all): LCMS-style raw exports (e.g.
# LCMS_L2/L3/R1/R2/R3.xlsx) label the per-crack area column simply "AREA" --
# none of the first 4 keys ever matched it, so _resolve_crack_area() silently
# returned None on every one of these lane workbooks. build_cracking() then
# fell back to LENGTH * Corrected Width, but 'Corrected Width' doesn't exist
# in these sheets either -> NaN -> Area_Sum/Percent_Area = 0.0 for every
# segment, indistinguishable from a genuine zero-crack lane, with NO
# WARNING/CRITICAL logged anywhere (unlike the "sheet not found" case).
#
# FIX (bug 2 -- unit mismatch): the "AREA" column is itself already in m^2,
# NOT mm^2 like the pre-existing keys assume. Verified against LCMS_L2.xlsx:
# raw AREA sums to ~568 across the sheet, which is ~0.84% of total lane area
# (1930 segments x 10m x 3.5m) -- a realistic cracking percentage. Passing
# that straight through the existing "divide by 1e6" mm^2->m^2 conversion
# shrank every result by another 1,000,000x (0.84% became 0.00000084%).
# Scaling by 1_000_000 here converts it to the mm^2 the rest of the function
# expects, so the *existing* /1e6 downstream conversion cancels it back out
# to the correct real-world m^2 value.
_CRACK_AREA_PRIORITY = [
    ('crack area',              1),
    ('area to be considered',   1),
    ('area considered',         1),
    ('total area',              1),
    ('area',               1_000_000),   # LCMS raw "AREA" column: native m^2
]

def _resolve_crack_area(df: pd.DataFrame):
    """Find the per-crack area column by exact name, checked in priority
    order, regardless of where it sits in the sheet. Returns a numeric
    Series already expressed in mm^2 (scaled per-key as needed), or None
    if no matching column exists."""
    norm = {c.strip().lower(): c for c in df.columns}
    for key, scale in _CRACK_AREA_PRIORITY:
        if key in norm:
            vals = pd.to_numeric(df[norm[key]], errors='coerce')
            return vals * scale if scale != 1 else vals
    return None

# COMMAND ----------

def build_cracking(file_path: str, geometry: pd.DataFrame, lane: str) -> pd.DataFrame:
    """Aggregate per-crack records onto 10m grid. Auto-detects header row."""
    crack_sheet = _resolve_sheet(file_path, 'Cracking')
    crack = None
    for hdr in range(0, 5):
        try:
            cand = read_sheet_fast(file_path, crack_sheet, hdr)
            cand = _apply_aliases(cand, _CRACK_ALIASES)
            if 'CHAINAGE' in cand.columns:
                crack = cand; break
        except Exception:
            continue
    if crack is None:
        try:
            crack = read_sheet_fast(file_path, crack_sheet, 2)
            crack = _apply_aliases(crack, _CRACK_ALIASES)
        except Exception:
            pass
    if crack is None or 'CHAINAGE' not in crack.columns:
        # By this point _resolve_sheet(file_path, 'Cracking') has ALREADY
        # tried every known alias for this table (Cracking, Cracks, Crack,
        # 'Cracks (MDB)', 'Final Crack Rating' -- see _SHEET_ALIASES), and
        # every header-row (0-4) has ALREADY been tried for a usable
        # CHAINAGE column -- so reaching here means the sheet is genuinely
        # missing/unusable, not just named differently. Previously this was
        # a silent print() only; the lane workbook would happily write
        # Cracking with Area_Sum/Percent_Area = 0.0 everywhere (indistinguishable
        # from a real "zero cracks" result) with no WARNING/CRITICAL anywhere
        # in the run log. Logged as CRITICAL to match GPS_Geometry's
        # treatment -- missing crack data is just as serious as missing GPS.
        log_issue("CRITICAL", "Backup",
                   "No usable Cracking sheet/data found in this lane workbook "
                   "(tried every known Cracking sheet-name alias and header "
                   "row) -- Cracking will be written with Area_Sum/Percent_Area "
                   "= 0.0 everywhere, indistinguishable from a genuine zero-crack "
                   "result.", file=file_path, table="Cracking")
        print(f"  [{lane}] Cracking: no usable sheet found — returning zeros.")
        out = geometry.copy()
        out['Area_Sum']    = 0.0
        out['Lane_Area']   = np.abs(out['Start_Chainage'] - out['End_Chainage']) * 3.5
        out['Percent_Area'] = 0.0
        out['Lane'] = lane
        return out

    area_mm2 = _resolve_crack_area(crack)
    if area_mm2 is None:
        length = pd.to_numeric(crack.get('LENGTH',          pd.Series(dtype=float)), errors='coerce')
        cw     = pd.to_numeric(crack.get('Corrected Width', pd.Series(dtype=float)), errors='coerce')
        area_mm2 = length * cw

    # FIX: previously silent. If _resolve_crack_area() found no column at
    # all (or the LENGTH * Corrected Width fallback also has nothing usable,
    # e.g. 'Corrected Width' doesn't exist), Area_Sum/Percent_Area end up
    # 0.0 for every 10m segment -- looking exactly like a genuine zero-crack
    # lane. This used to happen with zero visibility in the run log. Now
    # flagged so it can never again go unnoticed the way it did for
    # LCMS L2/R1/R2/R3.xlsx (their real area column is literally "AREA",
    # which is now in _CRACK_AREA_PRIORITY above -- but if a future project
    # uses yet another naming, this warning will catch it immediately).
    if area_mm2 is None or area_mm2.notna().sum() == 0:
        log_issue("WARNING", "Backup",
                   "No usable per-crack area column resolved in this Cracking "
                   "sheet (checked _CRACK_AREA_PRIORITY and LENGTH * Corrected "
                   "Width fallback) -- Area_Sum/Percent_Area will be 0.0 "
                   "everywhere, indistinguishable from a genuine zero-crack "
                   "result.", file=file_path, table="Cracking")

    ch   = pd.to_numeric(crack.get('CHAINAGE', pd.Series(dtype=float)), errors='coerce')
    mask = ch.notna() & area_mm2.notna()
    ch   = ch[mask].to_numpy(); area = area_mm2[mask].to_numpy()
    order     = np.argsort(ch, kind='stable')
    ch_sorted = ch[order]
    prefix    = np.concatenate([[0.0], np.cumsum(area[order])])

    out    = geometry.copy()
    starts = out['Start_Chainage'].to_numpy(dtype=float)
    ends   = out['End_Chainage'].to_numpy(dtype=float)
    upper  = np.maximum(starts, ends); lower = np.minimum(starts, ends)
    right_idx  = np.searchsorted(ch_sorted, upper, side='right')
    left_idx   = np.searchsorted(ch_sorted, lower, side='right')
    raw_sum_m2 = (prefix[right_idx] - prefix[left_idx]) / 1_000_000.0
    lane_area  = np.abs(starts - ends) * 3.5
    capped     = np.minimum(raw_sum_m2, lane_area)
    out['Area_Sum']     = capped
    out['Lane_Area']    = lane_area
    out['Percent_Area'] = np.where(lane_area > 0, capped / lane_area * 100.0, 0.0)
    out['Lane'] = lane
    return out

# COMMAND ----------

def safe_int_cast(df: pd.DataFrame, cols: list, context: str = "") -> pd.DataFrame:
    """
    Null-safe replacement for df[cols] = df[cols].astype(int). Coerces each
    column to numeric first; rows where any of `cols` end up null/non-numeric
    are dropped (can't form a valid chainage segment) instead of raising.
    Logs a concise WARNING with the drop count -- never raises, never stops
    the pipeline.
    """
    before = len(df)
    d = df.copy()
    for c in cols:
        d[c] = pd.to_numeric(d[c], errors='coerce')
    d = d.dropna(subset=cols)
    dropped = before - len(d)
    if dropped:
        log_issue("WARNING", "Backup",
                   f"{dropped} row(s) dropped: null/non-numeric {', '.join(cols)}",
                   table=context)
    for c in cols:
        d[c] = d[c].astype(int)
    return d.reset_index(drop=True)


def build_lane_stage_tables(file_path: str, lane: str) -> dict:
    """
    Run the same per-lane build as build_unified_lane_segments(), but
    return every intermediate stage table too, keyed by STAGE_TABLE_NAMES:
    GPS_Geometry, Cracking, Roughness_IRI, Rutting, Raveling, and the final
    Unified_Segments (identical to what build_unified_lane_segments()
    returns on its own).

    If the GPS sheet's chainage range is too short to form even one 10m
    segment (seen on Zone A's Kharar-Kurali RIGID L1/L2 files, whose GPS
    sheet only spans 7m of chainage), GPS_Geometry/Unified_Segments come
    back empty and the other four stages are empty placeholders too — the
    build never gets far enough to compute them.
    """
    gps = load_gps(file_path)

    # GUARD: some lane workbooks (seen on Zone E R1/R2 -- the GPS
    # Coordinates sheet exists but every data row is blank, so load_gps()
    # returns a 0-row DataFrame) used to crash build_geometry_10m() with
    # IndexError: single positional indexer is out-of-bounds, because that
    # function unconditionally did gps['Direction'].iloc[0] etc. before its
    # own empty-check ran. Caught here instead, BEFORE calling
    # build_geometry_10m(), and logged as CRITICAL (not WARNING) -- this is
    # a genuinely missing/unusable source sheet, not the separate "chainage
    # range too short" case a few lines below, which stays WARNING.
    if gps.empty:
        log_issue("CRITICAL", "Backup",
                   "GPS Coordinates sheet has no usable data rows (sheet "
                   "present but every row is blank) -- cannot build "
                   "geometry for this lane workbook.",
                   file=file_path, table="GPS_Geometry")
        empty = pd.DataFrame()
        return {
            'GPS_Geometry': empty, 'Cracking': empty, 'Roughness_IRI': empty,
            'Rutting': empty, 'Raveling': empty, 'Unified_Segments': empty,
        }

    lu = build_lookup(gps)
    sk = sorted(lu.keys())

    geom = build_geometry_10m(gps, lu, sk, lane)
    if geom.empty:
        empty = pd.DataFrame()
        return {
            'GPS_Geometry': geom, 'Cracking': empty, 'Roughness_IRI': empty,
            'Rutting': empty, 'Raveling': empty, 'Unified_Segments': geom,
        }
    geom = safe_int_cast(geom, ['Start_Chainage', 'End_Chainage'], context='GPS_Geometry')

    gc = build_cracking(file_path, geom, lane)
    gc = safe_int_cast(gc, ['Start_Chainage', 'End_Chainage'], context='Cracking')
    gc = _aggregate_raw_cracks(file_path, gc)

    # NOTE: build_roughness()/build_rutting() now log their own CRITICAL
    # issue directly (with full context on which strategies were tried)
    # right before returning empty on genuine failure -- see the comments
    # inside those functions. No separate logging needed here; this block
    # just normalises the empty result to a placeholder frame either way.
    rough_native = build_roughness(file_path, lu, sk, lane)
    if rough_native.empty or 'Start_Chainage' not in rough_native.columns:
        rough_native = pd.DataFrame(columns=['Lane', 'Start_Chainage', 'End_Chainage'])
    else:
        rough_native = safe_int_cast(rough_native, ['Start_Chainage', 'End_Chainage'], context='Roughness_IRI')

    rut_native = build_rutting(file_path, lu, sk, lane)
    if rut_native.empty or 'Start_Chainage' not in rut_native.columns:
        rut_native = pd.DataFrame(columns=['Lane', 'Start_Chainage', 'End_Chainage'])
    else:
        rut_native = safe_int_cast(rut_native, ['Start_Chainage', 'End_Chainage'], context='Rutting')

    ravel_native = read_raveling(file_path, lane)

    # Attach each stage onto GPS_Geometry's own 10m grid (see module
    # docstring) WITHOUT broadcasting coarser survey intervals: only
    # Roughness_IRI/Rutting rows already at 10m-or-finer native resolution
    # (native 10m, or 1m samples pre-averaged into 10m buckets) get
    # attached here. Native ~100m/1km Roughness/Rutting rows are left
    # null on the grid instead of being repeated across every 10m
    # sub-segment they span. Raveling is always native-10m already.
    rough = _disaggregate_to_grid(geom, rough_native, _ROUGH_COLS, stage_name='Roughness_IRI')
    rut = _disaggregate_to_grid(geom, rut_native, _RUT_COLS, stage_name='Rutting')
    ravel = _disaggregate_to_grid(geom, ravel_native, _RAVEL_COLS, bucket_width=10, stage_name='Raveling')

    # rough/rut/ravel are now on the exact same row-for-row 10m grid as
    # geom, and gc (cracking) was itself built from that same geom grid
    # (geometry.copy() in build_cracking) — so their value columns can be
    # assigned onto gc positionally instead of asof-joined a second time.
    base = gc.copy()
    for c in _ROUGH_COLS:
        base[c] = rough[c].values
    for c in _RUT_COLS:
        base[c] = rut[c].values
    for c in _RAVEL_COLS:
        base[c] = ravel[c].values

    return {
        'GPS_Geometry': geom,
        'Cracking': gc,
        'Roughness_IRI': rough,
        'Rutting': rut,
        'Raveling': ravel,
        'Unified_Segments': base,
    }

# COMMAND ----------

_WB_CACHE: dict = {}
def _get_wb(file_path: str):
    """Return cached fastexcel workbook; falls back to openpyxl shim.

    FIX: previously only caught (ModuleNotFoundError, ImportError) here --
    i.e. only "fastexcel isn't installed". If fastexcel.read_excel() raised
    ANY OTHER exception while opening a specific file (a corrupted zip
    entry, an unusual XML quirk, an unsupported feature -- fastexcel/
    calamine is stricter about some real-world xlsx quirks than openpyxl
    is), that exception was never caught here at all. It propagated up
    through _resolve_sheet()/read_sheet_fast() (neither of which catches
    anything) into the caller, where build_cracking()/build_roughness()/
    read_raveling() each wrap every header-row attempt in a bare
    "except Exception: continue". Because _WB_CACHE[file_path] never got
    populated (the assignment is INSIDE the failed try), the exact same
    exception recurred on every attempt in all three independent
    extractors for that file -- each one silently exhausted its retries
    and logged the generic, misleading "no usable sheet/data found"
    CRITICAL, even though the sheets were genuinely fine. This is exactly
    the pattern seen in the run log: Cracking + Roughness_IRI + Raveling
    all failed together on the SAME 4 files (L2/R1/R2/R3) and all
    succeeded together on the other 2 (L1/L3) -- three unrelated pieces of
    alias/header-matching logic don't coincidentally fail on the same
    files unless something shared underneath (opening the workbook) is
    what's actually breaking.
    Now: catch ANY exception from fastexcel, log what it actually was, and
    always fall back to the more lenient openpyxl shim (previously that
    shim was ONLY reachable when fastexcel was simply not installed).
    """
    if file_path not in _WB_CACHE:
        try:
            import fastexcel
            _devnull = open(os.devnull, 'w'); _old = sys.stderr; sys.stderr = _devnull
            try:
                _WB_CACHE[file_path] = fastexcel.read_excel(_long_path(file_path))
            finally:
                sys.stderr = _old; _devnull.close()
        except Exception as exc:
            if not isinstance(exc, (ModuleNotFoundError, ImportError)):
                log_issue("WARNING", "Backup",
                           f"fastexcel failed to open this workbook ({concise_exc(exc)}) "
                           f"-- every sheet-read attempt in this file would otherwise have "
                           f"silently failed and been misreported as 'no usable sheet found'. "
                           f"Falling back to openpyxl.",
                           file=file_path)
            import openpyxl as _opxl
            class _OpxlShim:
                def __init__(self, path):
                    self._wb = _opxl.load_workbook(_long_path(path), read_only=True, data_only=True)
                    self.sheet_names = self._wb.sheetnames
                def load_sheet(self, name, header_row=0):
                    return _OpxlSheet(self._wb[name], header_row)
            class _OpxlSheet:
                def __init__(self, ws, hr):
                    self._ws = ws; self._hr = hr
                def to_pandas(self):
                    rows = list(self._ws.iter_rows(values_only=True))
                    if not rows: return pd.DataFrame()
                    # FIX: in read_only mode, openpyxl's iter_rows() does NOT
                    # pad every row to the sheet's global max column -- each
                    # row's tuple length is that row's OWN last populated
                    # cell. Real LCMS exports commonly have a short header
                    # row (e.g. 9 populated cells) followed by data rows that
                    # extend much further right (e.g. 19+ cells, because of a
                    # trailing summary-formula block a few columns over) --
                    # pd.DataFrame(rows, columns=headers) then raises
                    # "N columns passed, passed data had M columns" and
                    # crashes outright instead of just reading the sheet.
                    # Pad every row up to the widest row in the whole sheet
                    # before building the header list or the DataFrame, so
                    # ragged rows can never mismatch column counts again.
                    max_len = max(len(r) for r in rows)
                    rows = [r + (None,) * (max_len - len(r)) if len(r) < max_len else r
                            for r in rows]
                    raw = [str(c) if c is not None else f'__UNNAMED__{i}'
                           for i, c in enumerate(rows[self._hr])]
                    seen, headers = {}, []
                    for col in raw:
                        if col in seen: seen[col] += 1; headers.append(f'{col}.{seen[col]}')
                        else: seen[col] = 0; headers.append(col)
                    return pd.DataFrame(rows[self._hr + 1:], columns=headers)
            try:
                _WB_CACHE[file_path] = _OpxlShim(file_path)
            except Exception as exc2:
                # Both fastexcel AND openpyxl failed to open this file -- this
                # is a genuinely broken/corrupted workbook, not a code bug.
                # Re-raise so build_lane_stage_tables()'s own except-block
                # (Cell 94) logs it as a WARNING with the real reason,
                # instead of three separate misleading "sheet not found"
                # CRITICALs downstream.
                raise RuntimeError(
                    f"Could not open workbook with fastexcel OR openpyxl "
                    f"(fastexcel: {concise_exc(exc)}; openpyxl: {concise_exc(exc2)})"
                ) from exc2
    return _WB_CACHE[file_path]

# COMMAND ----------


# ── GPS Fix detection ─────────────────────────────────────────────────────────
_GPS_FIX_ALIASES = [
    'GPS Fix', 'GPS_Fix', 'GPS FIX', 'GPS_FIX', 'FIX', 'Fix', 'fix',
    'GPSFIX', 'GPS Status', 'GPS_Status', 'Fix Type', 'Fix_Type',
    'FixType', 'STATUS', 'Status',
]

def _is_no_fix(series: pd.Series) -> pd.Series:
    s = series.fillna('').astype(str).str.strip().str.lower()
    return s.isin(['0', 'no', 'no fix', 'nofix', 'no_fix', 'invalid', 'false', ''])


# ── Sheet name resolution ─────────────────────────────────────────────────────
_SHEET_PREFIX_RE = re.compile(r'^\s*\d+(\.\d+)*[\.\)\-\s]+')

def _norm_sheet(name: str) -> str:
    """Strip leading numeric section prefix and normalise to lowercase."""
    s = _SHEET_PREFIX_RE.sub('', str(name))
    return ' '.join(s.strip().lower().split())

# COMMAND ----------

def _apply_aliases(df: pd.DataFrame, alias_map: dict) -> pd.DataFrame:
    """Rename columns whose lowercased+stripped name appears in alias_map.
    When multiple columns match the same alias key, the FIRST (leftmost)
    matching column wins — on dual-block sheets (e.g. LCMS exports that
    carry a raw per-metre point block AND a separately-formatted aggregated
    block later in the same sheet), the raw/leftmost block is always the
    one every extractor here is designed to read."""
    existing = set(df.columns); claimed, rename = set(), {}
    for col in df.columns:
        key = col.strip().lower().replace('\n', ' ')
        target = alias_map.get(key)
        if target and col != target and target not in existing and target not in claimed:
            rename[col] = target; claimed.add(target)
    return df.rename(columns=rename) if rename else df

# COMMAND ----------

# ── Geometry ──────────────────────────────────────────────────────────────────
def build_geometry_10m(gps: pd.DataFrame, lookup: dict, sorted_keys,
                        lane: str = '', max_gap_m: float = 50.0) -> pd.DataFrame:
    """
    Build 10m segment grid from GPS chainage range.
    Each row carries GPS_Coverage: 'fix' | 'interpolated' | 'gap'.
    """
    # Defensive safety net: build_lane_stage_tables() now catches an empty
    # `gps` earlier and never reaches this function in that case (see the
    # CRITICAL guard there) -- but this function can also be called
    # directly/standalone, so it stays safe on its own too instead of
    # relying on callers to check first.
    if gps.empty:
        return pd.DataFrame()

    direction    = gps['Direction'].iloc[0] if 'Direction' in gps.columns else 'Increasing'
    section_code = gps['Section_Code'].iloc[0] if 'Section_Code' in gps.columns else ''
    nh_number    = gps['NH_Number'].iloc[0]    if 'NH_Number'    in gps.columns else ''

    _ch_valid = gps['CHAINAGE'].dropna() if 'CHAINAGE' in gps.columns else pd.Series([], dtype=float)
    if _ch_valid.empty: return pd.DataFrame()
    fix_set = set(lookup.keys())

    def _cov(s_ch, e_ch, s_lat, e_lat):
        if s_lat is None or e_lat is None: return 'gap'
        if int(round(s_ch)) in fix_set and int(round(e_ch)) in fix_set: return 'fix'
        return 'interpolated'

    # Split GPS fixes into clusters separated by large chainage jumps (e.g. two road sections).
    # Avoids creating thousands of geometry rows across a 20+ km gap with no GPS data.
    CLUSTER_GAP_M = 500
    clusters: list = []
    if sorted_keys:
        cs = sorted_keys[0]; prev = sorted_keys[0]
        for k in sorted_keys[1:]:
            if k - prev > CLUSTER_GAP_M:
                clusters.append((int(cs), int(prev)))
                cs = k
            prev = k
        clusters.append((int(cs), int(prev)))
    else:
        ch_min = int(round(_ch_valid.min())); ch_max = int(round(_ch_valid.max()))
        clusters = [(ch_min, ch_max)]

    rows, dec = [], str(direction).strip().lower().startswith('dec')
    step = -10 if dec else 10
    for c_lo, c_hi in (reversed(clusters) if dec else clusters):
        start = c_hi if dec else c_lo
        while (start + step >= c_lo) if dec else (start + step <= c_hi):
            end = start + step
            slat, slon = nearest_lat_lon(lookup, sorted_keys, start, max_gap_m)
            elat, elon = nearest_lat_lon(lookup, sorted_keys, end,   max_gap_m)
            rows.append({'NH_Number': nh_number, 'Section_Code': section_code,
                         'Direction': direction, 'Lane': lane,
                         'Start_Chainage': start, 'End_Chainage': end,
                         'Start_Latitude': slat, 'Start_Longitude': slon,
                         'End_Latitude': elat,   'End_Longitude': elon,
                         'GPS_Coverage': _cov(start, end, slat, elat)})
            start = end
    df = pd.DataFrame(rows)
    if not df.empty:
        gaps = (df['GPS_Coverage'] == 'gap').sum()
        if gaps: print(f"  [{lane}] geometry: {gaps}/{len(df)} segments with GPS gaps — lat/lon null.")
    return df

# COMMAND ----------


def nearest_lat_lon(lookup: dict, sorted_keys, chainage, max_gap_m: float = 50.0):
    """
    Return (lat, lon) for chainage via exact hit or linear interpolation.
    Returns (None, None) if outside GPS range or gap > max_gap_m.
    """
    if pd.isna(chainage): return None, None
    ch = float(chainage); rounded = int(round(ch))
    if rounded in lookup: return lookup[rounded]
    idx = bisect.bisect_left(sorted_keys, ch)
    if idx == 0 or idx >= len(sorted_keys): return None, None
    lo, hi = sorted_keys[idx - 1], sorted_keys[idx]
    if hi == lo: return lookup[lo]
    if hi - lo > max_gap_m: return None, None
    t = (ch - lo) / (hi - lo)
    lat_lo, lon_lo = lookup[lo]; lat_hi, lon_hi = lookup[hi]
    return (lat_lo + t * (lat_hi - lat_lo), lon_lo + t * (lon_hi - lon_lo))

# COMMAND ----------

_SHEET_ALIASES = {
    'GPS':       ['GPS', 'GPS Coordinates', 'GPS coordinates', ' GPS',
                  'L1 GPS', 'L2 GPS', 'L3 GPS', 'R1 GPS', 'R2 GPS', 'R3 GPS'],
    'Roughness': ['Roughness', 'IRI', 'RAW IRI'],
    'Rutting':   ['Rutting', 'RutDepth', 'Rut Depth', 'Rut', 'RUT'],
    'Cracking':  ['Cracking', 'Cracks', 'Crack', 'Cracks (MDB)', 'Final Crack Rating'],
    'Raveling':  ['Raveling', 'Ravelling', 'RAW Raveling & Bleeding'],
    'Potholes':  ['Potholes', 'Pothole'],
    'Texture':   ['TextureDepth', 'Texture Depth', 'Texture'],
    'Bleeding':  ['Bleeding'],
    'Patching':  ['Patching', 'Patch'],
}


# COMMAND ----------

def detect_chainage_interval(df: pd.DataFrame,
                              start_col='Start_Chainage', end_col='End_Chainage') -> dict:
    if start_col not in df.columns or end_col not in df.columns:
        return {'interval_m': None, 'is_uniform': False, 'non_modal_pct': 100.0}
    s = pd.to_numeric(df[start_col], errors='coerce')
    e = pd.to_numeric(df[end_col], errors='coerce')
    steps = (s - e).abs().dropna(); steps = steps[steps > 0]
    if steps.empty:
        return {'interval_m': None, 'is_uniform': False, 'non_modal_pct': 100.0}
    modal = int(steps.mode().iloc[0]); non_modal = int((steps != modal).sum())
    return {'interval_m': modal, 'is_uniform': non_modal == 0,
            'non_modal_pct': round(non_modal / len(steps) * 100, 1)}

# COMMAND ----------

def _resample_to_10m(df: pd.DataFrame, numeric_cols: list, str_first_cols: list = None) -> pd.DataFrame:
    """Average sub-10m rows into 10m buckets."""
    str_first_cols = str_first_cols or []
    df = df.copy()
    df['_s'] = pd.to_numeric(df['Start_Chainage'], errors='coerce')
    df['_e'] = pd.to_numeric(df['End_Chainage'], errors='coerce')
    df['_lo'] = df[['_s', '_e']].min(axis=1)
    df['_hi'] = df[['_s', '_e']].max(axis=1)
    df['_bucket'] = (df['_lo'] // 10) * 10
    inc = (df['_s'].iloc[0] < df['_e'].iloc[0]) if not df.empty else True
    agg = {col: 'mean' for col in numeric_cols if col in df.columns}
    agg.update({col: 'first' for col in str_first_cols if col in df.columns})
    agg['_lo'] = 'min'; agg['_hi'] = 'max'
    grp = df.groupby('_bucket', sort=True).agg(agg).reset_index(drop=True)
    grp['Start_Chainage'] = grp['_lo'] if inc else grp['_hi']
    grp['End_Chainage']   = grp['_hi'] if inc else grp['_lo']
    return grp.drop(columns=['_lo', '_hi'], errors='ignore')

# COMMAND ----------

def _norm(s):
    return re.sub(r'[^A-Z0-9]', '', str(s).upper())

# COMMAND ----------

def _pick(cols, *subs):
    for c in cols:
        n = _norm(c)
        if all(_norm(s) in n for s in subs):
            return c
    return None

# COMMAND ----------

# ── Rutting ───────────────────────────────────────────────────────────────────
_RUT_KEEP = ['NH_Number', 'Section_Code', 'Start_Chainage', 'End_Chainage',
             'Direction', 'Lane_Number_Original', 'Rutting Left', 'Rutting Right',
             'Rutting Average', 'Survey Date']


def _rutting_from_block(df: pd.DataFrame, idx: int, lane: str, lookup, sorted_keys):
    """Extract rutting from located 10m block at column index idx."""
    blk = df.iloc[:, idx:idx + 5].copy(); bc = list(blk.columns)
    left  = _pick(bc[2:], 'LEFT',    'RUT') or _pick(bc[2:], 'LWP', 'RUT')
    right = _pick(bc[2:], 'RIGHT',   'RUT') or _pick(bc[2:], 'RWP', 'RUT')
    avg   = _pick(bc[2:], 'AVERAGE', 'RUT') or _pick(bc[2:], 'AVG', 'RUT')
    # Fallback: lone Average/Avg column not labelled with "Rut" (e.g. bare "Average (mm)")
    if not avg:
        claimed = {c for c in (left, right) if c}
        remaining = [c for c in bc[2:] if c not in claimed]
        avg = _pick(remaining, 'AVERAGE') or _pick(remaining, 'AVG')
    ren = {bc[0]: 'Start_Chainage', bc[1]: 'End_Chainage'}
    if left:  ren[left]  = 'Rutting Left'
    if right: ren[right] = 'Rutting Right'
    if avg:   ren[avg]   = 'Rutting Average'
    blk = blk.rename(columns=ren)
    blk = _apply_aliases(blk, _RUT_ALIASES)
    for c in ['Start_Chainage', 'End_Chainage', 'Rutting Left', 'Rutting Right', 'Rutting Average']:
        if c in blk.columns: blk[c] = pd.to_numeric(blk[c], errors='coerce')
    # Compute average from Left+Right when still absent
    if 'Rutting Average' not in blk.columns \
            and 'Rutting Left'  in blk.columns \
            and 'Rutting Right' in blk.columns:
        blk['Rutting Average'] = (blk['Rutting Left'] + blk['Rutting Right']) / 2
    blk = blk.dropna(subset=['Start_Chainage', 'End_Chainage'])
    if blk.empty: return None
    info = detect_chainage_interval(blk)
    if info['interval_m'] and info['interval_m'] != 10:
        print(f"  [{lane}] Rutting: {info['interval_m']}m — resampling to 10m.")
        blk = _resample_to_10m(blk,
            [c for c in ['Rutting Left', 'Rutting Right', 'Rutting Average'] if c in blk.columns],
            [c for c in ['NH_Number', 'Section_Code', 'Direction', 'Lane_Number_Original', 'Survey Date'] if c in blk.columns])
    slats, slons, elats, elons = [], [], [], []
    for s, e in zip(blk['Start_Chainage'], blk['End_Chainage']):
        sl, slo = nearest_lat_lon(lookup, sorted_keys, s)
        el, elo = nearest_lat_lon(lookup, sorted_keys, e)
        slats.append(sl); slons.append(slo); elats.append(el); elons.append(elo)
    blk['Start_Latitude']  = slats; blk['Start_Longitude'] = slons
    blk['End_Latitude']    = elats; blk['End_Longitude']   = elons
    blk['Lane'] = lane
    keep = [c for c in _RUT_KEEP + ['Start_Latitude', 'Start_Longitude', 'End_Latitude', 'End_Longitude', 'Lane'] if c in blk.columns]
    return blk[keep].reset_index(drop=True)

# COMMAND ----------

def _locate_10m_block(df: pd.DataFrame) -> int:
    """Find rightmost column index where adjacent Start/End pair has ~10m steps."""
    cols = list(df.columns)
    hits = []
    for i, h in enumerate(cols):
        hn = _norm(h)
        if not (hn.startswith('STCH') or hn.startswith('STARTCH') or hn.startswith('STARTCHAINAGE')):
            continue
        if i + 1 >= len(cols):
            continue
        s = pd.to_numeric(df[cols[i]], errors='coerce')
        e = pd.to_numeric(df[cols[i + 1]], errors='coerce')
        pair = pd.concat([s, e], axis=1).dropna()
        if len(pair) and (((pair.iloc[:, 1] - pair.iloc[:, 0]).abs() == 10).mean() >= 0.5):
            hits.append(i)
    return hits[-1] if hits else None

# COMMAND ----------

def _new_nh_ch_col(df, base: str) -> str:
    """Return second occurrence of a duplicate column (e.g. 'Start Chainage_1')."""
    candidates = [c for c in df.columns if c.startswith(base) and c != base]
    return candidates[0] if candidates else base

# COMMAND ----------

_ROUGH_RAW_VALUE_COLS = ['LWP IRI (m/km)', 'RWP IRI (m/km)', 'Lane IRI (m/km)', 'Average Speed of NSV']
def _roughness_from_raw_points(df: pd.DataFrame, lane: str, lookup, sorted_keys) -> pd.DataFrame:
    """
    Some sheets (e.g. LCMS exports) record IRI as raw per-metre point
    samples — a single CHAINAGE column plus LWP_IRI/RWP_IRI/LANE_IRI —
    with no Start/End Chainage pair at all. These files' CWP_IRI column is
    a -1 sentinel and must never be used as the lane-average IRI; LANE_IRI
    is the real per-metre lane-average reading.

    Bucket the raw 1m samples into 10m buckets (mean), matching the
    GPS-anchored 10m grid — the same averaging read_raveling() already does
    for Raveling's own raw per-metre block.
    """
    if 'CHAINAGE' not in df.columns:
        return pd.DataFrame()
    value_cols = [c for c in _ROUGH_RAW_VALUE_COLS if c in df.columns]
    if not value_cols:
        return pd.DataFrame()

    d = df.copy()
    d['CHAINAGE'] = pd.to_numeric(d['CHAINAGE'], errors='coerce')
    for c in value_cols:
        d[c] = pd.to_numeric(d[c], errors='coerce')
    d = d.dropna(subset=['CHAINAGE'])
    if d.empty:
        return pd.DataFrame()

    d['_seg'] = (d['CHAINAGE'] // 10 * 10).astype(int)
    out = d.groupby('_seg')[value_cols].mean().reset_index().rename(columns={'_seg': 'Start_Chainage'})
    out['End_Chainage'] = out['Start_Chainage'] + 10

    slats, slons, elats, elons = [], [], [], []
    for s, e in zip(out['Start_Chainage'], out['End_Chainage']):
        sl, slo = nearest_lat_lon(lookup, sorted_keys, s)
        el, elo = nearest_lat_lon(lookup, sorted_keys, e)
        slats.append(sl); slons.append(slo); elats.append(el); elons.append(elo)
    out['Start_Latitude']  = slats; out['Start_Longitude'] = slons
    out['End_Latitude']    = elats; out['End_Longitude']   = elons
    out['Lane'] = lane
    return out

# COMMAND ----------

# ──────────────────────────────────────────────────────────────────────
_RUT_RAW_VALUE_COLS = ['Rutting Left', 'Rutting Right']
def _rutting_from_raw_points(df: pd.DataFrame, lane: str, lookup, sorted_keys) -> pd.DataFrame:
    """
    Some sheets (e.g. LCMS exports) record rutting as raw per-metre point
    samples — a single CHAINAGE column plus LEFT_DEPTH/RIGHT_DEPTH — with
    no Start/End Chainage pair at all, mirroring the IRI sheet's raw block.
    Bucket the raw 1m samples into 10m buckets (mean), matching the
    GPS-anchored 10m grid, and derive Rutting Average from the bucketed
    Left/Right values.
    """
    if 'CHAINAGE' not in df.columns:
        return pd.DataFrame()
    value_cols = [c for c in _RUT_RAW_VALUE_COLS if c in df.columns]
    if not value_cols:
        return pd.DataFrame()

    d = df.copy()
    d['CHAINAGE'] = pd.to_numeric(d['CHAINAGE'], errors='coerce')
    for c in value_cols:
        d[c] = pd.to_numeric(d[c], errors='coerce')
    d = d.dropna(subset=['CHAINAGE'])
    if d.empty:
        return pd.DataFrame()

    d['_seg'] = (d['CHAINAGE'] // 10 * 10).astype(int)
    out = d.groupby('_seg')[value_cols].mean().reset_index().rename(columns={'_seg': 'Start_Chainage'})
    out['End_Chainage'] = out['Start_Chainage'] + 10
    if 'Rutting Left' in out.columns and 'Rutting Right' in out.columns:
        out['Rutting Average'] = (out['Rutting Left'] + out['Rutting Right']) / 2

    slats, slons, elats, elons = [], [], [], []
    for s, e in zip(out['Start_Chainage'], out['End_Chainage']):
        sl, slo = nearest_lat_lon(lookup, sorted_keys, s)
        el, elo = nearest_lat_lon(lookup, sorted_keys, e)
        slats.append(sl); slons.append(slo); elats.append(el); elons.append(elo)
    out['Start_Latitude']  = slats; out['Start_Longitude'] = slons
    out['End_Latitude']    = elats; out['End_Longitude']   = elons
    out['Lane'] = lane
    return out

# COMMAND ----------

def build_rutting(file_path: str, lookup: dict, sorted_keys, lane: str) -> pd.DataFrame:
    """
    Build rutting aligned to 10m grid.

    Preference order per header row:
      1. Raw per-metre point samples (LCMS-style CHAINAGE + LEFT/RIGHT_DEPTH).
      2. Explicit, already-named 'Start Chainage'/'End Chainage' + 'Rutting
         Left'/'Rutting Right'/'Rutting Average'/'Survey Date' columns.
         Tried BEFORE the positional 10m-block heuristic because exact
         column-name matching is always more reliable than a fixed-width
         column window -- some backup sheets place 'Direction'/'Lane
         Number' columns between the chainage pair and the rutting values
         (e.g. ...Start Chainage | End Chainage | Direction | Lane Number |
         Rutting Left | Rutting Right | Rutting Average | Survey Date...),
         which pushes Rutting Right/Average/Survey Date outside a naive
         idx:idx+5 window and silently drops them (verified on Zone D's
         L1 RutDepth sheet: old code only ever recovered 'Rutting Left').
      3. _locate_10m_block + _rutting_from_block -- positional heuristic,
         used only when (2) isn't available (genuinely unlabeled LCMS-style
         aggregated blocks, e.g. bare 'LWP Rut'/'RWP Rut'/'Average RUT').

    Output survey-date column is always named 'survey_date_rutting' (renamed
    from the sheet's own 'Survey Date') so it can never collide with
    Cracking's 'survey_date_crack' or Raveling's 'survey_date_raveling' when
    cast_dataframe_with_schema() normalises column names for schema
    matching -- 'Survey Date' and 'survey_date' both normalise to the same
    key, and colliding names used to make one silently overwrite the other.
    """
    sheet = _resolve_sheet(file_path, 'Rutting')
    for hdr in range(0, 8):
        try:
            d = read_sheet_fast(file_path, sheet, hdr)
            d = _apply_aliases(d, _RUT_ALIASES)
            d = d.rename(columns={'New NH number': 'NH_Number', 'New NH\nnumber': 'NH_Number',
                                   'Section Code': 'Section_Code', 'Lane Number': 'Lane_Number_Original'})

            # Raw per-metre point-sample block (LCMS-style) — preferred when
            # present, since it's the only source with real 1m granularity.
            res = _rutting_from_raw_points(d, lane, lookup, sorted_keys)
            if not res.empty:
                return res

            # Explicit Start/End Chainage columns -- checked FIRST (see
            # docstring). Captures Rutting Left/Right/Average/Survey Date
            # by exact name regardless of how many extra columns (NH
            # Number, Direction, Lane Number, ...) sit in between.
            # Checks BOTH the spaced ("Start Chainage") and no-space
            # ("StartChainage") naming conventions -- see the identical fix
            # + comment in build_roughness() above for why this is needed
            # (Zone E's L1/R1 RUT sheets use the no-space camelCase style).
            _sc_base = ('Start Chainage' if any(c.startswith('Start Chainage') for c in d.columns)
                        else 'StartChainage' if any(c.startswith('StartChainage') for c in d.columns)
                        else None)
            _ec_base = ('End Chainage' if any(c.startswith('End Chainage') for c in d.columns)
                        else 'EndChainage' if any(c.startswith('EndChainage') for c in d.columns)
                        else None)
            if _sc_base and _ec_base:
                sc = _new_nh_ch_col(d, _sc_base)
                ec = _new_nh_ch_col(d, _ec_base)
                d2 = d.rename(columns={sc: 'Start_Chainage', ec: 'End_Chainage'})
                keep = [c for c in _RUT_KEEP if c in d2.columns]
                out  = d2[keep].copy()
                out['Start_Chainage'] = pd.to_numeric(out['Start_Chainage'], errors='coerce')
                out['End_Chainage']   = pd.to_numeric(out['End_Chainage'],   errors='coerce')
                for c in ['Rutting Left', 'Rutting Right', 'Rutting Average']:
                    if c in out.columns:
                        out[c] = pd.to_numeric(out[c], errors='coerce')
                if 'Rutting Average' not in out.columns \
                        and 'Rutting Left'  in out.columns \
                        and 'Rutting Right' in out.columns:
                    out['Rutting Average'] = (out['Rutting Left'] + out['Rutting Right']) / 2
                out = out.dropna(subset=['Start_Chainage', 'End_Chainage'])
                if not out.empty and \
                   (('Rutting Left'  in out.columns and out['Rutting Left'].notna().any()) or
                    ('Rutting Right' in out.columns and out['Rutting Right'].notna().any())):
                    slats, slons, elats, elons = [], [], [], []
                    for s, e in zip(out['Start_Chainage'], out['End_Chainage']):
                        sl, slo = nearest_lat_lon(lookup, sorted_keys, s)
                        el, elo = nearest_lat_lon(lookup, sorted_keys, e)
                        slats.append(sl); slons.append(slo); elats.append(el); elons.append(elo)
                    out['Start_Latitude']  = slats; out['Start_Longitude'] = slons
                    out['End_Latitude']    = elats; out['End_Longitude']   = elons
                    out['Lane'] = lane
                    if 'Survey Date' in out.columns:
                        out = out.rename(columns={'Survey Date': 'survey_date_rutting'})
                    return out

            # Fallback: positional 10m-block heuristic, only reached when
            # there's no explicit 'Start Chainage' column to key off of.
            idx = _locate_10m_block(d)
            if idx is not None:
                res = _rutting_from_block(d, idx, lane, lookup, sorted_keys)
                if res is not None and not res.empty and \
                   (('Rutting Left'  in res.columns and res['Rutting Left'].notna().any()) or
                    ('Rutting Right' in res.columns and res['Rutting Right'].notna().any())):
                    if 'Survey Date' in res.columns:
                        res = res.rename(columns={'Survey Date': 'survey_date_rutting'})
                    return res
        except Exception:
            continue
    # Same reasoning as build_roughness() above -- every known Rutting
    # sheet-name alias (Rutting, RutDepth, Rut Depth, Rut, RUT), header row,
    # and extraction strategy has already been tried and failed by this point.
    log_issue("CRITICAL", "Backup",
               "No usable Rutting data found in this lane workbook (tried "
               "every known Rutting sheet-name alias, header row, and "
               "extraction strategy) -- Rutting will be written with all "
               "measurement columns null.",
               file=file_path, table="Rutting")
    print(f"  [{lane}] Rutting: no usable block found.")
    return pd.DataFrame()

# COMMAND ----------

def read_raveling(file_path: str, lane: str) -> pd.DataFrame:
    """
    Read raveling sheet; aggregates sub-10m rows to 10m (RI=mean, Area=sum).
    Handles Raveling/Ravelling/RAW Raveling & Bleeding sheet names.
    """
    _EMPTY = pd.DataFrame(columns=['Lane', 'Start_Chainage', 'Raveling_RI',
                                    'Raveling_Area_m2', 'survey_date_raveling'])
    sheet = _resolve_sheet(file_path, 'Raveling')
    df = None
    for hdr in [2, 0, 1, 3]:
        try:
            cand = read_sheet_fast(file_path, sheet, hdr)
            cand = _apply_aliases(cand, _RAVEL_ALIASES)
            if 'Start_Chainage' in cand.columns and \
               ('Raveling_RI' in cand.columns or 'Raveling_Area_m2' in cand.columns):
                df = cand; break
        except Exception:
            continue
    if df is None:
        # Same reasoning as Cracking/Roughness/Rutting above -- every known
        # Raveling sheet-name alias (Raveling, Ravelling, 'RAW Raveling &
        # Bleeding' -- see _SHEET_ALIASES) and header row (0-3) has already
        # been tried and failed by this point. Previously completely silent
        # (not even a print()) -- Raveling would write null measurement
        # columns with zero visibility anywhere in the run log.
        log_issue("CRITICAL", "Backup",
                   "No usable Raveling sheet/data found in this lane "
                   "workbook (tried every known Raveling sheet-name alias "
                   "and header row) -- Raveling will be written with all "
                   "measurement columns null.", file=file_path, table="Raveling")
        print(f"  [{lane}] Raveling: no usable sheet found.")
        return _EMPTY

    for c in ['Start_Chainage', 'Raveling_RI', 'Raveling_Area_m2']:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.dropna(subset=['Start_Chainage'])
    if df.empty: return _EMPTY

    df['_seg'] = (df['Start_Chainage'] // 10 * 10).astype(int)
    agg = {}
    if 'Raveling_RI'      in df.columns: agg['Raveling_RI']      = ('Raveling_RI', 'mean')
    if 'Raveling_Area_m2' in df.columns: agg['Raveling_Area_m2'] = ('Raveling_Area_m2', 'sum')
    out = df.groupby('_seg').agg(**agg).reset_index().rename(columns={'_seg': 'Start_Chainage'})
    if 'Raveling_RI'      not in out.columns: out['Raveling_RI']      = np.nan
    if 'Raveling_Area_m2' not in out.columns: out['Raveling_Area_m2'] = np.nan
    out['survey_date_raveling'] = None
    out['Lane'] = lane
    return out[['Lane', 'Start_Chainage', 'Raveling_RI', 'Raveling_Area_m2', 'survey_date_raveling']].reset_index(drop=True)


# COMMAND ----------

def _asof_attach(base: pd.DataFrame, other: pd.DataFrame, value_cols: list,
                  bucket_width: float = None, stage_name: str = "") -> pd.DataFrame:
    """
    Left-attach `other`'s value_cols onto `base` by chainage match: for
    each base row, find the `other` row whose measured interval contains
    base's chainage (nearest preceding lower bound), rather than requiring
    an exact chainage-pair match. If `other` has no End_Chainage (e.g.
    raveling), `bucket_width` supplies an implicit interval width (lower
    bound + bucket_width) for the containment check instead.

    Uses min/max of (Start_Chainage, End_Chainage) rather than assuming
    Start < End -- on real Zone A files, a lane's own Direction can
    disagree between sheets (e.g. GPS-derived geometry says 'Increasing'
    while that same lane's IRI/Rut sheet reports 'Decreasing', so its
    Start_Chainage > End_Chainage). build_cracking already normalises this
    way (`upper = np.maximum(...)`, `lower = np.minimum(...)`); mirrored
    here since an un-normalised containment check silently drops every row
    on any lane where Start > End (verified: 100% null on Zone A's R1/R2).

    NO 100m/1km -> 10m BROADCAST: a matched `other` row is only accepted
    if its own interval width is <= 10m -- i.e. `other` is itself already
    at native-10m resolution, or was averaged up from raw 1m samples into
    10m buckets. A coarser native interval (~100m/1km Roughness/Rutting
    survey rows) fails this width check and is rejected: those `base`
    rows are left null instead of repeating one coarse reading across
    every 10m sub-segment it spans.
    """
    result = base.copy()
    present_cols = [c for c in value_cols if c in other.columns]
    if other.empty or not present_cols:
        for c in value_cols:
            result[c] = pd.NA
        return result

    result['_lo'] = result[['Start_Chainage', 'End_Chainage']].min(axis=1)

    keep = ['Start_Chainage'] + (['End_Chainage'] if 'End_Chainage' in other.columns else []) + present_cols
    other_slim = other[keep].dropna(subset=['Start_Chainage']).copy()
    if 'End_Chainage' in other_slim.columns:
        other_slim['_other_hi'] = other_slim[['Start_Chainage', 'End_Chainage']].max(axis=1)
        other_slim['_lo'] = other_slim[['Start_Chainage', 'End_Chainage']].min(axis=1)
    elif bucket_width:
        other_slim['_lo'] = other_slim['Start_Chainage']
        other_slim['_other_hi'] = other_slim['Start_Chainage'] + bucket_width
    else:
        other_slim['_lo'] = other_slim['Start_Chainage']
        other_slim['_other_hi'] = pd.NA
    other_slim = other_slim.drop(columns=['Start_Chainage'] + (['End_Chainage'] if 'End_Chainage' in other_slim.columns else []))
    other_slim = other_slim.sort_values('_lo').reset_index(drop=True)

    result = result.reset_index(drop=False).sort_values('_lo')
    merged = pd.merge_asof(result, other_slim, on='_lo', direction='backward')
    in_range = merged['_other_hi'].isna() | (merged['_lo'] < merged['_other_hi'])

    # NO-BROADCAST GUARD (fully removes 100m/1km -> 10m conversion): only
    # accept a match whose native interval width is <= 10m. Anything wider
    # is a coarser survey row that must NOT be repeated across sub-segments.
    width_ok = merged['_other_hi'].isna() | ((merged['_other_hi'] - merged['_lo']) <= 10)

    rejected_for_width = int((in_range & ~width_ok).sum())
    if rejected_for_width:
        sev = "CRITICAL" if stage_name in REQUIRED_BACKUP_STAGES else "WARNING"
        log_issue(sev, "Backup",
                   f"{rejected_for_width} 10m grid row(s) left null: native survey "
                   f"interval wider than 10m (NO-BROADCAST GUARD -- a coarser "
                   f"reading is never repeated across the 10m sub-segments it spans).",
                   table=stage_name or None)

    in_range = in_range & width_ok

    for c in present_cols:
        merged[c] = merged[c].where(in_range)
    for c in value_cols:
        if c not in merged.columns:
            merged[c] = pd.NA
    return merged.drop(columns=['_lo', '_other_hi']).set_index('index').sort_index()

# COMMAND ----------

def _disaggregate_to_grid(grid: pd.DataFrame, native: pd.DataFrame, value_cols: list,
                           bucket_width: float = None, stage_name: str = "") -> pd.DataFrame:
    """
    Attach a native-interval stage table (Roughness/Rutting, or Raveling's
    own phase-offset 10m buckets) onto `grid`'s 10m master grid (GPS_
    Geometry's own Lane/Start_Chainage/End_Chainage/lat-lon columns), via
    the same chainage-matching join _asof_attach() uses for the Unified_
    Segments merge.

    NO 100m/1km -> 10m CONVERSION HAPPENS HERE: `native` rows only attach
    onto `grid` when `native` is itself already at 10m-or-finer resolution
    (native-10m, or 1m samples pre-averaged into 10m buckets). If `native`
    is at a coarser true survey interval (~100m/1km Roughness/Rutting),
    `_asof_attach`'s width guard rejects every match and the corresponding
    `grid` rows are left null -- the coarse reading is never repeated or
    broadcast across the 10m sub-segments it spans.

    Returns `grid`'s own columns with all-null value_cols if `native` is
    empty (mirrors _asof_attach's empty-input behaviour).
    """
    keep_grid_cols = [c for c in
                       ['Lane', 'Start_Chainage', 'End_Chainage',
                        'Start_Latitude', 'Start_Longitude',
                        'End_Latitude', 'End_Longitude']
                       if c in grid.columns]
    out = grid[keep_grid_cols].copy()
    if native is None or native.empty:
        for c in value_cols:
            out[c] = pd.NA
        return out
    return _asof_attach(out, native, value_cols, bucket_width=bucket_width, stage_name=stage_name)

# COMMAND ----------

_ROUGH_COLS = ['LWP IRI (m/km)', 'RWP IRI (m/km)', 'Lane IRI (m/km)',
               'Roughness Index (mm/km)', 'Average Speed of NSV',
               'Condition of Road Surface (As per IRC:SP:16-2019)']
_RUT_COLS = ['Rutting Left', 'Rutting Right', 'Rutting Average', 'survey_date_rutting']
_RAVEL_COLS = ['Raveling_RI', 'Raveling_Area_m2', 'survey_date_raveling']

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────────────────
# D4-missing fallback: pull pothole images from D3's "Potholes Images" folder
# and attach them to D2's Potholes table by matching the Chainage burned into
# each image's banner overlay (e.g. "Chainage: 6100  Lon: 77.11228332
# Lat: 28.27194788 Alt: 212.68600464") -- NOT by filename or row-position.
#
# Only ever invoked (from load_d4_pothole_points() in the next cell) when a
# project has NO Deliverable-4 pothole file at all.
# ─────────────────────────────────────────────────────────────────────────────

def _dms_to_decimal(dms, ref) -> float:
    """Convert an EXIF GPS coordinate (degrees, minutes, seconds tuple) to
    plain decimal degrees, applying sign from the N/S or E/W reference."""
    degrees, minutes, seconds = dms
    value = float(degrees) + float(minutes) / 60.0 + float(seconds) / 3600.0
    if ref in ('S', 'W'):
        value = -value
    return value


def extract_exif_gps(image_path: str):
    """
    Read the Lat/Lon GPS coordinates embedded in an image's own EXIF
    metadata -- per Prathap's exact instruction on the call ("we are not
    using the OCR, because that is not in our scope... if you are
    capturing in the mobile, it is showing the location data right into
    the image... we are easily able to extract it using the code, exif").

    Only Lat/Lon come from EXIF -- NOT Chainage ("we are unable to
    extract the chainage because chainage is another data, we are only
    able to get a lat long"). Chainage has to be derived separately, by
    mapping this Lat/Lon to the nearest point in the lane's own GPS
    backup data (map_latlon_to_chainage, below).

    Returns (lat, lon) as floats, or (None, None) if the image has no
    GPS EXIF tag at all (never raises).
    """
    try:
        img = PILImage.open(_long_path(image_path))
        exif = img._getexif()
        if not exif:
            return None, None
        gps_info = exif.get(34853)  # 34853 = GPSInfo EXIF tag ID
        if not gps_info:
            return None, None
        lat_dms = gps_info.get(2)   # GPSLatitude
        lat_ref = gps_info.get(1)   # GPSLatitudeRef ('N'/'S')
        lon_dms = gps_info.get(4)   # GPSLongitude
        lon_ref = gps_info.get(3)   # GPSLongitudeRef ('E'/'W')
        if not (lat_dms and lat_ref and lon_dms and lon_ref):
            return None, None
        lat = _dms_to_decimal(lat_dms, lat_ref)
        lon = _dms_to_decimal(lon_dms, lon_ref)
        return lat, lon
    except Exception as exc:
        log_issue("WARNING", "Deliverable", f"EXIF GPS read failed: {concise_exc(exc)}", file=image_path)
        return None, None


def map_latlon_to_chainage(lat: float, lon: float, geometry_df: pd.DataFrame,
                            max_gap_m: float = 10.0):
    """
    Map an image's own EXIF Lat/Lon to the NEAREST point in a lane's own
    GPS backup data (GPS_Geometry's Start_Latitude/Start_Longitude per
    10m segment), returning that segment's own Chainage -- per Prathap's
    exact described process: "mostly we are trying to map the latitude
    and longitude with our GPS-based data, and we are taking the
    corresponding chainage".

    max_gap_m=10.0 -- WIDENED from the originally-agreed 5m (Prathap's
    quote: "we are having plus or minus 5 meters accuracy distortion...
    distortion... this is acceptable"). If the nearest GPS point is
    farther than this, returns None rather than a wrong chainage --
    Prathap's own acknowledged failure mode ("if the particular lat long
    is more than 5 meters... you are unable to map the images").

    Returns the matched Chainage, or None if no GPS point is close enough
    (or geometry_df/lat/lon are unusable).
    """
    if lat is None or lon is None or geometry_df is None or geometry_df.empty:
        return None
    if 'Start_Latitude' not in geometry_df.columns or 'Start_Longitude' not in geometry_df.columns:
        return None

    # Same na_value= fix as build_potholes_stage_table -- Start_Latitude/
    # Start_Longitude can be a nullable dtype after schema-casting, and
    # np.isnan() below would raise TypeError on any leftover pd.NA.
    lats = pd.to_numeric(geometry_df['Start_Latitude'], errors='coerce').to_numpy(dtype='float64', na_value=np.nan)
    lons = pd.to_numeric(geometry_df['Start_Longitude'], errors='coerce').to_numpy(dtype='float64', na_value=np.nan)
    valid = ~(np.isnan(lats) | np.isnan(lons))
    if not valid.any():
        return None

    # Haversine distance from (lat, lon) to every valid GPS point.
    R = 6371000.0
    phi1 = math.radians(lat)
    phi2 = np.radians(lats[valid])
    dphi = phi2 - phi1
    dlambda = np.radians(lons[valid]) - math.radians(lon)
    a = np.sin(dphi / 2) ** 2 + math.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
    dists = 2 * R * np.arcsin(np.minimum(1.0, np.sqrt(a)))

    best_idx = np.argmin(dists)
    if dists[best_idx] > max_gap_m:
        return None
    return geometry_df.loc[geometry_df.index[valid][best_idx], 'Start_Chainage']


_IMAGE_LANE_RE = re.compile(r'^([LR]\d+)$', re.IGNORECASE)
# FIX: verified real case (PKG-3) -- some deliverables have NO lane
# subfolders at all (every photo sits flat directly in 'Potholes
# Images/', e.g. 'Potholes Images/Z2RJ_..._L1_001357.jpg'), with the
# lane ONLY encoded in the filename itself. The old folder-only check
# left image_lane='' for EVERY photo in such a folder, which in turn
# makes has_lane_signal False for the WHOLE project -- silently turning
# off lane-filtering entirely, so a D2 row for one lane could end up
# matched against a candidate image from a COMPLETELY DIFFERENT lane
# (and a genuinely close, same-lane image could lose to it). This
# pattern matches a lane token like '_L1_' or '_R3_' anywhere in the
# filename as a fallback when the folder name itself doesn't give one.
#
# BUG FIXED: verified a SECOND real naming convention (JADH project) --
# 'JADH-L1-S1_000173.jpg' -- the lane token is HYPHEN-delimited
# ('-L1-'), not underscore-delimited. The old pattern only recognised
# '_' as a boundary, so it silently missed this convention too --
# total=257 logged identically for BOTH lane L1 and lane R1 confirmed
# lane-filtering was disabled project-wide again, exactly the same
# failure mode as the original PKG-3 case. Boundary characters widened
# to accept '-', '_', OR start/end-of-string on either side.
_IMAGE_LANE_IN_FILENAME_RE = re.compile(r'(?:^|[-_])([LR]\d+)(?:[-_.]|$)', re.IGNORECASE)
# UPDATED per explicit instruction: use the 'Potholes Images'/'Cracking
# Images' folder (top-level, real photo folder) as the source of truth
# everywhere -- NOT the 'DVM PKG_N (Distress Images)' folder. The "
# Images" word is now OPTIONAL in the pattern so it matches BOTH real
# layouts seen ('Potholes_GPS_L1' inside (Distress Images), AND
# 'Potholes Images_GPS_L1' inside the top-level Potholes Images folder)
# -- keeps this regex in sync with find_d3_defect_image_folder()'s own
# folder choice below, so the reference-sheet filenames read here always
# come from the SAME physical folder the real jpg files get walked from.
_GPS_REF_LANE_RE_TEMPLATES = {
    'potholes': re.compile(r'Potholes(?: Images)?_GPS_([LR]\d+)', re.IGNORECASE),
    'cracking': re.compile(r'Cracking(?: Images)?_GPS_([LR]\d+)', re.IGNORECASE),
}


def build_defect_image_gps_lookup(d3_extraction_results: dict, defect_type: str) -> pd.DataFrame:
    """
    PRIMARY source for defect image Chainage/Lat/Lon -- reads the small
    '{DefectType}_GPS_{lane}/GPS{lane}.xlsx' reference sheets that sit
    inside D3's 'DVM Pkg-1 (Distress Images)' folder (already extracted
    into all_results by extract_folder() -- no extra file reading needed,
    pure in-memory lookup). Generalised so the exact same logic covers
    'Potholes_GPS_{lane}' and 'Cracking_GPS_{lane}' alike, via defect_type
    ('potholes' or 'cracking').

    VERIFIED against real Potholes data: these sheets give an EXACT
    filename -> Chainage -> Lat/Lon mapping for every photo -- including
    for an image whose OCR banner-read had previously failed outright.
    More reliable than OCR (no misreads, no failed crops) and is used
    FIRST; OCR is only a backup for any image this lookup doesn't cover.

    Column layout is POSITIONAL, not name-based -- header text varies per
    file (seen: '0', '18500', 'Unnamed: 3' all meaning "chainage" in
    different files, since the source Excel's own column header was
    blank/numeric-looking and pandas auto-named it differently). Always:
    column 0 = filename, column 1 = latitude, column 2 = longitude,
    column 3 = chainage. Any further columns are ignored.

    Returns: filename, Lane, Chainage, Lat, Lon -- one row per photo
    filename found in ANY '{DefectType}_GPS_{lane}' reference sheet.
    """
    lane_re = _GPS_REF_LANE_RE_TEMPLATES.get(defect_type)
    if lane_re is None:
        return pd.DataFrame(columns=['filename', 'Lane', 'Chainage', 'Lat', 'Lon'])

    records = []
    for file_key, file_results in d3_extraction_results.items():
        lane_match = lane_re.search(file_key)
        if not lane_match:
            continue
        lane = lane_match.group(1).upper()

        for _sheet_name, val in file_results.items():
            tables = val if isinstance(val, list) else [val]
            for tbl in tables:
                # FIX: chainage column position is NOT consistent across
                # real exports -- verified 3 different layouts across
                # PKG-6/7/8 alone:
                #   PKG-6: index 3 is blank, real chainage several
                #          columns further right
                #   PKG-7: sheet has only 4 columns total -- filename/
                #          lat/lon/Source_Subfolder -- NO chainage column
                #          exists at all in this file
                #   PKG-8: index 3 IS the correct chainage (matches D2
                #          within ~4m, verified)
                # So the column position genuinely varies per export --
                # hardcoding index 3 (old code) is wrong more often than
                # right. Instead: scan every column after lon for the
                # first one that's mostly non-null and numeric; if NONE
                # qualifies (PKG-7's case), still keep the row with
                # Chainage=NaN -- filename+Lat/Lon from this sheet are
                # still MORE ACCURATE than phone EXIF and should flow
                # through the same resolve_missing_chainages_via_exif()
                # chainage-resolution path used for EXIF-only images,
                # rather than being dropped entirely.
                if not isinstance(tbl, pd.DataFrame) or tbl.empty or len(tbl.columns) < 3:
                    continue
                cols = tbl.columns.tolist()
                chainage_col = None
                for c in cols[3:]:
                    numeric = pd.to_numeric(tbl[c], errors='coerce')
                    if numeric.notna().mean() > 0.5:  # majority non-null
                        chainage_col = c
                        break
                if chainage_col is not None:
                    sub = tbl[[cols[0], cols[1], cols[2], chainage_col]].copy()
                    sub.columns = ['filename', 'Lat', 'Lon', 'Chainage']
                else:
                    sub = tbl[[cols[0], cols[1], cols[2]]].copy()
                    sub.columns = ['filename', 'Lat', 'Lon']
                    sub['Chainage'] = pd.NA
                sub['Lane'] = lane
                records.append(sub)

    if not records:
        return pd.DataFrame(columns=['filename', 'Lane', 'Chainage', 'Lat', 'Lon'])

    out = pd.concat(records, ignore_index=True)
    out['Chainage'] = pd.to_numeric(out['Chainage'], errors='coerce')
    out['Lat'] = pd.to_numeric(out['Lat'], errors='coerce')
    out['Lon'] = pd.to_numeric(out['Lon'], errors='coerce')

    # FIX: verified on PKG-9 -- this reference sheet is actually a full
    # chainage/lat-lon grid (one row roughly every ~350m covering the
    # WHOLE lane), with 'filename' only populated on the small subset of
    # rows that have an actual defect photo. Blank Excel cells came
    # through as the literal STRING "nan" (not a true null) for ~98% of
    # rows (804/820 on PKG-9 L1) -- .dropna(subset=['filename']) alone
    # does NOT catch this, since these aren't real nulls. Strip out
    # literal "nan"/"none"/empty-string filenames explicitly so the
    # lookup dict isn't flooded with thousands of fake rows that all
    # collapse onto one bogus 'nan' key.
    bogus = out['filename'].astype(str).str.strip().str.lower().isin(['nan', 'none', ''])
    out = out[~bogus]

    return out.dropna(subset=['filename'])


def build_pothole_image_gps_lookup(d3_extraction_results: dict) -> pd.DataFrame:
    """Backwards-compatible wrapper -- see build_defect_image_gps_lookup."""
    return build_defect_image_gps_lookup(d3_extraction_results, 'potholes')




# ─────────────────────────────────────────────────────────────────────────────
def extract_image_metadata(image_folder: str, gps_lookup: pd.DataFrame = None,
                            consolidate: bool = False, defect_label: str = "Potholes") -> pd.DataFrame:
    """
    Get Chainage/Lat/Lon for every real jpg/png image found ANYWHERE
    beneath a D3 defect-images folder (any subfolder depth). Generalised
    via defect_label ("Potholes" or "Cracking") so the exact same logic
    consolidates either defect type's photos, under its own output
    folder names.

    PRIMARY source: gps_lookup -- the filename->Chainage->Lat/Lon table
    built by build_defect_image_gps_lookup() from D3's own
    '{DefectLabel}_GPS_{lane}' reference sheets (already-extracted data,
    exact values, no image reading at all). Matched by filename.

    BACKUP source: OCR on the image's own banner overlay -- used ONLY for
    images gps_lookup doesn't cover. Kept as a fallback rather than
    removed entirely, since a project might not always have these
    reference sheets.

    Recursively walks every subfolder (real structure is '{DefectLabel}
    Images/<Lane>/<photo>.jpg') and captures Lane from the image's own
    parent folder name when it matches a lane code (L1-4/R1-4).

    consolidate=False (default -- CHANGED per instruction: images/metadata
    consolidation is NOT required for matching at all, only for organised
    output, so it's off by default now to avoid the extra copy/write I/O
    on every run). Pass consolidate=True to restore the old behaviour --
    each image copied into ONE single consolidated '{DefectLabel}_Images'
    folder inside Deliverable-3's own bronze partition, with a sibling
    '{DefectLabel}_Images_Metadata' folder holding one CSV of every
    extracted image's Chainage/Lat/Lon. Matching itself (Chainage/Lat/Lon
    resolution, image_name in Potholes.parquet/Unified_Segments) is
    IDENTICAL either way -- image_path just points at the original source
    location instead of a consolidated copy when this is off. If a copy
    is attempted (consolidate=True) and fails, falls back to the
    original source path (logged as WARNING)

    image_url = the image's own FULL ABSOLUTE PATH (Volumes-style, e.g.
    '/Volumes/nhai_dhms/nsv/nsv_downloads/Zone_A/Phase_1/Cycle_1/
    N_01001_01001_HR/deliverable/Deliverable - 3 - GIS Mapping/Potholes
    Images/L2/Z1HR_NH148N_PKG1_L2_01_000610.jpg') -- per Prathap's own
    instruction on the call: the volume names in this path (nhai_dhms/
    nsv/etc.) are object IDs in production, not known yet; once they ARE
    known, only those first few segments get swapped out and everything
    else in the path stays identical -- so capturing the full path now
    is exactly what's needed, nothing more.

    Returns one row per image: image_path, Lane, Chainage, Lon, Lat, Alt,
    source ('gps_reference' or 'ocr'), image_url (same value as
    image_path). Chainage is None (row still kept) when NEITHER source
    could determine it.
    """
    dest_dir = None
    if consolidate:
        # BUG FIXED: this always wrote to Source=NSV/.../Deliverable=
        # Deliverable - 3/, with NO "Project" partition at all -- under
        # Layout A (real package subfolders), that meant a stray
        # "Deliverable=Deliverable - 3" folder sat OUTSIDE every
        # "Project=<package>" folder, floating alongside them with no
        # clear package identity (this search runs ONCE across the
        # WHOLE DELIVERABLE_ROOT, not per-package, so there's no single
        # "project" to attribute these images to even if we wanted to).
        #
        # Fixed: under Layout A specifically, these images move to the
        # General/_potholes_images meta-space instead (same convention
        # _summary/_manifest already use) -- clearly a cross-cutting,
        # not-tied-to-one-package output, rather than a confusing stray
        # folder sitting next to the real per-package ones. Under
        # Layout B (single project, no package ambiguity at all), it
        # stays exactly where it was -- inside Deliverable-3's own
        # partition, since there's nothing ambiguous about that case.
        if HAS_PACKAGE_LAYER:
            dest_dir = build_output_dir(f"_{defect_label.lower()}_images") / f"{defect_label}_Images"
        else:
            dest_dir = build_output_dir("NSV", extra_partitions={"Deliverable": "Deliverable - 3"}) / f"{defect_label}_Images"
        os.makedirs(_long_path(str(dest_dir)), exist_ok=True)

    gps_lookup_by_name = {}
    if gps_lookup is not None and not gps_lookup.empty:
        for _, r in gps_lookup.iterrows():
            gps_lookup_by_name[str(r['filename']).strip().lower()] = r

    records = []
    exif_used = 0
    for _root, _dirs, _files in os.walk(_long_path(image_folder)):
        parent_folder_name = os.path.basename(_root)
        _lane_match = _IMAGE_LANE_RE.match(parent_folder_name.strip())
        image_lane = _lane_match.group(1).upper() if _lane_match else ''

        for fname in sorted(_files):
            if not fname.lower().endswith(('.jpg', '.jpeg', '.png')):
                continue
            src_path = os.path.join(_root, fname)

            # FIX: fall back to a lane token embedded in the FILENAME
            # itself (e.g. '..._L1_001357.jpg') when the folder name
            # gave no lane at all -- see _IMAGE_LANE_IN_FILENAME_RE
            # above for why this matters (flat, no-subfolder photo
            # folders would otherwise silently disable lane-filtering
            # for the WHOLE project).
            _this_image_lane = image_lane
            if not _this_image_lane:
                _fname_lane_match = _IMAGE_LANE_IN_FILENAME_RE.search(fname)
                if _fname_lane_match:
                    _this_image_lane = _fname_lane_match.group(1).upper()

            final_path = src_path
            if dest_dir is not None:
                dst_path = os.path.join(str(dest_dir), fname)
                try:
                    shutil.copy2(_long_path(src_path), _long_path(dst_path))
                    final_path = dst_path
                except Exception as exc:
                    log_issue("WARNING", "Deliverable",
                               f"Could not copy {defect_label.lower()} image into consolidated folder "
                               f"(using original source path instead): {concise_exc(exc)}",
                               file=src_path)

            # PRIMARY: GPS reference sheet lookup by filename.
            ref = gps_lookup_by_name.get(fname.strip().lower())
            if ref is not None:
                records.append({
                    'image_path': final_path,
                    'Lane': ref['Lane'] or _this_image_lane,
                    'Chainage': ref['Chainage'],
                    'Lon': ref['Lon'], 'Lat': ref['Lat'], 'Alt': None,
                    'source': 'gps_reference',
                })
                continue

            # BACKUP: EXIF GPS metadata read from the image itself -- per
            # Prathap's exact instruction ("we are not using the OCR...
            # if you are capturing in the mobile, it is showing the
            # location data right into the image... we are easily able
            # to extract it using the code, exif"). Only reached when
            # gps_lookup has no entry for this exact filename.
            #
            # EXIF only ever gives Lat/Lon, NEVER Chainage ("we are
            # unable to extract the chainage... we are only able to get
            # a lat long") -- Chainage for these rows is left None here
            # and resolved LATER, per lane, via map_latlon_to_chainage()
            # against that lane's own GPS_Geometry (needs the lane's GPS
            # data, which isn't built yet at this point -- see the
            # per-lane resolution step in the driver loop).
            exif_used += 1
            lat, lon = extract_exif_gps(src_path)
            records.append({'image_path': final_path, 'Lane': _this_image_lane,
                             'Chainage': None, 'Lon': lon, 'Lat': lat, 'Alt': None,
                             'source': 'exif'})
            if lat is None:
                log_issue("WARNING", "Deliverable", "Image has no EXIF GPS tag at all.", file=src_path)

    out = pd.DataFrame(records)
    if not out.empty:
        log_issue("WARNING", "Deliverable",
                   f"{defect_label} image Chainage/Lat/Lon source: {len(out) - exif_used} from "
                   f"GPS reference sheet, {exif_used} fell back to EXIF (Chainage for these "
                   f"still needs per-lane resolution via map_latlon_to_chainage).")

    # Metadata CSV lives ONLY inside its own dedicated folder
    # ('{defect_label}_Images_Metadata'), never loose elsewhere.
    if consolidate and not out.empty:
        meta_dest_dir = build_output_dir("NSV", extra_partitions={"Deliverable": "Deliverable - 3"}) / f"{defect_label}_Images_Metadata"
        os.makedirs(_long_path(str(meta_dest_dir)), exist_ok=True)
        meta_path = os.path.join(str(meta_dest_dir), f"{defect_label.lower()}_image_metadata.csv")
        try:
            out.assign(image_filename=out['image_path'].apply(os.path.basename)).to_csv(
                _long_path(meta_path), index=False)
        except Exception as exc:
            log_issue("WARNING", "Deliverable",
                       f"Could not write consolidated {defect_label.lower()} image metadata CSV: {concise_exc(exc)}",
                       file=meta_path)

    # image_url = the image's own FULL ABSOLUTE PATH -- per Prathap's own
    # instruction on the call ("we can only replace these three things
    # [volume names] with the object IDs and all the other things will
    # be same folders"). No upload/CDN call happens here at all.
    out['image_url'] = out['image_path']

    return out




_DEFECT_IMAGE_SEEDS = {
    'potholes': {"pothole", "potholes", "minor pothole", "major pothole",
                 "minor potholes", "major potholes"},
    'cracking': {"cracking", "crack", "cracks"},
}


def _is_defect_folder_name(name: str, defect_type: str, threshold: int = 80) -> bool:
    """Generic version of _is_pothole_table_name -- same fuzzy approach,
    parameterised by defect_type ('potholes' or 'cracking') so the same
    folder/sheet-discovery logic serves both without duplicating it."""
    if not name:
        return False
    cleaned = str(name).lower().strip()
    seeds = _DEFECT_IMAGE_SEEDS.get(defect_type, set())
    return any(fuzz.partial_ratio(cleaned, seed) >= threshold for seed in seeds)


def resolve_missing_chainages_via_exif(image_meta_df: pd.DataFrame, geometry_df: pd.DataFrame,
                                         max_gap_m: float = 10.0, lane: str = "") -> pd.DataFrame:
    """
    For any image row whose Chainage is still None (i.e. it wasn't in a
    GPS-reference sheet, so extract_image_metadata() fell back to EXIF --
    Lat/Lon only, no Chainage) -- resolve its Chainage now, using THIS
    LANE's own already-built GPS_Geometry, per Prathap's exact process
    ("mostly we are trying to map the latitude and longitude with our
    GPS-based data, and we are taking the corresponding chainage").

    Called PER LANE, once that lane's GPS_Geometry exists (Cell 95's
    driver loop) -- this couldn't happen any earlier, since EXIF only
    gives a Lat/Lon point with no idea which lane's road it's even on
    until it's matched against that lane's own GPS backup data.

    Rows that already have a Chainage (from the GPS reference sheet) are
    left completely untouched. Rows resolved here get 'source' updated
    to 'exif_gps_matched'; rows where even this fails (no GPS point
    within max_gap_m) are left with Chainage=None -- Prathap's own
    acknowledged failure mode, not silently hidden.

    BUG FIXED: verified real case (BEPA project) -- an R1-tagged image
    got its Chainage "locked in" during an EARLIER lane's (L1's) call to
    this same function, because L1 and R1 run physically close/parallel
    and the image's EXIF happened to land within max_gap_m of an L1
    geometry point too (5.0m away, giving chainage~629945 from L1) --
    resolved BEFORE R1's own turn came up (R1's own geometry would have
    given the CORRECT chainage 629630, verified only 0.2m away). Since
    "missing" only checked Chainage.isna() with no lane awareness at
    all, this cross-lane resolution silently overwrote what should have
    been R1's own value with L1's, and once resolved this function
    never revisits it -- so the WRONG lane's chainage (629945, outside
    R1's own 629690 max) stuck permanently, later failing R1's grid-
    containment check downstream ("outside the grid's range").
    Restricted now to only resolve rows whose OWN Lane matches the lane
    THIS call is for (or has no lane tag at all, '' -- ambiguous/
    unknown-lane images still get a chance against every lane in turn,
    same as before) -- a genuinely lane-tagged image only ever resolves
    against its OWN lane's geometry, never a different lane's.
    """
    out = image_meta_df.copy()
    if out.empty or 'Chainage' not in out.columns:
        return out

    missing = out['Chainage'].isna() & out['Lat'].notna() & out['Lon'].notna()
    if lane and 'Lane' in out.columns:
        missing = missing & ((out['Lane'] == lane) | (out['Lane'] == ''))
    for idx in out[missing].index:
        chainage = map_latlon_to_chainage(out.at[idx, 'Lat'], out.at[idx, 'Lon'],
                                           geometry_df, max_gap_m=max_gap_m)
        if chainage is not None:
            out.at[idx, 'Chainage'] = chainage
            out.at[idx, 'source'] = 'exif_gps_matched'
    return out


def find_d3_defect_image_folder(project_deliverable_root: str, defect_type: str) -> str:
    """
    Locate the folder holding actual JPG/PNG photos for a given defect
    type ('potholes' or 'cracking') under a project's Deliverable-3 (GIS
    Mapping) directory. Generalised from the pothole-only version so the
    exact same logic covers 'Potholes Images' and 'Cracking Images' alike.

    Matches on: the folder's own name is defect-shaped (fuzzy), AND it
    contains at least one real .jpg/.jpeg/.png file ANYWHERE beneath it,
    at any subfolder depth (real structure is e.g. 'Potholes Images/R2/
    photo.jpg' -- the wrapper folder itself has no files directly inside
    it, only per-lane subfolders).

    Returns '' if nothing found.

    FIX: real deliverables can contain MORE THAN ONE folder whose name
    matches (verified: PKG-6 has both 'Potholes Images/' AND 'DVM PKG_6
    (Distress Images)/Potholes/' -- duplicate exports of the same
    survey). Picking whichever os.walk() happens to reach first is
    filesystem-order-dependent and, critically, may return a DIFFERENT
    physical folder than the one build_defect_image_gps_lookup() read
    its filenames from -- which silently guarantees 0 filename matches
    even when both folders describe the same real-world photos, if
    their exports were named even slightly differently.

    UPDATED per explicit instruction: 'Potholes Images'/'Cracking Images'
    (the plain top-level folder, NOT nested under 'DVM PKG_N (Distress
    Images)') is the correct source of truth to use everywhere. This now
    PREFERS a candidate folder whose path does NOT contain the
    '(Distress Images)' marker over one that does. If every candidate
    happens to be under '(Distress Images)' (no plain folder exists for
    this project), falls back to that instead so nothing breaks.
    build_defect_image_gps_lookup()'s regex was updated to match
    filenames from THIS SAME 'Potholes Images'/'Cracking Images' folder
    too, so the two stay in sync.
    """
    candidates = []
    for root, dirs, files in os.walk(project_deliverable_root):
        folder_name = os.path.basename(root)
        if not _is_defect_folder_name(folder_name, defect_type):
            continue
        for _sub_root, _sub_dirs, _sub_files in os.walk(root):
            if any(f.lower().endswith(('.jpg', '.jpeg', '.png')) for f in _sub_files):
                candidates.append(root)
                break

    if not candidates:
        return ''

    preferred = [c for c in candidates if 'distress images' not in c.lower()]
    if len(candidates) > 1:
        log_issue("WARNING", "Deliverable",
                   f"Multiple {defect_type} image folders found "
                   f"({len(candidates)}) -- using "
                   f"{'the plain Images folder' if preferred else 'the (Distress Images) one (no plain folder found)'}: "
                   f"{preferred[0] if preferred else candidates[0]}. Other candidates: "
                   f"{[c for c in candidates if c != (preferred[0] if preferred else candidates[0])]}",
                   file=project_deliverable_root)
    return preferred[0] if preferred else candidates[0]


def find_d3_images_by_filename(image_folder: str) -> dict:
    """
    Map filename (lowercased) -> full path for every real jpg/png image
    found ANYWHERE beneath image_folder, at any subfolder depth.

    Used by the D4-present branch of load_d4_pothole_points(): D4's own
    Potholes sheet already gives an exact image_name per record (a plain
    filename, e.g. 'DME_PKG_2_L1_000131.jpg') -- no chainage-matching is
    needed at all here, since D4 already tells us exactly which photo
    goes with which record. We just need to find WHERE that exact
    filename actually lives on disk (the same D3 'Potholes Images'
    folder used by the D4-missing fallback) so it can be uploaded to get
    a clickable URL.
    """
    mapping = {}
    if not image_folder:
        return mapping
    for root, _dirs, files in os.walk(_long_path(image_folder)):
        for f in files:
            if f.lower().endswith(('.jpg', '.jpeg', '.png')):
                mapping[f.strip().lower()] = os.path.join(root, f)
    return mapping


def find_d3_pothole_image_folder(project_deliverable_root: str) -> str:
    """Backwards-compatible wrapper -- see find_d3_defect_image_folder."""
    return find_d3_defect_image_folder(project_deliverable_root, 'potholes')


_D2D3_POTHOLE_SEEDS = {"pothole", "potholes", "minor pothole", "major pothole",
                        "minor potholes", "major potholes"}
_D2D3_POTHOLE_FUZZY_THRESHOLD = 80


def _is_pothole_table_name(name: str) -> bool:
    if not name:
        return False
    cleaned = str(name).lower().strip()
    return any(fuzz.partial_ratio(cleaned, seed) >= _D2D3_POTHOLE_FUZZY_THRESHOLD
               for seed in _D2D3_POTHOLE_SEEDS)


def collect_d2_pothole_tables(d2_file_results: dict) -> pd.DataFrame:
    """
    d2_file_results: {sheet_name: DataFrame or [DataFrame, ...]} -- one
    file's RAW extract_folder() output for the D2 'Comparision as per CA'
    file (BEFORE Cell 51's bronze-writing normalisation).

    Only considers sheets whose OWN NAME is pothole-shaped -- 'Potholes',
    'Pothole', 'Minor Potholes', 'Major Potholes', or any other close
    variant, fuzzy-matched via _is_pothole_table_name(). Summary sheets
    (or anything else) are never inspected at all -- potholes in this
    file always live in their own dedicated sheet tab, never embedded
    inside a Summary sheet, so there is nothing to gain by looking there.

    A matched sheet is normally a single flat DataFrame (extract_folder()
    routes any non-"summary"-named sheet through extract_sheet(), not
    run_d2()) -- handled directly. If a project ever names its pothole
    sheet in a way that somehow still gets routed as a list (defensive
    only), each sub-table is concatenated in too.

    Concatenates every matched sheet into one flat table -- handles a
    file with e.g. both 'Minor Potholes' and 'Major Potholes' as separate
    tabs, unifying them into one table.
    """
    frames = []
    for sheet_name, val in d2_file_results.items():
        if not _is_pothole_table_name(sheet_name):
            continue
        if isinstance(val, list):
            frames.extend(tbl for tbl in val if isinstance(tbl, pd.DataFrame) and not tbl.empty)
        elif isinstance(val, pd.DataFrame) and not val.empty:
            frames.append(val)

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out['Chainage'] = pd.to_numeric(out.get('Chainage'), errors='coerce')
    return out.dropna(subset=['Chainage'])


def attach_d3_images_to_d2_potholes(d2_potholes: pd.DataFrame, image_meta: pd.DataFrame,
                                     max_chainage_gap_m: float = 10.0) -> pd.DataFrame:
    """
    Join D2 pothole rows to D3 image metadata by NEAREST Chainage within
    max_chainage_gap_m -- kept at 10m per instruction (the CHAINAGE
    matching range stays 10m; it is the separate Lat/Lon-to-GPS-backup
    resolution step, in map_latlon_to_chainage below, that uses
    Prathap's own ±5m accuracy figure -- these are two different
    tolerances for two different steps, not the same number). Not an
    exact match, since the two chainage values being compared came from
    independent sources/roundings (verified real gaps of 7-8m for some
    genuinely correct matches, e.g. L4/L3).

    Any image whose Chainage remains unresolved (no GPS-reference-sheet
    entry AND no EXIF-derived match within 5m of any GPS backup point --
    see resolve_missing_chainages_via_exif) is simply left unmatched
    (counted in the accounting log below), rather than silently
    disappearing with no trace.

    When image_meta has a non-empty 'Lane' (from the image's own
    subfolder name, e.g. 'Potholes Images/R4/photo.jpg' -> 'R4'), matching
    is scoped to that lane only first -- an image filed under its own
    lane's subfolder should never cross-match a different lane.

    Logs total images found, how many matched by chainage, and how many
    were left unused -- plus how many D2 rows still have no image.

    Each D2 row gets at most one image; unmatched rows get image_path =
    "" (kept, not dropped -- a pothole record with no matched photo is
    still a real record).
    """
    out = d2_potholes.copy().reset_index(drop=True)

    _lane_col = next((c for c in out.columns if norm_key(c) in ('laneno', 'lane', 'laneno.')), None)
    out['Lane'] = out[_lane_col].astype(str).str.upper().str.strip() if _lane_col else ''

    out['image_path'] = ""
    out['image_name'] = ""
    out['image_lat']  = pd.NA
    out['image_lon']  = pd.NA
    out['image_url']  = ""

    total_images = len(image_meta)
    imgs_chainage = image_meta.dropna(subset=['Chainage']).reset_index(drop=True)

    has_lane_signal = 'Lane' in image_meta.columns and (image_meta['Lane'] != '').any()
    used_image_paths = set()

    # PERFORMANCE (no logic change -- verified identical output including
    # lane-signal, no-lane-signal, and empty-images cases): the same
    # lane's image subset used to be re-filtered AND re-sorted from
    # scratch on every D2 row that shares that lane (e.g. R2's 3 rows all
    # redid the exact same filter+sort). Cached per lane instead -- same
    # filter, same sort, computed once per distinct lane instead of once
    # per row.
    if not imgs_chainage.empty:
        _lane_cache = {}
        def _imgs_for_lane(lane):
            key = lane if (has_lane_signal and lane) else '__ALL__'
            if key not in _lane_cache:
                subset = imgs_chainage
                if key != '__ALL__':
                    subset = imgs_chainage[(imgs_chainage['Lane'] == lane) | (imgs_chainage['Lane'] == '')]
                _lane_cache[key] = subset.sort_values('Chainage').reset_index(drop=True)
            return _lane_cache[key]

        for idx, row in out.iterrows():
            imgs = _imgs_for_lane(row['Lane'])
            if imgs.empty:
                continue
            img_chainages = imgs['Chainage'].to_numpy()
            ch = row['Chainage']
            # FIX: verified real case (PKG-2, D2 chainage 35762) -- TWO
            # separate candidate images (35760 and 35770) both fall
            # within max_chainage_gap_m of this SAME single D2 record,
            # but neither has any OTHER D2 record within range (the next
            # nearest D2 rows were 15m+ away). The old code only ever
            # picked bisect's single closest neighbour (35760, 2m) and
            # discarded the other (35770, 8m) -- even though 8m is
            # itself well within the agreed 10m tolerance, it was simply
            # never given a chance because only one image was allowed
            # per D2 record. Changed to attach EVERY candidate image
            # within tolerance to this record (not just the closest
            # one), comma-joined -- same convention build_potholes_
            # stage_table() already uses when multiple pothole points
            # share one 10m segment, so this stays consistent with how
            # multi-image segments are represented elsewhere.
            diffs = np.abs(img_chainages - ch)
            within = np.where(diffs <= max_chainage_gap_m)[0]
            if len(within) == 0:
                continue
            within_sorted = within[np.argsort(diffs[within])]  # closest first
            _paths = [imgs.at[p, 'image_path'] for p in within_sorted]
            _urls  = [imgs.at[p, 'image_url'] if 'image_url' in imgs.columns else "" for p in within_sorted]
            _unique_paths = list(dict.fromkeys(str(p) for p in _paths if p))
            _unique_names = [os.path.basename(p) for p in _unique_paths]
            out.at[idx, 'image_path'] = ", ".join(_unique_paths)
            out.at[idx, 'image_name'] = ", ".join(_unique_names)
            out.at[idx, 'image_lat']  = imgs.at[within_sorted[0], 'Lat']  # closest image's own Lat/Lon, representative
            out.at[idx, 'image_lon']  = imgs.at[within_sorted[0], 'Lon']
            out.at[idx, 'image_url']  = ", ".join(dict.fromkeys(str(u) for u in _urls if u))
            for p in _paths:
                used_image_paths.add(p)

        # FIX per explicit instruction: "matched" means the image's OWN
        # chainage was successfully resolved via GPS (Step 1 -- EXIF
        # Lat/Lon compared against GPS_Geometry) -- NOT whether it also
        # happened to line up with an existing D2 pothole record. D2 is
        # sparse (only chainages where a defect was actually recorded),
        # unlike GPS_Geometry which covers the WHOLE lane, so plenty of
        # perfectly well-located images will have no D2 record anywhere
        # near them -- that's expected, not an error. For every
        # candidate image that resolved a Chainage but never got used by
        # any D2 row above, add it as its own new row here: Chainage/
        # Lane from the image itself (real, GPS-derived), every D2
        # measurement field (Area_Sq_m, Max_Depth_mm, Average_Depth_mm,
        # Severity, NH_No, etc.) left NULL since D2 genuinely has no
        # data for it -- "no problem in the parquet", exactly as
        # instructed. Flows downstream into build_potholes_stage_table()
        # and Unified_Segments like a normal D2-sourced point (same
        # chainage-containment binning).
        #
        # BUG FIXED: this used to pull orphan candidates from the FULL,
        # project-wide imgs_chainage pool (every lane's images, not just
        # THIS call's own lane) -- verified real case: lane R3's call
        # logged "total=4, matched=25, unmatched=-21" (matched > total,
        # a negative "unmatched" count is impossible) because it was
        # silently absorbing leftover unmatched images from L1/L2/L3/R1/
        # R2 too, on every single lane's call. Scoped to THIS lane's own
        # candidates only now -- same lane-scoping _imgs_for_lane() above
        # already uses for the main D2-row matching loop, kept consistent.
        _this_lane_for_orphans = out['Lane'].iloc[0] if not out.empty and 'Lane' in out.columns else ''
        if _this_lane_for_orphans and has_lane_signal:
            _orphan_candidates = imgs_chainage[(imgs_chainage['Lane'] == _this_lane_for_orphans) | (imgs_chainage['Lane'] == '')]
        else:
            _orphan_candidates = imgs_chainage
        if not _orphan_candidates.empty:
            _all_candidate_paths = set(_orphan_candidates['image_path'])
            _orphan_paths = _all_candidate_paths - used_image_paths
            if _orphan_paths:
                _orphans = _orphan_candidates[_orphan_candidates['image_path'].isin(_orphan_paths)]
                _template = {col: pd.NA for col in out.columns}
                _new_rows = []
                for _, oimg in _orphans.iterrows():
                    _r = dict(_template)
                    _r['Chainage']   = oimg['Chainage']
                    _r['Lane']       = oimg.get('Lane', '') or _this_lane_for_orphans
                    _r['image_path'] = str(oimg['image_path'])
                    _r['image_name'] = os.path.basename(str(oimg['image_path']))
                    _r['image_lat']  = oimg.get('Lat')
                    _r['image_lon']  = oimg.get('Lon')
                    _r['image_url']  = str(oimg.get('image_url', '')) if oimg.get('image_url') else ''
                    _new_rows.append(_r)
                    used_image_paths.add(oimg['image_path'])
                out = pd.concat([out, pd.DataFrame(_new_rows)], ignore_index=True)

    unused_images = total_images - len(used_image_paths)
    matched_images = len(used_image_paths)

    # FIX per explicit instruction: the OLD log always printed the FULL
    # PROJECT'S total image count (total=489 style) on every single lane
    # call, which reads as if every lane is failing against all 489
    # images -- confusing, since each lane can only ever match its OWN
    # candidate images. Now logs a per-LANE breakdown instead, on an
    # IMAGE-count basis (total = matched + unmatched, all counting
    # candidate IMAGES that belong to this lane -- not D2 rows, which
    # can be a different number entirely): e.g. lane L1 has 9 candidate
    # images total, 6 of them got used (matched) somewhere in this
    # lane's D2 rows, 3 never got used (unmatched) -- "L1: total=9,
    # matched=6, unmatched=3." used_image_paths/matched_images above are
    # ALREADY scoped to this lane's own candidates (matching only ever
    # draws from _imgs_for_lane(row['Lane']) for this lane's D2 rows),
    # so they can be reused directly here -- just need this lane's own
    # candidate total instead of the global 489.
    _this_lane = out['Lane'].iloc[0] if not out.empty and 'Lane' in out.columns else ''
    if _this_lane and has_lane_signal:
        _lane_total = int(((image_meta['Lane'] == _this_lane) | (image_meta['Lane'] == '')).sum())
    else:
        _lane_total = total_images
    _lane_matched = matched_images
    _lane_unmatched = _lane_total - _lane_matched
    log_issue("WARNING", "Deliverable",
               f"Potholes image matching for lane {_this_lane or '(unknown)'}: "
               f"total={_lane_total}, matched={_lane_matched}, unmatched={_lane_unmatched}.")

    # FIX per explicit instruction: name each UNMATCHED image individually
    # (filename + lane + WHY), instead of only reporting a bare count --
    # two distinct reasons are distinguished:
    #   1. "chainage never resolved" -- neither the GPS reference sheet
    #      nor EXIF+GPS_Geometry (within max_gap_m) could determine a
    #      Chainage for this image at all, so it never even had a number
    #      to compare against any D2 record.
    #   2. "resolved chainage too far from any D2 record" -- a Chainage
    #      WAS resolved, but the nearest D2 pothole record for this lane
    #      is farther than max_chainage_gap_m away -- reports the exact
    #      gap in meters.
    # Also flags images that share IDENTICAL Lat/Lon with other images in
    # this same lane -- a strong signal of a stuck/frozen GPS reading in
    # the source photos (verified real case: 3 different-chainage photos
    # in one lane all carrying the exact same banner-overlay GPS) rather
    # than a real distinct location, which explains why such images can
    # fail to distinguish between different D2 records.
    if _this_lane and has_lane_signal:
        _this_lane_all = image_meta[(image_meta['Lane'] == _this_lane) | (image_meta['Lane'] == '')]
    else:
        _this_lane_all = image_meta
    _d2_chainages_this_lane = pd.to_numeric(out['Chainage'], errors='coerce').dropna().to_numpy()
    # Count how many images in this lane share each exact (Lat, Lon) pair
    # (rounded to 6dp -- ~0.1m -- so float noise doesn't split real dupes).
    _latlon_counts = (_this_lane_all.dropna(subset=['Lat', 'Lon'])
                        .assign(_ll=lambda d: list(zip(d['Lat'].round(6), d['Lon'].round(6))))['_ll']
                        .value_counts())
    for _, img_row in _this_lane_all.iterrows():
        img_path = img_row.get('image_path', '')
        if img_path in used_image_paths:
            continue  # matched -- nothing to explain
        img_name = os.path.basename(str(img_path)) if img_path else '(unknown file)'
        ch = img_row.get('Chainage')
        ch_is_valid = pd.notna(ch)
        if not ch_is_valid:
            reason = "chainage could not be resolved (no GPS-reference-sheet match, and EXIF GPS was outside the GPS_Geometry tolerance)"
        elif len(_d2_chainages_this_lane) > 0:
            _gap = float(np.min(np.abs(_d2_chainages_this_lane - float(ch))))
            reason = f"chainage resolved to {ch:.0f}, but nearest D2 pothole record for this lane is {_gap:.1f}m away (tolerance={max_chainage_gap_m:.0f}m)"
        else:
            reason = f"chainage resolved to {ch:.0f}, but this lane has no D2 pothole records at all"
        dup_note = ""
        if pd.notna(img_row.get('Lat')) and pd.notna(img_row.get('Lon')):
            _ll = (round(float(img_row['Lat']), 6), round(float(img_row['Lon']), 6))
            _dup_count = int(_latlon_counts.get(_ll, 1)) - 1
            if _dup_count > 0:
                dup_note = (f" [shares identical GPS coordinates with {_dup_count} other "
                            f"image(s) in this lane -- possible stuck/frozen GPS reading]")
        log_issue("WARNING", "Deliverable",
                   f"Unmatched pothole image: lane={_this_lane or '(unknown)'}, "
                   f"file={img_name} -- {reason}.{dup_note}")

    return out


def consolidate_cracking_images(project_deliverable_root: str, d3_extraction_results: dict) -> pd.DataFrame:
    """
    Same consolidation as Potholes, for Cracking: finds D3's 'Cracking
    Images' folder (name-shaped + real jpg content, any subfolder depth),
    resolves each photo's Chainage/Lat/Lon primarily from D3's own
    'Cracking_GPS_{lane}' reference sheets (OCR as backup), copies every
    photo into one 'Cracking_Images' folder inside Deliverable-3's own
    bronze partition, and writes a sibling 'Cracking_Images_Metadata'
    folder with one CSV of the extracted values.

    Unlike Potholes, this doesn't depend on D4 at all -- Cracking's own
    measurement data already comes from the Backup lane workbooks
    regardless. This function locates/resolves the photos; the actual
    attach-to-segment step happens separately in
    attach_images_to_cracking_stage_table(), per lane, in the driver loop.

    image_url = the image's full absolute path (Volumes-style) -- per
    Prathap's own instruction on the call: the volume names in this path
    are object IDs in production, not known yet; capturing the full path
    now is what lets that swap happen later with no other work needed.
    No upload/CDN call happens anywhere in this pipeline.

    Returns whatever extract_image_metadata() returns (empty DataFrame if
    D3 has no Cracking Images folder with real photos in it).
    """
    image_folder = find_d3_defect_image_folder(str(project_deliverable_root), 'cracking')
    if not image_folder:
        return pd.DataFrame()
    gps_lookup = build_defect_image_gps_lookup(d3_extraction_results or {}, 'cracking')
    return extract_image_metadata(image_folder, gps_lookup=gps_lookup, defect_label="Cracking")


def attach_images_to_cracking_stage_table(cracking_df: pd.DataFrame, image_meta: pd.DataFrame,
                                            lane: str = "") -> pd.DataFrame:
    """
    Attach Cracking image_name/image_url onto the Cracking stage table's
    OWN existing 10m segments, by chainage-containment -- same min/max-
    direction-safe containment check build_potholes_stage_table() uses
    (handles both Increasing and Decreasing direction lanes), applied
    directly onto Cracking's own Start_Chainage/End_Chainage rows -- no
    new grid needs to be built here, Cracking's own rows already ARE the
    segments (unlike Potholes, which builds a fresh table from scratch).

    image_meta: the SAME 'Chainage/Lane/image_path/image_url' shape
    extract_image_metadata() returns for Cracking (via
    consolidate_cracking_images(), called once per project).

    Adds two NEW columns: cracking_image_name, cracking_image_url --
    prefixed with 'cracking_' (not just 'image_name'/'image_url') so
    they never collide with Potholes' own identically-purposed columns
    once both get merged onto the same Unified_Segments row.

    Multiple cracking photos in the same segment -> every distinct
    name/url, comma-joined (same convention as Potholes' image_name).
    A segment with no matched photo keeps "" (not dropped).
    """
    out = cracking_df.copy()
    out['cracking_image_name'] = ""
    out['cracking_image_url']  = ""

    if image_meta.empty or 'Start_Chainage' not in out.columns or 'End_Chainage' not in out.columns:
        log_issue("WARNING", "Deliverable",
                   f"Cracking image matching for lane {lane or '(unknown)'}: "
                   f"total=0, matched=0, unmatched=0 (no image metadata or no chainage grid available).")
        return out

    # Full candidate set for THIS lane, BEFORE dropping unresolved-chainage
    # rows -- needed further below to also report images whose chainage
    # never resolved at all (dropna would otherwise hide them entirely).
    if lane and 'Lane' in image_meta.columns and (image_meta['Lane'] != '').any():
        _lane_all = image_meta[(image_meta['Lane'] == lane) | (image_meta['Lane'] == '')]
    else:
        _lane_all = image_meta

    imgs = image_meta.dropna(subset=['Chainage'])
    if lane and 'Lane' in imgs.columns and (imgs['Lane'] != '').any():
        imgs = imgs[(imgs['Lane'] == lane) | (imgs['Lane'] == '')]
    if imgs.empty:
        log_issue("WARNING", "Deliverable",
                   f"Cracking image matching for lane {lane or '(unknown)'}: "
                   f"total={len(_lane_all)}, matched=0, unmatched={len(_lane_all)}.")
        for _, img_row in _lane_all.iterrows():
            _name = os.path.basename(str(img_row.get('image_path', ''))) or '(unknown file)'
            log_issue("WARNING", "Deliverable",
                       f"Unmatched cracking image: lane={lane or '(unknown)'}, "
                       f"file={_name} -- chainage could not be resolved (no "
                       f"GPS-reference-sheet match, and EXIF GPS was outside "
                       f"the GPS_Geometry tolerance).")
        return out

    # Same na_value= fix as build_potholes_stage_table/map_latlon_to_chainage
    # -- these columns can be a nullable dtype after schema-casting, and
    # np.isnan() further below would raise TypeError on any leftover pd.NA.
    starts = np.minimum(pd.to_numeric(out['Start_Chainage'], errors='coerce').to_numpy(dtype='float64', na_value=np.nan),
                         pd.to_numeric(out['End_Chainage'], errors='coerce').to_numpy(dtype='float64', na_value=np.nan))
    ends   = np.maximum(pd.to_numeric(out['Start_Chainage'], errors='coerce').to_numpy(dtype='float64', na_value=np.nan),
                         pd.to_numeric(out['End_Chainage'], errors='coerce').to_numpy(dtype='float64', na_value=np.nan))
    chainages = pd.to_numeric(imgs['Chainage'], errors='coerce').to_numpy(dtype='float64', na_value=np.nan)
    names = imgs['image_path'].apply(lambda p: os.path.basename(str(p)) if p else '').to_numpy()
    urls  = imgs['image_url'].to_numpy() if 'image_url' in imgs.columns else np.full(len(imgs), '', dtype=object)

    name_col = out.columns.get_loc('cracking_image_name')
    url_col  = out.columns.get_loc('cracking_image_url')

    from collections import defaultdict
    row_points = defaultdict(list)
    for pt_idx in range(len(chainages)):
        ch = chainages[pt_idx]
        if np.isnan(ch):
            continue
        in_row = (starts <= ch) & (ch < ends)
        for r in np.where(in_row)[0]:
            row_points[r].append(pt_idx)

    for idx, pt_list in row_points.items():
        pt_indices = np.array(pt_list)
        seg_names = sorted({str(n) for n in names[pt_indices] if n})
        seg_urls  = sorted({str(u) for u in urls[pt_indices] if u})
        out.iat[idx, name_col] = ", ".join(seg_names)
        out.iat[idx, url_col]  = ", ".join(seg_urls)

    # FIX: unlike Potholes' attach_d3_images_to_d2_potholes(), this
    # function never logged any total/matched/unmatched summary at all
    # -- Cracking image-matching results were completely invisible in
    # the validation log, verified missing on Zone D's run (Potholes
    # logged "D2+D3 pothole image matching: ..." repeatedly, Cracking
    # never logged anything equivalent). Added here, same per-lane,
    # IMAGE-count basis as the Potholes fix above (total = matched +
    # unmatched, counting candidate IMAGES for this lane -- not
    # segments): an image is "matched" if its chainage landed inside
    # ANY of this lane's 10m segments (row_points), "unmatched" if its
    # chainage never fell inside any segment (e.g. outside the grid's
    # own chainage range for this lane).
    _lane_label = lane or '(unknown)'
    _used_pt_indices = {p for pts in row_points.values() for p in pts}
    _lane_total = len(_lane_all)
    _lane_matched = len(_used_pt_indices)
    _lane_unmatched = _lane_total - _lane_matched
    log_issue("WARNING", "Deliverable",
               f"Cracking image matching for lane {_lane_label}: "
               f"total={_lane_total}, matched={_lane_matched}, unmatched={_lane_unmatched}.")

    # FIX per explicit instruction (same as Potholes): name each
    # UNMATCHED image individually (filename + lane + WHY). Two reason
    # categories, same logic as Potholes: chainage never resolved at
    # all (image dropped before even reaching `imgs`), or chainage
    # resolved but fell outside every one of this lane's chainage
    # segments (containment check never matched). Also flags images
    # sharing an identical Lat/Lon with another image in this lane --
    # a stuck/frozen GPS reading signal, same as Potholes.
    _matched_paths = {imgs.iloc[p]['image_path'] for p in _used_pt_indices}
    _latlon_counts = (_lane_all.dropna(subset=['Lat', 'Lon'])
                        .assign(_ll=lambda d: list(zip(d['Lat'].round(6), d['Lon'].round(6))))['_ll']
                        .value_counts())
    for _, img_row in _lane_all.iterrows():
        img_path = img_row.get('image_path', '')
        if img_path in _matched_paths:
            continue
        img_name = os.path.basename(str(img_path)) if img_path else '(unknown file)'
        ch = img_row.get('Chainage')
        if pd.isna(ch):
            reason = ("chainage could not be resolved (no GPS-reference-sheet "
                       "match, and EXIF GPS was outside the GPS_Geometry tolerance)")
        else:
            reason = (f"chainage resolved to {ch:.0f}, but it does not fall inside "
                       f"any of this lane's chainage segments (outside the grid's range)")
        dup_note = ""
        if pd.notna(img_row.get('Lat')) and pd.notna(img_row.get('Lon')):
            _ll = (round(float(img_row['Lat']), 6), round(float(img_row['Lon']), 6))
            _dup_count = int(_latlon_counts.get(_ll, 1)) - 1
            if _dup_count > 0:
                dup_note = (f" [shares identical GPS coordinates with {_dup_count} other "
                            f"image(s) in this lane -- possible stuck/frozen GPS reading]")
        log_issue("WARNING", "Deliverable",
                   f"Unmatched cracking image: lane={_lane_label}, "
                   f"file={img_name} -- {reason}.{dup_note}")

    return out


_RAW_D2_POTHOLE_TABLE = pd.DataFrame()
_RAW_POTHOLE_IMAGE_META = pd.DataFrame()


def d4_missing_pothole_image_fallback(project_deliverable_root: str, all_results: dict,
                                       d3_extraction_results: dict = None) -> pd.DataFrame:
    """
    Only call when the project has NO Deliverable-4 pothole file at all.

    Falls back to D2 ('Comparision as per CA') for measurements -- ALL
    pothole-shaped sub-tables in the file (Minor/Major/etc.) unified into
    one table via collect_d2_pothole_tables() -- plus D3 ('GIS Mapping' ->
    Potholes Images folder) for photos, joined by nearest Chainage.

    d3_extraction_results (NEW): D3's own raw extraction results, used to
    build the GPS-reference lookup (build_pothole_image_gps_lookup) --
    the PRIMARY source for each photo's Chainage/Lat/Lon (exact values
    from D3's own 'Potholes_GPS_{lane}' reference sheets). OCR is only a
    backup for any photo that lookup doesn't cover.

    Returns Lane/Chainage/image_path/image_lat/image_lon (+ the D2 file's
    own original columns), ready to be normalised further by
    load_d4_pothole_points(). Empty if D2 has no pothole-shaped table at
    all, or D3 has no matching image folder.
    """
    d2_pothole = pd.DataFrame()
    for _key, file_results in all_results.items():
        # BUG FIXED: previously pre-filtered to `isinstance(v, list)` only,
        # which discarded every dedicated single-sheet "Potholes" table
        # (extract_sheet() output, not run_d2() output) before
        # collect_d2_pothole_tables() ever got a chance to look at it.
        # collect_d2_pothole_tables() now handles both shapes itself, so
        # file_results is passed through as-is, unfiltered.
        candidate = collect_d2_pothole_tables(file_results)
        if not candidate.empty:
            d2_pothole = candidate
            break

    if d2_pothole.empty:
        # CRITICAL, not WARNING -- D4 is already confirmed missing (that's
        # the only reason this function got called at all), and now D2
        # has no pothole-shaped table either -- there is NO source of
        # pothole data for this project at all, which is a genuine
        # required-data failure, not a soft warning.
        # deliverable="Deliverable - 2" passed explicitly -- guessing it
        # from file=project_deliverable_root (the project's OWN root
        # path) hit the same false-positive-digit bug fixed earlier for
        # the whole-deliverable-missing check: that path contains the
        # word "deliverable" plus stray digits from Phase_1/Cycle_1,
        # which previously made this CRITICAL wrongly show up tagged
        # "Deliverable - 1" in the log even though it's specifically
        # about D2 having no pothole data.
        log_issue("CRITICAL", "Deliverable",
                   "D4 missing and no pothole-shaped table found in D2 either "
                   "-- no source of pothole data available for this project at all.",
                   file=str(project_deliverable_root),
                   deliverable="Deliverable - 2")
        return pd.DataFrame()

    image_folder = find_d3_pothole_image_folder(str(project_deliverable_root))
    if not image_folder:
        log_issue("WARNING", "Deliverable",
                   "D4 missing and no pothole images folder found under D3.",
                   file=str(project_deliverable_root))
        _lane_col = next((c for c in d2_pothole.columns if norm_key(c) in ('laneno', 'lane', 'laneno.')), None)
        d2_pothole['Lane'] = d2_pothole[_lane_col].astype(str).str.upper().str.strip() if _lane_col else ''
        return d2_pothole.assign(image_path="", image_lat=pd.NA, image_lon=pd.NA, image_url="")

    gps_lookup = build_pothole_image_gps_lookup(d3_extraction_results or {})
    # image_url = the image's full absolute path (Volumes-style) -- per
    # Prathap's own call: "we can only replace these three things
    # [volume names] with the object IDs and all the other things will
    # be same folders". No upload/CDN call happens anywhere here.
    image_meta = extract_image_metadata(image_folder, gps_lookup=gps_lookup)

    # Exposed as module-level globals (same single-project-per-run
    # convention this whole pipeline already uses elsewhere) so the
    # per-lane driver loop (Cell 95) can RETRY matching once each lane's
    # own GPS_Geometry exists -- images that only had EXIF Lat/Lon (no
    # GPS-reference-sheet Chainage) can't be chainage-matched here at
    # all yet; that only becomes possible per lane, later.
    _result = attach_d3_images_to_d2_potholes(d2_pothole, image_meta)

    # BUG FIXED: _RAW_D2_POTHOLE_TABLE used to be captured from d2_pothole
    # (the RAW table, BEFORE attach_d3_images_to_d2_potholes normalises
    # its own Lane column from whatever the raw sheet calls it, e.g.
    # "Lane No.") -- so it never actually had a 'Lane' column at all.
    # The per-lane retry code in the driver loop (Cell 95) does
    # _RAW_D2_POTHOLE_TABLE[_RAW_D2_POTHOLE_TABLE['Lane'] == ...], which
    # raised KeyError: 'Lane' the moment it ran for the very first lane.
    # Captured from `_result` instead -- the ALREADY-normalised output of
    # attach_d3_images_to_d2_potholes, which is guaranteed to have 'Lane'
    # (and every other original D2 column untouched, so nothing the retry
    # logic needs is lost).
    global _RAW_D2_POTHOLE_TABLE, _RAW_POTHOLE_IMAGE_META
    _RAW_D2_POTHOLE_TABLE = _result.copy()
    _RAW_POTHOLE_IMAGE_META = image_meta.copy()

    return _result



# COMMAND ----------

# ─────────────────────────────────────────────────────────────────────────────
# Potholes stage table -- built the SAME WAY (Lane, Start_Chainage,
# End_Chainage grid, Pothole_Count, Severity/Area/Depth aggregates,
# image_name) regardless of whether the points came from D4 (when present)
# or the D2+D3 OCR fallback (when D4 is missing) -- written as its own
# bronze parquet, same as GPS_Geometry/Cracking/Roughness_IRI/Rutting/
# Raveling, and reused as-is when attaching pothole info onto
# Unified_Segments (see the driver loop further below).
#
# Final Potholes.parquet columns (only these -- confirmed against your real
# D4/D2 bronze output, which has NO Width field at all -- a pothole record
# is Area + Max_Depth + Average_Depth + Severity, never a separate width
# measurement; that's a Cracking-only field):
#   Lane (join key only), Start_Chainage, End_Chainage, Pothole_Count,
#   Severity, Area_Sq_m, Max_Depth_mm, Average_Depth_mm, image_name
# ─────────────────────────────────────────────────────────────────────────────

def _clean(s) -> str:
    """Strip everything except letters/digits and lowercase -- used to match
    real-world column-name variants ('Area (in Sq m)', 'area_sq_m',
    'Max Depth (in mm)', 'max_depth_mm', ...) without depending on exact
    spacing/casing/punctuation."""
    return re.sub(r'[^a-z0-9]', '', str(s).lower())


def _find_pothole_field(columns, *keywords) -> str:
    """Return the first column whose cleaned name contains ALL given
    keywords (also cleaned) -- e.g. _find_pothole_field(cols, 'area') or
    _find_pothole_field(cols, 'max', 'depth'). Returns None if no match."""
    keywords = [_clean(k) for k in keywords]
    for c in columns:
        cc = _clean(c)
        if all(k in cc for k in keywords):
            return c
    return None


def _extract_d4_source_name(parquet_filename: str) -> str:
    """
    Best-effort subfolder-name guess from a D4 bronze filename, e.g.
    "ZD_P1_C2_..._Fly Over_SilkBoard FlyOver Pothole_Sheet_Sheet1.parquet"
    -> "Fly Over", or "ZD_..._MCW_Silkboard...parquet" -> "MCW". The bronze
    filename is "Z{zone}_P{phase}_C{cycle}_{upc}_{subfolder}_{original_stem}
    _{sheet}.parquet" when the source file lived in a subfolder (see Cell
    51's excel_stem/_subfolder_tag), so stripping the fixed Z/P/C/UPC
    prefix and taking the first underscore-delimited token recovers it.
    Returns '' if the filename doesn't start with the expected prefix at all.
    """
    prefix = f"Z{ZONE}_P{PHASE}_C{CYCLE}_{UPC}_"
    stem = parquet_filename
    if not stem.lower().startswith(prefix.lower()):
        return ''
    stem = re.sub(r'\.parquet$', '', stem[len(prefix):], flags=re.IGNORECASE)
    return stem.split('_', 1)[0] if stem else ''


def _best_matching_source(project_label: str, candidate_names: set, threshold: int = 60) -> str:
    """
    Fuzzy-match a Backup lane workbook's own package label against the
    distinct D4/fallback pothole source names actually found. Zero or one
    distinct source overall -> no ambiguity, return it (or '') unconditionally.
    Two or more -> pick the best fuzzy match to project_label; if even the
    best is weak (< threshold), return '' (don't filter) rather than guess
    wrong -- safer to risk including an unrelated source than wrongly drop
    a real one.
    """
    if not candidate_names:
        return ''
    if len(candidate_names) == 1:
        return next(iter(candidate_names))
    best, best_score = '', -1
    for name in candidate_names:
        score = fuzz.partial_ratio(str(project_label).lower(), str(name).lower())
        if score > best_score:
            best, best_score = name, score
    return best if best_score >= threshold else ''


def load_d4_pothole_points(fallback_project_dir: str = None,
                            fallback_extraction_results: dict = None,
                            d3_extraction_results: dict = None) -> pd.DataFrame:
    """
    Read every D4 Potholes bronze parquet for the current Zone/Phase/
    Cycle/UPC and return one flat table of individual pothole point
    records: Lane, Chainage, Severity, Area_Sq_m, Max_Depth_mm,
    Average_Depth_mm, image_name, image_path, image_lat, image_lon,
    Source_Tag.

    If no D4 Potholes bronze files exist at all for this project, and
    fallback_project_dir/fallback_extraction_results are supplied, falls
    back to d4_missing_pothole_image_fallback() -- D2's pothole
    measurements (Minor/Major/etc. unified into one table) joined to D3's
    'Potholes Images' folder by nearest OCR'd Chainage. Either way the
    returned columns are IDENTICAL, so build_potholes_stage_table() below
    never needs to know or care which source produced them.

    Field name matching uses _find_pothole_field() (cleans punctuation/
    spacing/casing) since D4's bronze-cast names ('area_sq_m',
    'max_depth_mm', 'average_depth_mm', 'severity') and D2's raw sheet
    names ('Area (in Sq m)', 'Max Depth (in mm)', ...) spell these
    differently.

    Always returns a DataFrame with these columns, even when nothing at
    all was found -- callers never need a None-check.
    """
    cols = ['Lane', 'Chainage', 'Severity', 'Area_Sq_m', 'Max_Depth_mm', 'Average_Depth_mm',
            'image_name', 'image_path', 'image_lat', 'image_lon', 'image_url', 'Source_Tag']
    empty = pd.DataFrame(columns=cols)

    deliv_root = (Path(OUTPUT_ROOT) / "Source=NSV" / f"Zone={ZONE}" / f"Phase={PHASE}"
                  / f"Cycle={CYCLE}" / f"UPC={UPC}")
    # Project=-based file discovery (merged in from the team's own fix):
    # Deliverable-stage bronze writes now have a "Project=<pkg>" partition
    # level (Cell 51), added because two or more packages whose D4
    # Potholes file lands at the same relative subfolder/filename used to
    # silently overwrite each other's bronze output with zero warning --
    # only the alphabetically-last package ever survived to be read here.
    # Every Project=* folder is searched now, and Source_Tag is taken
    # directly from that real partition value. A LEGACY fallback (no
    # Project= level at all) is also checked, for bronze files written
    # before this fix existed.
    files = []
    source_tag_by_file = {}
    if os.path.isdir(_long_path(str(deliv_root))):
        deliv_tag = slugify('Deliverable - 4')
        for date_dir in glob.glob(_long_path(str(deliv_root / "year=*" / "month=*" / "day=*"))):
            for project_dir_str in glob.glob(str(Path(date_dir) / "Project=*")):
                d4_folder = Path(project_dir_str) / f"Deliverable={deliv_tag}"
                if not os.path.isdir(_long_path(str(d4_folder))):
                    continue
                _proj_tag = os.path.basename(project_dir_str).split("=", 1)[-1]
                for f in os.listdir(_long_path(str(d4_folder))):
                    if f.lower().endswith('.parquet') and _filename_matches_required_patterns(f.lower(), ['pothole']):
                        fpath = d4_folder / f
                        files.append(fpath)
                        source_tag_by_file[str(fpath)] = _proj_tag
            # LEGACY: bronze written before the Project= fix existed.
            d4_folder_legacy = Path(date_dir) / f"Deliverable={deliv_tag}"
            if os.path.isdir(_long_path(str(d4_folder_legacy))):
                for f in os.listdir(_long_path(str(d4_folder_legacy))):
                    if f.lower().endswith('.parquet') and _filename_matches_required_patterns(f.lower(), ['pothole']):
                        fpath = d4_folder_legacy / f
                        files.append(fpath)
                        source_tag_by_file[str(fpath)] = ''

    if not files:
        if fallback_project_dir is None:
            log_issue("WARNING", "Backup", "No D4 Potholes bronze file found for this project.")
            return empty

        log_issue("WARNING", "Backup",
                   "No D4 Potholes bronze file found for this project -- "
                   "falling back to D2 measurements + D3 image OCR.")
        fb = d4_missing_pothole_image_fallback(fallback_project_dir, fallback_extraction_results or {},
                                                d3_extraction_results=d3_extraction_results)
        if fb.empty or 'Lane' not in fb.columns:
            return empty

        _area_col = _find_pothole_field(fb.columns, 'area')
        _maxd_col = _find_pothole_field(fb.columns, 'max', 'depth')
        _avgd_col = _find_pothole_field(fb.columns, 'average', 'depth') or \
                    _find_pothole_field(fb.columns, 'avg', 'depth')
        _sev_col  = _find_pothole_field(fb.columns, 'severity')

        out = pd.DataFrame({
            'Lane': fb['Lane'],
            'Chainage': pd.to_numeric(fb['Chainage'], errors='coerce'),
            'Severity': fb[_sev_col].astype(str).str.strip() if _sev_col else '',
            'Area_Sq_m': pd.to_numeric(fb[_area_col], errors='coerce') if _area_col else pd.NA,
            'Max_Depth_mm': pd.to_numeric(fb[_maxd_col], errors='coerce') if _maxd_col else pd.NA,
            'Average_Depth_mm': pd.to_numeric(fb[_avgd_col], errors='coerce') if _avgd_col else pd.NA,
            'image_name': fb['image_path'].apply(lambda p: os.path.basename(str(p)) if p else ''),
            'image_path': fb.get('image_path', ''),
            'image_lat': fb.get('image_lat', pd.NA),
            'image_lon': fb.get('image_lon', pd.NA),
            'image_url': fb.get('image_url', ''),
            'Source_Tag': '',
        })
        return out.dropna(subset=['Lane', 'Chainage']).reset_index(drop=True)

    frames = []
    for fpath in files:
        try:
            df = pd.read_parquet(_long_path(str(fpath)))
        except Exception as exc:
            log_issue("WARNING", "Backup",
                       f"Could not read D4 Potholes bronze file for pothole-count "
                       f"merge into Unified_Segments: {concise_exc(exc)}",
                       file=str(fpath))
            continue
        if df.empty:
            continue
        if 'defect_id' in df.columns:
            df = df[pd.to_numeric(df['defect_id'], errors='coerce') == 1]
        needed = {'lane_no', 'chainage', 'image_name'}
        if not needed.issubset(df.columns):
            continue

        _area_col = _find_pothole_field(df.columns, 'area')
        _maxd_col = _find_pothole_field(df.columns, 'max', 'depth')
        _avgd_col = _find_pothole_field(df.columns, 'average', 'depth') or \
                    _find_pothole_field(df.columns, 'avg', 'depth')
        _sev_col  = _find_pothole_field(df.columns, 'severity')

        sub = pd.DataFrame({
            'Lane': df['lane_no'],
            'Chainage': pd.to_numeric(df['chainage'], errors='coerce'),
            'Severity': df[_sev_col].astype(str).str.strip() if _sev_col else '',
            'Area_Sq_m': pd.to_numeric(df[_area_col], errors='coerce') if _area_col else pd.NA,
            'Max_Depth_mm': pd.to_numeric(df[_maxd_col], errors='coerce') if _maxd_col else pd.NA,
            'Average_Depth_mm': pd.to_numeric(df[_avgd_col], errors='coerce') if _avgd_col else pd.NA,
            'image_name': df['image_name'],
        })
        sub['image_path'] = ""
        sub['image_lat']  = pd.NA
        sub['image_lon']  = pd.NA
        # D4's own image_name is just a text reference typed into the
        # sheet, not a real file path we have access to -- nothing to
        # upload here, so image_url stays empty for D4-sourced rows.
        sub['image_url']  = ""
        _tag = source_tag_by_file.get(str(fpath), '')
        sub['Source_Tag'] = _tag if _tag else _extract_d4_source_name(fpath.name)
        frames.append(sub)

    if not frames:
        return empty
    out = pd.concat(frames, ignore_index=True)
    out['Chainage'] = pd.to_numeric(out['Chainage'], errors='coerce')
    out = out.dropna(subset=['Lane', 'Chainage']).reset_index(drop=True)

    # D4-PRESENT CASE: D4 already gives an exact image_name per record --
    # no chainage-matching needed here at all -- but that's still just a
    # bare filename, not a URL a frontend can open on click. Find that
    # exact filename inside D3's real 'Potholes Images' folder (same
    # folder the D4-missing fallback uses) and upload it, per instruction
    # that image_url should be populated regardless of whether D4 or the
    # D2+D3 fallback is the actual data source.
    # image_url = the found file's own FULL ABSOLUTE PATH by default --
    # CDN upload is ON HOLD (Prathap's own instruction: volume names in
    # this path are object IDs in production, not known yet -- capturing
    # the full path now is what lets the later swap happen with no other
    # work). D4 already gives an exact image_name per record; we just
    # need to find WHERE that exact filename lives in D3's real
    # 'Potholes Images' folder to get its full path.
    if fallback_project_dir:
        _d4_image_folder = find_d3_defect_image_folder(fallback_project_dir, 'potholes')
        if _d4_image_folder:
            _name_to_path = find_d3_images_by_filename(_d4_image_folder)
            out['image_path'] = out['image_name'].apply(
                lambda n: _name_to_path.get(str(n).strip().lower(), ''))
            # image_url = the found file's own FULL ABSOLUTE PATH -- per
            # Prathap's own instruction on the call. No upload/CDN call.
            out['image_url'] = out['image_path']
        else:
            log_issue("WARNING", "Deliverable",
                       "D4 present with image_name values, but no D3 'Potholes Images' "
                       "folder found -- image_url stays empty for these rows.")

    # D4-side accounting (mirrors the D2+D3 fallback's own total/matched/
    # left log): D4's own image_name comes directly from the sheet's own
    # text column; image_url reflects whether that named file was actually
    # found in D3 (full path captured, or uploaded when requested).
    _total_d4_rows = len(out)
    _d4_with_image = (out['image_name'].astype(str).str.strip() != '').sum()
    _d4_with_url = (out['image_url'].astype(str).str.strip() != '').sum()
    log_issue("WARNING", "Deliverable",
               f"D4 pothole image accounting: {_total_d4_rows} pothole record(s) total -- "
               f"{_d4_with_image} already have an image_name in the source sheet, "
               f"{_total_d4_rows - _d4_with_image} do not; {_d4_with_url} got a clickable "
               f"image_url after locating the file in D3 and uploading it.")
    return out


_SEVERITY_RANK = {'low': 1, 'moderate': 2, 'high': 3, 'severe': 4, 'critical': 5}


def _worst_severity(values) -> str:
    """Pick the worst-case Severity label present (Low < Moderate < High <
    Severe/Critical) -- unrecognised labels are kept too (returned as-is if
    nothing recognised ranks higher), never silently dropped."""
    present = [str(v).strip() for v in values if pd.notna(v) and str(v).strip()]
    if not present:
        return ""
    ranked = [(p, _SEVERITY_RANK.get(p.strip().lower().rstrip('.'), 0)) for p in present]
    ranked.sort(key=lambda x: x[1], reverse=True)
    return ranked[0][0]


def build_potholes_stage_table(geometry_df: pd.DataFrame, pothole_points: pd.DataFrame,
                                project_label: str = "", lane: str = "") -> pd.DataFrame:
    """
    Build a standalone 'Potholes' stage table on the SAME 10m grid as
    GPS_Geometry/Cracking/Roughness_IRI/Rutting/Raveling -- one row per
    10m segment. Written as its own bronze parquet under table_key
    "Potholes_Segments" (see the driver loop) so it never gets force-
    matched against the unrelated raw-per-defect "Potholes" schema entry.

    pothole_points columns expected: Lane, Chainage, Severity, Area_Sq_m,
    Max_Depth_mm, Average_Depth_mm, image_name, image_path, image_lat,
    image_lon, Source_Tag -- exactly what load_d4_pothole_points() returns,
    whether the points came from D4 or the D2+D3 OCR fallback.

    NH_Number/Section_Code come from geometry_df itself (GPS_Geometry
    already carries these per row, built in build_geometry_10m -- Cell
    76), NOT from pothole_points -- D2's own Potholes sheet has no
    Section_Code column at all in this project's schema, so pulling it
    from the grid instead means it's always populated whenever GPS_Geometry
    itself has it, regardless of what the pothole source table carries.

    Per-segment aggregation when more than one pothole point falls in the
    same 10m segment:
      Pothole_Count    -> count of points in the segment
      Severity         -> worst-case label present (Low < Moderate < High
                          < Severe/Critical)
      Area_Sq_m        -> SUM (total distressed area in that segment)
      Max_Depth_mm     -> MAX (worst single pothole depth in the segment)
      Average_Depth_mm -> MEAN (average across the segment's own potholes)
      image_name       -> every distinct image name, comma-joined

    FINAL COLUMNS, in this exact order (per your request):
      Start_Chainage, End_Chainage, Severity, Area_Sq_m, Max_Depth_mm,
      Average_Depth_mm, image_name, Lane, NH_Number, Section_Code,
      Pothole_Count
    No Width column -- confirmed against your real D4/D2 bronze schema,
    there is no such field anywhere; a pothole record here is Area +
    Max/Average Depth + Severity only.

    project_label scopes matching to the same package (Fly Over / MCW /
    etc.) via _best_matching_source().
    """
    _FINAL_COLS = ['Start_Chainage', 'End_Chainage', 'Severity', 'Area_Sq_m', 'Max_Depth_mm',
                   'Average_Depth_mm', 'Width_mm', 'image_name', 'image_url', 'Lane', 'NH_Number',
                   'Section_Code', 'Latitude', 'Longitude', 'Pothole_Count']
 
    # GUARD (FIX): geometry_df comes back completely empty (zero columns,
    # not just zero rows) whenever this lane's GPS sheet had no usable
    # data rows (or chainage range too short) -- see build_geometry_10m()/
    # build_lane_stage_tables(), which both deliberately return a bare
    # pd.DataFrame() in that case. The driver loop calls this function
    # BEFORE it ever checks Unified_Segments.empty, so without this guard
    # geometry_df[['Lane','Start_Chainage','End_Chainage']] below raises
    # KeyError the moment any lane's GPS sheet is empty. Bail out early
    # with a correctly-shaped empty table instead.
    if geometry_df is None or geometry_df.empty or not \
            {'Lane', 'Start_Chainage', 'End_Chainage'}.issubset(geometry_df.columns):
        return pd.DataFrame(columns=_FINAL_COLS)
 
    out = geometry_df[['Lane', 'Start_Chainage', 'End_Chainage']].copy()
    out['NH_Number']    = geometry_df['NH_Number'] if 'NH_Number' in geometry_df.columns else ''
    out['Section_Code'] = geometry_df['Section_Code'] if 'Section_Code' in geometry_df.columns else ''
    # NEW: Latitude/Longitude for each pothole segment, taken directly
    # from GPS_Geometry's own Start_Latitude/Start_Longitude for that
    # exact segment -- GPS_Geometry already has Chainage-to-Lat/Lon for
    # every 10m segment, so this is a direct lookup (the segment this
    # pothole was already matched into via chainage containment), not a
    # separate interpolation step.
    out['Latitude']  = geometry_df['Start_Latitude']  if 'Start_Latitude'  in geometry_df.columns else pd.NA
    out['Longitude'] = geometry_df['Start_Longitude'] if 'Start_Longitude' in geometry_df.columns else pd.NA
    out['Pothole_Count']    = 0
    out['Severity']         = ""
    out['Area_Sq_m']        = 0.0
    out['Max_Depth_mm']     = pd.NA
    out['Average_Depth_mm'] = pd.NA
    # Width_mm: structural column only -- included because it was
    # explicitly requested as one of the required Potholes columns, but
    # NEITHER D4's own 21-column schema NOR D2's raw sheet ("NH No.,
    # Chainage, Lane No., Area (in Sq m), Max Depth (in mm), Average
    # Depth (in mm), Severity") has ever had a width field for potholes
    # -- confirmed against both real schemas multiple times. Always
    # null; kept here so the column exists in the output shape rather
    # than being silently absent, in case a downstream consumer expects
    # a fixed 5-field structure (Chainage/Severity/Area/Depth/Width) even
    # when this particular source data has nothing to put in it.
    out['Width_mm']         = pd.NA
    out['image_name']       = ""
    out['image_url']        = ""

    _FINAL_COLS = ['Start_Chainage', 'End_Chainage', 'Severity', 'Area_Sq_m', 'Max_Depth_mm',
                   'Average_Depth_mm', 'Width_mm', 'image_name', 'image_url', 'Lane', 'NH_Number',
                   'Section_Code', 'Latitude', 'Longitude', 'Pothole_Count']

    if pothole_points.empty:
        return out[_FINAL_COLS]

    candidates = set(pothole_points.get('Source_Tag', pd.Series(dtype=str)).unique()) - {''}
    match = _best_matching_source(project_label, candidates) if candidates else ''
    pts = pothole_points
    if match:
        pts = pts[(pts['Source_Tag'] == match) | (pts['Source_Tag'] == '')]
    if lane:
        pts = pts[pts['Lane'] == lane]
    if pts.empty:
        return out[_FINAL_COLS]

    # BUG FIXED: min/max of (Start_Chainage, End_Chainage) instead of raw
    # start/end -- a "Decreasing" direction lane (verified real case: R1,
    # rows like Start_Chainage=18500 -> End_Chainage=18490) has Start >
    # End, so the old "chainages >= s & chainages < e" check was
    # structurally IMPOSSIBLE to satisfy (s > e means no chainage can ever
    # be both >= s and < e) -- R1 always showed Pothole_Count = 0
    # regardless of real data, purely because of this. Same min/max
    # normalisation _asof_attach() (Cell 90) and build_cracking() already
    # use for exactly this reason.
    starts    = np.minimum(out['Start_Chainage'].to_numpy(), out['End_Chainage'].to_numpy())
    ends      = np.maximum(out['Start_Chainage'].to_numpy(), out['End_Chainage'].to_numpy())
    # BUG FIXED: .to_numpy() without na_value=, on a column that's a
    # pandas NULLABLE dtype (Float64/Int64/string -- common here since
    # these columns pass through write_bronze_table()'s schema-casting,
    # which uses nullable dtypes), can leave pd.NA sentinels in the
    # resulting array instead of a genuine np.nan. np.isnan() -- used
    # further below to check for missing values -- does not support
    # arrays containing pd.NA at all, and raises TypeError the moment any
    # row actually has one (a plain dtype='float64' cast is NOT enough on
    # its own -- casting an object array that still holds real pd.NA
    # values raises a DIFFERENT TypeError: "float() argument must be a
    # string or a real number, not 'NAType'"). na_value=np.nan explicitly
    # converts ANY pandas NA sentinel to np.nan during the to_numpy()
    # call itself, verified against nullable Float64 and string dtypes
    # both -- so every np.isnan() call downstream works correctly.
    chainages = pd.to_numeric(pts['Chainage'], errors='coerce').to_numpy(dtype='float64', na_value=np.nan)
    severities = pts['Severity'].to_numpy()
    areas      = pd.to_numeric(pts['Area_Sq_m'], errors='coerce').to_numpy(dtype='float64', na_value=np.nan)
    maxdepths  = pd.to_numeric(pts['Max_Depth_mm'], errors='coerce').to_numpy(dtype='float64', na_value=np.nan)
    avgdepths  = pd.to_numeric(pts['Average_Depth_mm'], errors='coerce').to_numpy(dtype='float64', na_value=np.nan)
    names      = pts['image_name'].to_numpy()
    urls       = pts['image_url'].to_numpy() if 'image_url' in pts.columns else np.full(len(pts), '', dtype=object)

    count_col = out.columns.get_loc('Pothole_Count')
    sev_col   = out.columns.get_loc('Severity')
    area_col  = out.columns.get_loc('Area_Sq_m')
    maxd_col  = out.columns.get_loc('Max_Depth_mm')
    avgd_col  = out.columns.get_loc('Average_Depth_mm')
    name_col  = out.columns.get_loc('image_name')
    url_col   = out.columns.get_loc('image_url')

    # PERFORMANCE (no logic change -- verified byte-identical output on
    # real-like data across 4 test scenarios including decreasing-
    # direction lanes, NaN chainages/fields, and empty inputs):
    # the loop used to run once per GRID ROW (~1850 per lane), checking
    # each row against all (few) pothole points -- most iterations do
    # nothing. Inverted to loop once per POINT (typically <10) and look
    # up its matching row via vectorised comparison instead -- same exact
    # containment check, same exact aggregation functions
    # (_worst_severity/nansum/nanmax/nanmean/sorted-join), just restructured
    # so the expensive Python-level loop runs ~10 times instead of ~1850.
    from collections import defaultdict
    row_points = defaultdict(list)
    for pt_idx in range(len(chainages)):
        ch = chainages[pt_idx]
        if np.isnan(ch):
            continue
        in_row = (starts <= ch) & (ch < ends)
        for r in np.where(in_row)[0]:
            row_points[r].append(pt_idx)

    for idx, _pt_list in row_points.items():
        pt_indices = np.array(_pt_list)
        out.iat[idx, count_col] = len(pt_indices)
        out.iat[idx, sev_col]   = _worst_severity(severities[pt_indices])
        _seg_areas = areas[pt_indices]
        out.iat[idx, area_col]  = float(np.nansum(_seg_areas)) if not np.all(np.isnan(_seg_areas)) else 0.0
        _seg_maxd = maxdepths[pt_indices]
        if not np.all(np.isnan(_seg_maxd)):
            out.iat[idx, maxd_col] = float(np.nanmax(_seg_maxd))
        _seg_avgd = avgdepths[pt_indices]
        if not np.all(np.isnan(_seg_avgd)):
            out.iat[idx, avgd_col] = float(np.nanmean(_seg_avgd))
        seg_names = sorted({str(n) for n in names[pt_indices] if pd.notna(n) and str(n)})
        out.iat[idx, name_col] = ", ".join(seg_names)
        seg_urls = sorted({str(u) for u in urls[pt_indices] if pd.notna(u) and str(u)})
        out.iat[idx, url_col] = ", ".join(seg_urls)

    return out[_FINAL_COLS]


def attach_pothole_info(unified_df: pd.DataFrame, potholes_stage_df: pd.DataFrame) -> pd.DataFrame:
    """
    Attach the ALREADY-BUILT Potholes stage table onto Unified_Segments by
    Lane + Start_Chainage + End_Chainage (the same 10m grid both tables
    share) -- a plain merge, since all the actual point-matching and
    aggregation work already happened once in build_potholes_stage_table().
    Keeps Unified_Segments and the standalone Potholes parquet always in
    sync, since they now come from the exact same computation.

    Unified_Segments keeps Pothole_Count + image_name + image_url --
    image_url added so the frontend has a real clickable link straight
    from Unified_Segments, not just from the standalone Potholes
    parquet. Severity/Area/Depth still live on the standalone Potholes
    parquet only, per your column list; ask if you want those on
    Unified_Segments too.
    """
    out = unified_df.copy()
    if potholes_stage_df.empty or 'Lane' not in out.columns:
        out['Pothole_Count'] = 0
        out['image_name'] = ""
        out['image_url'] = ""
        return out

    merge_cols = ['Lane', 'Start_Chainage', 'End_Chainage', 'Pothole_Count', 'image_name', 'image_url']
    out = out.merge(potholes_stage_df[merge_cols],
                     on=['Lane', 'Start_Chainage', 'End_Chainage'], how='left')
    out['Pothole_Count'] = out['Pothole_Count'].fillna(0)
    out['image_name'] = out['image_name'].fillna('')
    out['image_url'] = out['image_url'].fillna('')
    return out


# COMMAND ----------

# ─────────────────────────────────────────────────────────────────────────────
# RAW passthrough for the OTHER backup sheets
# ─────────────────────────────────────────────────────────────────────────────
# The pipeline only has purpose-built logic for 5 source sheets (GPS, Cracks,
# IRI/Roughness, Rut, Raveling) -> the 6 BACKUP_STAGE_TABLES. A lane workbook
# typically has ~13 sheets; the rest (Geometry, Bleeding, Patch, Pothole,
# Disintegration, Texture, Visual Condition, Survey Header, ...) were never
# bronzed at all.
#
# These are now written AS-IS: raw sheet -> parquet, in the same
# Project/Pavement/Variant/Lane partition as the stage tables. Deliberately:
#   * NO schema enforcement  (schema_df=None -> auto dtype-inference only)
#   * NO log_schema_mapping_result() / CRITICAL checks
# because there is no schema or required-column contract defined for them.
# Table name is "Raw_<SheetName>" so it can never collide with a stage table.

# Source sheets the stage builders already consume -- excluded from the raw pass.
_HANDLED_SOURCE_SHEETS = ("GPS", "Cracks", "IRI", "Rut", "Raveling")

def _handled_sheet_names(file_path: str) -> set:
    """Actual sheet names in THIS workbook that the stage builders already use
    (resolved via _resolve_sheet so aliases / numeric prefixes are covered)."""
    out = set()
    for req in _HANDLED_SOURCE_SHEETS:
        try:
            out.add(_norm_sheet(_resolve_sheet(file_path, req)))
        except Exception:
            pass
    return out

def extract_raw_other_sheets(file_path: str) -> dict:
    """{sheet_name: DataFrame} for every sheet NOT already handled by a stage
    builder. Uses the same cached reader; a sheet that fails to read is skipped
    with a WARNING (never CRITICAL -- these are best-effort extras)."""
    out = {}
    try:
        wb = _get_wb(file_path)
        available = list(wb.sheet_names)
    except Exception as exc:
        log_issue("WARNING", "Backup",
                   f"Could not list sheets for raw passthrough: {concise_exc(exc)}",
                   file=file_path)
        return out

    handled = _handled_sheet_names(file_path)
    for sheet in available:
        if _norm_sheet(sheet) in handled:
            continue
        try:
            df = read_sheet_fast(file_path, sheet, header=0)
        except Exception as exc:
            log_issue("WARNING", "Backup",
                       f"Raw sheet '{sheet}' could not be read: {concise_exc(exc)}",
                       file=file_path, table=f"Raw_{sheet}")
            continue
        if df is None or df.empty:
            continue
        # Every column forced to string: these sheets have no schema and no
        # declared types, so dtype-inference would guess differently from one
        # workbook to the next (and choke on mixed / Excel-error cells like
        # #DIV/0!). Keeping them all as text means the parquet is a faithful,
        # predictable copy of what the sheet actually contains.
        df = df.astype(str)
        df = df.where(df.notna(), "")
        out[sheet] = df
    return out


# COMMAND ----------

# Stage tables written per lane workbook, in build order. Unified_Segments
# is the final merged table (as before); the other five are the
# intermediate build stages (GPS_Geometry, Cracking, Roughness_IRI,
# Rutting, Raveling, Potholes) that used to be computed and discarded —
# now each one is bronzed on its own too, in the same Pavement/Variant/Lane
# partition as Unified_Segments, so they're inspectable independently and
# Silver can read whichever stage it needs instead of only the fully-merged
# table.
#
# "Potholes" is NEW here -- previously pothole info only ever existed as
# extra columns bolted onto Unified_Segments (Pothole_Count/image_name),
# never as its own standalone parquet. It's built and written per-lane
# exactly like the other four stages, whether its points came from D4
# (when present) or the D2+D3 image-OCR fallback (when D4 is missing for
# this project) -- attach_pothole_info() further below then just merges
# THIS SAME table onto Unified_Segments instead of recomputing anything,
# so the two can never drift out of sync with each other.

BACKUP_STAGE_TABLES = ("GPS_Geometry", "Cracking", "Roughness_IRI", "Rutting", "Raveling", "Potholes")
backup_summary_rows = []
backup_skipped = []

# Read/write tally for the closing report: one entry per lane workbook.
backup_read_stats = []   # {file, sheets_total, sheets_raw, parquet_written}

# Loaded ONCE for the whole project (not per lane -- the pothole source
# data doesn't change across lanes, only the Lane column inside it does),
# then matched per-lane inside build_potholes_stage_table() below.
#
# fallback_project_dir/fallback_extraction_results let load_d4_pothole_
# points() fall back to D2+D3 OCR when this project has no D4 Potholes
# bronze file at all -- reusing the SAME extract_folder() results already
# gathered in Cell 51 rather than re-parsing every Excel file again.
# BUG FIXED: this used to point at deliverable_projects[0] -- which,
# since DELIVERABLE_ROOT already points directly at ONE project's own
# deliverable/ folder, is just "Deliverable - 1" (the FIRST of its three
# subfolders), NOT the parent that actually contains Deliverable - 1/2/3
# together. That meant the fallback only ever walked/searched inside
# "Deliverable - 1" -- which has RIS/LRMS/Roughness/RutDepth/
# VisualCondition, but never D2's Potholes sheet OR D3's image folder at
# all. Every single lane came back with Pothole_Count = 0 everywhere
# because the code was never even looking in the right place.
#
# Fix: search root is DELIVERABLE_ROOT itself (the true parent of all
# three Deliverable-N subfolders), and extraction results are MERGED
# across every Deliverable-N folder's own results (not just the first),
# so both D2's Potholes sheet and D3's image folder are actually reachable.
_fallback_project_dir = str(DELIVERABLE_ROOT)
# BUG FIXED: this used to merge D1+D2+D3's extraction results together,
# which meant collect_d2_pothole_tables() could technically match a
# pothole-shaped sheet found in D1 or D3's own files too -- not scoped to
# D2 only, even though the intent was always "look in D2's Comparision-
# as-per-CA file specifically." Now scoped to ONLY the Deliverable folder
# whose own digit is '2', found via normalize_deliverable() (handles
# naming variants like "Deliverable-2" / "Deliverable - 2" / etc.) rather
# than hardcoding the exact string. D3's image folder is still searched
# via _fallback_project_dir=DELIVERABLE_ROOT (a plain filesystem walk,
# unrelated to extraction results) -- unaffected by this change.
# BUG FIXED (compatibility with the Project-folder-wrapping fix above):
# this used to scope by normalize_deliverable(_proj_name) -- checking
# only the PACKAGE's own name (e.g. "DME Pkg-1"). That name contains no
# "Deliverable" word and no digit at all when a project uses the
# package-wrapped layout (.../DME Pkg-1/Deliverable - 2 - .../...), so
# this scoping silently matched NOTHING for any such project -- D2/D3
# extraction results would come back empty even though the files were
# genuinely there, just one folder level deeper than expected.
#
# Now scoped by each FILE's own key instead (extract_folder()'s keys are
# "<rel_folder>/<filename>", relative to that package's own root -- so
# "Deliverable - 2 - Comparision as per CA/....xlsx" still contains
# "Deliverable - 2" regardless of which package folder wraps it).
# Falls back to the package/project name for the OLDER flat layout
# (where project_dir.name WAS "Deliverable - N" directly) -- handles
# both structures without needing to know which one a given project uses.
# BUG FIXED: checking the FULL file key (folder + filename) broke on
# real data -- a file like "Deliverable - 2 - Comparision as per CA/
# Comparision as per CA DVM Pkg-1.xlsx" contains TWO distinct digits
# once the filename is included ('2' from the folder, '1' from "Pkg-1"
# in the filename itself) -- normalize_deliverable() requires EXACTLY
# ONE distinct digit to resolve, so it silently returned "" and D2/D3
# extraction results came back completely empty even though the files
# were genuinely there (this is exactly why D2 stopped being found under
# Layout B once this fallback was added).
#
# Fixed to check only the file key's FIRST FOLDER SEGMENT instead (no
# filename noise) -- works for both Layout A (package-wrapped) and
# Layout B (flat) alike.
_fallback_extraction_results = {}
_d3_extraction_results = {}
for _proj_name, _proj_results in deliverable_extraction_results.items():
    for _file_key, _file_val in _proj_results.items():
        _first_segment = _file_key.replace('\\', '/').split('/')[0]
        _nd = normalize_deliverable(_first_segment) or normalize_deliverable(_proj_name)
        if not _nd:
            continue
        _digit = _nd.rsplit('-', 1)[-1]
        if _digit == '2':
            _fallback_extraction_results[_file_key] = _file_val
        elif _digit == '3':
            # D3's own extraction results, used to build the GPS-
            # reference lookup (build_pothole_image_gps_lookup) -- the
            # PRIMARY source for pothole photo Chainage/Lat/Lon, with
            # EXIF only as a backup for any photo the lookup doesn't cover.
            _d3_extraction_results[_file_key] = _file_val

d4_pothole_points = load_d4_pothole_points(
    fallback_project_dir=_fallback_project_dir,
    fallback_extraction_results=_fallback_extraction_results,
    d3_extraction_results=_d3_extraction_results,
)
_pothole_source = "none"
if not d4_pothole_points.empty:
    _pothole_source = "D4" if (d4_pothole_points['Source_Tag'] != '').any() else "D2+D3 fallback"
print(f"Pothole points available for Unified_Segments merge: {len(d4_pothole_points)} "
      f"(source: {_pothole_source})")


# Cracking image consolidation -- independent of D4/pothole logic above;
# runs once per project whenever D3 has a real Cracking Images folder.
_cracking_image_meta = consolidate_cracking_images(_fallback_project_dir, _d3_extraction_results)
print(f"Cracking images consolidated: {len(_cracking_image_meta)}")

for i, rec in enumerate(lane_files, 1):
    t0 = time.time()
    print(f"\n[{i}/{len(lane_files)}] {rec['project'][:45]} | {rec['lane']} | "
          f"{rec['variant']} | {rec['pavement']} | {rec['size_mb']}MB")

    _pq_written = 0          # parquet files written for THIS workbook
    _sheets_raw = 0          # raw (non-stage) sheets extracted from it

    try:
        stage_tables = build_lane_stage_tables(rec['file_path'], rec['lane'])
    except Exception as exc:
        short = concise_exc(exc)
        print(f"  ERROR extracting workbook: {short}")
        backup_skipped.append({**rec, 'reason': short})
        log_issue("CRITICAL", "Backup",
                   f"Workbook extraction failed (one or more of GPS/Roughness/Rutting/"
                   f"Raveling/Cracking is missing or unreadable): {short}",
                   file=rec['file_path'])
        clear_wb_cache()
        continue

    # Build the Potholes stage table for THIS lane -- same 10m grid as
    # GPS_Geometry, filtered/fuzzy-matched to this lane + package -- and
    # Retry Potholes image-matching for THIS lane, now that its own
    # GPS_Geometry exists -- d4_pothole_points was built upfront (before
    # any lane's GPS data existed), so it could only use images already
    # resolved via a GPS-reference-sheet at that point. Any image that
    # only had EXIF Lat/Lon (Chainage still None) gets its Chainage
    # resolved NOW against this lane's own GPS backup data, then
    # D2 rows of this lane that still have no matched image get a
    # fresh attempt with those newly-resolved images.
    if not _RAW_POTHOLE_IMAGE_META.empty and not _RAW_D2_POTHOLE_TABLE.empty:
        _lane_upper = rec['lane'].upper()
        _lane_d2 = _RAW_D2_POTHOLE_TABLE[_RAW_D2_POTHOLE_TABLE['Lane'] == _lane_upper]
        if not _lane_d2.empty:
            # Resolved cumulatively -- rows already resolved (this lane
            # or an earlier one) are left untouched each time.
            _RAW_POTHOLE_IMAGE_META = resolve_missing_chainages_via_exif(
                _RAW_POTHOLE_IMAGE_META, stage_tables['GPS_Geometry'], lane=_lane_upper)
            _retry = attach_d3_images_to_d2_potholes(_lane_d2, _RAW_POTHOLE_IMAGE_META)
            # BUG FIXED: attach_d3_images_to_d2_potholes() can now return
            # rows whose Chainage never existed in d4_pothole_points at
            # all -- images whose own GPS-resolved chainage had no D2
            # record nearby (see that function's own docstring: matched
            # means the image's location resolved via GPS, NOT that it
            # lined up with an existing D2 defect). The old code here
            # only ever UPDATED an existing d4_pothole_points row via
            # _mask -- for these new-chainage rows _mask always matched
            # ZERO rows, so the image was silently dropped and never
            # reached Unified_Segments/Potholes.parquet at all, even
            # though the per-lane log correctly reported it as matched.
            # Any _retry row that doesn't correspond to an existing
            # d4_pothole_points row is now APPENDED as a new point
            # instead of being discarded.
            _new_points = []
            for _, _rrow in _retry.iterrows():
                if _rrow['image_path']:
                    _mask = ((d4_pothole_points['Lane'] == _rrow['Lane']) &
                             (d4_pothole_points['Chainage'] == _rrow['Chainage']) &
                             (d4_pothole_points['image_path'] == ''))
                    if _mask.any():
                        d4_pothole_points.loc[_mask, 'image_path'] = _rrow['image_path']
                        d4_pothole_points.loc[_mask, 'image_lat']  = _rrow['image_lat']
                        d4_pothole_points.loc[_mask, 'image_lon']  = _rrow['image_lon']
                        d4_pothole_points.loc[_mask, 'image_url']  = _rrow['image_url']
                    else:
                        _new_point = {col: pd.NA for col in d4_pothole_points.columns}
                        _new_point['Lane']       = _rrow['Lane']
                        _new_point['Chainage']   = _rrow['Chainage']
                        _new_point['image_path'] = _rrow['image_path']
                        _new_point['image_lat']  = _rrow['image_lat']
                        _new_point['image_lon']  = _rrow['image_lon']
                        _new_point['image_url']  = _rrow['image_url']
                        if 'Source_Tag' in d4_pothole_points.columns:
                            _new_point['Source_Tag'] = ''
                        _new_points.append(_new_point)
            if _new_points:
                d4_pothole_points = pd.concat(
                    [d4_pothole_points, pd.DataFrame(_new_points)], ignore_index=True)

    # slot it into stage_tables so the write loop below (over
    # BACKUP_STAGE_TABLES) picks it up and bronzes it exactly like the
    # other four stages, with no special-casing needed there.
    stage_tables['Potholes'] = build_potholes_stage_table(
        stage_tables['GPS_Geometry'], d4_pothole_points,
        project_label=rec['project'], lane=rec['lane'].upper(),
    )

    # Attach Cracking images onto THIS lane's own Cracking segments --
    # same chainage-containment approach as Potholes, using the SAME
    # _cracking_image_meta computed once per project above. Adds
    # cracking_image_name/cracking_image_url onto stage_tables['Cracking']
    # so it flows into Cracking.parquet via the normal write loop below,
    # with no special-casing needed there.
    if not stage_tables['Cracking'].empty:
        # DEFENSIVE: wrapped in try/except -- a bug in this image-matching
        # step must never crash the whole lane (and every lane after it)
        # when the rest of the pipeline (Cracking/Roughness/Rutting/
        # Raveling measurements) is working fine. On failure, this lane's
        # Cracking table just keeps its measurement columns with no image
        # columns attached, and the full traceback is written to the
        # validation log/csv so it can be diagnosed precisely instead of
        # stopping the entire run.
        try:
            # Resolve any EXIF-only images (Chainage still None) for THIS
            # lane, now that its own GPS_Geometry exists -- per Prathap's
            # process (map Lat/Lon -> nearest GPS backup point -> chainage).
            # _cracking_image_meta is shared/mutated across the whole lane
            # loop -- rows already resolved (or belonging to a different
            # lane) are left untouched each time.
            if not _cracking_image_meta.empty:
                _cracking_image_meta = resolve_missing_chainages_via_exif(
                    _cracking_image_meta, stage_tables['GPS_Geometry'], lane=rec['lane'].upper())
            stage_tables['Cracking'] = attach_images_to_cracking_stage_table(
                stage_tables['Cracking'], _cracking_image_meta, lane=rec['lane'].upper(),
            )
        except Exception as _exc:
            import traceback as _tb
            log_issue("WARNING", "Backup",
                       f"Cracking image attachment failed for this lane, skipped "
                       f"(Cracking measurements are unaffected): {concise_exc(_exc)}\n"
                       f"{_tb.format_exc()}",
                       file=rec['file_path'], table="Cracking")

    unified_df = stage_tables['Unified_Segments']
    if not unified_df.empty:
        unified_df = attach_pothole_info(unified_df, stage_tables['Potholes'])
        # Same merge idea as attach_pothole_info, for Cracking's own
        # images -- Lane + Start_Chainage + End_Chainage keys, same grid
        # both tables already share.
        if not stage_tables['Cracking'].empty and 'Lane' in unified_df.columns:
            _crack_merge_cols = ['Lane', 'Start_Chainage', 'End_Chainage',
                                  'cracking_image_name', 'cracking_image_url']
            unified_df = unified_df.merge(
                stage_tables['Cracking'][_crack_merge_cols],
                on=['Lane', 'Start_Chainage', 'End_Chainage'], how='left')
            unified_df['cracking_image_name'] = unified_df['cracking_image_name'].fillna('')
            unified_df['cracking_image_url']  = unified_df['cracking_image_url'].fillna('')
        else:
            unified_df['cracking_image_name'] = ""
            unified_df['cracking_image_url']  = ""
    if unified_df.empty:
        # Some files' GPS sheet spans too short a chainage range to form
        # even one 10m segment — not an error, just no usable data here.
        print(f"  SKIPPED: no segments (GPS chainage range too short), {round(time.time() - t0, 1)}s")
        backup_skipped.append({**rec, 'reason': 'empty geometry (chainage range too short)'})
        log_issue("WARNING", "Backup",
                   "GPS chainage range too short to form a 10m segment; Unified_Segments skipped.",
                   file=rec['file_path'])
        clear_wb_cache()
        continue

    # BUG FIXED: "Project" was unconditionally removed here, so EVERY
    # run showed just "Pavement=.../Lane=..." with no package identity at
    # all -- even for a genuinely multi-package UPC, where Backup_Path.xlsx
    # lists real package names (e.g. "New_Aspahlt Analaysis",
    # "New_Cement concrete analysis") rather than the single generic
    # "ROOT" placeholder. That real package identity was getting silently
    # dropped, showing only the raw Pavement type (e.g. "Rigid") or
    # "Unknown" instead.
    #
    # Same "Project only when a real package layer exists" principle as
    # the Deliverable stage: BACKUP_HAS_PACKAGE_LAYER (Cell 58) is True
    # only when at least one lane file's own project isn't the generic
    # "ROOT" placeholder.
    extra_partitions = ({"Project": rec['project'], "Pavement": rec['pavement'],
                          "Variant": rec['variant'], "Lane": rec['lane']}
                         if BACKUP_HAS_PACKAGE_LAYER else
                         {"Pavement": rec['pavement'], "Variant": rec['variant'], "Lane": rec['lane']})

    # Write the 5 intermediate stages first (each into its own
    # respective table under the same Project/Pavement/Variant/Lane
    # partition), then the final Unified_Segments — one write per stage,
    # each producing its own row in backup_summary_rows / pipeline_summary.
    stage_statuses = []
    for stage_name in BACKUP_STAGE_TABLES:
        stage_df = stage_tables[stage_name]
        if stage_df is None or stage_df.empty:
            # CRITICAL only for the 5 required sheets (GPS/Cracking/
            # Roughness/Rutting/Raveling) -- "Potholes" is no longer in
            # REQUIRED_BACKUP_STAGES, so an empty Potholes table here is
            # WARNING instead, per instruction.
            _sev = "CRITICAL" if stage_name in REQUIRED_BACKUP_STAGES else "WARNING"
            log_issue(_sev, "Backup",
                       f"'{stage_name}' produced no data for this lane workbook "
                       f"(sheet missing, unreadable, or empty).",
                       file=rec['file_path'], table=stage_name)
            continue
        # "Potholes" gets its OWN distinct schema key, never the raw
        # stage_name -- your schema file's "Potholes" entry describes
        # D4's raw PER-DEFECT record (nh_no/chainage/carriageway/
        # defect_id/... 21 columns), but this stage table is a completely
        # different shape: a 10m-SEGMENT grid (Lane/Start_Chainage/
        # End_Chainage/Pothole_Count/Severity/Area_Sq_m/...). Matching it
        # against that raw-record schema produced 20 false CRITICALs per
        # lane (16 "missing from source" + 4 "not in schema") purely from
        # the shape mismatch -- nothing was actually wrong with the data.
        # "Potholes_Segments" has no entry in schema_df at all, so it
        # falls through to auto dtype-inference instead (same as any
        # other table with no schema row), same as intended.
        _table_key = "Potholes_Segments" if stage_name == "Potholes" else stage_name
        stage_rec = write_bronze_table(
            stage_df, OUTPUT_ROOT, source="NSV_Backup", project=rec['project'],
            extra_partitions=extra_partitions,
            table_name=stage_name,
            table_key=_table_key,
            schema_df=schema_df,
            run_date=RUN_DATE, source_file=rec['file_path'],
            zone=ZONE, upc=UPC, phase=PHASE, cycle=CYCLE,
        )
        backup_summary_rows.append(stage_rec)
        _pq_written += 1
        stage_statuses.append(f"{stage_name}={stage_rec['status']}({stage_df.shape[0]})")
        # CHANGED: require_schema=True -- full schema now enforced on these
        # 5 stage tables too, same as Unified_Segments (was False before).
        # Potholes_Segments has NO schema entry by design (it's a new
        # 10m-segment aggregate shape, not the raw per-defect Potholes
        # schema) -- require_schema=False for it specifically, so the
        # "no matching Table_Name" case correctly falls through to
        # WARNING ("expected -- no Schema.xlsx entry by design") instead
        # of CRITICAL. Every other stage keeps require_schema=True
        # unchanged.
        log_schema_mapping_result(stage_rec, stage="Backup",
                                   require_data=stage_name in REQUIRED_BACKUP_STAGES,
                                   require_schema=(stage_name != "Potholes"),
                                   lenient_columns=BACKUP_LENIENT_COLUMNS)

    # ── RAW passthrough: every OTHER sheet in this workbook ──────────────
    # No schema, no CRITICAL checks -- straight sheet -> parquet, in the
    # same partition as the stage tables above.
    # Source workbook stem goes into the table name so two different lane
    # workbooks that both contain e.g. a "Geometry" sheet land in distinct
    # parquet files instead of colliding into "... (2).parquet". Final name:
    #   Z<zone>_P<phase>_C<cycle>_<UPC>_<ExcelFileName>_Raw_<SheetName>.parquet
    _excel_stem = slugify(Path(rec['file_path']).stem)
    for _raw_sheet, _raw_df in extract_raw_other_sheets(rec['file_path']).items():
        _raw_name = f"{_excel_stem}_Raw_{slugify(_raw_sheet)}"
        _raw_rec = write_bronze_table(
            _raw_df, OUTPUT_ROOT, source="NSV_Backup", project=rec['project'],
            extra_partitions=extra_partitions,
            table_name=_raw_name,
            table_key=_raw_name,
            schema_df=None,                     # <- no schema enforcement
            run_date=RUN_DATE, source_file=rec['file_path'],
            zone=ZONE, upc=UPC, phase=PHASE, cycle=CYCLE,
        )
        backup_summary_rows.append(_raw_rec)
        _pq_written += 1
        _sheets_raw += 1
        stage_statuses.append(f"{_raw_name}={_raw_rec['status']}({_raw_df.shape[0]})")
        # deliberately NO log_schema_mapping_result() for raw sheets

    summary_rec = write_bronze_table(
        unified_df, OUTPUT_ROOT, source="NSV_Backup", project=rec['project'],
        extra_partitions=extra_partitions,
        table_name="Unified_Segments",
        table_key="Unified_Segments",
        schema_df=schema_df,
        run_date=RUN_DATE, source_file=rec['file_path'],
        zone=ZONE, upc=UPC, phase=PHASE, cycle=CYCLE,
    )
    backup_summary_rows.append(summary_rec)
    _pq_written += 1
    try:
        _sheets_total = len(_get_wb(rec['file_path']).sheet_names)
    except Exception:
        _sheets_total = 0
    backup_read_stats.append({"file": rec['file_path'], "lane": rec['lane'],
                               "sheets_total": _sheets_total,
                               "sheets_raw": _sheets_raw,
                               "parquet_written": _pq_written})
    # No log_schema_mapping_result() call for Unified_Segments -- per your
    # request, it gets NO CRITICAL/WARNING schema-mapping checks at all
    # (unlike every other Backup/Deliverable table). It's still written to
    # bronze exactly as before (write_bronze_table() call above is
    # untouched) -- this only removes the validation LOGGING for it, not
    # the table itself, and not casting/dtype handling (schema_df is still
    # passed to write_bronze_table so columns still get cast against
    # Unified_Segments' schema entry when one matches; this just stops any
    # mismatch from being logged as an issue).

    clear_wb_cache()  # free the cached workbook before the next (large) file
    status = summary_rec['status']
    print(f"  {status}: {unified_df.shape[0]} segment(s) | " + ", ".join(stage_statuses)
          + f" | {round(time.time() - t0, 1)}s")

# ── Read/write report: workbooks read, sheets in each, parquet written ──
print(f"\n{'='*70}")
print("BACKUP READ / WRITE REPORT")
print(f"{'='*70}")
print(f"{'lane':<6} {'sheets':>7} {'raw':>5} {'parquet':>8}  file")
for _st in backup_read_stats:
    print(f"{_st['lane']:<6} {_st['sheets_total']:>7} {_st['sheets_raw']:>5} "
          f"{_st['parquet_written']:>8}  {os.path.basename(_st['file'])}")
print(f"{'-'*70}")
print(f"Excel workbooks read      : {len(backup_read_stats)}")
print(f"Sheets seen (all books)   : {sum(s['sheets_total'] for s in backup_read_stats)}")
print(f"  of which raw-extracted  : {sum(s['sheets_raw'] for s in backup_read_stats)}")
print(f"Parquet files written     : {sum(s['parquet_written'] for s in backup_read_stats)}")
if backup_skipped:
    print(f"Workbooks skipped (error) : {len(backup_skipped)}")

print(f"\nBackup bronze tables written: "
      f"{sum(1 for r in backup_summary_rows if r['status'] == 'SUCCESS')} / {len(backup_summary_rows)}")
print(f"Schema-matched   : {sum(1 for r in backup_summary_rows if r.get('schema_matched'))} / {len(backup_summary_rows)}")
if backup_skipped:
    print(f"Skipped ({len(backup_skipped)}):")
    for s in backup_skipped:
        print(f"  {s['project'][:45]} | {s['lane']} | {s['variant']} -> {s['reason']}")

# Left/Right lane-pairing completeness check, per package (rec['project']
# -- e.g. "Fly-Road Backups" and "MCW Backups" are checked independently,
# since one package's own lane layout says nothing about another's): a
# carriageway survey is normally done in matched L/R pairs, so a lone L1
# with no R1 (or L2/no R2, L3/no R3, ...) almost always means a lane
# workbook is missing from the package, not that the road genuinely has
# only one side. Based on lane_files (every lane workbook FOUND on disk
# for this UPC), not on build/write success -- file availability and
# processing success are separate concerns (a found-but-unbuildable file
# already gets its own WARNING/CRITICAL elsewhere).
from collections import defaultdict as _defaultdict
_lanes_by_project = _defaultdict(set)
for _rec in lane_files:
    _lanes_by_project[_rec['project']].add(_rec['lane'].upper())

_LR_RE = re.compile(r'^([LR])-?(\d+)$', re.IGNORECASE)  # optional '-' so 'R-1' pairs with 'L1' correctly, not just bare 'R1'
for _project, _lanes in _lanes_by_project.items():
    _sides_by_num = _defaultdict(set)
    for _lane in _lanes:
        _m = _LR_RE.match(_lane)
        if _m:
            _sides_by_num[_m.group(2)].add(_m.group(1))
    for _num, _found_sides in _sides_by_num.items():
        # Demoted to WARNING -- lane-pairing completeness isn't one of
        # the 6 required CRITICAL cases (it's not about a column feeding
        # Unified_Segments or a required sheet being missing).
        if _found_sides == {'L'}:
            log_issue("WARNING", "Backup",
                       f"L{_num} lane workbook found but its pair R{_num} is "
                       f"missing for this package.",
                       file=_project, table=f"R{_num}")
        elif _found_sides == {'R'}:
            log_issue("WARNING", "Backup",
                       f"R{_num} lane workbook found but its pair L{_num} is "
                       f"missing for this package.",
                       file=_project, table=f"L{_num}")

raise_if_critical("Backup Bronze (LCMS lane workbooks)")


# COMMAND ----------

# MAGIC %md
# MAGIC # 2.5 Dashcam Bronze
# MAGIC

# COMMAND ----------

def write_dashcam_bronze(dashcam_file: str, output_root: str, run_date: _dt.date, schema_df: pd.DataFrame) -> dict:
    """
    Ingest the dashcam defects workbook into bronze.

    Project is deliberately NOT set to one of the 4 known Zone-A projects —
    the file's embedded location code (in 'Defect image name', e.g.
    'ZA_PB_AMS_WGA_BOR_NH3') doesn't match any of their NH numbers or
    chainage ranges, and the data's own regularity (perfectly alternating
    Crack/Pothole, exactly +10m chainage per row, dates into 2028) strongly
    suggests this is a schema/format sample rather than a real survey pass
    for one of those roads. Tagging it under its own project keeps that
    honest instead of silently mis-joining it to a real project.
    """
    df = pd.read_excel(_long_path(dashcam_file), sheet_name=0)
    rec = write_bronze_table(
        df, output_root, source='Dashcam', project='Unmapped (ZA_PB_AMS_WGA_BOR_NH3)',
        extra_partitions={}, table_name='Dashcam_Defects', table_key='Dashcam_Defects',
        schema_df=schema_df, run_date=run_date, source_file=dashcam_file,
    )
    return rec

# COMMAND ----------

dashcam_summary_rows = []

if os.path.isfile(DASHCAM_FILE):
    rec = write_dashcam_bronze(DASHCAM_FILE, OUTPUT_ROOT, RUN_DATE, schema_df)
    dashcam_summary_rows.append(rec)
    print(f"Dashcam bronze written: status={rec['status']}, schema_matched={rec.get('schema_matched')}")
    print(f"  -> {rec['output_path']}")
else:
    print(f"[SKIP] Dashcam file not found: {DASHCAM_FILE}")

print(f"\nDashcam bronze tables written: "
      f"{sum(1 for r in dashcam_summary_rows if r['status'] == 'SUCCESS')} / {len(dashcam_summary_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 DAMS Bronze
# MAGIC

# COMMAND ----------

def write_dams_bronze(dams_file: str, output_root: str, run_date: _dt.date, schema_df: pd.DataFrame) -> dict:
    """
    Ingest the DAMS defects export (CSV) into bronze.

    Project is deliberately NOT set to one of the 4 known Zone-A projects —
    its 'upc' ('N/04009/02002/MH') doesn't match any of their NH numbers,
    and it shares the exact same synthetic chainage/lat-lon grid as the
    dashcam sample file (120250->130240 in +10m steps, same start/end
    coordinates), strongly suggesting both are a matched schema/format
    sample pair rather than a real survey pass. Tagged under its own
    project for the same reason as write_dashcam_bronze().
    """
    df = pd.read_csv(_long_path(dams_file))
    rec = write_bronze_table(
        df, output_root, source='DAMS', project='Unmapped (N/04009/02002/MH)',
        extra_partitions={}, table_name='DAMS_Defects', table_key='DAMS_Defects',
        schema_df=schema_df, run_date=run_date, source_file=dams_file,
    )
    return rec

# COMMAND ----------

dams_summary_rows = []

if os.path.isfile(DAMS_FILE):
    rec = write_dams_bronze(DAMS_FILE, OUTPUT_ROOT, RUN_DATE, schema_df)
    dams_summary_rows.append(rec)
    print(f"DAMS bronze written: status={rec['status']}, schema_matched={rec.get('schema_matched')}")
    print(f"  -> {rec['output_path']}")
else:
    print(f"[SKIP] DAMS file not found: {DAMS_FILE}")

print(f"\nDAMS bronze tables written: "
      f"{sum(1 for r in dams_summary_rows if r['status'] == 'SUCCESS')} / {len(dams_summary_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Summary & sanity checks

# COMMAND ----------

all_summary = pd.DataFrame(deliverable_summary_rows + backup_summary_rows + dashcam_summary_rows + dams_summary_rows)

summary_path = build_output_dir("_summary") / f"pipeline_summary_{RUN_DATE.isoformat()}.csv"
all_summary.to_csv(summary_path, index=False)

print(f"Summary written -> {summary_path}\n")
print(all_summary['status'].value_counts())
print()
print(f"Deliverable tables : {len(deliverable_summary_rows)}")
print(f"Backup tables      : {len(backup_summary_rows)}")
print(f"Dashcam tables     : {len(dashcam_summary_rows)}")
print(f"DAMS tables        : {len(dams_summary_rows)}")

# COMMAND ----------

# Show any errors up front so they're not missed in the row counts above.
errors = all_summary[all_summary['status'] == 'ERROR']
if not errors.empty:
    display(errors[['source', 'project', 'table_name', 'source_file', 'error']])
else:
    print("No errors.")

# COMMAND ----------

# Read back a handful of written parquet files as a sanity check.
success = all_summary[all_summary['status'] == 'SUCCESS']
sample = success.sample(min(5, len(success)), random_state=0) if len(success) else success

for _, row in sample.iterrows():
    df = pd.read_parquet(_long_path(row['output_path']))
    print(f"{row['output_path']}\n  -> {df.shape[0]} rows x {df.shape[1]} cols, dtypes: {row.get('dtype_counts', '')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Excel verification export
# MAGIC

# COMMAND ----------

_PROJECT_CODE_KEYWORDS = [
    ('yamunanagar', 'Yamunanagar'),
    ('ludhiana', 'Kharar_Ludhiana'),
    ('kurali(kn', 'Kharar_Kurali'),
    ('kiratpur', 'Kiratpur'),
]
def project_code(project_name: str) -> str:
    """Short, Excel-sheet-name-friendly code for a (long) Zone-A project name."""
    low = str(project_name).lower()
    for kw, code in _PROJECT_CODE_KEYWORDS:
        if kw in low:
            return code
    return re.sub(r'[^A-Za-z0-9]+', '_', str(project_name)).strip('_')[:20] or 'Project'

# COMMAND ----------

def parse_partitions(bronze_root: str, file_path: str) -> dict:
    """
    Split a bronze parquet path into its Hive-style partition key/values,
    plus the table name (file stem).

    e.g. Source=NSV_Backup/Project=X/Pavement=Y/Variant=Z/Lane=L1/
         year=2026/month=07/day=02/IRI_analysis.parquet
      -> {'Source': 'NSV_Backup', 'Project': 'X', 'Pavement': 'Y',
          'Variant': 'Z', 'Lane': 'L1', 'year': '2026', 'month': '07',
          'day': '02', 'table_name': 'IRI_analysis'}
    """
    def _strip_prefix(p):
        p = str(p)
        return p[4:] if p.startswith('\\\\?\\') else p

    rel = os.path.relpath(_strip_prefix(file_path), _strip_prefix(bronze_root))
    parts = Path(rel).parts
    out = {}
    for part in parts[:-1]:
        if '=' in part:
            k, v = part.split('=', 1)
            out[k] = v
    out['table_name'] = Path(parts[-1]).stem
    return out

# COMMAND ----------

def find_bronze_files(root: str, filename_pattern: str) -> list:
    """
    Recursively find files under root matching filename_pattern (fnmatch
    syntax, e.g. '*_analysis.parquet').

    Deliberately NOT glob.glob(..., recursive=True): the bronze output
    partition paths for the long-named Zone-A projects exceed Windows'
    260-char MAX_PATH once Project=/Pavement=/Variant=/Lane=/year=/month=/
    day= all stack up (~290 chars measured). Neither glob's recursive '**'
    NOR plain os.walk() can descend past that depth — os.walk() doesn't
    raise, it just silently yields nothing for the unreachable branch, so
    this failure mode is easy to miss. Prefixing the WALK ROOT (not just
    file-open calls) with \\\\?\\ fixes it: verified empirically — root
    itself was only 150 chars, plain os.walk found 0 files under it, the
    long-path-prefixed root found all 103.
    """
    matches = []
    for dirpath, _dirnames, filenames in os.walk(_long_path(root)):
        for fname in filenames:
            if fnmatch.fnmatch(fname, filename_pattern):
                matches.append(os.path.join(dirpath, fname))
    return matches

# COMMAND ----------

def build_bronze_export_sheets(bronze_root: str) -> dict:
    """
    Group every bronze parquet file by (project, table_name) -- unioning
    across Lane/Pavement/Variant/Deliverable within a project so e.g. all
    of one project's L1/L2/L3/R1/R2/R3 'Cracks_raw' files land on one
    sheet-equivalent.

    Returns {sheet_key: {'total_rows': int, 'columns': int}} -- METADATA
    ONLY, not the actual data. write_excel_workbook()'s _Index sheet never
    used anything but len(df) and len(df.columns) from these (the actual
    per-sheet data tabs are intentionally not written -- see its own
    docstring), so the previous version's pd.read_parquet() + pd.concat()
    over every parquet file across the WHOLE historical bronze lake
    (every past run_date, not just today's) was pure wasted work and the
    actual source of the ~20 min runtime. Row/column counts live in every
    Parquet file's footer metadata, so pyarrow can read them near-
    instantly without touching a single data page -- this is the same
    reason write_bronze_table() itself is fast despite writing full data.
    """
    files = []
    for source in ('Source=NSV', 'Source=NSV_Backup', 'Source=Dashcam', 'Source=DAMS'):
        source_dir = os.path.join(bronze_root, source)
        if os.path.isdir(_long_path(source_dir)):
            files += find_bronze_files(source_dir, '*.parquet')

    groups: dict = {}
    for f in files:
        tags = parse_partitions(bronze_root, f)
        key = (tags.get('Project'), tags['table_name'])
        groups.setdefault(key, []).append((f, tags))

    import pyarrow.parquet as pq

    sheets = {}
    # NOTE: tags.get('Project') is None for sources with no Project= partition
    # level (Deliverable/Dashcam/DAMS only have it for Backup) -- sorting the
    # raw tuples mixes None and str and raises TypeError. Sort on a
    # None-safe key instead (actual data/keys are untouched).
    for (project, table_name), items in sorted(
        groups.items(), key=lambda kv: (kv[0][0] or '', kv[0][1] or '')
    ):
        total_rows = 0
        max_base_cols = 0
        extra_tag_cols = set()
        for f, tags in items:
            try:
                meta = pq.ParquetFile(_long_path(f)).metadata
            except Exception as exc:
                log_issue("WARNING", "ExcelExport",
                           f"Could not read Parquet footer, skipped in export: {concise_exc(exc)}",
                           file=f, table=table_name)
                continue
            total_rows    += meta.num_rows
            max_base_cols  = max(max_base_cols, meta.num_columns)
            for tag_key in ('Deliverable', 'Pavement', 'Variant', 'Lane'):
                if tag_key in tags:
                    extra_tag_cols.add(tag_key)
        sheet_key = f"{project_code(project)}_{table_name}"
        sheets[sheet_key] = {
            'total_rows': total_rows,
            'columns': max_base_cols + len(extra_tag_cols),
        }

    return sheets

# COMMAND ----------

_SHEET_NAME_INVALID = re.compile(r'[\[\]:*?/\\]')


def _safe_sheet_name(name: str, used: set) -> str:
    """Sanitise + truncate to Excel's 31-char sheet-name limit, de-duplicated."""
    base = _SHEET_NAME_INVALID.sub('_', str(name))[:31]
    candidate = base
    i = 1
    while candidate in used:
        suffix = f"~{i}"
        candidate = base[:31 - len(suffix)] + suffix
        i += 1
    used.add(candidate)
    return candidate

# COMMAND ----------

def write_excel_workbook(sheets: dict, output_path: str, row_cap: int = None) -> str:
    """
    Write one '_Index' sheet summarising every bronze table (true row/
    column counts) to a single .xlsx workbook. Verification/spot-check
    view, not a substitute for the parquet lake.

    `sheets` is {sheet_name: {'total_rows': int, 'columns': int}} --
    lightweight metadata from build_bronze_export_sheets(), not actual
    DataFrames; the per-sheet data tabs were already being skipped here
    (only the _Index summary is written) so no DataFrame content was ever
    needed for this function to do its job.
    """
    os.makedirs(os.path.dirname(_long_path(output_path)) or '.', exist_ok=True)
    used_names = set()
    index_rows = []
    # xlsxwriter is meaningfully faster than openpyxl for bulk writes like
    # this (measured ~12 min for a 285-sheet/174MB workbook on openpyxl).
    try:
        import xlsxwriter  # noqa: F401
        engine = 'xlsxwriter'
    except ImportError:
        engine = 'openpyxl'
    with pd.ExcelWriter(_long_path(output_path), engine=engine) as writer:
        for name, meta in sheets.items():
            total_rows = meta['total_rows']
            n_cols     = meta['columns']
            shown_rows = min(total_rows, row_cap) if row_cap else total_rows
            shown_rows = min(shown_rows, 1_048_575)  # Excel's hard per-sheet row limit
            safe_name = _safe_sheet_name(name, used_names)
            index_rows.append({
                'Sheet': safe_name, 'Source_Name': name,
                'Total_Rows': total_rows, 'Rows_Shown': shown_rows,
                'Truncated': total_rows > shown_rows, 'Columns': n_cols,
            })
        pd.DataFrame(index_rows).to_excel(writer, sheet_name='_Index', index=False)
    return output_path


# COMMAND ----------

# DBTITLE 1,Cell 21

# # This step is a human-readable VERIFICATION export only -- the parquet
# # bronze layer written throughout this notebook is the actual pipeline
# # output; nothing downstream reads this .xlsx.
# #
# # Used to take ~20 min: build_bronze_export_sheets() (Cell 108) was
# # calling pd.read_parquet() + pd.concat() on every bronze parquet file
# # across the ENTIRE historical lake (every past run_date, not just
# # today's), just to report row/column counts in the _Index sheet. It now
# # reads only each file's Parquet footer metadata (pyarrow, no data pages
# # touched) since that's all this export ever needed -- should be a couple
# # of minutes at most now, no matter how large the lake has grown.
# # EXCEL_EXPORT_ROW_CAP only affects the 'Rows_Shown'/'Truncated' columns
# # reported in the _Index sheet, not runtime (no per-sheet data tabs are
# # written).
# RUN_EXCEL_EXPORT = True
# EXCEL_EXPORT_ROW_CAP = 50000

# if RUN_EXCEL_EXPORT:
#     excel_path = build_output_dir("_excel_export") / f"Zone_A_Bronze_Export_{RUN_DATE.isoformat()}.xlsx"

#     # XLSX (ZIP) format requires seekable file handles; Unity Catalog Volumes
#     # (object storage on AWS) do not support seek. Write to local /tmp first,
#     # then copy to the Volume.
#     with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
#         tmp_path = tmp.name

#     write_excel_workbook(build_bronze_export_sheets(OUTPUT_ROOT), tmp_path, row_cap=EXCEL_EXPORT_ROW_CAP)
#     shutil.copy2(tmp_path, str(excel_path))
#     os.remove(tmp_path)
#     print(f"Excel verification export written -> {excel_path}")
# else:
#     print("Skipping Excel verification export (RUN_EXCEL_EXPORT=False) -- "
#           "the parquet bronze layer is the real pipeline output; set "
#           "RUN_EXCEL_EXPORT=True above to regenerate this spot-check workbook.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validation summary
# MAGIC

# COMMAND ----------

# Final flush + human-readable roll-up of every issue logged this run
# (log_issue() calls throughout the notebook already wrote to LOG_FILE as
# they happened; this just re-summarises VALIDATION_ISSUES for a quick
# look here in the notebook, and makes sure the CSV is up to date).
write_validation_csv()

if VALIDATION_ISSUES:
    issues_df = pd.DataFrame(VALIDATION_ISSUES)
    print(issues_df['severity'].value_counts())
    print()
    display(issues_df)
else:
    print("No validation issues logged this run.")

print(f"\nFull log : {LOG_FILE}")
print(f"Full csv : {ISSUE_CSV}")
