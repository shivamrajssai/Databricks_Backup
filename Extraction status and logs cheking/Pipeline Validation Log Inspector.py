# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Pipeline Validation Log Inspector
# MAGIC
# MAGIC Filter pipeline validation logs by UPC, find the most recent log file, and display only **CRITICAL** entries.

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install openpyxl -q

# COMMAND ----------

# DBTITLE 1,Imports, configuration and widgets
import os
import glob
import re
import pandas as pd

LOGS_DIR = "/Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output/_logs"

# ── Input widgets ──
dbutils.widgets.text("zone",  "", "Zone (e.g. A, B, E)")
dbutils.widgets.text("phase", "", "Phase (e.g. 1)")
dbutils.widgets.text("cycle", "", "Cycle (e.g. 1, 2)")
dbutils.widgets.text("upc",   "", "UPC (e.g. N_02026_06003_AS)")

# COMMAND ----------

# DBTITLE 1,Helper functions
def find_upc_logs(zone, phase, cycle, upc, logs_dir=LOGS_DIR):
    """
    Find all pipeline validation log CSVs matching Zone, Phase, Cycle, and UPC.
    Filename pattern: pipeline_validation_Zone{z}_Phase{p}_Cycle{c}_{upc}_{date}.csv
    Returns list sorted by date descending (most recent first).
    """
    pattern = f"pipeline_validation_Zone{zone}_Phase{phase}_Cycle{cycle}_{upc}_*.csv"
    matched = glob.glob(os.path.join(logs_dir, pattern))

    if not matched:
        print(f"\u274c No log files found for Zone={zone}, Phase={phase}, Cycle={cycle}, UPC={upc}")
        print(f"   Search pattern: {pattern}")
        return []

    def extract_date(path):
        m = re.search(r'(\d{4}-\d{2}-\d{2})\.csv$', os.path.basename(path))
        return m.group(1) if m else "0000-00-00"

    matched.sort(key=extract_date, reverse=True)
    print(f"\u2705 Found {len(matched)} log file(s) for Zone={zone}, Phase={phase}, Cycle={cycle}, UPC={upc}")
    for f in matched:
        print(f"   {os.path.basename(f)}")
    return matched


def get_criticals(csv_path):
    """
    Load a pipeline validation log CSV and return only CRITICAL severity rows.
    """
    df = pd.read_csv(csv_path)
    criticals = df[df["severity"].str.upper() == "CRITICAL"].reset_index(drop=True)
    return criticals

# COMMAND ----------

# DBTITLE 1,Filter logs by UPC and display CRITICALs
# \u2500\u2500 Read widget values \u2500\u2500
ZONE  = dbutils.widgets.get("zone").strip()
PHASE = dbutils.widgets.get("phase").strip()
CYCLE = dbutils.widgets.get("cycle").strip()
UPC   = dbutils.widgets.get("upc").strip()

if not all([ZONE, PHASE, CYCLE, UPC]):
    print("\u26a0\ufe0f  Please fill in all four widgets: Zone, Phase, Cycle, UPC")
else:
    # Find all matching logs and pick the most recent
    logs = find_upc_logs(ZONE, PHASE, CYCLE, UPC)

    if logs:
        most_recent = logs[0]
        print(f"\n\U0001f4c4 Most recent log: {os.path.basename(most_recent)}")

        # \u2500\u2500 Check if Backup & Deliverable stages exist in the log CSV \u2500\u2500
        _full_log = pd.read_csv(most_recent)
        _stages_in_log = _full_log["stage"].dropna().str.strip().unique().tolist()
        _missing_stages = [s for s in ["Backup", "Deliverable"] if s not in _stages_in_log]
        if _missing_stages:
            for _m in _missing_stages:
                print(f"\n\u274c {_m} data is NOT present in logs \u2014 needs to be downloaded")
        else:
            print(f"\n\u2705 Both Backup and Deliverable stages present in logs")

        criticals_df = get_criticals(most_recent)

        if criticals_df.empty:
            print(f"\n\u2705 No CRITICAL entries found \u2014 all clear!")
        else:
            print(f"\n\U0001f534 {len(criticals_df)} CRITICAL entries found:\n")
            display(criticals_df)

# COMMAND ----------

# DBTITLE 1,Recursive folder listing for UPC downloads
# \u2500\u2500 Recursive folder listing for UPC in nsv_downloads \u2500\u2500
DOWNLOADS_BASE = "/Volumes/nhai_dhms/nsv/nsv_downloads"

zone_val  = dbutils.widgets.get("zone").strip()
phase_val = dbutils.widgets.get("phase").strip()
cycle_val = dbutils.widgets.get("cycle").strip()
upc_val   = dbutils.widgets.get("upc").strip()

if not all([zone_val, phase_val, cycle_val, upc_val]):
    print("\u26a0\ufe0f  Please fill in all four widgets first.")
else:
    upc_root = os.path.join(
        DOWNLOADS_BASE,
        f"Zone_{zone_val}",
        f"Phase_{phase_val}",
        f"Cycle_{cycle_val}",
        upc_val,
    )

    if not os.path.isdir(upc_root):
        print(f"\u274c Folder not found: {upc_root}")
    else:
        print(f"\u2705 UPC folder: {upc_root}\n")

        # \u2500\u2500 Tree view \u2500\u2500
        folder_rows = []
        for root, dirs, files in os.walk(upc_root):
            rel   = os.path.relpath(root, upc_root)
            depth = 0 if rel == "." else rel.count(os.sep) + 1
            name  = os.path.basename(root) if rel != "." else upc_val
            indent = "  " * depth
            print(f"{indent}\U0001f4c1 {name}/  ({len(dirs)} subfolders, {len(files)} files)")

            folder_rows.append({
                "Folder":     rel if rel != "." else upc_val,
                "Subfolders": len(dirs),
                "Files":      len(files),
                "Depth":      depth,
            })

        print(f"\n\u2500\u2500 Summary: {len(folder_rows)} folders total \u2500\u2500\n")
        display(pd.DataFrame(folder_rows))

# COMMAND ----------

# DBTITLE 1,Comprehensive verification — extraction, download and CRITICALs
# \u2500\u2500 Comprehensive verification: extraction output, download source, and CRITICALs \u2500\u2500
from openpyxl import load_workbook as _lwb
import re as _re_c7

EXTRACTION_BASE = "/Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output"
DOWNLOADS_BASE  = "/Volumes/nhai_dhms/nsv/nsv_downloads"

# Stage \u2192 extraction source folder, download subfolder
STAGE_MAP = {
    "Backup":      {"extraction": "Source=NSV_Backup", "download": "backup"},
    "Deliverable": {"extraction": "Source=NSV",        "download": "deliverable"},
}

zone_v  = dbutils.widgets.get("zone").strip()
phase_v = dbutils.widgets.get("phase").strip()
cycle_v = dbutils.widgets.get("cycle").strip()
upc_v   = dbutils.widgets.get("upc").strip()

if not all([zone_v, phase_v, cycle_v, upc_v]):
    print("\u26a0\ufe0f  Fill in all four widgets first.")
else:
    # \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    # PART 1 \u2014 Stage-level: log vs extraction vs download
    # \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    print("PART 1 \u2014 Stage-level verification\n")

    # Stages present in log CSV (from Cell 5)
    log_stages = set()
    if "_stages_in_log" in dir():
        log_stages = set(_stages_in_log)

    dl_root = os.path.join(DOWNLOADS_BASE, f"Zone_{zone_v}", f"Phase_{phase_v}",
                           f"Cycle_{cycle_v}", upc_v)

    def _count_extraction(ext_base):
        """Navigate to latest year/month/day partition and count files."""
        if not os.path.isdir(ext_base):
            return False, 0, "", ""
        # Walk year > month > day picking the latest at each level
        path = ext_base
        partition_parts = []
        for _ in range(3):  # year, month, day
            subs = sorted([d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))])
            if not subs:
                break
            latest = subs[-1]
            partition_parts.append(latest)
            path = os.path.join(path, latest)
        # Count files recursively in the deepest partition
        file_count = sum(len(f) for _, _, f in os.walk(path))
        partition = "/".join(partition_parts)
        return True, file_count, partition, path

    def _count_download(dl_root, subfolder_name):
        """Count files recursively in download subfolder, searching nested paths."""
        # 1. Try direct child first
        direct = os.path.join(dl_root, subfolder_name)
        if os.path.isdir(direct):
            fc = sum(len(f) for _, _, f in os.walk(direct))
            if fc > 0:
                return True, fc, direct

        # 2. Search recursively for a matching folder (handles Stretch subfolders)
        for root, dirs, _ in os.walk(dl_root):
            for d in dirs:
                if d.lower() == subfolder_name.lower():
                    found = os.path.join(root, d)
                    fc = sum(len(f) for _, _, f in os.walk(found))
                    if fc > 0:
                        return True, fc, found

        # 3. Folder exists but empty, or not found at all
        if os.path.isdir(direct):
            return True, 0, direct
        return False, 0, ""

    stage_rows = []
    for stage, paths in STAGE_MAP.items():
        ext_base = os.path.join(EXTRACTION_BASE, paths["extraction"],
                                f"Zone={zone_v}", f"Phase={phase_v}",
                                f"Cycle={cycle_v}", f"UPC={upc_v}")

        in_log = stage in log_stages
        ext_exists, ext_files, ext_partition, ext_leaf = _count_extraction(ext_base)
        dl_exists, dl_files, dl_path = _count_download(dl_root, paths["download"])

        # Determine action based on actual file counts
        has_extraction = ext_exists and ext_files > 0
        has_download   = dl_exists and dl_files > 0

        if in_log and has_extraction:
            action = "\u2705 OK \u2014 extracted successfully"
        elif not has_extraction and has_download:
            action = "\U0001f534 Re-Extraction required (download has data but extraction is empty)"
        elif not has_extraction and not has_download and not dl_exists:
            action = "\u274c Download required (not downloaded yet)"
        elif not has_extraction and dl_exists and dl_files == 0:
            action = "\u274c Download folder exists but is empty \u2014 re-download needed"
        elif has_extraction and not in_log:
            action = "\u26a0\ufe0f  Extraction exists but missing from latest log \u2014 may need re-run"
        elif in_log and ext_exists and ext_files == 0 and has_download:
            action = "\U0001f534 Re-Extraction required (extraction folder empty despite log entry)"
        else:
            action = "\u2753 Unexpected state \u2014 check manually"

        mark = lambda b: "\u2705" if b else "\u274c"
        print(f"  {stage}:")
        print(f"    Log CSV      : {mark(in_log)}")
        print(f"    Extraction   : {mark(has_extraction)}  ({ext_files} files in {ext_partition})")
        print(f"                   {ext_base}")
        print(f"    Download     : {mark(has_download)}  ({dl_files} files)")
        print(f"                   {dl_path}")
        print(f"    \u27a1\ufe0f  {action}\n")

        stage_rows.append({
            "Stage": stage, "In Log": mark(in_log),
            "Extraction": f"{mark(has_extraction)} ({ext_files} files)",
            "Download": f"{mark(has_download)} ({dl_files} files)",
            "Action": action,
        })

    display(pd.DataFrame(stage_rows))

    # \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    # PART 2 \u2014 CRITICAL file-level: is the data really missing?
    # \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
    ALIASES = {
        "Rutting":       ["rutting", "rutdepth", "rut depth", "rut_depth", "rut"],
        "Raveling":      ["raveling", "ravelling", "ravel"],
        "Roughness":     ["roughness", "iri"],
        "Cracking":      ["cracking", "crack", "cracks"],
        "Deflection":    ["deflection", "fwd", "deflect"],
        "GPS_Geometry":  ["gps_geometry", "gps coordinates", "gps", "geometry",
                          "gps coordinate", " gps"],
        "TextureDepth":  ["texturedepth", "texture depth", "texture_depth", "texture"],
    }

    def _find_sheets(sheetnames, target):
        aliases = ALIASES.get(target, [target.lower()])
        return [s for s in sheetnames if s.lower().strip() in aliases]

    def _probe(wb, sheet, scan=10):
        ws = wb[sheet]
        hdr, hdr_s, n = None, None, 0
        for i, row in enumerate(ws.iter_rows(min_row=1, max_row=scan, values_only=True), 1):
            nns = [v for v in row if v is not None]
            if len(nns) >= 3 and hdr is None:
                hdr = i; hdr_s = [str(v) for v in nns][:8]
            elif hdr and len(nns) >= 2:
                n += 1
        return hdr, hdr_s, n, ws.max_row or 0, ws.max_column or 0

    def _extract_lane_from_file(fname):
        """Extract lane tag (L1, R2 etc.) from a CRITICAL filename."""
        m = _re_c7.search(r'([LR]\d+)', fname, _re_c7.IGNORECASE)
        return m.group(1).upper() if m else None

    def _check_extraction_parquet(stage_name, target):
        """Check parquets matching 'target' in extraction output.
        Returns list of dicts: {path, lane, project, date_key}."""
        if stage_name not in STAGE_MAP:
            return []
        _ext_src = STAGE_MAP[stage_name]["extraction"]
        _ext_base = os.path.join(EXTRACTION_BASE, _ext_src,
                                 f"Zone={zone_v}", f"Phase={phase_v}",
                                 f"Cycle={cycle_v}", f"UPC={upc_v}")
        hits = []
        if os.path.isdir(_ext_base):
            _tgt = target.lower().replace("_", "")
            for _r, _, _fs in os.walk(_ext_base):
                for _f in _fs:
                    if _tgt in _f.lower().replace("_", "") and _f.endswith('.parquet'):
                        rel = os.path.relpath(os.path.join(_r, _f), _ext_base)
                        lane_m = _re_c7.search(r'Lane=([^/]+)', rel)
                        proj_m = _re_c7.search(r'Project=([^/]+)', rel)
                        yr_m   = _re_c7.search(r'year=(\d+)/month=(\d+)/day=(\d+)', rel)
                        hits.append({
                            "path": rel,
                            "lane": lane_m.group(1).upper() if lane_m else None,
                            "project": proj_m.group(1) if proj_m else None,
                            "project_tag": "CC" if (proj_m and proj_m.group(1).upper().startswith("CC")) else "BT",
                            "date_key": f"{yr_m.group(1)}-{yr_m.group(2).zfill(2)}-{yr_m.group(3).zfill(2)}" if yr_m else "",
                        })
        return hits

    file_crits = pd.DataFrame()  # reset so Cell 8 sees current UPC state

    if "criticals_df" not in dir() or criticals_df.empty:
        print("\n\u2139\ufe0f  No CRITICALs to verify at file level.")
    else:
        file_crits = criticals_df[
            criticals_df["file"].notna()
        ].reset_index(drop=True)

        if file_crits.empty:
            print("\n\u2139\ufe0f  No file-specific CRITICALs to verify.")
        else:
            print(f"\n\nPART 2 \u2014 File-level CRITICAL verification (checking download files)\n")
            print(f"\U0001f50d Verifying {len(file_crits)} CRITICALs...\n")
            verdicts = []

            # Also try the download folder if CRITICAL path doesn't exist
            _dl_root = os.path.join(DOWNLOADS_BASE, f"Zone_{zone_v}", f"Phase_{phase_v}",
                                    f"Cycle_{cycle_v}", upc_v)

            for _, cr in file_crits.iterrows():
                fpath, target = cr["file"], cr["table"]
                fname = os.path.basename(fpath)
                print(f"{'\u2500'*80}")
                print(f"\U0001f4c4 {fname}  |  Target: {target}")
                print(f"   {cr['issue'][:120]}")

                # Find the file: try CRITICAL path first, then search download folder
                actual_path = None
                if os.path.isfile(fpath):
                    actual_path = fpath
                else:
                    # Search download folder recursively
                    for root, _, fls in os.walk(_dl_root):
                        if fname in fls:
                            actual_path = os.path.join(root, fname)
                            print(f"   \U0001f50d Found in download: {os.path.relpath(actual_path, _dl_root)}")
                            break

                if not actual_path:
                    print(f"   \u274c File not found in CRITICAL path or download folder")
                    verdicts.append({"File": fname, "Target": target,
                                     "Sheet Found": "\u2014", "Rows": "\u2014",
                                     "Verdict": "\u274c FILE MISSING"})
                    continue

                wb = _lwb(actual_path, read_only=True, data_only=True)

                # Always list all sheets so user can see what's inside
                print(f"   Sheets in file: {wb.sheetnames}")

                matches = _find_sheets(wb.sheetnames, target)

                # Lat/Long column aliases for GPS_Geometry verification
                _GPS_COL_ALIASES = {"latitude", "lat", "longitude", "long", "lng",
                                    "lon", "start_lat", "start_long", "end_lat",
                                    "end_long", "start latitude", "start longitude",
                                    "end latitude", "end longitude", "gps_latitude",
                                    "gps_longitude", "start_latitude", "start_longitude",
                                    "end_latitude", "end_longitude"}

                if not matches:
                    print(f"   \u274c No sheet matching '{target}' \u2014 data not present in download")
                    v = "\u274c MISSING in download"
                    verdicts.append({"File": fname, "Target": target,
                                     "Sheet Found": "\u2014", "Rows": "\u2014",
                                     "Verdict": v})

                elif target == "GPS_Geometry":
                    # Special GPS handling: look for Lat/Long columns across
                    # all matching sheets (GPS, GPS Coordinates, Geometry)
                    gps_found_sheet = None
                    gps_cols_found = []
                    for _sh in matches:
                        ws = wb[_sh]
                        # Scan first 5 rows for header columns
                        _all_hdrs = []
                        for _row in ws.iter_rows(min_row=1, max_row=5, values_only=True):
                            for _cell in _row:
                                if _cell is not None:
                                    _all_hdrs.append(str(_cell).lower().strip())
                        _found = [h for h in _all_hdrs if h in _GPS_COL_ALIASES]
                        print(f"   Sheet '{_sh}' headers (first 5 rows): "
                              f"{[h for h in _all_hdrs if h][:10]}")
                        if _found:
                            gps_found_sheet = _sh
                            gps_cols_found = _found
                            break

                    if gps_found_sheet:
                        hdr_row, hdr_s, data_n, tot_r, tot_c = _probe(wb, gps_found_sheet)
                        print(f"   \u2705 Lat/Long columns found in '{gps_found_sheet}': {gps_cols_found}")
                        print(f"   ~{tot_r:,} rows, {tot_c} cols")
                        if data_n > 0:
                            print(f"   \U0001f534 CODE ISSUE \u2014 Lat/Long data present but extraction failed")
                            v = "CODE ISSUE \U0001f534"
                        else:
                            print(f"   \u2705 CONFIRMED \u2014 Lat/Long columns exist but rows are empty")
                            v = "\u2705 CONFIRMED empty"
                        verdicts.append({"File": fname, "Target": target,
                                         "Sheet Found": gps_found_sheet,
                                         "Rows": f"{tot_r:,}", "Verdict": v})
                    else:
                        sheets_checked = [s for s in matches]
                        print(f"   \u274c No Lat/Long columns found in sheets: {sheets_checked}")
                        print(f"   \u26a0\ufe0f  Check Google Drive manually for GPS base data")
                        v = "\u26a0\ufe0f No Lat/Long \u2014 check Google Drive"
                        verdicts.append({"File": fname, "Target": target,
                                         "Sheet Found": ", ".join(sheets_checked),
                                         "Rows": "\u2014", "Verdict": v})

                else:
                    # Normal verification for non-GPS targets
                    sheet = matches[0]
                    hdr_row, hdr_s, data_n, tot_r, tot_c = _probe(wb, sheet)
                    print(f"   Matched sheet '{sheet}': ~{tot_r:,} rows, {tot_c} cols")
                    if hdr_s:
                        print(f"   Header (row {hdr_row}): {hdr_s}")
                    if data_n > 0:
                        print(f"   \U0001f534 CODE ISSUE \u2014 data IS present in download but extraction failed")
                        v = "CODE ISSUE \U0001f534"
                    else:
                        print(f"   \u2705 CONFIRMED \u2014 sheet exists but empty in download too")
                        v = "\u2705 CONFIRMED empty in download"
                    verdicts.append({"File": fname, "Target": target,
                                     "Sheet Found": sheet, "Rows": f"{tot_r:,}",
                                     "Verdict": v})

                wb.close()
                print()

            print(f"{'\u2500'*80}")
            print(f"\n\U0001f4ca FILE-LEVEL VERDICT SUMMARY\n")
            # Cross-reference each verdict with extraction bronze output
            print(f"\n{'='*80}")
            print(f"PART 2b — Bronze extraction cross-reference (per-lane, latest date)\n")
            any_override = False
            for i, _vd in enumerate(verdicts):
                _cr = file_crits.iloc[i]
                _cr_stage = str(_cr.get("stage", "")).strip()
                _target = _cr["table"]
                _all_pq = _check_extraction_parquet(_cr_stage, _target)
                _lane = _extract_lane_from_file(os.path.basename(_cr["file"]))

                # Filter to this lane
                if _lane:
                    _lane_pq = [p for p in _all_pq if p["lane"] == _lane]
                else:
                    _lane_pq = _all_pq

                if _lane_pq:
                    any_override = True
                    # Sort by date descending — show latest first
                    _lane_pq.sort(key=lambda x: x["date_key"], reverse=True)
                    _latest_date = _lane_pq[0]["date_key"]
                    _latest = [p for p in _lane_pq if p["date_key"] == _latest_date]
                    _projects = sorted(set(p["project_tag"] for p in _lane_pq))
                    _all_dates = sorted(set(p["date_key"] for p in _lane_pq), reverse=True)

                    print(f"  >> {_vd['File']} / {_target} ({_cr_stage}) — Lane {_lane or '?'}:")
                    print(f"     {len(_lane_pq)} parquet(s) for this lane | "
                          f"Latest: {_latest_date} | Projects: {', '.join(_projects)}")
                    if len(_all_dates) > 1:
                        print(f"     All extraction dates: {', '.join(_all_dates)}")
                    for _pm in _latest[:3]:
                        print(f"       {_pm['path']}")
                    if len(_latest) > 3:
                        print(f"       ... and {len(_latest) - 3} more from {_latest_date}")

                    # Override verdict for non-code-issue cases
                    if any(k in _vd["Verdict"] for k in
                           ("CONFIRMED empty", "FILE MISSING", "MISSING in download", "No Lat/Long")):
                        verdicts[i]["Verdict"] = (f"Download issue but EXTRACTED "
                            f"({len(_lane_pq)} parquet, latest {_latest_date}, "
                            f"{', '.join(_projects)})")
                elif _all_pq:
                    # No lane-specific hit, but parquets exist for other lanes
                    any_override = True
                    _other_lanes = sorted(set(p["lane"] for p in _all_pq if p["lane"]))
                    print(f"  >> {_vd['File']} / {_target} ({_cr_stage}) — Lane {_lane or '?'}:")
                    print(f"     No parquet for lane {_lane}, but {len(_all_pq)} "
                          f"parquets exist for other lanes: {', '.join(_other_lanes)}")
                    if any(k in _vd["Verdict"] for k in
                           ("CONFIRMED empty", "FILE MISSING", "MISSING in download", "No Lat/Long")):
                        verdicts[i]["Verdict"] = (f"Download issue — no lane-{_lane} parquet "
                            f"but {len(_all_pq)} exist for {', '.join(_other_lanes)}")
                else:
                    print(f"  >> {_vd['File']} / {_target} ({_cr_stage}) — "
                          f"Lane {_lane or '?'}: No parquet in bronze")
            if not any_override:
                print("  No extraction parquets found for any CRITICAL.")
            print()

            display(pd.DataFrame(verdicts))

# COMMAND ----------

# DBTITLE 1,Open CRITICAL files from download — inspect data
# ── Open each CRITICAL file from the download folder and display the matched sheet ──
from openpyxl import load_workbook as _lwb2

if "file_crits" not in dir() or file_crits.empty:
    print("\u26a0\ufe0f  No file-level CRITICALs \u2014 run Cell 7 first.")
else:
    _dl_root2 = os.path.join(
        "/Volumes/nhai_dhms/nsv/nsv_downloads",
        f"Zone_{dbutils.widgets.get('zone').strip()}",
        f"Phase_{dbutils.widgets.get('phase').strip()}",
        f"Cycle_{dbutils.widgets.get('cycle').strip()}",
        dbutils.widgets.get("upc").strip(),
    )

    # Deduplicate: one entry per unique file + target
    seen = set()
    for _, cr in file_crits.iterrows():
        fpath, target = cr["file"], cr["table"]
        fname = os.path.basename(fpath)
        key = (fname, target)
        if key in seen:
            continue
        seen.add(key)

        # Resolve file path
        actual = fpath if os.path.isfile(fpath) else None
        if not actual:
            for root, _, fls in os.walk(_dl_root2):
                if fname in fls:
                    actual = os.path.join(root, fname)
                    break
        if not actual:
            print(f"\u274c {fname} not found \u2014 skipping\n")
            continue

        # Find matched sheet via aliases
        wb = _lwb2(actual, read_only=True, data_only=True)
        matches = _find_sheets(wb.sheetnames, target)

        if not matches:
            wb.close()
            print(f"\u274c {fname}: no sheet matching '{target}' \u2014 skipping\n")
            continue

        # For GPS_Geometry: only open if the sheet actually has Lat/Long columns
        _GPS_COL_ALIASES2 = {"latitude", "lat", "longitude", "long", "lng",
                             "lon", "start_lat", "start_long", "end_lat",
                             "end_long", "start latitude", "start longitude",
                             "end latitude", "end longitude", "gps_latitude",
                             "gps_longitude", "start_latitude", "start_longitude",
                             "end_latitude", "end_longitude"}

        sheet = None
        if target == "GPS_Geometry":
            for _sh in matches:
                ws = wb[_sh]
                _hdrs = []
                for _row in ws.iter_rows(min_row=1, max_row=5, values_only=True):
                    for _cell in _row:
                        if _cell is not None:
                            _hdrs.append(str(_cell).lower().strip())
                if any(h in _GPS_COL_ALIASES2 for h in _hdrs):
                    sheet = _sh
                    break
            if not sheet:
                wb.close()
                print(f"\u26a0\ufe0f {fname}: GPS sheets have no Lat/Long \u2014 check Google Drive manually\n")
                continue
        else:
            sheet = matches[0]

        wb.close()

        print(f"{'\u2500'*80}")
        print(f"\U0001f4c4 {fname}  \u2192  sheet '{sheet}'")
        print(f"   Source: {actual}\n")

        # Read into pandas
        df_sheet = pd.read_excel(actual, sheet_name=sheet, engine="openpyxl")

        # Drop rows where every cell is NaN
        df_sheet = df_sheet.dropna(how="all")

        # For GPS_Geometry: check if Lat/Long columns actually have data
        _is_gps_empty = False
        if target == "GPS_Geometry" and df_sheet.shape[0] > 0:
            _lat_cols = [c for c in df_sheet.columns if c.lower().strip() in _GPS_COL_ALIASES2]
            if _lat_cols:
                _has_data = df_sheet[_lat_cols].notna().any().any()
                if not _has_data:
                    _is_gps_empty = True

        # Summary stats
        print(f"   Shape: {df_sheet.shape[0]:,} non-blank rows \u00d7 {df_sheet.shape[1]} columns")
        if df_sheet.shape[0] == 0 or df_sheet.shape[1] == 0 or _is_gps_empty:
            print("   \u26a0\ufe0f  No usable Lat/Long data in this sheet \u2014 nothing to display")
            print("   \u27a1\ufe0f  Check Google Drive manually for this data\n")
        else:
            null_pct = df_sheet.isnull().mean() * 100
            high_null = null_pct[null_pct > 50]
            if not high_null.empty:
                print(f"   Columns >50% null: {', '.join(high_null.index.tolist())}")
            print()
            display(df_sheet.head(20).astype(str))
        print()

# COMMAND ----------

# DBTITLE 1,GPS Alternative Source Finder — scan all sheets for chainage + lat/long
# ── GPS Alternative Source Finder ──
# When GPS sheet is empty, scan ALL other sheets in each LCMS file
# for chainage + latitude/longitude columns that could populate GPS_Geometry

from openpyxl import load_workbook as _lwb3

_LAT_ALIASES = {"latitude", "lat", "gps_latitude", "start_lat",
                "start_latitude", "start latitude", "northing"}
_LON_ALIASES = {"longitude", "long", "lng", "lon", "gps_longitude",
                "start_long", "start_longitude", "start longitude", "easting"}
_CHAIN_ALIASES = {"chainage", "chain", "km", "distance",
                  "lrp_offset", "lrp_offset_start", "lrp_number"}

if "file_crits" not in dir() or file_crits.empty:
    print("\u26a0\ufe0f  Run Cell 7 first.")
else:
    gps_crits = file_crits[file_crits["table"] == "GPS_Geometry"].copy()
    if gps_crits.empty:
        print("\u2139\ufe0f  No GPS_Geometry CRITICALs to analyze.")
    else:
        _dl = os.path.join(
            "/Volumes/nhai_dhms/nsv/nsv_downloads",
            f"Zone_{dbutils.widgets.get('zone').strip()}",
            f"Phase_{dbutils.widgets.get('phase').strip()}",
            f"Cycle_{dbutils.widgets.get('cycle').strip()}",
            dbutils.widgets.get("upc").strip(),
        )

        print("GPS ALTERNATIVE SOURCE SCAN")
        print("=" * 80)
        print("Scanning ALL sheets in LCMS files for chainage + lat/long data")
        print("that could populate GPS_Geometry when GPS sheet is empty\n")

        seen = set()

        for _, cr in gps_crits.iterrows():
            fpath = cr["file"]
            fname = os.path.basename(fpath)
            stage = str(cr.get("stage", "")).strip()

            if fname in seen:
                continue
            seen.add(fname)

            # Resolve path (search recursively through download root)
            actual = fpath if os.path.isfile(fpath) else None
            if not actual:
                for r, _, fs in os.walk(_dl):
                    if fname in fs:
                        actual = os.path.join(r, fname)
                        break
            if not actual:
                print(f"\u274c {fname} not found\n")
                continue

            print(f"{'\u2500' * 80}")
            print(f"\U0001f4c4 {fname}  ({stage})")
            print(f"   {actual}\n")

            wb = _lwb3(actual, read_only=True, data_only=True)
            findings = []

            for sname in wb.sheetnames:
                ws = wb[sname]
                # Read headers from first 3 rows
                hdrs = []
                for row in ws.iter_rows(min_row=1, max_row=3, values_only=True):
                    for c in row:
                        if c is not None:
                            hdrs.append(str(c).lower().strip())

                found_lat = [h for h in hdrs if h in _LAT_ALIASES]
                found_lon = [h for h in hdrs if h in _LON_ALIASES]
                found_chain = [h for h in hdrs if h in _CHAIN_ALIASES]

                if not found_lat and not found_lon and not found_chain:
                    continue

                # Check data rows (scan first 25 rows)
                _, _, data_n, max_r, _ = _probe(wb, sname, scan=25)

                findings.append({
                    "sname": sname,
                    "lat": found_lat, "lon": found_lon, "chain": found_chain,
                    "data_n": data_n, "max_r": max_r,
                    "has_lat": bool(found_lat), "has_lon": bool(found_lon),
                    "has_chain": bool(found_chain),
                })

            wb.close()

            # Build summary table
            tbl_rows = []
            for f in findings:
                if f["has_lat"] and f["has_lon"] and f["has_chain"] and f["data_n"] > 0:
                    tag = "\U0001f7e2 FULL GPS + DATA"
                elif f["has_lat"] and f["has_lon"] and f["data_n"] > 0:
                    tag = "\U0001f7e1 Lat/Long + data (no chainage)"
                elif f["has_chain"] and f["data_n"] > 0:
                    tag = "\U0001f7e1 Chainage + data (no lat/long)"
                elif (f["has_lat"] or f["has_lon"]) and f["data_n"] == 0:
                    tag = "\U0001f534 Lat/Long cols but EMPTY rows"
                elif f["has_chain"] and f["data_n"] == 0:
                    tag = "\u26aa Chainage only, EMPTY"
                else:
                    tag = "\u26aa Partial"

                tbl_rows.append({
                    "Sheet": f["sname"],
                    "Lat Cols": ", ".join(f["lat"]) or "\u2014",
                    "Lon Cols": ", ".join(f["lon"]) or "\u2014",
                    "Chainage Cols": ", ".join(f["chain"]) or "\u2014",
                    "~Rows": f"{f['max_r']:,}",
                    "Has Data": "\u2705" if f["data_n"] > 0 else "\u274c",
                    "Usability": tag,
                })

            if tbl_rows:
                display(pd.DataFrame(tbl_rows))
            else:
                print("   No GPS-related columns in any sheet.\n")
                continue

            # ── Deep-verify best candidates with pandas ──
            best = [f for f in findings
                    if f["has_lat"] and f["has_lon"] and f["has_chain"]
                    and f["data_n"] > 0]
            partial = [f for f in findings
                       if f["has_lat"] and f["has_lon"] and f["data_n"] > 0
                       and f not in best]

            _latlong_data_ok = False
            candidates = best or partial
            if candidates:
                for cand in candidates[:2]:  # top 2 candidates
                    sn = cand["sname"]
                    print(f"\n   \U0001f50d Deep-verifying sheet '{sn}' with pandas...\n")
                    df_s = pd.read_excel(actual, sheet_name=sn,
                                         engine="openpyxl", nrows=200)
                    df_s.columns = [str(c).lower().strip() for c in df_s.columns]

                    lat_c = [c for c in df_s.columns if c in _LAT_ALIASES]
                    lon_c = [c for c in df_s.columns if c in _LON_ALIASES]
                    ch_c  = [c for c in df_s.columns if c in _CHAIN_ALIASES]
                    gps_cols = ch_c + lat_c + lon_c

                    if gps_cols:
                        sub = df_s[gps_cols].dropna(how="all")
                        nn = sub.notna().sum()
                        print(f"      Column non-null counts (first 200 rows):")
                        for col in gps_cols:
                            n = int(nn.get(col, 0))
                            m = "\u2705" if n > 0 else "\u274c"
                            print(f"        {m} {col}: {n:,} non-null")

                        lat_ok = any(nn.get(c, 0) > 0 for c in lat_c)
                        lon_ok = any(nn.get(c, 0) > 0 for c in lon_c)
                        ch_ok  = any(nn.get(c, 0) > 0 for c in ch_c)
                        if lat_ok or lon_ok:
                            _latlong_data_ok = True

                        if lat_ok and lon_ok and ch_ok:
                            print(f"\n      \U0001f7e2 CONFIRMED: '{sn}' has chainage + lat/long with real data!")
                            print(f"         \u27a1\ufe0f  GPS_Geometry CAN be populated from this sheet")
                            print(f"\n      Sample ({len(sub):,} data rows):\n")
                            display(sub.head(10))
                        elif ch_ok and not (lat_ok or lon_ok):
                            print(f"\n      \U0001f7e1 Sheet has chainage with data but lat/long are NULL")
                            print(f"         \u27a1\ufe0f  Could provide chainage mapping if lat/long found elsewhere")
                            print(f"\n      Sample:\n")
                            display(sub.head(10))
                        elif lat_ok or lon_ok:
                            print(f"\n      \U0001f7e1 Partial lat/long data found \u2014 incomplete but usable")
                            print(f"\n      Sample:\n")
                            display(sub.head(10))
                        else:
                            print(f"\n      \U0001f534 All GPS columns are NULL despite headers existing")
                    else:
                        print(f"      No GPS columns found after pandas read")

                # If candidates are partial (lat/long only), suggest combining with chainage
                if not best and _latlong_data_ok:
                    _ch_sheets = [f for f in findings if f["has_chain"] and f["data_n"] > 0]
                    if _ch_sheets:
                        print(f"\n   \U0001f4a1 RECOMMENDATION: Combine lat/long from above with chainage from:")
                        for _cs in _ch_sheets[:5]:
                            print(f"      \u2022 Sheet '{_cs['sname']}' ({_cs['max_r']:,} rows)")
                        if len(_ch_sheets) > 5:
                            print(f"      ... and {len(_ch_sheets) - 5} more")
                        print(f"   \u27a1\ufe0f  Pipeline could join on row index or nearest chainage to build GPS_Geometry")
                elif not best:
                    _ch2 = [f for f in findings if f["has_chain"] and f["data_n"] > 0]
                    if _ch2:
                        print(f"\n   \U0001f534 Lat/Long columns exist but ALL data is NULL across checked sheets")
                        print(f"      Chainage data IS available in {len(_ch2)} sheets (Geometry, IRI, Rut, etc.)")
                        print(f"   \u27a1\ufe0f  Lat/long must come from GPS device file or Google Drive")
                    else:
                        print(f"\n   \U0001f534 No usable GPS data in any sheet \u2014 check Google Drive")
            else:
                # No sheet has lat/long + chainage with data
                chain_only = [f for f in findings
                              if f["has_chain"] and f["data_n"] > 0]
                if chain_only:
                    print(f"\n   \U0001f7e1 No sheet has lat/long data, but chainage is available in:")
                    for co in chain_only:
                        print(f"      \u2022 Sheet '{co['sname']}' ({co['max_r']:,} rows)")
                    print(f"   \u27a1\ufe0f  Chainage exists \u2014 lat/long must come from GPS device/Google Drive")
                else:
                    print(f"\n   \U0001f534 No alternative GPS source in this file")
                    print(f"      \u27a1\ufe0f  GPS data must come from Google Drive or another source")

            print()

# COMMAND ----------

# DBTITLE 1,GPS Download-wide lane-wise scan
# ── GPS: Recursive lane-wise scan of backup folder ──
# Scans every xlsx/xls/csv in backup for GPS lat/long data per lane.
# When found, opens and displays the data like Cell 8.

from openpyxl import load_workbook as _lwb_gps
import re as _re_gps

_LAT_ALL = {"latitude", "lat", "gps_latitude", "start_lat", "start_latitude",
            "start latitude", "northing"}
_LON_ALL = {"longitude", "long", "lng", "lon", "gps_longitude", "start_long",
            "start_longitude", "start longitude", "easting"}

_dl_root_gps = os.path.join(
    "/Volumes/nhai_dhms/nsv/nsv_downloads",
    f"Zone_{dbutils.widgets.get('zone').strip()}",
    f"Phase_{dbutils.widgets.get('phase').strip()}",
    f"Cycle_{dbutils.widgets.get('cycle').strip()}",
    dbutils.widgets.get("upc").strip(),
    "backup",
)

def _detect_lane(fpath):
    """Extract lane tag (L1, R2 etc.) from filename or parent folder."""
    fname = os.path.basename(fpath).upper()
    m = _re_gps.search(r'\b([LR]\d+)\b', fname)
    if m:
        return m.group(1)
    for p in reversed(fpath.upper().split(os.sep)[:-1]):
        m = _re_gps.search(r'\b([LR]\d+)\b', p)
        if m:
            return m.group(1)
    return "UNKNOWN"


print("GPS LANE-WISE BACKUP SCAN")
print("=" * 80)
print(f"Scanning all files in backup for GPS data (lat/long) per lane")
print(f"Root: {_dl_root_gps}\n")

# Collect all data files
all_files = []
for root, dirs, files in os.walk(_dl_root_gps):
    for f in files:
        fl = f.lower()
        if fl.endswith((".xlsx", ".xls", ".csv")) and not f.startswith("~$"):
            all_files.append(os.path.join(root, f))

print(f"Found {len(all_files)} data files (xlsx/xls/csv) in backup\n")

# ── Scan every file for GPS columns, record per lane ──
lane_hits = {}     # {lane: [{path, file, sheet, lat_cols, lon_cols, has_data, non_null, total_rows}]}

for fpath in sorted(all_files):
    lane = _detect_lane(fpath)
    fname = os.path.basename(fpath)

    try:
        if fpath.lower().endswith(".csv"):
            # CSV: read header
            hdr_df = pd.read_csv(fpath, nrows=0)
            hdrs = [str(c).lower().strip() for c in hdr_df.columns]
            lat_c = [h for h in hdrs if h in _LAT_ALL]
            lon_c = [h for h in hdrs if h in _LON_ALL]
            if not lat_c and not lon_c:
                continue
            # Verify with actual data
            sample = pd.read_csv(fpath, nrows=200)
            sample.columns = [str(c).lower().strip() for c in sample.columns]
            gps_cols = [c for c in sample.columns if c in _LAT_ALL | _LON_ALL]
            nn = int(sample[gps_cols].notna().any(axis=1).sum()) if gps_cols else 0
            with open(fpath, 'r') as fh:
                total = sum(1 for _ in fh) - 1
            lane_hits.setdefault(lane, []).append({
                "path": fpath, "file": fname, "sheet": "(csv)",
                "lat_cols": lat_c, "lon_cols": lon_c,
                "has_data": nn > 0, "non_null": nn, "total_rows": total,
            })
        else:
            wb = _lwb_gps(fpath, read_only=True, data_only=True)
            for sname in wb.sheetnames:
                ws = wb[sname]
                hdrs = []
                for row in ws.iter_rows(min_row=1, max_row=5, values_only=True):
                    for c in row:
                        if c is not None:
                            hdrs.append(str(c).lower().strip())
                lat_c = [h for h in hdrs if h in _LAT_ALL]
                lon_c = [h for h in hdrs if h in _LON_ALL]
                if not lat_c and not lon_c:
                    continue
                max_r = ws.max_row or 0
                # Deep verify
                try:
                    sample = pd.read_excel(fpath, sheet_name=sname,
                                           engine="openpyxl", nrows=200)
                    sample.columns = [str(c).lower().strip() for c in sample.columns]
                    gps_cols = [c for c in sample.columns if c in _LAT_ALL | _LON_ALL]
                    nn = int(sample[gps_cols].notna().any(axis=1).sum()) if gps_cols else 0
                except Exception:
                    nn = 0
                lane_hits.setdefault(lane, []).append({
                    "path": fpath, "file": fname, "sheet": sname,
                    "lat_cols": lat_c, "lon_cols": lon_c,
                    "has_data": nn > 0, "non_null": nn, "total_rows": max_r,
                })
            wb.close()
    except Exception as e:
        print(f"  \u26a0\ufe0f Error reading {fname}: {str(e)[:80]}")

# ── Determine expected lanes from criticals ──
expected_lanes = set()
if "file_crits" in dir() and not file_crits.empty:
    for fp in file_crits["file"].dropna():
        m = _re_gps.search(r'([LR]\d+)', os.path.basename(fp), _re_gps.IGNORECASE)
        if m:
            expected_lanes.add(m.group(1).upper())
for ln in lane_hits:
    if ln != "UNKNOWN":
        expected_lanes.add(ln)

# ── Display per lane — open and show data when found ──
gps_found_lanes = []
gps_missing_lanes = []

for lane in sorted(expected_lanes | set(lane_hits.keys())):
    entries = lane_hits.get(lane, [])
    data_entries = [e for e in entries if e.get("has_data")]
    blank_entries = [e for e in entries if not e.get("has_data")]

    print(f"{'\u2500' * 80}")
    print(f"Lane {lane}:")

    if data_entries:
        gps_found_lanes.append(lane)
        print(f"  \U0001f7e2 GPS DATA FOUND in {len(data_entries)} source(s):\n")

        for e in data_entries:
            fpath = e["path"]
            fname = e["file"]
            sheet = e["sheet"]
            sheet_label = "CSV file" if sheet == "(csv)" else f"sheet '{sheet}'"
            print(f"  \U0001f4c4 {fname}  \u2192  {sheet_label}")
            print(f"     Source: {fpath}")

            # Read and display the GPS data
            try:
                if fpath.lower().endswith(".csv"):
                    df_gps = pd.read_csv(fpath)
                else:
                    df_gps = pd.read_excel(fpath, sheet_name=sheet, engine="openpyxl")
                df_gps = df_gps.dropna(how="all")

                # Identify GPS columns
                df_gps.columns = [str(c).strip() for c in df_gps.columns]
                gps_col_lower = {c: c.lower().strip() for c in df_gps.columns}
                lat_disp = [c for c, cl in gps_col_lower.items() if cl in _LAT_ALL]
                lon_disp = [c for c, cl in gps_col_lower.items() if cl in _LON_ALL]

                print(f"     Shape: {df_gps.shape[0]:,} non-blank rows \u00d7 {df_gps.shape[1]} columns")
                print(f"     Lat: {', '.join(lat_disp)}  |  Lon: {', '.join(lon_disp)}")

                # Check for high-null columns
                null_pct = df_gps.isnull().mean() * 100
                high_null = null_pct[null_pct > 50]
                if not high_null.empty:
                    print(f"     Columns >50% null: {', '.join(high_null.index.tolist())}")
                print()
                display(df_gps.head(20))
            except Exception as ex:
                print(f"     \u274c Could not read: {str(ex)[:80]}")
            print()

    elif blank_entries:
        gps_missing_lanes.append(lane)
        print(f"  \U0001f534 GPS columns found but ALL BLANK:")
        for e in blank_entries:
            print(f"     \u2022 {e['file']} \u2192 {e['sheet']} ({e['total_rows']:,} rows, all null)")
    else:
        gps_missing_lanes.append(lane)
        print(f"  \u274c No files with GPS columns found for this lane")

# ── Summary ──
print(f"\n{'=' * 80}")
print(f"GPS LANE SUMMARY")
print(f"{'=' * 80}")
if gps_found_lanes:
    print(f"\n\U0001f7e2 GPS data available: {', '.join(sorted(gps_found_lanes))}")
    print(f"   \u2192 Ask extraction team to pick up GPS from these files")
if gps_missing_lanes:
    print(f"\n\U0001f534 GPS data missing: {', '.join(sorted(gps_missing_lanes))}")
    print(f"   \u2192 Need GPS device files or Google Drive data for these lanes")
if not gps_found_lanes and not gps_missing_lanes:
    print("\n\u2139\ufe0f  No GPS-related files found in backup")
print()

# COMMAND ----------

# DBTITLE 1,Generate consultant remark from Cell 7 verdicts
# ── Generate consultant remark using Cell 7 verdicts directly ──
import re as _re_msg

def _lane_tag(fpath):
    """Extract lane tag like L1, R2, LHS-L1, RHS-R2 from filename."""
    fname = os.path.basename(fpath)
    # Try to match LHS/RHS-L/R pattern first
    m = _re_msg.search(r'((?:LHS|RHS)\s*[-_]?\s*)?([LR]\d+)', fname, _re_msg.IGNORECASE)
    if m:
        prefix = (m.group(1) or "").strip().rstrip("-_")
        lane = m.group(2).upper()
        return f"{prefix}-{lane}".strip("-") if prefix else lane
    return os.path.splitext(fname)[0][:20]  # fallback: short filename

def _get_ext_projects(stage_name, target):
    """Find which Project folders have parquets for this target in extraction."""
    STAGE_MAP_LOCAL = {"Backup": "Source=NSV_Backup", "Deliverable": "Source=NSV"}
    src = STAGE_MAP_LOCAL.get(stage_name)
    if not src:
        return set()
    ext_base = os.path.join(
        "/Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output",
        src, f"Zone={dbutils.widgets.get('zone').strip()}",
        f"Phase={dbutils.widgets.get('phase').strip()}",
        f"Cycle={dbutils.widgets.get('cycle').strip()}",
        f"UPC={dbutils.widgets.get('upc').strip()}")
    projects = set()
    if not os.path.isdir(ext_base):
        return projects
    tgt = target.lower().replace("_", "")
    for r, _, fs in os.walk(ext_base):
        for f in fs:
            if tgt in f.lower().replace("_", "") and f.endswith(".parquet"):
                # Extract Project= value from path
                pm = _re_msg.search(r'Project=([^/]+)', r)
                if pm:
                    pname = pm.group(1)
                    projects.add("CC" if pname.upper().startswith("CC") else "BT")
    return projects


if "verdicts" not in dir() or not verdicts:
    print("\u26a0\ufe0f  Run Cell 7 first to get verdicts.")
elif "file_crits" not in dir() or file_crits.empty:
    print("\u2705 No CRITICALs \u2014 nothing to report.")
else:
    # Build enriched records from cell 7 verdicts + file_crits
    records = []
    for i, vd in enumerate(verdicts):
        cr = file_crits.iloc[i]
        records.append({
            "file": cr["file"],
            "stage": str(cr.get("stage", "")).strip(),
            "target": str(cr["table"]).strip(),
            "sheet_found": vd.get("Sheet Found", "\u2014"),
            "rows": vd.get("Rows", "\u2014"),
            "verdict": vd["Verdict"],
            "lane": _lane_tag(cr["file"]),
        })
    rdf = pd.DataFrame(records)

    # Categorise each verdict
    def _vcat(v):
        vl = v.lower()
        if "code issue" in vl:           return "code_issue"
        if "extracted" in vl:            return "extracted_ok"
        if "confirmed empty" in vl:      return "confirmed_empty"
        if "missing in download" in vl:  return "missing_dl"
        if "file missing" in vl:         return "file_missing"
        if "no lat/long" in vl:          return "no_latlong"
        return "other"
    rdf["vcat"] = rdf["verdict"].apply(_vcat)

    # Group by (stage, target) then sub-group by verdict category
    remarks = []
    for (stage, target), grp in rdf.groupby(["stage", "target"]):
        for vcat, sub in grp.groupby("vcat"):
            lanes = ", ".join(sub["lane"].unique())
            sheet = sub["sheet_found"].iloc[0]
            stage_l = stage.lower() if stage else "unknown"

            if target == "GPS_Geometry":
                # GPS remarks
                if vcat == "confirmed_empty":
                    remark = f"GPS Sheet is Empty in {stage_l} {lanes} | Lat-Long data is not in other sheets as well"
                elif vcat == "extracted_ok":
                    remark = f"GPS Sheet is Empty in {stage_l} {lanes} but GPS Parquet is present in extraction (please check why the critical came)"
                elif vcat == "no_latlong":
                    remark = f"GPS Sheet is Empty in {stage_l} {lanes} | Lat-Long data is not in other sheets as well"
                elif vcat == "code_issue":
                    remark = f"GPS data is present in download {lanes} but extraction failed \u2014 code issue"
                else:
                    remark = f"GPS issue in {stage_l} {lanes}"

            elif vcat == "code_issue":
                # Sheet found with data but extraction flagged critical
                projects = _get_ext_projects(stage, target)
                if sheet != "\u2014" and sheet.lower().strip() != target.lower().strip():
                    # Non-standard sheet name (e.g. RutDepth for Rutting)
                    if projects:
                        remark = (f"structure of sheet is different re-extraction required "
                                  f"after code updation {lanes} {target} "
                                  f"sheet name found as {sheet}")
                    else:
                        remark = (f"structure of sheet is different re-extraction required "
                                  f"after code updation {lanes} {target} "
                                  f"sheet name found as {sheet}")
                else:
                    if projects:
                        remark = (f"{target} data is present in download as well as "
                                  f"{target} parquet is present in extraction {lanes} "
                                  f"(please check why the critical came)")
                    else:
                        remark = (f"Re-Extraction required (download has {stage_l} data "
                                  f"but extraction is empty) {lanes} {target} data is "
                                  f"present in the {sheet} sheet itself")

            elif vcat == "extracted_ok":
                # Sheet not in download file but extraction parquets exist
                projects = _get_ext_projects(stage, target)
                if "CC" in projects and "BT" not in projects:
                    # Only CC has it — extracted from CC source
                    remark = (f"{target.lower()} not available in bt {lanes} but in cc "
                              f"its available so extracted from cc")
                elif "BT" in projects and "CC" not in projects:
                    # Only BT extraction has it (sheet missing but parquet exists)
                    remark = (f"{target.lower()} sheet not in {stage_l} download {lanes} "
                              f"but extraction parquet is present "
                              f"(please check why the critical came)")
                elif "CC" in projects and "BT" in projects:
                    # Both BT and CC extraction have it
                    remark = (f"{target.lower()} sheet not in {stage_l} download {lanes} "
                              f"but extraction parquet is present in both BT and CC "
                              f"(please check why the critical came)")
                else:
                    remark = (f"{target.lower()} not available in {stage_l} download {lanes} "
                              f"but extraction present")

            elif vcat == "confirmed_empty":
                remark = f"{target} sheet is empty in {stage_l} {lanes}"

            elif vcat == "missing_dl":
                projects = _get_ext_projects(stage, target)
                if projects:
                    remark = (f"{target.lower()} not available in {stage_l} download {lanes} "
                              f"but extraction parquet present "
                              f"(please check why the critical came)")
                else:
                    remark = f"{target.lower()} not available in {stage_l} {lanes}"

            elif vcat == "file_missing":
                remark = f"files not found in download {lanes} for {target}"

            else:
                remark = f"{target} issue in {stage_l} {lanes}"

            remarks.append(remark)

    print("=" * 80)
    print("CONSULTANT REMARK (copy below)")
    print("=" * 80)
    print()
    for r in remarks:
        print(r)
    print()
    print("=" * 80)