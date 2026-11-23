# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # GPS Missing Validation and Inspection
# MAGIC
# MAGIC Validate GPS data availability across backup and deliverable files for a given UPC. Inspects pipeline logs for **CRITICAL** GPS issues, cross-references the **Comparison as per CA** sheet chainages against LCMS backup GPS sources, and confirms whether GPS geometry data can be built.

# COMMAND ----------

# DBTITLE 1,Setup — install, imports and widgets
# MAGIC %pip install openpyxl -q
# MAGIC
# MAGIC import os, glob, re, datetime as _dt
# MAGIC import pandas as pd
# MAGIC from openpyxl import load_workbook
# MAGIC
# MAGIC # ── Paths ──
# MAGIC LOGS_DIR       = "/Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output/_logs"
# MAGIC EXTRACTION_BASE = "/Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output"
# MAGIC DOWNLOADS_BASE  = "/Volumes/nhai_dhms/nsv/nsv_downloads"
# MAGIC
# MAGIC # ── Input widgets (dropdowns for zone/phase/cycle, text for UPC) ──
# MAGIC dbutils.widgets.removeAll()
# MAGIC dbutils.widgets.dropdown("zone",  "E", ["A", "B", "C", "D", "E"], "Zone")
# MAGIC dbutils.widgets.dropdown("phase", "1", ["1", "2", "3"], "Phase")
# MAGIC dbutils.widgets.dropdown("cycle", "1", ["1", "2", "3"], "Cycle")
# MAGIC dbutils.widgets.text("upc", "", "UPC (e.g. N_02026_06003_AS)")
# MAGIC
# MAGIC # ── Helper: discover section-level subfolders under a UPC directory ──
# MAGIC def get_section_roots(upc_root):
# MAGIC     """Some UPCs have intermediate section folders (e.g. 'Nalbari-Bijni( AS-08)...')
# MAGIC     each containing backup/ and deliverable/. Others have them directly.
# MAGIC     Returns a list of root paths that contain backup and/or deliverable folders."""
# MAGIC     if not os.path.isdir(upc_root):
# MAGIC         return []
# MAGIC     # Check if backup or deliverable exist directly under upc_root
# MAGIC     direct = any(os.path.isdir(os.path.join(upc_root, d)) for d in ("backup", "deliverable"))
# MAGIC     if direct:
# MAGIC         return [upc_root]
# MAGIC     # Otherwise, look one level deeper for section subfolders
# MAGIC     sections = []
# MAGIC     for entry in sorted(os.listdir(upc_root)):
# MAGIC         sub = os.path.join(upc_root, entry)
# MAGIC         if os.path.isdir(sub):
# MAGIC             if any(os.path.isdir(os.path.join(sub, d)) for d in ("backup", "deliverable")):
# MAGIC                 sections.append(sub)
# MAGIC     return sections if sections else []

# COMMAND ----------

# DBTITLE 1,Log inspection — find CRITICALs and summarise issues
# \u2500\u2500 Helper functions + Log inspection \u2500\u2500

def find_upc_logs(zone, phase, cycle, upc, logs_dir=LOGS_DIR):
    """Find all pipeline validation log CSVs matching the widget values."""
    pattern = f"pipeline_validation_Zone{zone}_Phase{phase}_Cycle{cycle}_{upc}_*.csv"
    matched = glob.glob(os.path.join(logs_dir, pattern))
    if not matched:
        return []
    def _date(p):
        m = re.search(r'(\d{4}-\d{2}-\d{2})\.csv$', os.path.basename(p))
        return m.group(1) if m else "0000-00-00"
    matched.sort(key=_date, reverse=True)
    return matched

def get_criticals(csv_path):
    """Return only CRITICAL severity rows from a pipeline validation log."""
    df = pd.read_csv(csv_path)
    return df[df["severity"].str.upper() == "CRITICAL"].reset_index(drop=True)

# \u2500\u2500 Read widgets \u2500\u2500
ZONE  = dbutils.widgets.get("zone").strip()
PHASE = dbutils.widgets.get("phase").strip()
CYCLE = dbutils.widgets.get("cycle").strip()
UPC   = dbutils.widgets.get("upc").strip()

criticals_df = pd.DataFrame()   # reset for downstream cells

if not all([ZONE, PHASE, CYCLE, UPC]):
    print("\u26a0\ufe0f  Please fill in all four widgets: Zone, Phase, Cycle, UPC")
else:
    logs = find_upc_logs(ZONE, PHASE, CYCLE, UPC)

    if not logs:
        print(f"\u274c No log files found for Zone={ZONE}, Phase={PHASE}, Cycle={CYCLE}, UPC={UPC}")
    else:
        most_recent = logs[0]
        print(f"{'\u2550'*90}")
        print(f"\U0001f4c4 LOG INSPECTION  \u2014  Zone {ZONE} / Phase {PHASE} / Cycle {CYCLE} / {UPC}")
        print(f"{'\u2550'*90}")
        print(f"   Found {len(logs)} log file(s)  |  Using: {os.path.basename(most_recent)}\n")

        # Stage check
        _full_log = pd.read_csv(most_recent)
        _stages_in_log = _full_log["stage"].dropna().str.strip().unique().tolist()
        for s in ["Backup", "Deliverable"]:
            mark = "\u2705" if s in _stages_in_log else "\u274c"
            print(f"   {mark} {s} stage {'present' if s in _stages_in_log else 'MISSING \u2014 needs download'}")

        # CRITICALs
        criticals_df = get_criticals(most_recent)

        if criticals_df.empty:
            print(f"\n   \u2705 No CRITICAL entries \u2014 all clear!")
        else:
            n = len(criticals_df)
            # Build a plain-English summary of what's wrong
            targets = criticals_df["table"].dropna().unique().tolist()
            files_affected = criticals_df["file"].dropna().apply(os.path.basename).unique().tolist()
            lanes = set()
            for f in files_affected:
                m = re.search(r'([LR]\d+)', f, re.IGNORECASE)
                if m:
                    lanes.add(m.group(1).upper())
            lanes_str = ", ".join(sorted(lanes)) if lanes else "unknown lanes"

            print(f"\n{'\u2500'*90}")
            print(f"\U0001f534 {n} CRITICAL issue(s) found\n")
            print(f"   \U0001f4dd SUMMARY: The pipeline flagged {n} CRITICAL entries")
            print(f"      targeting {', '.join(targets)} across lanes {lanes_str}.")
            if "GPS_Geometry" in targets:
                print(f"      \u27a1\ufe0f  GPS sheets in the LCMS backup files are EMPTY \u2014 no lat/long data")
                print(f"         to build geometry. Cells below will check if alternative")
                print(f"         GPS sources exist and whether chainages align with the CA sheet.")
            print(f"{'\u2500'*90}\n")
            display(criticals_df)

# COMMAND ----------

# DBTITLE 1,Backup folder — detailed file listing
# \u2500\u2500 Backup folder \u2014 detailed file listing with icons \u2500\u2500
import os, datetime as _dt_c6
import pandas as pd

DOWNLOADS_BASE = "/Volumes/nhai_dhms/nsv/nsv_downloads"

# File-type icons for quick visual scanning
_ICON_MAP = {
    ".xlsx": "\U0001f4ca", ".xls": "\U0001f4ca", ".xlsm": "\U0001f4ca", ".xlsb": "\U0001f4ca",
    ".csv":  "\U0001f4cb", ".tsv": "\U0001f4cb",
    ".pdf":  "\U0001f4d5",
    ".doc":  "\U0001f4dd", ".docx": "\U0001f4dd",
    ".jpg":  "\U0001f5bc\ufe0f", ".jpeg": "\U0001f5bc\ufe0f", ".png": "\U0001f5bc\ufe0f", ".tif": "\U0001f5bc\ufe0f", ".tiff": "\U0001f5bc\ufe0f", ".bmp": "\U0001f5bc\ufe0f",
    ".zip":  "\U0001f4e6", ".rar": "\U0001f4e6", ".7z": "\U0001f4e6", ".gz": "\U0001f4e6",
    ".parquet": "\U0001f5c2\ufe0f", ".json": "\U0001f5c2\ufe0f",
    ".txt":  "\U0001f4c4",
    ".ppt":  "\U0001f4fd\ufe0f", ".pptx": "\U0001f4fd\ufe0f",
}

def _icon(fname):
    ext = os.path.splitext(fname)[1].lower()
    return _ICON_MAP.get(ext, "\U0001f4ce")

def _human_size(b):
    for unit in ["B", "KB", "MB", "GB"]:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"

zone_val  = dbutils.widgets.get("zone").strip()
phase_val = dbutils.widgets.get("phase").strip()
cycle_val = dbutils.widgets.get("cycle").strip()
upc_val   = dbutils.widgets.get("upc").strip()

if not all([zone_val, phase_val, cycle_val, upc_val]):
    print("\u26a0\ufe0f  Please fill in all four widgets first.")
else:
    upc_root = os.path.join(
        DOWNLOADS_BASE,
        f"Zone_{zone_val}", f"Phase_{phase_val}",
        f"Cycle_{cycle_val}", upc_val,
    )
    section_roots = get_section_roots(upc_root)

    if not section_roots:
        print(f"\u274c UPC folder not found or empty: {upc_root}")
    else:
        print(f"{'\u2550'*90}")
        print(f"\U0001f4c2 BACKUP FILES  \u2014  Zone {zone_val} / Phase {phase_val} / Cycle {cycle_val} / {upc_val}")
        print(f"{'\u2550'*90}")
        print(f"   \U0001f4c1 {len(section_roots)} section(s) found under UPC\n")

        file_rows = []
        total_size = 0
        folder_idx = 0

        for sec_root in section_roots:
            sec_name = os.path.basename(sec_root) if sec_root != upc_root else upc_val
            backup_root = os.path.join(sec_root, "backup")

            if not os.path.isdir(backup_root):
                print(f"   \u274c No backup/ folder in: {sec_name}")
                continue

            print(f"   \U0001f4d6 Section: {sec_name}")
            print(f"   \U0001f4cd {backup_root}\n")

            for root, dirs, files in os.walk(backup_root):
                rel = os.path.relpath(root, backup_root)
                depth = 0 if rel == "." else rel.count(os.sep) + 1
                folder_name = os.path.basename(root) if rel != "." else "backup"
                indent = "    " * (depth + 1)

                # Print folder header
                n_files = len(files)
                folder_idx += 1
                if n_files > 0 or dirs:
                    print(f"{indent}\U0001f4c1 {folder_name}/  ({n_files} files)")

                # Sort files by extension then name for grouped viewing
                for fname in sorted(files, key=lambda f: (os.path.splitext(f)[1].lower(), f.lower())):
                    fpath = os.path.join(root, fname)
                    try:
                        stat = os.stat(fpath)
                        fsize = stat.st_size
                        mtime = _dt_c6.datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M")
                    except OSError:
                        fsize = 0
                        mtime = "\u2014"

                    total_size += fsize
                    icon = _icon(fname)
                    ext  = os.path.splitext(fname)[1].lower() or "(none)"
                    print(f"{indent}    {icon} {fname}   [{_human_size(fsize)}, {mtime}]")

                    file_rows.append({
                        "Icon":     icon,
                        "Section":  sec_name,
                        "File Name": fname,
                        "Type":     ext,
                        "Size":     _human_size(fsize),
                        "Modified": mtime,
                        "Subfolder": rel if rel != "." else "(root)",
                    })

            print()  # blank line between sections

        # \u2500\u2500 Summary \u2500\u2500
        ext_counts = {}
        for r in file_rows:
            ext_counts[r["Type"]] = ext_counts.get(r["Type"], 0) + 1

        print(f"\n{'\u2500'*90}")
        print(f"\U0001f4ca BACKUP SUMMARY")
        print(f"{'\u2500'*90}")
        print(f"   Total files : {len(file_rows)}")
        print(f"   Total size  : {_human_size(total_size)}")
        print(f"   Folders     : {folder_idx}")
        print(f"\n   By type:")
        for ext, cnt in sorted(ext_counts.items(), key=lambda x: -x[1]):
            sample_icon = _ICON_MAP.get(ext, "\U0001f4ce")
            print(f"     {sample_icon}  {ext:<10} \u2192 {cnt} file(s)")

        print(f"{'\u2500'*90}\n")
        display(pd.DataFrame(file_rows))

# COMMAND ----------

# DBTITLE 1,Deliverable D2 — Comparison as per CA sheet
# ── Deliverable D2: open "Comparison as per CA" sheet and display chainage data ──

zone_v  = dbutils.widgets.get("zone").strip()
phase_v = dbutils.widgets.get("phase").strip()
cycle_v = dbutils.widgets.get("cycle").strip()
upc_v   = dbutils.widgets.get("upc").strip()

ca_df = pd.DataFrame()  # will hold chainage data for Cell 6
all_ca_dfs = []         # collect CA data from all sections

if not all([zone_v, phase_v, cycle_v, upc_v]):
    print("⚠️  Fill in all four widgets first.")
else:
    upc_root = os.path.join(DOWNLOADS_BASE, f"Zone_{zone_v}", f"Phase_{phase_v}",
                            f"Cycle_{cycle_v}", upc_v)
    section_roots = get_section_roots(upc_root)

    if not section_roots:
        print(f"❌  UPC folder not found or empty: {upc_root}")

    # Search for CA comparison files across all section roots
    ca_files = []  # list of (section_name, file_path)
    for sec_root in section_roots:
        sec_name = os.path.basename(sec_root) if sec_root != upc_root else upc_v
        deliverable_root = os.path.join(sec_root, "deliverable")
        if not os.path.isdir(deliverable_root):
            continue
        # Search entire deliverable tree for Comparison CA xlsx
        for root, _, files in os.walk(deliverable_root):
            for f in files:
                if "compar" in f.lower() and f.endswith(".xlsx"):
                    ca_files.append((sec_name, os.path.join(root, f)))

    if not ca_files:
        print("❌  No 'Comparison as per CA' file found in any section's Deliverable folder")
    else:
        print(f"{'='*90}")
        print(f"📊 DELIVERABLE D2 — Comparison as per CA")
        print(f"{'='*90}")
        print(f"   Found {len(ca_files)} CA file(s) across {len(section_roots)} section(s)\n")

        for file_idx, (sec_name, ca_file) in enumerate(ca_files, 1):
            print(f"   {'─'*80}")
            print(f"   📖 Section: {sec_name}")
            print(f"   📄 {os.path.basename(ca_file)}")
            print(f"   📍 {ca_file}\n")

            wb = load_workbook(ca_file, read_only=True, data_only=True)
            print(f"   📋 Sheets in file: {wb.sheetnames}\n")

            # Look for the comparison sheet — try "compar"+"ca" first, fall back to
            # "compar"+"irc" or "repair"+"maintenance"+"ca", since naming varies
            ca_sheet = None
            compar_sheets = [sn for sn in wb.sheetnames if "compar" in sn.lower()]
            ca_candidates  = [sn for sn in compar_sheets if "ca" in sn.lower()]
            irc_candidates = [sn for sn in compar_sheets if "irc" in sn.lower()]
            repair_ca      = [sn for sn in wb.sheetnames if "repair" in sn.lower() and "ca" in sn.lower()]

            if ca_candidates:
                ca_sheet = ca_candidates[0]
            elif irc_candidates:
                ca_sheet = irc_candidates[0]
            elif repair_ca:
                ca_sheet = repair_ca[0]
            elif compar_sheets:
                ca_sheet = compar_sheets[0]

            if not ca_sheet:
                print(f"   ❌ No comparison sheet found. Available: {wb.sheetnames}")
                wb.close()
                continue

            print(f"   ➡️  Opening sheet: '{ca_sheet}'\n")
            ws = wb[ca_sheet]

            # Read all rows into a list of lists
            all_rows = []
            for row in ws.iter_rows(values_only=True):
                all_rows.append(list(row))

            if len(all_rows) < 2:
                print("   ⚠️  Sheet is empty or has only headers")
                wb.close()
                continue

            # Use row 1 as headers — pad to widest row and deduplicate names
            max_cols = max(len(r) for r in all_rows)
            hdr_raw = all_rows[0]
            headers = [str(h).strip() if h else f"Col_{i}" for i, h in enumerate(hdr_raw)]
            headers += [f"Col_{i}" for i in range(len(headers), max_cols)]
            # Deduplicate column names by appending _2, _3, etc.
            seen = {}
            deduped = []
            for h in headers:
                if h in seen:
                    seen[h] += 1
                    deduped.append(f"{h}_{seen[h]}")
                else:
                    seen[h] = 1
                    deduped.append(h)
            headers = deduped
            data_rows = [r + [None] * (max_cols - len(r)) for r in all_rows[1:]]

            section_df = pd.DataFrame(data_rows, columns=headers)
            section_df.dropna(how="all", inplace=True)
            section_df["_section"] = sec_name  # tag with section name

            # Identify start/end chainage columns
            chain_start_col = None
            chain_end_col = None
            for c in section_df.columns:
                cl = c.lower()
                if "start" in cl and "chain" in cl:
                    chain_start_col = c
                elif "end" in cl and "chain" in cl:
                    chain_end_col = c

            print(f"   📌 Columns found: {list(section_df.columns[:8])} ... ({len(section_df.columns)} total)")
            if chain_start_col and chain_end_col:
                print(f"   ✅ Start Chainage column: '{chain_start_col}'")
                print(f"   ✅ End Chainage column  : '{chain_end_col}'")

                section_df[chain_start_col] = pd.to_numeric(section_df[chain_start_col], errors="coerce")
                section_df[chain_end_col]   = pd.to_numeric(section_df[chain_end_col], errors="coerce")
                valid = section_df.dropna(subset=[chain_start_col, chain_end_col])

                if not valid.empty:
                    ch_min = valid[chain_start_col].min()
                    ch_max = valid[chain_end_col].max()
                    print(f"\n   📍 CA Chainage Range: {ch_min:,.0f} m  →  {ch_max:,.0f} m")
                    print(f"   📊 {len(valid)} data rows with valid chainage\n")
            else:
                print(f"   ⚠️  Could not auto-detect Start/End Chainage columns")

            all_ca_dfs.append(section_df)

            # Display section data
            ca_display = section_df.drop(columns=["_section"]).dropna(axis=1, how="all").astype(str).replace("None", "")
            display(ca_display)

            wb.close()

        # Merge all sections into ca_df for downstream cells
        if all_ca_dfs:
            ca_df = pd.concat(all_ca_dfs, ignore_index=True)
            print(f"\n{'─'*90}")
            print(f"   📊 COMBINED: {len(ca_df)} total rows from {len(all_ca_dfs)} section(s)")
            # Show combined chainage range
            for c in ca_df.columns:
                cl = c.lower()
                if "start" in cl and "chain" in cl:
                    ca_df[c] = pd.to_numeric(ca_df[c], errors="coerce")
                elif "end" in cl and "chain" in cl:
                    ca_df[c] = pd.to_numeric(ca_df[c], errors="coerce")
            chain_start_col = next((c for c in ca_df.columns if "start" in c.lower() and "chain" in c.lower()), None)
            chain_end_col = next((c for c in ca_df.columns if "end" in c.lower() and "chain" in c.lower()), None)
            if chain_start_col and chain_end_col:
                combined_valid = ca_df.dropna(subset=[chain_start_col, chain_end_col])
                if not combined_valid.empty:
                    print(f"   📍 Combined CA Chainage Range: {combined_valid[chain_start_col].min():,.0f} m  →  {combined_valid[chain_end_col].max():,.0f} m")
            print(f"{'─'*90}")

# COMMAND ----------

# DBTITLE 1,GPS chainage match — all backup files vs CA sheet
# ── GPS Chainage Match: scan ALL backup files for GPS data, compare with CA chainages ──
import re as _re_gps

_LAT_KW  = {"latitude", "lat", "gps_latitude", "start_lat", "start_latitude", "northing"}
_LON_KW  = {"longitude", "long", "lng", "lon", "gps_longitude", "start_long", "start_longitude", "easting"}

def _detect_lane(fpath):
    m = _re_gps.search(r'\b([LR]\d+)\b', os.path.basename(fpath).upper())
    return m.group(1) if m else "UNKNOWN"

def _scan_csv_for_gps(fpath, ca_min=None, ca_max=None):
    """Scan a CSV file for lat/long + chainage columns. Returns info dict or None."""
    try:
        df = pd.read_csv(fpath, nrows=0)  # headers only first
    except Exception:
        return None
    cols_lower = {c: c.lower().strip() for c in df.columns}
    has_lat = any(any(kw in cl for kw in _LAT_KW) for cl in cols_lower.values())
    has_lon = any(any(kw in cl for kw in _LON_KW) for cl in cols_lower.values())
    if not (has_lat and has_lon):
        return None
    # Read full file
    try:
        df = pd.read_csv(fpath)
    except Exception:
        return None
    # Find lat column and count filled rows
    lat_col = next((c for c, cl in cols_lower.items() if any(kw in cl for kw in _LAT_KW)), None)
    if lat_col:
        lat_vals = pd.to_numeric(df[lat_col], errors="coerce")
        lat_filled = int((lat_vals.notna() & (lat_vals != 0)).sum())
    else:
        lat_filled = 0
    # Find chainage column
    chain_col = next((c for c, cl in cols_lower.items()
                      if "chainage" in cl or ("chain" in cl and len(cl) > 4)), None)
    best_min, best_max, best_lbl = None, None, None
    if chain_col:
        chain_vals = pd.to_numeric(df[chain_col], errors="coerce").dropna()
        if not chain_vals.empty:
            best_min, best_max = float(chain_vals.min()), float(chain_vals.max())
            best_lbl = chain_col
    return {
        "sheet": "(CSV)",
        "data_rows_sampled": len(df),
        "lat_filled": lat_filled,
        "chain_cols": [(0, best_lbl, best_min, best_max)] if best_lbl and best_min is not None else [],
        "best_chain_min": best_min,
        "best_chain_max": best_max,
        "best_chain_col": best_lbl,
        "total_rows": len(df),
    }

def _scan_sheet_for_gps(wb, sheet_name, ca_min=None, ca_max=None, max_scan=None):
    """Scan a sheet for lat/long + chainage columns (multi-row headers, substring match).
    When multiple chainage column pairs exist, pick the one whose values
    overlap with the CA range.  max_scan=None → scan ALL data rows."""
    ws = wb[sheet_name]

    # --- Collect ALL header texts across first 5 rows (handles multi-row headers) ---
    hdr_texts = {}          # col_idx -> list of text values from rows 1-5
    hdr_row_idx = None       # the row that contains lat/long headers
    for ri, row in enumerate(ws.iter_rows(min_row=1, max_row=5, values_only=True), 1):
        if row is None:
            continue
        for ci, v in enumerate(row):
            if v is not None:
                txt = str(v).lower().strip()
                hdr_texts.setdefault(ci, []).append(txt)
                if any(kw in txt for kw in _LAT_KW):
                    hdr_row_idx = ri  # remember which row has the lat header

    # Flatten all header texts for quick keyword search
    flat = [(ci, t) for ci, ts in hdr_texts.items() for t in ts]

    has_lat = any(any(kw in t for kw in _LAT_KW) for _, t in flat)
    has_lon = any(any(kw in t for kw in _LON_KW) for _, t in flat)
    if not (has_lat and has_lon):
        return None

    # --- Identify lat column index ---
    lat_idx = next((ci for ci, t in flat if any(kw in t for kw in _LAT_KW)), None)

    # --- Identify ALL chainage columns (substring match on 'chainage' or 'chain') ---
    chain_cols = []  # list of (col_idx, header_label)
    for ci, t in flat:
        if "chainage" in t or ("chain" in t and len(t) > 4):
            chain_cols.append((ci, t))

    # --- Deduplicate: keep unique col indices, prefer "start chainage" over generic ---
    seen_ci = set()
    unique_chain = []
    for ci, t in chain_cols:
        if ci not in seen_ci:
            seen_ci.add(ci)
            unique_chain.append((ci, t))

    # --- Sample data rows ---
    start_row = (hdr_row_idx + 1) if hdr_row_idx else 2
    data_rows = 0
    lat_filled = 0
    col_values = {ci: [] for ci, _ in unique_chain}  # chainages per col

    iter_kw = {"min_row": start_row, "values_only": True}
    if max_scan is not None:
        iter_kw["max_row"] = start_row + max_scan - 1
    for row in ws.iter_rows(**iter_kw):
        if row is None:
            continue
        nns = [v for v in row if v is not None]
        if len(nns) < 2:
            continue
        data_rows += 1
        # Count lat filled
        if lat_idx is not None and lat_idx < len(row) and row[lat_idx] is not None:
            try:
                if float(row[lat_idx]) != 0:
                    lat_filled += 1
            except (ValueError, TypeError):
                pass
        # Collect chainage values per column
        for ci, _ in unique_chain:
            if ci < len(row) and row[ci] is not None:
                try:
                    col_values[ci].append(float(row[ci]))
                except (ValueError, TypeError):
                    pass

    # --- Pick the best chainage column: prefer the one overlapping with CA range ---
    best_ci = None
    best_min = None
    best_max = None
    for ci, lbl in unique_chain:
        vals = col_values[ci]
        if not vals:
            continue
        cmin, cmax = min(vals), max(vals)
        if ca_min is not None and ca_max is not None:
            overlap_s = max(cmin, ca_min)
            overlap_e = min(cmax, ca_max)
            if overlap_s <= overlap_e:  # overlaps with CA → pick this one
                best_ci, best_min, best_max = ci, cmin, cmax
                break
        if best_ci is None:  # fallback: first column with data
            best_ci, best_min, best_max = ci, cmin, cmax

    return {
        "sheet": sheet_name,
        "data_rows_sampled": data_rows,
        "lat_filled": lat_filled,
        "chain_cols": [(ci, lbl, min(col_values[ci]) if col_values[ci] else None,
                        max(col_values[ci]) if col_values[ci] else None)
                       for ci, lbl in unique_chain if col_values[ci]],
        "best_chain_min": best_min,
        "best_chain_max": best_max,
        "best_chain_col": next((lbl for ci2, lbl in unique_chain if ci2 == best_ci), None) if best_ci is not None else None,
        "total_rows": ws.max_row or 0,
    }

# ── Main ──
zone_v  = dbutils.widgets.get("zone").strip()
phase_v = dbutils.widgets.get("phase").strip()
cycle_v = dbutils.widgets.get("cycle").strip()
upc_v   = dbutils.widgets.get("upc").strip()

if not all([zone_v, phase_v, cycle_v, upc_v]):
    print("⚠️  Fill in all four widgets first.")
elif "ca_df" not in dir() or ca_df.empty:
    print("⚠️  Run the previous cell first to load the CA comparison sheet.")
else:
    upc_root = os.path.join(DOWNLOADS_BASE, f"Zone_{zone_v}", f"Phase_{phase_v}",
                             f"Cycle_{cycle_v}", upc_v)
    section_roots = get_section_roots(upc_root)

    # Get CA chainage range from previous cell
    ca_chain_min, ca_chain_max = None, None
    for c in ca_df.columns:
        cl = c.lower()
        if "start" in cl and "chain" in cl:
            vals = pd.to_numeric(ca_df[c], errors="coerce").dropna()
            if not vals.empty:
                ca_chain_min = vals.min()
        elif "end" in cl and "chain" in cl:
            vals = pd.to_numeric(ca_df[c], errors="coerce").dropna()
            if not vals.empty:
                ca_chain_max = vals.max()

    print(f"{'='*90}")
    print(f"📍 GPS CHAINAGE MATCH — LCMS Backup vs CA Sheet")
    print(f"{'='*90}")
    if ca_chain_min is not None and ca_chain_max is not None:
        print(f"   📊 CA Sheet chainage range: {ca_chain_min:,.0f} m  →  {ca_chain_max:,.0f} m")
    else:
        print("   ⚠️  Could not determine CA chainage range")
    print()

    # Scan ALL xlsx/xls files recursively in backup across all sections
    all_xlsx = []
    for sec_root in section_roots:
        backup_root = os.path.join(sec_root, "backup")
        if os.path.isdir(backup_root):
            for root, _, files in os.walk(backup_root):
                for f in files:
                    if f.lower().endswith((".xlsx", ".xls", ".csv")):
                        all_xlsx.append(os.path.join(root, f))
    all_xlsx.sort()

    if not section_roots:
        print(f"   ❌ UPC folder not found or empty: {upc_root}")
    elif not all_xlsx:
        print("   ❌ No Excel files found in backup across any section")
    else:
        print(f"   🔍 Scanning ALL {len(all_xlsx)} Excel files in backup for GPS data...\n")
        results = []

        for fpath in all_xlsx:
            fname = os.path.basename(fpath)
            lane = _detect_lane(fpath)

            # Skip workbook open for CSV files — handled separately below
            if fname.lower().endswith(".csv"):
                wb = None
            else:
                try:
                    wb = load_workbook(fpath, read_only=True, data_only=True)
                except Exception as _open_err:
                    print(f"   {'\u2500'*80}")
                    print(f"   \U0001f4ca {fname}  |  Lane: {lane}")
                    print(f"      \u26a0\ufe0f  Skipped \u2014 could not open file: {type(_open_err).__name__}")
                    results.append({
                        "\U0001f4ca File": fname, "\U0001f6a6 Lane": lane, "\U0001f4c4 Sheet": "\u2014",
                        "\U0001f4cd Lat/Long": "\u26a0\ufe0f Unreadable file",
                        "\U0001f4ca Total Rows": "0",
                        "Best Chain Col": "\u2014",
                        "Chain Min": "\u2014", "Chain Max": "\u2014",
                        "CA Match": "\u26a0\ufe0f UNREADABLE",
                        "\U0001f4c2 File Path": fpath,
                    })
                    print()
                    continue

            print(f"   {'\u2500'*80}")
            print(f"   📊 {fname}  |  Lane: {lane}")

            found_gps = False
            # --- CSV files: use pandas scanner ---
            if fname.lower().endswith(".csv"):
                info = _scan_csv_for_gps(fpath, ca_min=ca_chain_min, ca_max=ca_chain_max)
                if info is not None:
                    found_gps = True
                    has_data = info["lat_filled"] > 0
                    icon = "✅" if has_data else "❌"
                    print(f"      {icon} CSV: "
                          f"{info['lat_filled']}/{info['data_rows_sampled']} rows with lat/long data")
                    ca_match = False
                    if info["best_chain_min"] is not None:
                        bmin, bmax = info["best_chain_min"], info["best_chain_max"]
                        print(f"         ➡️  Chainage col '{info['best_chain_col']}': "
                              f"{bmin:,.0f} → {bmax:,.0f} m")
                        if ca_chain_min is not None and ca_chain_max is not None:
                            ov_s = max(bmin, ca_chain_min)
                            ov_e = min(bmax, ca_chain_max)
                            if ov_s <= ov_e:
                                pct = (ov_e - ov_s) / (ca_chain_max - ca_chain_min) * 100
                                print(f"         ✅ MATCH — overlaps with CA range ({pct:.1f}% coverage)")
                                ca_match = True
                            else:
                                print(f"         ❌ NO MATCH — chainage does NOT overlap with CA range")
                    match_lbl = "✅ MATCH" if ca_match else ("❌ EMPTY" if not has_data else "❌ NO MATCH")
                    ll_status = f"✅ Yes ({info['lat_filled']:,} rows filled)" if has_data else "❌ Columns exist but EMPTY"
                    results.append({
                        "📊 File": fname, "🚦 Lane": lane, "📄 Sheet": "(CSV)",
                        "📍 Lat/Long": ll_status,
                        "📊 Total Rows": f"{info['data_rows_sampled']:,}",
                        "Best Chain Col": info["best_chain_col"] or "—",
                        "Chain Min": f"{info['best_chain_min']:,.0f}" if info["best_chain_min"] else "—",
                        "Chain Max": f"{info['best_chain_max']:,.0f}" if info["best_chain_max"] else "—",
                        "CA Match": match_lbl,
                        "📂 File Path": fpath,
                    })
                if not found_gps:
                    print(f"      ❌ No lat/long columns found in CSV")
                    results.append({
                        "📊 File": fname, "🚦 Lane": lane, "📄 Sheet": "—",
                        "📍 Lat/Long": "— No lat/long columns",
                        "📊 Total Rows": "0",
                        "Best Chain Col": "—", "Chain Min": "—", "Chain Max": "—",
                        "CA Match": "❌ NONE",
                        "📂 File Path": fpath,
                    })
                wb = None  # skip xlsx loop
                print()
                continue

            for sn in wb.sheetnames:
                info = _scan_sheet_for_gps(wb, sn, ca_min=ca_chain_min, ca_max=ca_chain_max)
                if info is None:
                    continue

                found_gps = True
                has_data = info["lat_filled"] > 0
                icon = "✅" if has_data else "❌"
                print(f"      {icon} Sheet '{info['sheet']}': "
                      f"{info['lat_filled']}/{info['data_rows_sampled']} rows with lat/long data "
                      f"(~{info['total_rows']:,} total rows)")

                # Show all chainage columns found
                if info["chain_cols"]:
                    for ci, lbl, cmin, cmax in info["chain_cols"]:
                        print(f"         Col {chr(65+ci)} '{lbl}': {cmin:,.0f} → {cmax:,.0f} m")

                # Match best chainage column against CA range
                ca_match = False
                if info["best_chain_min"] is not None:
                    bmin, bmax = info["best_chain_min"], info["best_chain_max"]
                    print(f"         ➡️  Best chainage col '{info['best_chain_col']}': "
                          f"{bmin:,.0f} → {bmax:,.0f} m")
                    if ca_chain_min is not None and ca_chain_max is not None:
                        ov_s = max(bmin, ca_chain_min)
                        ov_e = min(bmax, ca_chain_max)
                        if ov_s <= ov_e:
                            pct = (ov_e - ov_s) / (ca_chain_max - ca_chain_min) * 100
                            print(f"         ✅ MATCH — overlaps with CA range ({pct:.1f}% coverage)")
                            ca_match = True
                        else:
                            print(f"         ❌ NO MATCH — chainage does NOT overlap with CA range")
                elif not info["chain_cols"]:
                    print(f"         ⚠️  No chainage columns found in this sheet")

                match_lbl = "✅ MATCH" if ca_match else ("❌ EMPTY" if not has_data else "❌ NO MATCH")
                if has_data:
                    ll_status = f"✅ Yes ({info['lat_filled']:,} rows filled)"
                else:
                    ll_status = "❌ Columns exist but EMPTY"
                results.append({
                    "📊 File": fname,
                    "🚦 Lane": lane,
                    "📄 Sheet": info["sheet"],
                    "📍 Lat/Long": ll_status,
                    "📊 Total Rows": f"{info['data_rows_sampled']:,}",
                    "Best Chain Col": info["best_chain_col"] or "—",
                    "Chain Min": f"{info['best_chain_min']:,.0f}" if info["best_chain_min"] else "—",
                    "Chain Max": f"{info['best_chain_max']:,.0f}" if info["best_chain_max"] else "—",
                    "CA Match": match_lbl,
                    "📂 File Path": fpath,
                })

            if not found_gps:
                print(f"      ❌ No sheets with lat/long columns found")
                results.append({
                    "📊 File": fname, "🚦 Lane": lane, "📄 Sheet": "—",
                    "📍 Lat/Long": "— No lat/long columns",
                    "📊 Total Rows": "0",
                    "Best Chain Col": "—", "Chain Min": "—", "Chain Max": "—",
                    "CA Match": "❌ NONE",
                    "📂 File Path": fpath,
                })

            if wb is not None:
                wb.close()
            print()

        # Summary table
        print(f"\n{'='*90}")
        print(f"📊 GPS CHAINAGE MATCH SUMMARY")
        print(f"{'='*90}\n")

        matched  = sum(1 for r in results if "MATCH" in r["CA Match"] and "✅" in r["CA Match"])
        empty    = sum(1 for r in results if "EMPTY" in r["CA Match"])
        no_match = sum(1 for r in results if ("NO MATCH" in r["CA Match"] or "NONE" in r["CA Match"]) and "✅" not in r["CA Match"])

        print(f"   ✅ Matched with CA chainages : {matched}")
        print(f"   ❌ Empty GPS (no data)       : {empty}")
        print(f"   ❌ No chainage overlap       : {no_match}")

        if empty > 0 and matched == 0:
            print(f"\n   🚨 VERDICT: GPS sheets are EMPTY across all lanes.")
            print(f"      The GPS data is missing from the backup download.")
            print(f"      ➡️  Check Google Drive or request GPS data from the survey team.")
        elif no_match > 0 and matched == 0:
            print(f"\n   🚨 VERDICT: GPS data exists but chainages do NOT match the CA range.")
            print(f"      The GPS data cannot be used for this UPC section.")
        elif matched > 0:
            print(f"\n   ✅ VERDICT: GPS data found with matching chainages for {matched} source(s).")

        print(f"{'='*90}\n")
        display(pd.DataFrame(results))