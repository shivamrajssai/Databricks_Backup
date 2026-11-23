# Databricks notebook source
# MAGIC %md
# MAGIC ###Step 1 - Install dependencies

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install openpyxl -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Step 2 Description
# MAGIC %md
# MAGIC ###Step 2 - Extract GPS data from Bronze Layer (DATABRICKS)
# MAGIC
# MAGIC **Input:**  
# MAGIC - Get this sheet from vipin - Survey list: `Input/NSV_1202_Suveys 3.xlsx` (sheet `NSV_1201`) - contains Zone, Phase, Cycle, UPC for each survey  
# MAGIC - Bronze parquet files: `/Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output/`
# MAGIC
# MAGIC **Process:**  
# MAGIC For each UPC in the survey list, navigates the bronze layer folder structure (`Zone → Phase → Cycle → UPC → latest date → Lane L1`) and reads the GPS parquet file. Extracts the **min Start Chainage** and **max End Chainage** along with their corresponding Lat/Long coordinates.
# MAGIC
# MAGIC **Output:**  
# MAGIC `Output/Step_02_GPS_Extent_Raw.csv` - Full survey list with appended GPS columns (Start/End Chainage, Start/End Lat/Long, L1 File Path) 
# MAGIC

# COMMAND ----------

import os, re
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
 
# ═══════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════
INPUT_XLSX  = "/Workspace/Users/shivam.raj9226@softsensor.ai/Extraction Logs (lat Long)/Input/NSV_1202_Suveys 3.xlsx"
INPUT_SHEET = "NSV_1201"
OUT_CSV     = "/Workspace/Users/shivam.raj9226@softsensor.ai/Extraction Logs (lat Long)/Output/Step_02_GPS_Extent_Raw.csv"
BRONZE_BASE = "/Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output"
 
LANE       = "L1"     # read GPS from this lane only
LIMIT_ROWS = None     # <-- 10 for a test run, None for everything
VERBOSE    = False    # <-- False for the full run
SAVE_CSV   = True
 
# ═══════════════════════════════════════════════════════════════════════════
def _lp(p):
    s = str(p)
    if os.name == "nt":
        s = os.path.abspath(s)
        if not s.startswith("\\\\?\\"): s = "\\\\?\\" + s
    return s
 
def subdirs(path: Path):
    lp = _lp(str(path))
    if not Path(lp).is_dir(): return []
    return sorted(path / n for n in os.listdir(lp)
                  if Path(_lp(str(path / n))).is_dir())
 
def clean(v):
    if v is None: return ""
    s = str(v).strip()
    return "" if s.lower() in ("", "nan", "none") else s
 
def digits_only(v):
    s = clean(v); d = "".join(c for c in s if c.isdigit())
    return d if d else s
 
def kv(name):
    return tuple(name.split("=", 1)) if "=" in name else (name, None)
 
def _pick_name(names, *cands):
    norm = {re.sub(r'[^a-z0-9]', '', str(c).lower()): c for c in names}
    for cand in cands:
        k = re.sub(r'[^a-z0-9]', '', cand.lower())
        if k in norm: return norm[k]
    return None
 
# ═══════════════════════════════════════════════════════════════════════════
GPS_COL_NAMES = ["Start Chainage", "End Chainage",
                 "Start Lat", "Start Long", "End Lat", "End Long"]
OUT_COLS      = GPS_COL_NAMES + ["L1 File Path"]
 
GPS_FAILS = {"no_upc_dir": 0, "no_date": 0, "no_l1_file": 0,
             "read_error": 0, "no_chainage_col": 0, "empty_after_clean": 0, "other": 0}
GPS_ERRORS = []
_GPS_CACHE = {}
 
def _latest_date_dir(upc_base: Path):
    best = None
    for y in subdirs(upc_base):
        if not y.name.startswith("year="): continue
        for mo in subdirs(y):
            if not mo.name.startswith("month="): continue
            for dy in subdirs(mo):
                if not dy.name.startswith("day="): continue
                try:
                    k = (int(kv(y.name)[1]), int(kv(mo.name)[1]), int(kv(dy.name)[1]))
                except (TypeError, ValueError):
                    continue
                if best is None or k > best[0]:
                    best = (k, dy)
    return best[1] if best else None
 
def _find_one_l1_gps(day_dir: Path, max_depth=8):
    """Return (first_L1_gps_path_or_None, lanes_seen, total_L1_files).
    Stops as soon as one Lane=<LANE> GPS parquet is found — no concatenation."""
    found, lanes_seen, n_l1 = [None], set(), [0]
 
    def _walk(p: Path, lane, depth=0):
        if depth > max_depth: return
        try: entries = sorted(os.listdir(_lp(str(p))))
        except Exception: return
        for e in entries:
            q = p / e
            if Path(_lp(str(q))).is_dir():
                k, v = kv(e)
                _walk(q, v if k.lower() == "lane" else lane, depth + 1)
            elif e.lower().endswith(".parquet"):
                n = e.lower()
                if ("gps" in n or "geometry" in n) and "unified_segments" not in n:
                    if lane: lanes_seen.add(lane)
                    if lane and lane.strip().upper() == LANE.upper():
                        n_l1[0] += 1
                        if found[0] is None:
                            found[0] = str(q)      # keep only the FIRST one
 
    _walk(day_dir, None)
    return found[0], sorted(lanes_seen), n_l1[0]
 
def gps_extent(zone, phase, cycle, upc, verbose=False):
    """Read ONE L1 GPS parquet and take:
         first = row with MIN Start_Chainage -> Start Chainage/Lat/Long
         last  = row with MAX End_Chainage   -> End Chainage/Lat/Long
       Lat/long always come from the SAME row as their chainage."""
    ck = (zone, phase, cycle, upc)
    if ck in _GPS_CACHE: return _GPS_CACHE[ck]
    blank = {k: "" for k in OUT_COLS}
 
    def _done(res, reason=None):
        if reason: GPS_FAILS[reason] += 1
        _GPS_CACHE[ck] = res
        return res
 
    try:
        upc_base = (Path(BRONZE_BASE) / "Source=NSV_Backup" / f"Zone={zone}" /
                    f"Phase={phase}" / f"Cycle={cycle}" / f"UPC={upc}")
        if not Path(_lp(str(upc_base))).is_dir():
            if verbose: print("      no UPC folder in bronze")
            return _done(blank, "no_upc_dir")
 
        day_dir = _latest_date_dir(upc_base)
        if day_dir is None:
            if verbose: print("      no date partition")
            return _done(blank, "no_date")
        if verbose: print(f"      date: {day_dir.name}")
 
        fpath, lanes, n_l1 = _find_one_l1_gps(day_dir)
        if verbose:
            extra = f"  (using 1 of {n_l1})" if n_l1 > 1 else ""
            print(f"      lanes present: {lanes} | {LANE} files: {n_l1}{extra}")
        if fpath is None:
            return _done(blank, "no_l1_file")
        if verbose: print(f"      file: {Path(fpath).name}")
 
        # ── read that ONE file (no concat) ───────────────────────────────────
        # 'Lane' is excluded on purpose: it is dictionary-encoded in some row
        # groups and plain string in others -> pyarrow ArrowTypeError on merge.
        try:
            pf    = pq.ParquetFile(fpath)
            names = pf.schema_arrow.names
            # GPS_Geometry files:       Start_Chainage / End_Chainage / Start_Latitude / …
            # LCMS_L1_Raw_Geometry files: BgnLogm / EndLogm / GPSLat / GPSLng
            sc = _pick_name(names, 'Start_Chainage', 'StartChainage', 'start chainage', 'BgnLogm')
            ec = _pick_name(names, 'End_Chainage', 'EndChainage', 'end chainage', 'EndLogm')
            if not sc or not ec:
                if len(GPS_ERRORS) < 5:
                    GPS_ERRORS.append(f"no chainage col; have: {list(names)[:12]}")
                return _done({**blank, "L1 File Path": fpath}, "no_chainage_col")
            want = [c for c in [sc, ec,
                _pick_name(names, 'Start_Latitude',  'StartLat',  'start latitude',  'GPSLat'),
                _pick_name(names, 'Start_Longitude', 'StartLong', 'start longitude', 'GPSLng'),
                _pick_name(names, 'End_Latitude',    'EndLat',    'end latitude',    'GPSLat'),
                _pick_name(names, 'End_Longitude',   'EndLong',   'end longitude',   'GPSLng'),
            ] if c]
            g = pf.read(columns=want).to_pandas(timestamp_as_object=True)
        except Exception as exc:
            if len(GPS_ERRORS) < 5:
                GPS_ERRORS.append(f"{Path(fpath).name}: {type(exc).__name__}: {exc}")
            return _done({**blank, "L1 File Path": fpath}, "read_error")
 
        c_sc   = _pick_name(g.columns, 'Start_Chainage', 'StartChainage', 'start chainage', 'BgnLogm')
        c_ec   = _pick_name(g.columns, 'End_Chainage', 'EndChainage', 'end chainage', 'EndLogm')
        c_slat = _pick_name(g.columns, 'Start_Latitude', 'StartLat', 'start latitude', 'GPSLat')
        c_slon = _pick_name(g.columns, 'Start_Longitude', 'StartLong', 'start longitude', 'GPSLng')
        c_elat = _pick_name(g.columns, 'End_Latitude', 'EndLat', 'end latitude', 'GPSLat')
        c_elon = _pick_name(g.columns, 'End_Longitude', 'EndLong', 'end longitude', 'GPSLng')
 
        g[c_sc] = pd.to_numeric(g[c_sc], errors='coerce')
        g[c_ec] = pd.to_numeric(g[c_ec], errors='coerce')
        g = g.dropna(subset=[c_sc, c_ec])
        if verbose: print(f"      rows: {len(g)}")
        if g.empty:
            return _done({**blank, "L1 File Path": fpath}, "empty_after_clean")
 
        first = g.loc[g[c_sc].idxmin()]     # row holding the minimum start chainage
        last  = g.loc[g[c_ec].idxmax()]     # row holding the maximum end chainage
 
        def val(row, col):
            if col is None: return ""
            v = row.get(col)
            return "" if pd.isna(v) else v
 
        return _done({
            "Start Chainage": first[c_sc],
            "End Chainage":   last[c_ec],
            "Start Lat":      val(first, c_slat),    # same row as min chainage
            "Start Long":     val(first, c_slon),
            "End Lat":        val(last,  c_elat),    # same row as max chainage
            "End Long":       val(last,  c_elon),
            "L1 File Path":   fpath,
        })
    except Exception as exc:
        if len(GPS_ERRORS) < 5:
            GPS_ERRORS.append(f"{type(exc).__name__}: {exc}")
        return _done(blank, "other")
 
# ═══════════════════════════════════════════════════════════════════════════
# RUN
# ═══════════════════════════════════════════════════════════════════════════
df_full = pd.read_excel(INPUT_XLSX, sheet_name=INPUT_SHEET, dtype=str)
df_full.columns = [str(c).strip() for c in df_full.columns]
df = df_full.head(LIMIT_ROWS).copy() if LIMIT_ROWS else df_full.copy()
print(f"Input rows: {len(df_full)} | processing: {len(df)}"
      f"{'  (TEST RUN)' if LIMIT_ROWS else ''}\n")
 
cols = {k: [] for k in OUT_COLS}
 
for i, r in enumerate(df.itertuples(index=False), start=1):
    row   = dict(zip(df.columns, r))
    zone  = clean(row.get("Zone")).upper().replace("ZONE", "").strip()
    phase = digits_only(row.get("Phase"))
    cycle = digits_only(row.get("Cycle"))
    upc   = clean(row.get("UPC"))
 
    if VERBOSE:
        print(f"[{i}] Zone {zone} / Phase {phase} / Cycle {cycle} / {upc}")
 
    ext = gps_extent(zone, phase, cycle, upc, verbose=VERBOSE) if upc \
          else {k: "" for k in OUT_COLS}
    for k in OUT_COLS:
        cols[k].append(ext[k])
 
    if VERBOSE:
        if ext["Start Chainage"] != "":
            print(f"      -> chainage {ext['Start Chainage']} .. {ext['End Chainage']}")
            print(f"         start ({ext['Start Lat']}, {ext['Start Long']})")
            print(f"         end   ({ext['End Lat']}, {ext['End Long']})")
        else:
            print("      -> blank")
    elif i % 50 == 0:
        got = sum(1 for v in cols["Start Chainage"] if v != "")
        print(f"  {i}/{len(df)} rows | GPS resolved: {got}")
 
for k in OUT_COLS:
    df[k] = cols[k]
 
got = (df["Start Chainage"].astype(str) != "").sum()
print(f"\n{'='*60}")
print(f"Rows: {len(df)} | GPS resolved: {got} | blank: {len(df)-got}")
print(f"blank reasons: { {k: v for k, v in GPS_FAILS.items() if v} }")
if GPS_ERRORS:
    print("first few errors:")
    for e in GPS_ERRORS: print("   -", e[:200])
 
if SAVE_CSV:
    os.makedirs(str(Path(OUT_CSV).parent), exist_ok=True)
    df.to_csv(OUT_CSV, index=False)
    print(f"\nwritten -> {OUT_CSV}")
else:
    print("\n(SAVE_CSV = False — nothing written)")
 
_d = df.copy()
for k in GPS_COL_NAMES:
    _d[k] = _d[k].astype(str)
display(_d)
 

# COMMAND ----------

# DBTITLE 1,Step 3 Description
# MAGIC %md
# MAGIC ###Step 3 - Mapping the step 2 output with UPC's from O&M master
# MAGIC
# MAGIC **Input:**  
# MAGIC - Step 2 output (`df`) - all survey rows with GPS data  
# MAGIC - Master file: `Input/20260519_Consolidated Master_Ver7.xlsx` — Zone sheets (A–E) containing the official 887 O&M UPCs
# MAGIC
# MAGIC **Process:**  
# MAGIC Collects all UPCs from the 5 zone sheets, normalizes both survey and master UPCs (strips `/`, `_`, `-`) to handle format differences, then filters Step 2 output to keep only rows whose UPC exists in the master.
# MAGIC
# MAGIC **Output:**  
# MAGIC `Output/Step_03_Master_Matched_Lat_Long.csv` — Survey rows that match O&M master UPCs only

# COMMAND ----------

# DBTITLE 1,Filter by O&M Master UPCs and save
# ═══════════════════════════════════════════════════════════════════════════
# Match UPCs from Cell 2 output against Zone sheets in Consolidated Master
# ═══════════════════════════════════════════════════════════════════════════
import pandas as pd, os, re

# Fallback: if Cell 2 didn't run in this session, load from saved CSV
try:
    _ = df.columns  # check if df exists
except NameError:
    _step2_csv = "/Workspace/Users/shivam.raj9226@softsensor.ai/Extraction Logs (lat Long)/Output/Step_02_GPS_Extent_Raw.csv"
    df = pd.read_csv(_step2_csv, dtype=str)
    print(f"Loaded df from saved CSV: {len(df)} rows")

MASTER_XLSX = "/Workspace/Users/shivam.raj9226@softsensor.ai/Extraction Logs (lat Long)/Input/20260519_Consolidated Master_Ver7.xlsx"
# Zone D has header at row 3 (header=2), all others at row 2 (header=1)
ZONE_SHEETS = {
    "Zone A": 1, "Zone B": 1, "Zone C": 1,
    "Zone D": 2,
    "Zone E": 1,
}
OUT_PATH = "/Workspace/Users/shivam.raj9226@softsensor.ai/Extraction Logs (lat Long)/Output/Step_03_Master_Matched_Lat_Long.csv"

# Robust UPC normalization: strip ALL whitespace (tabs, newlines, spaces),
# uppercase, remove separators (/, _, -)
def norm_upc(v):
    s = re.sub(r'\s+', '', str(v))   # strip ALL whitespace incl. \n \t
    s = s.upper()
    return re.sub(r'[/_\-]', '', s)

# ── 1. Clean survey UPC column (strip newlines, tabs, trailing spaces) ───
raw_upcs = df["UPC"].astype(str).tolist()
df["UPC"] = df["UPC"].astype(str).str.strip()
dirty_survey = sum(1 for a, b in zip(raw_upcs, df["UPC"]) if a != b)
if dirty_survey:
    print(f"⚠️  Cleaned {dirty_survey} survey UPCs (had trailing newlines/spaces/tabs)")

# ── 2. Collect all UPCs from zone sheets ─────────────────────────────────
master_upcs_raw = set()
master_upcs_norm = set()
master_dirty = 0
for sheet, hdr_row in ZONE_SHEETS.items():
    tmp = pd.read_excel(MASTER_XLSX, sheet_name=sheet, header=hdr_row, dtype=str)
    tmp.columns = [str(c).strip() for c in tmp.columns]
    if "UPC" in tmp.columns:
        vals_raw = tmp["UPC"].dropna().astype(str)
        # Count dirty master UPCs (leading/trailing whitespace, tabs, etc.)
        for v in vals_raw:
            if v != v.strip() or '\t' in v or '\n' in v:
                master_dirty += 1
        vals = vals_raw.str.strip()
        vals = vals[(vals != "") & (vals.str.lower() != "nan")]
        master_upcs_raw.update(vals)
        master_upcs_norm.update(vals.apply(norm_upc))
        print(f"  {sheet} (header={hdr_row}): {len(vals)} UPCs found")
    else:
        print(f"  {sheet}: ⚠ 'UPC' column not found – cols: {list(tmp.columns)[:10]}")

if master_dirty:
    print(f"  ⚠️  {master_dirty} master UPCs had dirty formatting (tabs/newlines/spaces) — cleaned")
print(f"\nTotal unique UPCs (raw): {len(master_upcs_raw)} | (normalized): {len(master_upcs_norm)}")

# ── 3. Filter Cell 2 output (df) to matching UPCs ───────────────────────
df["_upc_norm"] = df["UPC"].apply(norm_upc)
mask = df["_upc_norm"].isin(master_upcs_norm)
df_matched = df[mask].drop(columns=["_upc_norm"]).copy()
df_dropped = df[~mask].drop(columns=["_upc_norm"]).copy()

print(f"\nCell 2 rows: {len(df)} | Matched with master: {len(df_matched)} | Dropped: {len(df_dropped)}")

# Show dropped UPCs so nothing slips through silently
if not df_dropped.empty:
    dropped_upcs = sorted(df_dropped["UPC"].unique())
    print(f"\n❌ Dropped UPCs not in master ({len(dropped_upcs)}):")
    for u in dropped_upcs[:20]:
        print(f"   {u}")
    if len(dropped_upcs) > 20:
        print(f"   ... +{len(dropped_upcs)-20} more")
else:
    print("✅ All survey UPCs validated against master — zero orphans")

# ── 3. Save ──────────────────────────────────────────────────────────────
os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
df_matched.to_csv(OUT_PATH, index=False)
print(f"\nSaved -> {OUT_PATH}")

if not df_matched.empty:
    _disp = df_matched.copy()
    for c in ["Start Chainage", "End Chainage", "Start Lat", "Start Long", "End Lat", "End Long"]:
        if c in _disp.columns:
            _disp[c] = _disp[c].astype(str)
    display(_disp)
else:
    print("No matching rows found.")

# COMMAND ----------

# DBTITLE 1,Step 4 Description
# MAGIC %md
# MAGIC ###Step 4 - Gives report out of 886 UPC's how many Lat Long is extracted from Bronze layer / Checking remaining UPC's for KML links  
# MAGIC
# MAGIC **Input:**  
# MAGIC - Step 3 matched data (`df_matched`) - to identify which UPCs have GPS data  
# MAGIC - Master file: `Input/20260519_Consolidated Master_Ver7.xlsx` - zone sheets (A–E), all 887 UPCs with KML file info (columns 19–20)
# MAGIC
# MAGIC **Process:**  
# MAGIC For each of the 887 master UPCs, assigns one of three statuses:  
# MAGIC 1. **Lat Long Extracted** (green) — GPS data was successfully extracted in Step 2/3  
# MAGIC 2. **KML Links Present** (yellow) — No GPS extracted, but KML file links exist in the master  
# MAGIC 3. **No KML Links Present** (red) — Neither GPS data nor KML links available
# MAGIC
# MAGIC **Output:**  
# MAGIC `Output/Step_04_All_UPC_Status_Report.csv` — 4 columns: S.No, Zone, UPC, Status (colour-coded)

# COMMAND ----------

# DBTITLE 1,All 887 UPCs Status Report
# ═══════════════════════════════════════════════════════════════════════════
# Cell 4: All master UPCs — Status Report (colour-coded)
# ═══════════════════════════════════════════════════════════════════════════
import pandas as pd, os, re

MASTER_XLSX = "/Workspace/Users/shivam.raj9226@softsensor.ai/Extraction Logs (lat Long)/Input/20260519_Consolidated Master_Ver7.xlsx"
ZONE_SHEETS = {
    "Zone A": 1, "Zone B": 1, "Zone C": 1,
    "Zone D": 2,
    "Zone E": 1,
}
OUT_STATUS = "/Workspace/Users/shivam.raj9226@softsensor.ai/Extraction Logs (lat Long)/Output/Step_04_All_UPC_Status_Report.csv"

def norm_upc(v):
    return re.sub(r'[/_\-]', '', str(v).strip().upper())

# ── 1. UPCs with actual Lat/Long from Cell 3 ────────────────────────────
gps_resolved = set()
for _, row in df_matched.iterrows():
    sc = str(row.get("Start Chainage", "")).strip()
    if sc not in ("", "nan", "None"):
        gps_resolved.add(norm_upc(row["UPC"]))

# ── 2. Read zone sheets → Zone, UPC, Status ─────────────────────────────
rows = []
for sheet, hdr_row in ZONE_SHEETS.items():
    tmp = pd.read_excel(MASTER_XLSX, sheet_name=sheet, header=hdr_row, dtype=str)
    tmp.columns = [str(c).strip() for c in tmp.columns]
    if "UPC" not in tmp.columns:
        continue

    kml_files_col = next((c for c in tmp.columns if c.lower() == "kml files"), None)
    kml_link_col  = next((c for c in tmp.columns if c.lower() == "kml file link"), None)

    for _, r in tmp.iterrows():
        upc_raw = str(r.get("UPC", "")).strip()
        if not upc_raw or upc_raw.lower() == "nan":
            continue

        upc_norm = norm_upc(upc_raw)

        if upc_norm in gps_resolved:
            status = "Lat Long Extracted"
        else:
            kml_val  = str(r.get(kml_files_col, "")).strip().lower() if kml_files_col else ""
            link_val = str(r.get(kml_link_col, "")).strip().lower()  if kml_link_col  else ""
            has_kml  = kml_val in ("yes", "y") or link_val not in ("", "nan", "none", "no")
            status   = "KML Links Present" if has_kml else "No KML Links Present"

        rows.append({
            "Zone":   sheet,
            "UPC":    upc_raw,
            "Status": status,
        })

df_status = pd.DataFrame(rows)

# ── 3. Incremental S.No from 1 to N ─────────────────────────────────────
df_status.insert(0, "S. No.", range(1, len(df_status) + 1))

# ── 4. Summary ───────────────────────────────────────────────────────────
print(f"Total UPCs: {len(df_status)}")
print(df_status["Status"].value_counts().to_string())

# ── 5. Save ──────────────────────────────────────────────────────────────
os.makedirs(os.path.dirname(OUT_STATUS), exist_ok=True)
df_status.to_csv(OUT_STATUS, index=False)
print(f"\nSaved -> {OUT_STATUS}")

# ── 6. Colour-coded display ──────────────────────────────────────────────
def colour_status(val):
    if val == "Lat Long Extracted":
        return "background-color: #d4edda"   # light green
    elif val == "KML Links Present":
        return "background-color: #fff3cd"   # light yellow
    elif val == "No KML Links Present":
        return "background-color: #f8d7da"   # light red
    return ""

styled = (
    df_status.style
    .map(colour_status, subset=["Status"])
    .set_properties(**{"text-align": "left"})
    .set_table_styles([{"selector": "th", "props": [("text-align", "left")]}])
    .hide(axis="index")
)
displayHTML(styled.to_html())

# COMMAND ----------

# DBTITLE 1,Step 5 Description
# MAGIC %md
# MAGIC ###Step 5 - Here we will filter for only those UPC's which has Chainage populated becuse remaing has no data in the bronze layer itself 
# MAGIC
# MAGIC **Input:**  
# MAGIC - Step 3 matched data (`df_matched`) - survey rows matching master UPCs
# MAGIC
# MAGIC **Process:**  
# MAGIC Filters to only rows where Start Chainage is actually populated (GPS data present). Keeps columns: Zone, Phase, Cycle, UPC, Survey_UPC, Project Name, Start/End Chainage, Start/End Lat/Long. Adds incremental S.No.
# MAGIC
# MAGIC **Output:**  
# MAGIC `Output/Step_05_{count}_Lat_Long_Extracted.csv` - Clean list of surveys with valid GPS coordinates (count in filename)

# COMMAND ----------

# DBTITLE 1,Lat Long Extracted list
# ═══════════════════════════════════════════════════════════════════════════
# Cell 5: Only rows where Lat Long was actually extracted
# ═══════════════════════════════════════════════════════════════════════════
import pandas as pd, os, re

def norm_upc(v):
    return re.sub(r'[/_\-]', '', str(v).strip().upper())

# Filter df_matched (Cell 3) to rows with actual GPS data
df_ll = df_matched.copy()
df_ll["_sc"] = df_ll["Start Chainage"].astype(str).str.strip()
df_ll = df_ll[~df_ll["_sc"].isin(["", "nan", "None"])].drop(columns=["_sc"])

# Keep only the requested columns
keep_cols = ["Zone", "Phase", "Cycle", "UPC", "Survey_UPC", "Project Name",
             "Start Chainage", "End Chainage", "Start Lat", "Start Long",
             "End Lat", "End Long"]
df_ll = df_ll[[c for c in keep_cols if c in df_ll.columns]].copy()

# Incremental S. No.
df_ll.insert(0, "S. No.", range(1, len(df_ll) + 1))

count = len(df_ll)
print(f"Total Lat Long Extracted rows: {count}")

# Save with count in filename
out_dir = "/Workspace/Users/shivam.raj9226@softsensor.ai/Extraction Logs (lat Long)/Output"
out_path = os.path.join(out_dir, f"Step_05_{count}_Lat_Long_Extracted.csv")
os.makedirs(out_dir, exist_ok=True)
df_ll.to_csv(out_path, index=False)
print(f"Saved -> {out_path}")

# Display
_d5 = df_ll.copy()
for c in ["Start Chainage", "End Chainage", "Start Lat", "Start Long", "End Lat", "End Long"]:
    if c in _d5.columns:
        _d5[c] = _d5[c].astype(str)
display(_d5)

# COMMAND ----------

# DBTITLE 1,Step 6 Description
# MAGIC %md
# MAGIC ###Step 6 - All the discrpency of zone C and E are taken care of here (manually filled the data for those UPC's) + Zone correction from master
# MAGIC
# MAGIC **Input:**  
# MAGIC - Step 5 output (`df_ll`) - extracted Lat Long list  
# MAGIC - Correction file: `Input/Network_Overview_UPC_Lat_Long_Summary (1).xlsx` (Sheet2) - manually verified GPS corrections  
# MAGIC - Master file: `Input/20260519_Consolidated Master_Ver7.xlsx` - zone sheets (A-E) used as source of truth for Zone assignment
# MAGIC
# MAGIC **Process:**  
# MAGIC 1. Matches rows on (Zone, Phase, Cycle, UPC, Survey_UPC) and replaces Start/End Chainage and Lat/Long with GIS-verified values from Sheet2  
# MAGIC 2. **Zone correction**: Cross-checks every UPC's Zone against the master file and corrects any mismatches (survey list had 6 UPCs with wrong zones)  
# MAGIC 3. Tracks all changed cells (GPS + Zone)
# MAGIC
# MAGIC **Output:**  
# MAGIC `Output/Step_06_{count}_Lat_Long_Corrected_Final.csv` - Final corrected GPS data with changed cells highlighted in light green

# COMMAND ----------

# DBTITLE 1,Apply discrepancy corrections from GIS mapping
# ═══════════════════════════════════════════════════════════════════════════
# Cell 6: Replace Cell 5 GPS data with corrected GIS mapping
#         + Zone correction from master file
# ═══════════════════════════════════════════════════════════════════════════
import pandas as pd, os, re

CORRECTION_FILE = "/Workspace/Users/shivam.raj9226@softsensor.ai/Extraction Logs (lat Long)/Input/Network_Overview_UPC_Lat_Long_Summary (1).xlsx"
OUT_DIR = "/Workspace/Users/shivam.raj9226@softsensor.ai/Extraction Logs (lat Long)/Output"

# ── 1. Read correction sheet (Sheet2, header=0) ─────────────────────────
corr = pd.read_excel(CORRECTION_FILE, sheet_name="Sheet2", header=0, dtype=str)
corr.columns = [str(c).strip() for c in corr.columns]
print(f"Correction rows in Sheet2: {len(corr)}")

# Column mapping from Sheet2 to Cell 5 column names
col_map = {
    "Start Chianage as per GIS": "Start Chainage",   # col O
    "End Chainage as per GIS":   "End Chainage",     # col P
    "Start lat":                 "Start Lat",        # col T
    "Start lon":                 "Start Long",       # col U
    "End Lat":                   "End Lat",          # col V
    "End lon":                   "End Long",         # col W
}

# Build lookup: key = (Zone, Phase, Cycle, UPC, Survey_UPC) -> corrected values
corr_lookup = {}
for _, r in corr.iterrows():
    key = (
        str(r.get("Zone", "")).strip(),
        str(r.get("Phase", "")).strip(),
        str(r.get("Cycle", "")).strip(),
        str(r.get("UPC", "")).strip(),
        str(r.get("Survey_UPC", "")).strip(),
    )
    vals = {}
    for src_col, tgt_col in col_map.items():
        v = str(r.get(src_col, "")).strip()
        if v and v.lower() not in ("nan", "none", ""):
            vals[tgt_col] = v
    if vals:
        corr_lookup[key] = vals

print(f"Correction entries with valid GPS: {len(corr_lookup)}")

# ── 2. Start from Cell 5 output and apply corrections ───────────────────
df_final = df_ll.drop(columns=["S. No."], errors="ignore").copy()

# Track which cells were changed: set of (row_idx, col_name)
changed_cells = set()
matched_count = 0

for idx, row in df_final.iterrows():
    key = (
        str(row.get("Zone", "")).strip(),
        str(row.get("Phase", "")).strip(),
        str(row.get("Cycle", "")).strip(),
        str(row.get("UPC", "")).strip(),
        str(row.get("Survey_UPC", "")).strip(),
    )
    if key in corr_lookup:
        matched_count += 1
        for col, new_val in corr_lookup[key].items():
            old_val = str(df_final.at[idx, col]).strip()
            if old_val != new_val:
                df_final.at[idx, col] = new_val
                changed_cells.add((idx, col))

print(f"\nRows matched with corrections: {matched_count}")
print(f"Individual cell values changed: {len(changed_cells)}")

# ── 2b. Zone correction from master file ─────────────────────────────────
MASTER_XLSX = "/Workspace/Users/shivam.raj9226@softsensor.ai/Extraction Logs (lat Long)/Input/20260519_Consolidated Master_Ver7.xlsx"
ZONE_SHEETS = {
    "Zone A": 1, "Zone B": 1, "Zone C": 1,
    "Zone D": 2, "Zone E": 1,
}

def norm_upc(v):
    return re.sub(r'[/_\-]', '', str(v).strip().upper())

# Build UPC → correct zone letter lookup from master
master_zone_lookup = {}  # norm_upc -> zone letter (A, B, C, D, E)
for sheet, hdr_row in ZONE_SHEETS.items():
    tmp = pd.read_excel(MASTER_XLSX, sheet_name=sheet, header=hdr_row, dtype=str)
    tmp.columns = [str(c).strip() for c in tmp.columns]
    if "UPC" not in tmp.columns:
        continue
    zone_letter = sheet.replace("Zone ", "")  # "Zone A" -> "A"
    for _, r in tmp.iterrows():
        upc_raw = str(r.get("UPC", "")).strip()
        if upc_raw and upc_raw.lower() != "nan":
            master_zone_lookup[norm_upc(upc_raw)] = zone_letter

print(f"\nMaster zone lookup: {len(master_zone_lookup)} UPCs loaded")

# Apply zone corrections
zone_corrections = 0
zone_correction_details = []
for idx, row in df_final.iterrows():
    upc_n = norm_upc(str(row.get("UPC", "")))
    survey_zone = str(row.get("Zone", "")).strip().upper()
    master_zone = master_zone_lookup.get(upc_n)
    if master_zone and survey_zone != master_zone:
        zone_corrections += 1
        zone_correction_details.append({
            "UPC": row.get("UPC", ""),
            "Survey Zone": survey_zone,
            "Master Zone": master_zone,
        })
        df_final.at[idx, "Zone"] = master_zone
        changed_cells.add((idx, "Zone"))

print(f"Zone corrections applied: {zone_corrections}")
if zone_correction_details:
    print("Zone mismatches fixed:")
    for d in zone_correction_details:
        print(f"   {d['UPC']}: {d['Survey Zone']} → {d['Master Zone']}")

# Re-add incremental S. No.
df_final = df_final.reset_index(drop=True)
df_final.insert(0, "S. No.", range(1, len(df_final) + 1))

count = len(df_final)
print(f"Final output rows: {count}")

# ── 3. Save ──────────────────────────────────────────────────────────────
out_path = os.path.join(OUT_DIR, f"Step_06_{count}_Lat_Long_Corrected_Final.csv")
df_final.to_csv(out_path, index=False)
print(f"\nSaved -> {out_path}")

# ── 4. Colour-coded display: light green for corrected cells ───────────
# Build a boolean mask DataFrame for highlighting
changed_mask = pd.DataFrame(False, index=df_final.index, columns=df_final.columns)
for orig_idx, col in changed_cells:
    new_idx = df_final.index[df_final.index == orig_idx]
    if len(new_idx) > 0:
        changed_mask.at[new_idx[0], col] = True

def highlight_changed(val, props=""):
    return props

def apply_highlight(row):
    return [
        "background-color: #d4edda" if changed_mask.at[row.name, c] else ""
        for c in row.index
    ]

_d6 = df_final.copy()
for c in ["Start Chainage", "End Chainage", "Start Lat", "Start Long", "End Lat", "End Long"]:
    if c in _d6.columns:
        _d6[c] = _d6[c].astype(str)

styled = (
    _d6.style
    .apply(apply_highlight, axis=1)
    .set_properties(**{"text-align": "left"})
    .set_table_styles([{"selector": "th", "props": [("text-align", "left")]}])
    .hide(axis="index")
)
displayHTML(styled.to_html())

# COMMAND ----------

# DBTITLE 1,Add Active Flag (latest Phase+Cycle per UPC)
# ═══════════════════════════════════════════════════════════════════════════
# Cell 7: Active Flag — 1 for latest Phase+Cycle per UPC, 0 otherwise
# ═══════════════════════════════════════════════════════════════════════════
import pandas as pd, re, os

# ── 1. Extract numeric Phase and Cycle for sorting ───────────────────────
def extract_num(val):
    """Extract trailing integer from 'Phase 3' → 3, 'Cycle 2' → 2."""
    m = re.search(r'(\d+)', str(val))
    return int(m.group(1)) if m else 0

df_active = df_final.copy()
df_active["_phase_n"] = df_active["Phase"].apply(extract_num)
df_active["_cycle_n"] = df_active["Cycle"].apply(extract_num)

# ── 2. For each UPC, mark the row with the highest (Phase, Cycle) as 1 ──
# rank descending: highest Phase first, then highest Cycle → rank 1 = latest
df_active["_rank"] = (
    df_active
    .groupby("UPC")[["_phase_n", "_cycle_n"]]
    .apply(lambda g: g.apply(tuple, axis=1).rank(method="first", ascending=False))
    .droplevel(0)
)

df_active["Active Flag"] = (df_active["_rank"] == 1).astype(int)
df_active.drop(columns=["_phase_n", "_cycle_n", "_rank"], inplace=True)

# ── 3. Reorder columns: Active Flag right after UPC/Phase/Cycle ──────────
# This prevents confusion with the wide coordinate columns
id_cols = ["S. No.", "Zone", "Phase", "Cycle", "UPC", "Active Flag"]
rest_cols = [c for c in df_active.columns if c not in id_cols]
df_active = df_active[id_cols + rest_cols]

# ── 4. Re-number S. No. ──────────────────────────────────────────────────
df_active["S. No."] = range(1, len(df_active) + 1)

# ── 5. Summary ───────────────────────────────────────────────────────────
total     = len(df_active)
active    = (df_active["Active Flag"] == 1).sum()
inactive  = (df_active["Active Flag"] == 0).sum()
uniq_upcs = df_active["UPC"].nunique()
multi     = df_active.groupby("UPC").size()
multi_cnt = (multi > 1).sum()

print(f"Total rows       : {total}")
print(f"Unique UPCs      : {uniq_upcs}")
print(f"UPCs with multi  : {multi_cnt}")
print(f"Active Flag = 1  : {active}")
print(f"Active Flag = 0  : {inactive}")

# ── 6. Save as both CSV and Excel ────────────────────────────────────────
out_dir = "/Workspace/Users/shivam.raj9226@softsensor.ai/Extraction Logs (lat Long)/Output"
csv_path  = os.path.join(out_dir, f"Step_07_{total}_Active_Flag_Final.csv")
xlsx_path = os.path.join(out_dir, f"Step_07_{total}_Active_Flag_Final.xlsx")

df_active.to_csv(csv_path, index=False)
df_active.to_excel(xlsx_path, index=False, engine="openpyxl")
print(f"\nSaved CSV  -> {csv_path}")
print(f"Saved XLSX -> {xlsx_path}")

# ── 7. Colour-coded display: green=active, grey=inactive ────────────────
def colour_flag(row):
    if row["Active Flag"] == 0:
        return ["background-color: #f0f0f0"] * len(row)   # grey for inactive
    return [""] * len(row)

_d7 = df_active.copy()
for c in ["Start Chainage", "End Chainage", "Start Lat", "Start Long", "End Lat", "End Long"]:
    if c in _d7.columns:
        _d7[c] = _d7[c].astype(str)

styled = (
    _d7.style
    .apply(colour_flag, axis=1)
    .set_properties(**{"text-align": "left"})
    .set_table_styles([{"selector": "th", "props": [("text-align", "left")]}])
    .hide(axis="index")
)
displayHTML(styled.to_html())