# Databricks notebook source
# DBTITLE 1,Install dependencies
# MAGIC %pip install geopandas openpyxl

# COMMAND ----------

# DBTITLE 1,State_UPC Mapping — Functions & Config
"""
===============================================================================
State_UPC Mapping — Per-Lane Chainage-Based State Detection
===============================================================================
Fixed output: Zone | Phase | Cycle | UPC | Project_Name |
  State_{1..5}_Name | State_{1..5}_Length |
  State_{1..5}_{L1,L2,L3,L4,R1,R2,R3,R4}

Chainage: metres → km.  Gaps excluded from lane lengths.
State_Length = MAX(chainage) – MIN(chainage) across all lanes in that state.
===============================================================================
"""

import os
import glob
import re
import logging
from collections import defaultdict

import geopandas as gpd
import pandas as pd
import numpy as np

# =============================================================================
# CONFIGURATION
# =============================================================================

CATALOG_BASE_PATH    = "/Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output"
NSV_BACKUP_BASE_PATH = os.path.join(CATALOG_BASE_PATH, "Source=NSV_Backup")
RIS_BASE_PATH        = os.path.join(CATALOG_BASE_PATH, "Source=NSV")

STATE_BOUNDARY_PATH  = "/Workspace/Users/shivam.raj9226@softsensor.ai/Survey of india shap files/State Boundary.shp"
OUTPUT_DIR_BASE      = "/Workspace/Users/shivam.raj9226@softsensor.ai/Drafts"

STATE_NAME_COLUMN = "STATE"
COMMON_CRS        = "EPSG:4326"
MAX_STATES        = 5
VALID_LANES       = {"L1", "L2", "L3", "L4", "R1", "R2", "R3", "R4"}
GAP_THRESHOLD_M   = 100  # step > max(this, 2×median) → gap

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
LOGGER = logging.getLogger("state_upc_mapping")

# Fixed output column order (55 columns)
OUTPUT_COLUMNS = ["Zone", "Phase", "Cycle", "UPC", "Project_Name"]
for _i in range(1, MAX_STATES + 1):
    _pfx = f"State_{_i}"
    OUTPUT_COLUMNS += [f"{_pfx}_Name", f"{_pfx}_Length"]
    OUTPUT_COLUMNS += [f"{_pfx}_{ln}" for ln in ["L1","L2","L3","L4","R1","R2","R3","R4"]]

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def sanitize_param(value, prefix=""):
    val = str(value).strip()
    if prefix:
        val = val.replace(prefix, "").strip()
    return val.replace("/", "_")

def resolve_shapefile(path_input):
    if os.path.isfile(path_input):
        return path_input
    search_dir = path_input if os.path.isdir(path_input) else os.path.dirname(path_input)
    if os.path.isdir(search_dir):
        hits = glob.glob(os.path.join(search_dir, "**", "*.shp"), recursive=True)
        if hits:
            return hits[0]
    return path_input

def find_ris_file(zone, phase, cycle, upc):
    z = sanitize_param(zone, "Zone")
    p = sanitize_param(phase, "Phase")
    c = sanitize_param(cycle, "Cycle")
    u = sanitize_param(upc)
    pat = os.path.join(RIS_BASE_PATH, f"Zone={z}", f"Phase={p}", f"Cycle={c}",
                       f"UPC={u}", "**", "*.parquet")
    hits = glob.glob(pat, recursive=True)
    ris = [f for f in hits if "RIS" in os.path.basename(f).upper() or "DELIVERABLE" in f.upper()]
    return ris[0] if ris else (hits[0] if hits else None)

# =============================================================================
# LANE DISCOVERY (dynamic from Lane=* folders)
# =============================================================================

def discover_lanes(zone, phase, cycle, upc, base_path=NSV_BACKUP_BASE_PATH):
    """
    Dynamically discovers Lane=* folders under a UPC directory.
    Returns ({lane_name: [gps_parquet_paths]}, warnings)
    """
    z = sanitize_param(zone, "Zone")
    p = sanitize_param(phase, "Phase")
    c = sanitize_param(cycle, "Cycle")
    u = sanitize_param(upc)
    upc_root = os.path.join(base_path, f"Zone={z}", f"Phase={p}", f"Cycle={c}", f"UPC={u}")
    warnings = []
    lanes = defaultdict(list)

    pat = os.path.join(upc_root, "**", "*.parquet")
    all_files = [f for f in glob.glob(pat, recursive=True) if "$" not in os.path.basename(f)]

    all_lane_names = set()
    for f in all_files:
        m = re.search(r"Lane=([^/]+)", f)
        if m:
            ln = m.group(1).upper()
            all_lane_names.add(ln)
            if "GPS" in os.path.basename(f).upper():
                lanes[ln].append(f)

    # Warn: unexpected lane names (L5, R5, ServiceRoad, etc.)
    for ln in sorted(all_lane_names):
        if ln not in VALID_LANES:
            warnings.append(f"Unexpected lane name '{ln}' found (valid: {sorted(VALID_LANES)})")

    # Warn: lane folder present but no GPS data inside
    for ln in sorted(all_lane_names):
        if ln not in lanes or not lanes[ln]:
            warnings.append(f"Lane={ln} folder exists but contains no GPS parquet files")

    LOGGER.info(f"UPC={u}: {len(lanes)} lane(s) with GPS: {sorted(lanes.keys())} "
                f"({sum(len(v) for v in lanes.values())} files)")
    return dict(lanes), warnings

# =============================================================================
# GPS LOADING
# =============================================================================

def load_gps(parquet_files):
    """Load & concat GPS parquets. Returns (df, lat_col, lon_col, chainage_col) or Nones."""
    dfs = []
    for f in parquet_files:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception as e:
            LOGGER.warning(f"Cannot read {f}: {e}")
    if not dfs:
        return None, None, None, None

    df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]

    lat = lon = ch = None
    for col in df.columns:
        cl = str(col).lower()
        if "lat" in cl and not lat:
            lat = col
        elif ("lon" in cl or "lng" in cl) and not lon:
            lon = col
        elif "chainage" in cl and not ch:
            ch = col
    return df, lat, lon, ch

# =============================================================================
# CHAINAGE-BASED LENGTH CALCULATIONS
# =============================================================================

def gap_aware_length_m(chainage_arr):
    """
    Actual covered length in metres — step-by-step through sorted chainage,
    skipping any jump that exceeds an adaptive gap threshold.

    Example: chainage [0, 10, 20, ..., 1000, 1500, 1510, ..., 3000]
      → 1000→1500 is a 500 m gap → skipped
      → covered = 1000 + 1500 = 2500 m
    """
    ch = np.sort(chainage_arr[~np.isnan(chainage_arr)])
    if len(ch) < 2:
        return 0.0
    diffs = np.diff(ch)
    pos = diffs[diffs > 0]
    if len(pos) == 0:
        return 0.0
    threshold = max(GAP_THRESHOLD_M, 2.0 * float(np.median(pos)))
    return float(np.sum(pos[pos <= threshold]))

def state_extent_m(chainage_arr):
    """
    State Length = MAX(chainage) – MIN(chainage) across all lanes in that
    state (metres).  Gives overall chainage extent of the state.
    """
    ch = chainage_arr[~np.isnan(chainage_arr)]
    return float(np.ptp(ch)) if len(ch) >= 2 else 0.0

# =============================================================================
# STATE ASSIGNMENT (spatial join against State Boundary shapefile)
# =============================================================================

def assign_states_to_gps(df, lat_col, lon_col, state_gdf):
    """Spatial join: assign STATE column to every GPS row."""
    df = df.copy()
    df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
    df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")
    valid = df.dropna(subset=[lat_col, lon_col])
    gdf = gpd.GeoDataFrame(
        valid,
        geometry=gpd.points_from_xy(valid[lon_col], valid[lat_col]),
        crs=COMMON_CRS
    )
    return gpd.sjoin(gdf, state_gdf, how="left", predicate="within")

# =============================================================================
# CORE: PROCESS ONE UPC (all lanes, all states)
# =============================================================================

def process_upc(zone, phase, cycle, upc, state_gdf):
    """
    Full UPC processing pipeline:
      1. Discover all Lane=* folders dynamically
      2. For each lane: load GPS → sort by chainage → spatial-join states
      3. Collect chainage values per (state, lane)
      4. Determine state order by ascending chainage
      5. Compute: State_Length = extent (MAX–MIN km),
                  Lane length  = gap-aware covered km
      6. Build fixed-schema 55-column record
    Returns (record_dict | None, warnings_list)
    """
    warnings = []

    # ── 1. Discover lanes under NSV_Backup; fallback to NSV source ──
    lanes, lw = discover_lanes(zone, phase, cycle, upc, NSV_BACKUP_BASE_PATH)
    warnings += lw
    if not any(lanes.values()):
        lanes2, lw2 = discover_lanes(zone, phase, cycle, upc, RIS_BASE_PATH)
        warnings += lw2
        for k, v in lanes2.items():
            if v:
                lanes[k] = v

    if not any(lanes.values()):
        warnings.append("No GPS data found in any lane")
        return None, warnings

    # ── 2. NH Number from RIS ──
    nh_number = ""
    rf = find_ris_file(zone, phase, cycle, upc)
    if rf:
        try:
            rdf = pd.read_parquet(rf)
            rcm = {str(c).strip().lower(): c for c in rdf.columns}
            nhc = rcm.get("nhnumber") or rcm.get("nh_number") or rcm.get("nhnumber_old")
            if nhc and not rdf.empty:
                nh_number = str(rdf.iloc[0][nhc])
        except Exception:
            pass

    # ── 3. Process each lane ──
    state_lane_ch = defaultdict(lambda: defaultdict(list))  # state → lane → [chainage]
    state_all_ch  = defaultdict(list)                        # state → [chainage]
    all_ch_state  = []                                       # [(ch, state)] for ordering

    for lane_name in sorted(lanes.keys()):
        files = lanes[lane_name]
        if not files:
            continue

        df, lat_col, lon_col, ch_col = load_gps(files)
        if df is None or lat_col is None or lon_col is None:
            warnings.append(f"Lane={lane_name}: GPS file present but no valid lat/lon columns")
            continue
        if ch_col is None:
            warnings.append(f"Lane={lane_name}: no chainage column found")
            continue

        df[ch_col] = pd.to_numeric(df[ch_col], errors="coerce")
        df = df.sort_values(ch_col).reset_index(drop=True)
        LOGGER.info(f"  Lane={lane_name}: {len(df)} GPS rows loaded")

        # Spatial join → assign state to every point
        joined = assign_states_to_gps(df, lat_col, lon_col, state_gdf)
        if STATE_NAME_COLUMN not in joined.columns:
            warnings.append(f"Lane={lane_name}: spatial join returned no STATE column")
            continue

        # Collect per-state per-lane chainage (vectorised)
        mask = joined[STATE_NAME_COLUMN].notna() & joined[ch_col].notna()
        sub = joined.loc[mask, [STATE_NAME_COLUMN, ch_col]].copy()
        sub["_st"] = sub[STATE_NAME_COLUMN].astype(str).str.strip()
        sub["_ch"] = sub[ch_col].astype(float)
        sub = sub[sub["_st"] != ""]

        for st_name, grp in sub.groupby("_st", sort=False):
            ch_vals = grp["_ch"].values
            state_lane_ch[st_name][lane_name].extend(ch_vals.tolist())
            state_all_ch[st_name].extend(ch_vals.tolist())

        # State ordering pairs
        s_sorted = sub.sort_values("_ch")
        all_ch_state.extend(zip(s_sorted["_ch"].values, s_sorted["_st"].values))

    if not all_ch_state:
        warnings.append("No state assignments produced for any lane")
        return None, warnings

    # ── 4. Determine state order by ascending chainage ──
    all_ch_state.sort(key=lambda x: x[0])
    state_order = []
    for _, sn in all_ch_state:
        if sn and (not state_order or state_order[-1] != sn):
            state_order.append(sn)
    state_order = list(dict.fromkeys(state_order))  # deduplicate

    if len(state_order) > MAX_STATES:
        warnings.append(
            f"{len(state_order)} states detected (max {MAX_STATES}): {state_order}"
        )

    LOGGER.info(f"  States detected ({len(state_order)}): {state_order}")

    # ── 5. Build fixed-schema record ──
    rec = {
        "Zone": str(zone), "Phase": str(phase), "Cycle": str(cycle),
        "UPC": str(upc), "Project_Name": "",  # TODO: lookup from Project/O&M Master
    }

    for i in range(1, MAX_STATES + 1):
        pfx = f"State_{i}"
        if i <= len(state_order):
            sn = state_order[i - 1]
            a_ch = np.array(state_all_ch[sn])
            rec[f"{pfx}_Name"]   = sn
            rec[f"{pfx}_Length"] = round(state_extent_m(a_ch) / 1000.0, 3)
            for ln in ["L1","L2","L3","L4","R1","R2","R3","R4"]:
                vals = state_lane_ch[sn].get(ln)
                if vals:
                    rec[f"{pfx}_{ln}"] = round(
                        gap_aware_length_m(np.array(vals)) / 1000.0, 3
                    )
                else:
                    rec[f"{pfx}_{ln}"] = ""
        else:
            rec[f"{pfx}_Name"] = ""
            rec[f"{pfx}_Length"] = ""
            for ln in ["L1","L2","L3","L4","R1","R2","R3","R4"]:
                rec[f"{pfx}_{ln}"] = ""

    return rec, warnings

# =============================================================================
# SAVE OUTPUT
# =============================================================================

def save_mapping(records, all_warnings, output_dir=OUTPUT_DIR_BASE,
                 file_name="State_UPC_Mapping.xlsx"):
    os.makedirs(output_dir, exist_ok=True)
    df = pd.DataFrame(records)
    for c in OUTPUT_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    df = df[OUTPUT_COLUMNS]

    wdf = pd.DataFrame({"Warning": all_warnings or ["No warnings"]})

    path = os.path.join(output_dir, file_name)
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="State_UPC_Mapping", index=False)
        wdf.to_excel(w, sheet_name="Warnings", index=False)
    LOGGER.info(f"Saved \u2192 {path}")
    return df

print(f"\u2705 State_UPC Mapping module loaded  |  {len(OUTPUT_COLUMNS)} output columns")
print(f"   Schema: Zone, Phase, Cycle, UPC, Project_Name + [State_1..5 \u00d7 (Name + Length + 8 lanes)]")

# COMMAND ----------

# DBTITLE 1,Test: Run for 1 UPC
import time as _t

# \u2500\u2500 Load state boundaries ONCE \u2500\u2500
shp_path = resolve_shapefile(STATE_BOUNDARY_PATH)
state_gdf = gpd.read_file(shp_path).to_crs(COMMON_CRS)
_ = state_gdf.sindex   # pre-build spatial index
LOGGER.info(f"Loaded {len(state_gdf)} state boundary polygons")

# \u2500\u2500 Test UPC (crosses RAJASTHAN \u2192 GUJARAT) \u2500\u2500
TEST = {"zone": "B", "phase": "1", "cycle": "1", "upc": "N_02001_09001_RJ"}

t0 = _t.time()
record, warnings = process_upc(**TEST, state_gdf=state_gdf)
elapsed = _t.time() - t0

if warnings:
    print(f"\n\u26a0\ufe0f  WARNINGS ({len(warnings)}):")
    for w in warnings:
        print(f"  \u2022 {w}")

if record:
    df_out = save_mapping([record], warnings, file_name="State_UPC_Mapping_TEST.xlsx")
    print(f"\n\u2705 UPC={TEST['upc']} processed in {elapsed:.1f}s")
    print(f"\nOutput ({len(OUTPUT_COLUMNS)} columns) \u2014 transposed for readability:")
    display(df_out.T.astype(str))
else:
    print(f"\n\u274c No output produced for UPC={TEST['upc']}")

# COMMAND ----------

# DBTITLE 1,Batch: Process all UPCs in all zones
import time as _t

# =============================================================================
# DISCOVERY & DEDUPLICATION
# =============================================================================

def discover_all_upcs(base_path=NSV_BACKUP_BASE_PATH):
    """Auto-discover all Zone/Phase/Cycle/UPC combos from directory tree."""
    discovered = []
    if not os.path.isdir(base_path):
        LOGGER.error(f"Base path not found: {base_path}")
        return discovered
    for zone_dir in sorted(os.listdir(base_path)):
        if not zone_dir.startswith("Zone="): continue
        zone_val = zone_dir.split("=", 1)[1]
        for phase_dir in sorted(os.listdir(os.path.join(base_path, zone_dir))):
            if not phase_dir.startswith("Phase="): continue
            phase_val = phase_dir.split("=", 1)[1]
            ph_path = os.path.join(base_path, zone_dir, phase_dir)
            for cycle_dir in sorted(os.listdir(ph_path)):
                if not cycle_dir.startswith("Cycle="): continue
                cycle_val = cycle_dir.split("=", 1)[1]
                cy_path = os.path.join(ph_path, cycle_dir)
                for upc_dir in sorted(os.listdir(cy_path)):
                    if not upc_dir.startswith("UPC="): continue
                    discovered.append({
                        "zone": zone_val,
                        "phase": phase_val,
                        "cycle": cycle_val,
                        "upc": upc_dir.split("=", 1)[1]
                    })
    LOGGER.info(f"Discovered {len(discovered)} UPC entries across all zones")
    return discovered

def deduplicate_latest(entries):
    """
    For the same UPC within the same zone, keep ONLY the latest Phase/Cycle.
    Groups by (zone, upc), picks highest phase then highest cycle.
    """
    groups = defaultdict(list)
    for e in entries:
        groups[(e["zone"], e["upc"])].append(e)

    def _num(s):
        """Extract leading integer from strings like 'Phase 1', '2', etc."""
        m = re.search(r'\d+', str(s))
        return int(m.group()) if m else 0

    deduped = []
    for key, items in groups.items():
        items.sort(key=lambda x: (_num(x["phase"]), _num(x["cycle"])), reverse=True)
        deduped.append(items[0])  # always pick the latest

    deduped.sort(key=lambda x: (x["zone"], x["upc"]))
    skipped = len(entries) - len(deduped)
    if skipped:
        LOGGER.info(f"Deduplicated: {len(entries)} \u2192 {len(deduped)} "
                    f"({skipped} older Phase/Cycle entries removed)")
    return deduped

# =============================================================================
# BATCH ZONE PROCESSOR
# =============================================================================

def process_zone_batch(zones, state_gdf, output_dir=OUTPUT_DIR_BASE):
    """
    Process all UPCs in one or more zones.
    zones: list of zone names, e.g. ["B"] or ["A","B"]
    """
    t_start = _t.time()

    all_entries = discover_all_upcs()
    deduped = deduplicate_latest(all_entries)

    zone_set = set(zones)
    entries = [e for e in deduped if e["zone"] in zone_set]
    LOGGER.info(f"Will process {len(entries)} UPCs in zone(s): {sorted(zone_set)}")

    zone_groups = defaultdict(list)
    for e in entries:
        zone_groups[e["zone"]].append(e)

    all_records, all_warnings = [], []

    for zone_name in sorted(zone_groups.keys()):
        zone_entries = zone_groups[zone_name]
        zone_records, zone_warnings = [], []
        zone_t0 = _t.time()

        LOGGER.info(f"\n{'='*60}")
        LOGGER.info(f"ZONE {zone_name}: {len(zone_entries)} UPCs (after dedup)")
        LOGGER.info(f"{'='*60}")

        for idx, entry in enumerate(zone_entries, 1):
            upc = entry["upc"]
            LOGGER.info(f"[{idx}/{len(zone_entries)}] UPC={upc} "
                        f"(Phase={entry['phase']}, Cycle={entry['cycle']})")
            try:
                rec, warns = process_upc(
                    zone=entry["zone"], phase=entry["phase"],
                    cycle=entry["cycle"], upc=upc, state_gdf=state_gdf
                )
                if rec:
                    zone_records.append(rec)
                else:
                    blank = {c: "" for c in OUTPUT_COLUMNS}
                    blank.update({"Zone": entry["zone"], "Phase": entry["phase"],
                                  "Cycle": entry["cycle"], "UPC": upc})
                    zone_records.append(blank)
                    zone_warnings.append(
                        f"UPC={upc}: Latest cycle (Phase={entry['phase']}/"
                        f"Cycle={entry['cycle']}) GPS data is not present"
                    )
                for w in warns:
                    zone_warnings.append(f"UPC={upc}: {w}")
            except Exception as exc:
                LOGGER.error(f"FAILED UPC={upc}: {exc}")
                blank = {c: "" for c in OUTPUT_COLUMNS}
                blank.update({"Zone": entry["zone"], "Phase": entry["phase"],
                              "Cycle": entry["cycle"], "UPC": upc})
                zone_records.append(blank)
                zone_warnings.append(f"UPC={upc}: FAILED \u2014 {exc}")

        # \u2500\u2500 Per-zone Excel \u2500\u2500
        zone_elapsed = _t.time() - zone_t0
        n_ok = sum(1 for r in zone_records if r.get("State_1_Name"))
        n_skip = len(zone_records) - n_ok

        df_zone = save_mapping(
            zone_records, zone_warnings,
            output_dir=output_dir,
            file_name=f"State_UPC_Mapping_Zone_{zone_name}.xlsx"
        )
        LOGGER.info(f"ZONE {zone_name} DONE \u2014 {n_ok} mapped, {n_skip} no-data, "
                    f"{zone_elapsed/60:.1f} min")
        print(f"\n{'='*60}")
        print(f"ZONE {zone_name} \u2014 {n_ok} mapped, {n_skip} no-data | {zone_elapsed/60:.1f} min")
        print(f"{'='*60}")
        display(df_zone.astype(str))

        all_records.extend(zone_records)
        all_warnings.extend(zone_warnings)

    # \u2500\u2500 Consolidated Excel \u2500\u2500
    if all_records:
        df_all = save_mapping(
            all_records, all_warnings,
            output_dir=output_dir,
            file_name="State_UPC_Mapping_ALL.xlsx"
        )
        total = _t.time() - t_start
        print(f"\n{'='*60}")
        print(f"ALL ZONES: {len(all_records)} UPCs | {total/60:.1f} min")
        print(f"{'='*60}")

    return all_records, all_warnings

# =============================================================================
# RUN: Zone B (all UPCs, latest Phase/Cycle only)
# =============================================================================

# Reuse state_gdf from cell 3 if available, otherwise reload
try:
    _ = state_gdf
except NameError:
    shp_path = resolve_shapefile(STATE_BOUNDARY_PATH)
    state_gdf = gpd.read_file(shp_path).to_crs(COMMON_CRS)
    _ = state_gdf.sindex
    LOGGER.info(f"Loaded {len(state_gdf)} state boundary polygons")

SAVE_DIR = "/Workspace/Users/shivam.raj9226@softsensor.ai/Script testing"
records, warnings = process_zone_batch(zones=["A","B","C","D","E"], state_gdf=state_gdf, output_dir=SAVE_DIR)