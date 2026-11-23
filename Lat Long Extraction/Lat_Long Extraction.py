# Databricks notebook source
# DBTITLE 1,Install dependencies
# MAGIC %pip install openpyxl

# COMMAND ----------

# DBTITLE 1,Extract UPC GPS Extent from Bronze Layer
import os, re
import pandas as pd
import pyarrow.parquet as pq

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════
BRONZE_BASE = "/Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output"
NSV_BACKUP  = os.path.join(BRONZE_BASE, "Source=NSV_Backup")
OUT_XLSX    = "/Workspace/Users/shivam.raj9226@softsensor.ai/Lat_Long_Extraction_Output.xlsx"

# ═══════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════
def subdirs(path):
    if not os.path.isdir(path): return []
    return sorted(os.path.join(path, n) for n in os.listdir(path)
                  if os.path.isdir(os.path.join(path, n)))

def _pick_name(names, *cands):
    norm = {re.sub(r'[^a-z0-9]', '', str(c).lower()): c for c in names}
    for cand in cands:
        k = re.sub(r'[^a-z0-9]', '', cand.lower())
        if k in norm: return norm[k]
    return None

# ═══════════════════════════════════════════════════════════════════════════
# DISCOVER EXTRACTED UPCs FROM BRONZE LAYER
# ═══════════════════════════════════════════════════════════════════════════
def _has_data(upc_dir):
    """Check year=/month=/day= partitions exist with content."""
    for y in subdirs(upc_dir):
        if not os.path.basename(y).startswith("year="): continue
        for mo in subdirs(y):
            if not os.path.basename(mo).startswith("month="): continue
            for dy in subdirs(mo):
                if os.path.basename(dy).startswith("day="): return True
    return False

def discover_upcs(base_path):
    """Walk Source=NSV_Backup to find all extracted Zone/Phase/Cycle/UPC."""
    records = []
    if not os.path.isdir(base_path):
        print(f"WARNING: {base_path} not found"); return records
    for zone_dir in subdirs(base_path):
        zn = os.path.basename(zone_dir)
        if not zn.startswith("Zone="): continue
        zone = zn.split("=", 1)[1]
        for phase_dir in subdirs(zone_dir):
            pn = os.path.basename(phase_dir)
            if not pn.startswith("Phase="): continue
            phase = pn.split("=", 1)[1]
            for cycle_dir in subdirs(phase_dir):
                cn = os.path.basename(cycle_dir)
                if not cn.startswith("Cycle="): continue
                cycle = cn.split("=", 1)[1]
                for upc_dir in subdirs(cycle_dir):
                    un = os.path.basename(upc_dir)
                    if not un.startswith("UPC="): continue
                    upc = un.split("=", 1)[1]
                    if _has_data(upc_dir):
                        records.append({"UPC": upc, "Zone": zone,
                                        "Phase": phase, "Cycle": cycle,
                                        "upc_path": upc_dir})
    return records

# ═══════════════════════════════════════════════════════════════════════════
# GPS EXTENT EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════
def _latest_date_dir(upc_dir):
    best = None
    for y in subdirs(upc_dir):
        if not os.path.basename(y).startswith("year="): continue
        for mo in subdirs(y):
            if not os.path.basename(mo).startswith("month="): continue
            for dy in subdirs(mo):
                if not os.path.basename(dy).startswith("day="): continue
                try:
                    k = (int(os.path.basename(y).split("=")[1]),
                         int(os.path.basename(mo).split("=")[1]),
                         int(os.path.basename(dy).split("=")[1]))
                except (ValueError, IndexError): continue
                if best is None or k > best[0]: best = (k, dy)
    return best[1] if best else None

def _find_gps_parquets(day_dir, max_depth=6):
    out = []
    def _scan(p, d=0):
        if d > max_depth: return
        try:
            for e in os.listdir(p):
                q = os.path.join(p, e)
                if os.path.isdir(q): _scan(q, d + 1)
                elif (e.lower().endswith(".parquet")
                      and "unified_segments" not in e.lower()
                      and ("gps" in e.lower() or "geometry" in e.lower())):
                    out.append(q)
        except Exception: pass
    _scan(day_dir)
    return out

def extract_gps(upc_path):
    """Extract Start/End Lat/Long from GPS parquets for a UPC."""
    blank = {"Start Lat": "", "Start Long": "", "End Lat": "", "End Long": ""}
    day_dir = _latest_date_dir(upc_path)
    if not day_dir: return blank
    files = _find_gps_parquets(day_dir)
    if not files: return blank

    frames = []
    for fp in files:
        try:
            pf = pq.ParquetFile(fp)
            names = pf.schema_arrow.names
            sc = _pick_name(names, 'Start_Chainage', 'StartChainage', 'start chainage')
            ec = _pick_name(names, 'End_Chainage',   'EndChainage',   'end chainage')
            if not sc or not ec:
                frames.append(pf.read().to_pandas())
            else:
                wanted = [c for c in [
                    sc, ec,
                    _pick_name(names, 'Start_Latitude',  'StartLat',  'start latitude'),
                    _pick_name(names, 'Start_Longitude', 'StartLong', 'start longitude'),
                    _pick_name(names, 'End_Latitude',    'EndLat',    'end latitude'),
                    _pick_name(names, 'End_Longitude',   'EndLong',   'end longitude'),
                ] if c]
                frames.append(pf.read(columns=wanted).to_pandas())
        except Exception: continue
    if not frames: return blank

    g = pd.concat(frames, ignore_index=True)
    c_sc   = _pick_name(g.columns, 'Start_Chainage', 'StartChainage', 'start chainage')
    c_ec   = _pick_name(g.columns, 'End_Chainage',   'EndChainage',   'end chainage')
    c_slat = _pick_name(g.columns, 'Start_Latitude',  'StartLat',  'start latitude')
    c_slon = _pick_name(g.columns, 'Start_Longitude', 'StartLong', 'start longitude')
    c_elat = _pick_name(g.columns, 'End_Latitude',    'EndLat',    'end latitude')
    c_elon = _pick_name(g.columns, 'End_Longitude',   'EndLong',   'end longitude')
    if not c_sc or not c_ec: return blank

    g[c_sc] = pd.to_numeric(g[c_sc], errors='coerce')
    g[c_ec] = pd.to_numeric(g[c_ec], errors='coerce')
    g = g.dropna(subset=[c_sc, c_ec])
    if g.empty: return blank

    first = g.loc[g[c_sc].idxmin()]
    last  = g.loc[g[c_ec].idxmax()]
    def val(row, col):
        if col is None: return ""
        v = row.get(col)
        return "" if pd.isna(v) else v

    return {"Start Lat": val(first, c_slat), "Start Long": val(first, c_slon),
            "End Lat":   val(last,  c_elat), "End Long":   val(last,  c_elon)}

# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════
print("Discovering extracted UPCs from Bronze layer...")
records = discover_upcs(NSV_BACKUP)
print(f"Found {len(records)} UPC entries — processing all")

print("Extracting GPS coordinates...")
results = []
for i, rec in enumerate(records, 1):
    gps = extract_gps(rec["upc_path"])
    results.append({"UPC": rec["UPC"], "Zone": rec["Zone"],
                    "Phase": rec["Phase"], "Cycle": rec["Cycle"], **gps})
    if i % 25 == 0:
        print(f"  Processed {i}/{len(records)}")

df = pd.DataFrame(results)

# ── Active Flag: 1 for the latest Phase + Cycle per UPC ──
df["_phase"] = pd.to_numeric(df["Phase"], errors="coerce").fillna(0).astype(int)
df["_cycle"] = pd.to_numeric(df["Cycle"], errors="coerce").fillna(0).astype(int)

latest_idx = (
    df.sort_values(["_phase", "_cycle"], ascending=[False, False])
      .groupby("UPC", sort=False)
      .head(1)
      .index
)
df["Active Flag"] = 0
df.loc[latest_idx, "Active Flag"] = 1

# ── Clean GPS columns to numeric ──
for col in ["Start Lat", "Start Long", "End Lat", "End Long"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# ── Final output ──
df = df[["UPC", "Zone", "Phase", "Cycle",
         "Start Lat", "Start Long", "End Lat", "End Long", "Active Flag"]]

os.makedirs(os.path.dirname(OUT_XLSX), exist_ok=True)
df.to_excel(OUT_XLSX, index=False)
print(f"\nOutput: {len(df)} rows written to {OUT_XLSX}")
print(f"Active: {df['Active Flag'].sum()} | Inactive: {(df['Active Flag'] == 0).sum()}")
display(df)