# Databricks notebook source
# DBTITLE 1,Defect Image Upload Pipeline
# MAGIC %md
# MAGIC # Defect Image Upload Pipeline
# MAGIC
# MAGIC Uploads pothole and cracking images from D3 (NSV downloads) to the CDN via `POST /defects/images/upload`, then MERGEs the returned CDN URLs back into the warehouse (`dlm_postgres.master_pothole` / `master_cracking`).
# MAGIC
# MAGIC **Run order (3 cells, handles all UPCs automatically):**
# MAGIC 1. **Configuration** — auto-discovers all UPCs with missing `image_url`, resolves volume paths (handles varying folder naming)
# MAGIC 2. **Bulk Upload** — loops over each UPC, matches `image_name` to files on disk, uploads to CDN, saves results to temp view
# MAGIC 3. **MERGE** — writes CDN URLs into both `dlm_postgres` and `dlm_kpi` (`master_pothole` / `master_cracking`); `dlm_kpi` is filled from `dlm_postgres` by `display_defect_id`, falling back to `upc_code` + `image_name` (safe to run without the Bulk Upload temp view)

# COMMAND ----------

# DBTITLE 1,Build RUN_LIST from Old silver population (Fail Issue = 0)
import pandas as pd

# ── Run list source: surveys with Fail Issue = 0 (blank Fail Issue is NOT included) ──
RUN_LIST_CSV = "/Workspace/Users/shivam.raj9226@softsensor.ai/SIlver layer defect data extraction/Input/Old silver population(As per contract).csv"

_src = pd.read_csv(RUN_LIST_CSV)
_fail = pd.to_numeric(_src['Fail Issue'], errors='coerce')
_ok = _src[_fail.notna() & (_fail == 0)].dropna(subset=['Zone', 'Phase', 'Cycle', 'UPC'])

def _num_str(v):
    """1 / 1.0 / '1' -> '1' (same string format as the old hardcoded list)"""
    return str(int(float(v)))

# ── Surveys already processed: always removed from RUN_LIST ──
# (Zone, Phase, Cycle, UPC)
EXCLUDE_LIST = [
    ("B", "1", "2", "N_04006_03001_GJ"),
    ("B", "1", "1", "N_01002_01001_GJ"),
    ("B", "1", "2", "N_01002_01001_GJ"),
    ("B", "1", "1", "N_02001_12001_GJ"),
    ("B", "2", "1", "N_08009_08002_GJ"),
    ("B", "2", "1", "N_08009_07002_GJ"),
    ("B", "1", "2", "N_08081_01002_GJ"),
    ("B", "2", "1", "N_08009_05001_GJ"),
    ("B", "2", "1", "N_08009_04002_GJ"),
    ("B", "2", "1", "N_08009_04001_GJ"),
]

# (Zone, Phase, Cycle, UPC) - same order as the CSV, duplicates removed
_all = list(dict.fromkeys(
    (str(r['Zone']).strip(), _num_str(r['Phase']), _num_str(r['Cycle']), str(r['UPC']).strip())
    for _, r in _ok.iterrows()
))
_excl = set(EXCLUDE_LIST)
RUN_LIST = [t for t in _all if t not in _excl]
_removed = [t for t in _all if t in _excl]

print(f"CSV rows: {len(_src)} | Fail Issue = 0: {int((_fail == 0).sum())} | blank Fail Issue: {int(_fail.isna().sum())} | "
      f"Fail Issue > 0: {int((_fail > 0).sum())}")
print(f"Fail Issue = 0 surveys: {len(_all)} | removed (in EXCLUDE_LIST): {len(_removed)} | RUN_LIST: {len(RUN_LIST)} surveys")
for t in _removed:
    print("  removed:", t)
for t in RUN_LIST[:10]:
    print(" ", t)

# COMMAND ----------

# DBTITLE 1,Configuration
import requests
import os
import pandas as pd

# ── Endpoint Configuration ──────────────────────────────────────────────────
# Old BASE_URL (Softsensor development server): https://development-api.dhms.prr.ai
# dbutils.widgets.text("BASE_URL", "https://development-api.dhms.prr.ai") # Softsensor Server? change this to NHAI/DIC Server ? BLOCKER
dbutils.widgets.text("BASE_URL", "https://nhaidhms-stage-api.dic.org.in")  # NHAI / DIC stage server
BASE_URL = dbutils.widgets.get("BASE_URL").rstrip("/")
UPLOAD_URL = f"{BASE_URL}/defects/images/upload"
SCHEMA = "dlm_postgres"

# ── Run list: built in the previous cell from Old silver population(As per contract).csv ──
# (Zone, Phase, Cycle, UPC) - only surveys with Fail Issue = 0. Run the previous cell first.
assert 'RUN_LIST' in globals() and RUN_LIST, "RUN_LIST is empty - run the 'Build RUN_LIST' cell first"

# DRY_RUN = True  -> no image upload and no MERGE; only shows what would be done
# DRY_RUN = False -> uploads images to the CDN and updates dlm_postgres + dlm_kpi
DRY_RUN = True

def run_filter(alias=""):
    """SQL condition that keeps only the rows of the surveys in RUN_LIST."""
    a = f"{alias}." if alias else ""
    return "(" + " OR ".join(
        f"({a}zone = '{z}' AND CAST({a}phase AS STRING) = '{p}' AND CAST({a}cycle AS STRING) = '{c}' AND {a}upc_code = '{u}')"
        for z, p, c, u in RUN_LIST) + ")"

# ── Find missing image_url for the surveys in the run list ───────────────────
_df_status = spark.sql(f"""
    SELECT upc_code, zone, CAST(phase AS STRING) AS phase, CAST(cycle AS STRING) AS cycle,
           COUNT(*) AS with_image_name,
           SUM(CASE WHEN image_url IS NULL THEN 1 ELSE 0 END) AS missing
    FROM (
        SELECT upc_code, zone, phase, cycle, image_url FROM nhai_dhms.{SCHEMA}.master_pothole WHERE image_name IS NOT NULL AND {run_filter()}
        UNION ALL
        SELECT upc_code, zone, phase, cycle, image_url FROM nhai_dhms.{SCHEMA}.master_cracking WHERE image_name IS NOT NULL AND {run_filter()}
    )
    GROUP BY upc_code, zone, CAST(phase AS STRING), CAST(cycle AS STRING)
""").toPandas()

print(f"Upload endpoint: {UPLOAD_URL}")
print(f"DRY_RUN = {DRY_RUN}")
print(f"\nRun list ({len(RUN_LIST)} surveys):")
_rows = []
for z, p, c, u in RUN_LIST:
    m = _df_status[(_df_status.upc_code == u) & (_df_status.zone == z) & (_df_status.phase == p) & (_df_status.cycle == c)]
    if m.empty:
        print(f"  {u}  Zone={z} Phase={p} Cycle={c}  -> NOT FOUND in {SCHEMA} (no rows with image_name) - skipped")
        continue
    r = m.iloc[0]
    note = "" if r.missing > 0 else "  -> nothing missing - skipped"
    print(f"  {u}  Zone={z} Phase={p} Cycle={c}  with image_name={r.with_image_name}  missing image_url={r.missing}{note}")
    if r.missing > 0:
        _rows.append(dict(upc_code=u, zone=z, phase=p, cycle=c, missing=int(r.missing)))
df_upcs = pd.DataFrame(_rows, columns=["upc_code", "zone", "phase", "cycle", "missing"])

# ── Auto-discover all defect images per UPC ────────────────────────────────
def build_image_index(upc, zone, phase, cycle):
    """Walk the entire deliverable tree and index all images.
       Categorise by 'crack' or 'pothole' in the path.
       Returns (crack_idx, pot_idx) where each is {filename: full_path}.
       Also indexes by stem (no extension) for image_names missing .jpg."""
    base = f"/Volumes/nhai_dhms/nsv/nsv_downloads/Zone_{zone}/Phase_{phase}/Cycle_{cycle}/{upc}/deliverable"
    crack_idx, pot_idx = {}, {}
    if not os.path.exists(base):
        return crack_idx, pot_idx, base
    for root, _, files in os.walk(base):
        path_lower = root.lower()
        for f in files:
            if not f.lower().endswith(('.jpg', '.jpeg', '.png', '.webp')):
                continue
            full = os.path.join(root, f)
            stem = os.path.splitext(f)[0]
            if 'crack' in path_lower:
                crack_idx[f] = full
                if stem not in crack_idx:
                    crack_idx[stem] = full
            elif 'pothole' in path_lower or 'potholes' in path_lower:
                pot_idx[f] = full
                if stem not in pot_idx:
                    pot_idx[stem] = full
    return crack_idx, pot_idx, base

# Build per-UPC config
upc_configs = []
for _, r in df_upcs.iterrows():
    crack_idx, pot_idx, base = build_image_index(r.upc_code, r.zone, r.phase, r.cycle)
    # count unique files (each file indexed twice: by name and by stem)
    crack_count = len(set(crack_idx.values()))
    pot_count = len(set(pot_idx.values()))
    upc_configs.append({
        "upc": r.upc_code, "zone": r.zone, "phase": r.phase, "cycle": r.cycle,
        "crack_idx": crack_idx, "pot_idx": pot_idx,
    })
    print(f"\n  {r.upc_code} Zone={r.zone} Phase={r.phase} Cycle={r.cycle} (base: {base}):")
    print(f"    Cracking images: {crack_count}")
    print(f"    Pothole images:  {pot_count}")

print(f"\n{len(upc_configs)} survey(s) ready for upload")

# COMMAND ----------

# DBTITLE 1,Bulk Upload — All UPCs
import time, re
from concurrent.futures import ThreadPoolExecutor, as_completed

MAX_WORKERS = 4
RETRY_LIMIT = 2
RETRY_DELAY = 1

session = requests.Session()
session.headers.update({"Connection": "keep-alive"})

def sanitize_id(raw_id):
    return re.sub(r'[^A-Za-z0-9._-]', '-', raw_id)[:128]

def bulk_upload_one(image_path, defect_type, display_defect_id):
    fname = os.path.basename(image_path)
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            with open(image_path, "rb") as fh:
                resp = session.post(
                    UPLOAD_URL,
                    files={"image": (fname, fh, "image/jpeg")},
                    data={"defect_type": defect_type, "display_defect_id": display_defect_id},
                    timeout=30,
                )
            if resp.status_code == 201:
                url = (resp.json().get("data") or {}).get("url", "")
                return (display_defect_id, True, url)
            else:
                err = f"HTTP {resp.status_code}: {resp.text[:200]}"
        except Exception as e:
            err = str(e)[:200]
        if attempt < RETRY_LIMIT:
            time.sleep(RETRY_DELAY)
    return (display_defect_id, False, err)

# ═════ LOOP OVER ALL UPCs WITH MISSING image_url ════════════════════════════════════
all_results = []
t_start = time.time()

for cfg in upc_configs:
    upc = cfg["upc"]
    crack_idx = cfg["crack_idx"]
    pot_idx = cfg["pot_idx"]
    # only this survey (zone / phase / cycle), so cycle 1 and cycle 2 of the same UPC are not mixed
    _svy = (f"upc_code = '{upc}' AND zone = '{cfg['zone']}' AND CAST(phase AS STRING) = '{cfg['phase']}' "
            f"AND CAST(cycle AS STRING) = '{cfg['cycle']}'")
    print(f"\n{'='*60}")
    print(f"UPC: {upc}  Zone={cfg['zone']} Phase={cfg['phase']} Cycle={cfg['cycle']}")
    print(f"{'='*60}")

    # Read warehouse records with missing image_url
    df_wh_pot = spark.sql(f"""
        SELECT display_defect_id, image_name
        FROM nhai_dhms.{SCHEMA}.master_pothole
        WHERE {_svy} AND image_name IS NOT NULL AND image_url IS NULL
    """).toPandas()

    df_wh_crack = spark.sql(f"""
        SELECT display_defect_id, image_name
        FROM nhai_dhms.{SCHEMA}.master_cracking
        WHERE {_svy} AND image_name IS NOT NULL AND image_url IS NULL
    """).toPandas()

    # Build tasks: match image_name to file index (handles missing .jpg extension)
    tasks, id_map = [], {}
    for _, row in df_wh_pot.iterrows():
        img = row["image_name"]
        path = pot_idx.get(img) or pot_idx.get(img.replace('.jpg','')) or crack_idx.get(img)
        if not path:
            continue
        safe_id = sanitize_id(row["display_defect_id"])
        tasks.append((path, "pothole", safe_id))
        id_map[safe_id] = row["display_defect_id"]

    for _, row in df_wh_crack.iterrows():
        img = row["image_name"]
        path = crack_idx.get(img) or crack_idx.get(img.replace('.jpg','')) or pot_idx.get(img)
        if not path:
            continue
        safe_id = sanitize_id(row["display_defect_id"])
        tasks.append((path, "cracking", safe_id))
        id_map[safe_id] = row["display_defect_id"]

    print(f"  Pothole:  {len(df_wh_pot)} warehouse rows → {sum(1 for t in tasks if t[1]=='pothole')} matched")
    print(f"  Cracking: {len(df_wh_crack)} warehouse rows → {sum(1 for t in tasks if t[1]=='cracking')} matched")

    if not tasks:
        print(f"  ⚠️  No images to upload for this UPC")
        continue

    if DRY_RUN:
        print(f"  DRY RUN: {len(tasks)} image(s) would be uploaded - nothing sent. Examples:")
        for t in tasks[:3]:
            print(f"    {t[1]:8s} {id_map[t[2]]}  <-  {t[0]}")
        continue

    # Upload
    ok, fail = 0, 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(bulk_upload_one, *t): t[2] for t in tasks}
        for i, fut in enumerate(as_completed(futures), 1):
            safe_id, success, info = fut.result()
            if success:
                ok += 1
                all_results.append((id_map[safe_id], info, "pothole" if any(t[2]==safe_id and t[1]=="pothole" for t in tasks) else "cracking"))
            else:
                fail += 1
                if fail <= 3:
                    print(f"    FAIL [{safe_id}]: {info}")
            if i % 100 == 0 or i == len(tasks):
                print(f"  Progress: {i}/{len(tasks)}  ✅{ok} ❌{fail}  ({time.time()-t0:.0f}s)")

    print(f"  Done: ✅ {ok}  ❌ {fail}  ({time.time()-t0:.1f}s)")

# Save all results to temp view for MERGE
from pyspark.sql.types import StructType, StructField, StringType
schema = StructType([
    StructField("display_defect_id", StringType()),
    StructField("cdn_url", StringType()),
    StructField("defect_type", StringType()),
])
df_urls = spark.createDataFrame(all_results if all_results else [], schema)
df_urls.createOrReplaceTempView("cdn_upload_results")

print(f"\n{'='*60}")
print("DRY RUN COMPLETE - no images uploaded" if DRY_RUN else "ALL UPLOADS COMPLETE — run next cell to MERGE")
print(f"{'='*60}")
print(f"Total: {len(all_results)} images uploaded across {len(upc_configs)} UPC(s)")
print(f"Total time: {time.time()-t_start:.0f}s")

# COMMAND ----------

# DBTITLE 1,MERGE CDN URLs into dlm_postgres and dlm_kpi
TABLES = {"master_pothole": "pothole", "master_cracking": "cracking"}

# Only the surveys in RUN_LIST (cell 2) are touched. DRY_RUN = True skips every MERGE.
if DRY_RUN:
    print("DRY RUN: no MERGE. Rows in dlm_kpi that would get an image_url from dlm_postgres (run-list surveys only):")
    for tbl in TABLES:
        display(spark.sql(f"""
            SELECT '{tbl}' AS tbl, t.upc_code, t.zone, t.phase, t.cycle, COUNT(*) AS would_update
            FROM nhai_dhms.dlm_kpi.{tbl} t
            JOIN (SELECT display_defect_id, MIN(image_url) AS image_url FROM nhai_dhms.dlm_postgres.{tbl}
                  WHERE display_defect_id IS NOT NULL AND image_url IS NOT NULL AND {run_filter()}
                  GROUP BY display_defect_id) s
              ON t.display_defect_id = s.display_defect_id
            WHERE {run_filter('t')} AND (t.image_url IS NULL OR t.image_url <> s.image_url)
            GROUP BY t.upc_code, t.zone, t.phase, t.cycle
        """))

# Step 1: dlm_postgres <- this session's uploads (skipped if Bulk Upload was not run in this session)
if DRY_RUN:
    pass
elif spark.catalog.tableExists("cdn_upload_results"):
    for tbl, dtype in TABLES.items():
        r = spark.sql(f"""
            MERGE INTO nhai_dhms.dlm_postgres.{tbl} AS t
            USING (
                SELECT display_defect_id, cdn_url FROM (
                    SELECT display_defect_id, cdn_url,
                           ROW_NUMBER() OVER (PARTITION BY display_defect_id ORDER BY cdn_url) AS rn
                    FROM cdn_upload_results WHERE defect_type = '{dtype}'
                ) WHERE rn = 1
            ) AS s
            ON t.display_defect_id = s.display_defect_id
            WHEN MATCHED THEN UPDATE SET t.image_url = s.cdn_url
        """).collect()[0]
        print(f"dlm_postgres.{tbl}: {r.num_updated_rows} rows updated")
else:
    print("cdn_upload_results not in this session - skipping dlm_postgres MERGE, using existing URLs")

# Step 2: dlm_kpi <- dlm_postgres (source of truth), run-list surveys only
for tbl in ([] if DRY_RUN else TABLES):
    # 2a. match on display_defect_id
    r1 = spark.sql(f"""
        MERGE INTO nhai_dhms.dlm_kpi.{tbl} AS t
        USING (
            SELECT display_defect_id, image_url FROM (
                SELECT display_defect_id, image_url,
                       ROW_NUMBER() OVER (PARTITION BY display_defect_id ORDER BY image_url) AS rn
                FROM nhai_dhms.dlm_postgres.{tbl}
                WHERE display_defect_id IS NOT NULL AND image_url IS NOT NULL AND {run_filter()}
            ) WHERE rn = 1
        ) AS s
        ON t.display_defect_id = s.display_defect_id AND {run_filter('t')}
        WHEN MATCHED AND (t.image_url IS NULL OR t.image_url <> s.image_url)
            THEN UPDATE SET t.image_url = s.image_url
    """).collect()[0]
    # 2b. rows with NULL display_defect_id: match on upc_code + image_name
    r2 = spark.sql(f"""
        MERGE INTO nhai_dhms.dlm_kpi.{tbl} AS t
        USING (
            SELECT upc_code, zone, phase, cycle, image_name, image_url FROM (
                SELECT upc_code, zone, CAST(phase AS STRING) AS phase, CAST(cycle AS STRING) AS cycle, image_name, image_url,
                       ROW_NUMBER() OVER (PARTITION BY upc_code, zone, CAST(phase AS STRING), CAST(cycle AS STRING), image_name ORDER BY image_url) AS rn
                FROM nhai_dhms.dlm_postgres.{tbl}
                WHERE image_name IS NOT NULL AND image_url IS NOT NULL AND {run_filter()}
            ) WHERE rn = 1
        ) AS s
        ON t.display_defect_id IS NULL AND t.upc_code = s.upc_code AND t.zone = s.zone
           AND CAST(t.phase AS STRING) = s.phase AND CAST(t.cycle AS STRING) = s.cycle
           AND t.image_name = s.image_name AND {run_filter('t')}
        WHEN MATCHED AND t.image_url IS NULL THEN UPDATE SET t.image_url = s.image_url
    """).collect()[0]
    print(f"dlm_kpi.{tbl}: {r1.num_updated_rows} by display_defect_id, {r2.num_updated_rows} by image_name")

# Step 3: verify both schemas per survey (run-list surveys only)
for tbl in TABLES:
    print(f"\n{tbl}:")
    display(spark.sql(f"""
        SELECT 'dlm_postgres' AS schema, upc_code, zone, CAST(phase AS STRING) AS phase, CAST(cycle AS STRING) AS cycle,
               COUNT(*) AS total, COUNT(image_name) AS has_name, COUNT(image_url) AS has_url,
               SUM(CASE WHEN image_name IS NOT NULL AND image_url IS NULL THEN 1 ELSE 0 END) AS name_no_url
        FROM nhai_dhms.dlm_postgres.{tbl} WHERE {run_filter()} GROUP BY ALL
        UNION ALL
        SELECT 'dlm_kpi', upc_code, zone, CAST(phase AS STRING), CAST(cycle AS STRING),
               COUNT(*), COUNT(image_name), COUNT(image_url),
               SUM(CASE WHEN image_name IS NOT NULL AND image_url IS NULL THEN 1 ELSE 0 END)
        FROM nhai_dhms.dlm_kpi.{tbl} WHERE {run_filter()} GROUP BY ALL
        ORDER BY upc_code, phase, cycle, schema
    """))