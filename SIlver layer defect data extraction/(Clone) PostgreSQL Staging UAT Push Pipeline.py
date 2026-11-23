# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # PostgreSQL Staging & UAT Push Pipeline
# MAGIC
# MAGIC Filtered push of Databricks data (2 UPC/phase/cycle combinations) to both `dhms_staging` and `dhms_uat` PostgreSQL databases. Includes truncate, alter, push, UUID mapping, asset mapping, and validation.

# COMMAND ----------

# DBTITLE 1,Configuration
import psycopg2
from psycopg2 import extras
import pandas as pd
import numpy as np
import json
from pyspark.sql import functions as F

# ============================================================
# CONFIG
# ============================================================

DB_HOST     = "dhms-development-dhms-development-postgresql.cdk480kqufch.ap-south-1.rds.amazonaws.com"
DB_PORT     = 5432
DB_USER     = "dhms_admin"
DB_PASSWORD = "36D95ACB4CE7uiunD7DF18F4928F26836"
DB_SCHEMA   = "public"

# DRY_RUN = True  -> only read and report (Dry Run Preview cell); every cell that writes to PostgreSQL stops with a message
# DRY_RUN = False -> the write cells (ALTER, push, mapping, delete + re-push) run for real
DRY_RUN = True

# Target databases (dhms_staging is not used for now)
TARGET_DBS = ["dhms_uat"]

# Databricks source
#   dlm_postgres = DLM output, breach rows only (the copy made for PostgreSQL)
#   dlm_kpi      = DLM output, all rows (breach + non-breach)
DBX_CATALOG = "nhai_dhms"
DBX_SCHEMA  = "dlm_kpi"      # to push all rows both BREACH and NON BREACH

# Tables to process
TABLE_LIST = [
    "master_pothole",
    "master_cracking",
    "master_roughness_parent",
    "master_roughness_child_10m",
    "master_roughness_child_100m",
    "master_rutting_parent",
    "master_rutting_child_10m",
    "master_rutting_child_100m",
    "master_ravelling_parent",
    "master_ravelling_child_10m",
    "master_ravelling_child_100m",
]

# Zone / Phase / Cycle / UPC combinations to push: (zone, phase, cycle, upc_code)
RUN_LIST = [
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

# Same format as before (cycle as int) so all later cells keep working
UPC_FILTER = [{"zone": z, "upc_code": u, "phase": str(p), "cycle": int(c)} for z, p, c, u in RUN_LIST]

# Same surveys in the slash format used by the GPS / geometry tables (N/04006/03001/GJ)
GPS_UPC_FILTER = [{"upc": f["upc_code"].replace("_", "/"), "phase": f["phase"], "cycle": f["cycle"]} for f in UPC_FILTER]

# Single-survey targets used by the check / delete + re-push / verify / sample cells (first entry of UPC_FILTER)
TARGET_ZONE           = UPC_FILTER[0]["zone"]
TARGET_UPC_UNDERSCORE = UPC_FILTER[0]["upc_code"]           # defect tables
TARGET_UPC_SLASH      = GPS_UPC_FILTER[0]["upc"]            # geometry tables
TARGET_PHASE          = UPC_FILTER[0]["phase"]
TARGET_CYCLE          = UPC_FILTER[0]["cycle"]

BATCH_SIZE = 500

# Columns needing special PostgreSQL type casting
JSONB_COLUMNS = {
    "nsv", "dams", "dashcam", "dlm",
    "project_details", "standard_flags", "thresholds", "measures"
}
UUID_COLUMNS = {
    "project_id", "region_id", "piu_id", "state_id",
    "zone_id", "contractor_id", "ae_ie_id"
}

# Missing columns to add during ALTER
MISSING_COLUMNS = {
    "affected_length":        "DOUBLE PRECISION",
    "total_length":           "DOUBLE PRECISION",
    "affected_length_ca":     "DOUBLE PRECISION",
    "affected_length_irc":    "DOUBLE PRECISION",
    "display_defect_id_old":  "TEXT",
    "asset_mapping_id":       "UUID",
    "recurring_defect":       "BOOLEAN DEFAULT FALSE",
}

# Type fixes
TYPE_FIXES = {
    "pci":          "DOUBLE PRECISION",
    "breach_flag":  "INTEGER",
}

# Master lookup tables for ID resolution
MASTER_TABLES = [
    "master_upc_project", "master_region_zone", "master_piu",
    "master_state", "master_zone", "master_contractor", "master_ae_ie",
]

print("\u2705 Configuration loaded")
print(f"DRY_RUN: {DRY_RUN}")
print(f"Source: {DBX_CATALOG}.{DBX_SCHEMA}")
print(f"Target databases: {TARGET_DBS}")
print(f"Tables to process: {len(TABLE_LIST)}")
print(f"UPC filter: {len(UPC_FILTER)} combinations")
for f in UPC_FILTER:
    print(f"   zone={f['zone']}  {f['upc_code']}  phase={f['phase']}  cycle={f['cycle']}")

# COMMAND ----------

# DBTITLE 1,Dry Run Preview (read-only)
# ============================================================
# DRY RUN PREVIEW — READ ONLY (no write to Databricks or PostgreSQL)
# Shows what the write cells would do for UPC_FILTER / TARGET_DBS
# ============================================================
_src_filter = None
for f in UPC_FILTER:
    c = ((F.col("zone") == f["zone"]) & (F.col("upc_code") == f["upc_code"])
         & (F.col("phase").cast("string") == f["phase"]) & (F.col("cycle").cast("string") == str(f["cycle"])))
    _src_filter = c if _src_filter is None else _src_filter | c

# ---- 1. Databricks source rows ----
print(f"SOURCE {DBX_CATALOG}.{DBX_SCHEMA}  (DRY_RUN = {DRY_RUN})")
_src = {}
for t in TABLE_LIST:
    try:
        sdf = spark.table(f"{DBX_CATALOG}.{DBX_SCHEMA}.{t}").filter(_src_filter)
        n = sdf.count()
        _src[t] = (n, sdf.columns)
        print(f"   {t:<32s} {n:>8,} rows to push")
    except Exception as e:
        _src[t] = (0, [])
        print(f"   {t:<32s} READ ERROR: {e}")
_gps_filter = None
for f in GPS_UPC_FILTER:
    c = (F.col("upc") == f["upc"]) & (F.col("phase") == f["phase"]) & (F.col("cycle") == f["cycle"])
    _gps_filter = c if _gps_filter is None else _gps_filter | c
_gps_n = spark.table("nhai_dhms.bronze_layer.nsv_gps_geometry").filter(_gps_filter).count()
print(f"   {'nsv_gps_geometry (segment/child)':<32s} {_gps_n:>8,} rows to push")

# ---- 2. PostgreSQL targets (read-only session) ----
for db_name in TARGET_DBS:
    print(f"\n{'='*70}\nTARGET {db_name}.{DB_SCHEMA}\n{'='*70}")
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=db_name, user=DB_USER, password=DB_PASSWORD)
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()
    rows = []
    for t in TABLE_LIST:
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s", (DB_SCHEMA, t))
        pg_cols = {r[0] for r in cur.fetchall()}
        n_src, src_cols = _src[t]
        if not pg_cols:
            rows.append(dict(table=t, pg_table="NOT FOUND", src_rows=n_src)); continue
        cur.execute(f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."{t}"')
        pg_total = cur.fetchone()[0]
        cur.execute(f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."{t}" WHERE upc_code=%s AND phase=%s AND cycle=%s',
                    (TARGET_UPC_UNDERSCORE, TARGET_PHASE, TARGET_CYCLE))
        pg_target = cur.fetchone()[0]
        rows.append(dict(
            table=t, pg_table="OK", src_rows=n_src,
            pg_rows_now_for_target=pg_target,      # DELETE & RE-PUSH would delete these
            pg_rows_total=pg_total,
            cols_pushed=len([c for c in src_cols if c in pg_cols]),
            src_cols_not_in_pg=", ".join(sorted(c for c in src_cols if c not in pg_cols)),      # dropped on push
            pg_cols_to_add=", ".join(c for c in MISSING_COLUMNS if c not in pg_cols),          # ALTER would add
        ))
    display(pd.DataFrame(rows))

    # geometry tables
    for gt in ["master_geometry_segment", "master_geometry_parent"]:
        try:
            cur.execute(f'SELECT COUNT(*), COUNT(*) FILTER (WHERE upc = %s) FROM "{DB_SCHEMA}"."{gt}"', (TARGET_UPC_SLASH,))
            tot, tgt = cur.fetchone()
            print(f"   {gt:<26s} total={tot:,}  target UPC={tgt:,}  (GPS cell TRUNCATES all {tot:,}; re-push cell deletes {tgt:,})")
        except Exception as e:
            print(f"   {gt}: {e}")

    # lookups used by the ID mapping
    cur.execute(f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."master_upc_project" WHERE upc = %s', (TARGET_UPC_SLASH,))
    print(f"   master_upc_project rows for {TARGET_UPC_SLASH}: {cur.fetchone()[0]}  (0 -> project_id etc. stay empty)")
    cur.execute(f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."master_zone" WHERE zone_code = %s', (TARGET_ZONE,))
    print(f"   master_zone rows for zone {TARGET_ZONE}: {cur.fetchone()[0]}  (0 -> zone_id stays empty)")
    cur.close(); conn.close()

print("\nDRY RUN: nothing was written." if DRY_RUN else "\nDRY_RUN = False: the write cells below will change PostgreSQL.")

# COMMAND ----------

# DBTITLE 1,Truncate All Tables in Both Databases
# # ============================================================
# # TRUNCATE ALL TABLES IN BOTH DATABASES
# # ============================================================

# for db_name in TARGET_DBS:
#     print(f"\n{'='*70}")
#     print(f"TRUNCATING tables in: {db_name}")
#     print(f"{'='*70}\n")

#     conn = psycopg2.connect(
#         host=DB_HOST, port=DB_PORT, database=db_name,
#         user=DB_USER, password=DB_PASSWORD
#     )
#     conn.autocommit = True
#     cur = conn.cursor()

#     total_removed = 0

#     for table in TABLE_LIST:
#         try:
#             cur.execute(f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."{table}"')
#             count = cur.fetchone()[0]
#             cur.execute(f'TRUNCATE TABLE "{DB_SCHEMA}"."{table}" CASCADE')
#             total_removed += count
#             print(f"   \u2705 TRUNCATED {table:40s} ({count:,} rows removed)")
#         except Exception as e:
#             print(f"   \u274c {table}: {e}")

#     cur.close()
#     conn.close()

#     print(f"\n\u2705 {db_name}: All tables truncated \u2014 {total_removed:,} total rows removed")

# COMMAND ----------

# DBTITLE 1,ALTER Tables — Add Missing Columns & Fix Types
# ============================================================
# ALTER TABLES — Add Missing Columns & Fix Type Mismatches
# ============================================================
if DRY_RUN:
    raise RuntimeError("DRY_RUN = True: this cell writes to PostgreSQL (ALTER TABLE), so it was not run. "
                       "Check the Dry Run Preview cell, then set DRY_RUN = False in Configuration.")

for db_name in TARGET_DBS:
    print(f"\n{'='*70}")
    print(f"ALTERING tables in: {db_name}")
    print(f"{'='*70}\n")

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=db_name,
        user=DB_USER, password=DB_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()

    success_count = 0
    skip_count = 0

    for pg_table in TABLE_LIST:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (DB_SCHEMA, pg_table))
        existing_cols = {row[0] for row in cur.fetchall()}

        # ADD missing columns
        for col_name, col_type in MISSING_COLUMNS.items():
            if col_name in existing_cols:
                skip_count += 1
                continue
            try:
                cur.execute(f'ALTER TABLE "{DB_SCHEMA}"."{pg_table}" ADD COLUMN "{col_name}" {col_type}')
                print(f"   \u2705 {pg_table}.{col_name}  ADD {col_type}")
                success_count += 1
            except Exception as e:
                print(f"   \u274c {pg_table}.{col_name} ADD \u2014 {e}")

        # ALTER column types
        for col_name, new_type in TYPE_FIXES.items():
            if col_name not in existing_cols:
                skip_count += 1
                continue
            try:
                cur.execute(f'ALTER TABLE "{DB_SCHEMA}"."{pg_table}" ALTER COLUMN "{col_name}" TYPE {new_type} USING "{col_name}"::{new_type}')
                print(f"   \u2705 {pg_table}.{col_name}  ALTER -> {new_type}")
                success_count += 1
            except Exception as e:
                print(f"   \u274c {pg_table}.{col_name} ALTER \u2014 {e}")

    cur.close()
    conn.close()

    print(f"\n\u2705 {db_name}: ALTER complete \u2014 {success_count} changes, {skip_count} skipped")

# COMMAND ----------

# DBTITLE 1,Push Filtered Data to Both Databases
# ============================================================
# PUSH FILTERED DATA FROM DATABRICKS TO BOTH DATABASES
# NOTE: plain INSERT, no delete first -> running it twice duplicates rows.
#       For one survey prefer the DELETE & RE-PUSH cell below.
# ============================================================
if DRY_RUN:
    raise RuntimeError("DRY_RUN = True: this cell writes to PostgreSQL (INSERT), so it was not run. "
                       "Check the Dry Run Preview cell, then set DRY_RUN = False in Configuration.")

def clean_value(val, col_name, pg_udt):
    """Convert a single Python/pandas value for PostgreSQL insertion."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, pd._libs.tslibs.nattype.NaTType):
        return None
    if col_name in JSONB_COLUMNS:
        if isinstance(val, dict):
            return json.dumps(val)
        if isinstance(val, str):
            return val
        return json.dumps(val)
    if col_name in UUID_COLUMNS:
        s = str(val).strip()
        return s if s and s.lower() != "none" else None
    if pg_udt in ("_text", "_varchar"):
        if isinstance(val, (list, np.ndarray)):
            return list(val)
        if isinstance(val, str):
            try:
                parsed = json.loads(val)
                if isinstance(parsed, list):
                    return parsed
            except Exception:
                pass
            return [val]
        return None
    if pg_udt == "bool":
        if isinstance(val, (bool, np.bool_)):
            return bool(val)
        return val
    return val


def get_pg_columns(cur, table_name):
    """Return ordered dict: column_name -> udt_name."""
    cur.execute("""
        SELECT column_name, udt_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (DB_SCHEMA, table_name))
    return {row[0]: row[1] for row in cur.fetchall()}


# Build Spark filter condition for the specified UPC/phase/cycle combos
filter_cond = None
for f in UPC_FILTER:
    cond = (
        (F.col("zone") == f["zone"]) &
        (F.col("upc_code") == f["upc_code"]) &
        (F.col("phase") == f["phase"]) &
        (F.col("cycle") == f["cycle"])
    )
    filter_cond = cond if filter_cond is None else filter_cond | cond

# Process each database
for db_name in TARGET_DBS:
    print(f"\n{'='*70}")
    print(f"\U0001f680 PUSHING DATA TO: {db_name}")
    print(f"{'='*70}\n")

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=db_name,
        user=DB_USER, password=DB_PASSWORD
    )
    cur = conn.cursor()

    # Disable FK constraints
    cur.execute("SET session_replication_role = 'replica';")
    conn.commit()
    print("\u26a0\ufe0f  FK constraints DISABLED\n")

    push_summary = []

    for dbx_table in TABLE_LIST:
        fq_table = f"{DBX_CATALOG}.{DBX_SCHEMA}.{dbx_table}"
        pg_table = dbx_table

        print(f"  \U0001f680 {dbx_table}")

        pg_cols_info = get_pg_columns(cur, pg_table)
        if not pg_cols_info:
            print(f"     \u26a0\ufe0f  PG table not found, skipping")
            push_summary.append((dbx_table, pg_table, 0, "TABLE NOT FOUND"))
            continue

        try:
            spark_df = spark.table(fq_table)
            filtered_df = spark_df.filter(filter_cond)
            row_count = filtered_df.count()
            print(f"     Filtered rows: {row_count:,}")

            if row_count == 0:
                print(f"     \u26a0\ufe0f  No matching data")
                push_summary.append((dbx_table, pg_table, 0, "NO MATCH"))
                continue

            pdf = filtered_df.toPandas()
        except Exception as e:
            print(f"     \u274c Read failed: {e}")
            push_summary.append((dbx_table, pg_table, 0, f"ERROR: {e}"))
            continue

        # Find common columns
        common_cols = [c for c in pg_cols_info.keys() if c in pdf.columns]
        pdf = pdf[common_cols]

        col_names = ", ".join(f'"{ c}"' for c in common_cols)
        placeholders = []
        for c in common_cols:
            if c in JSONB_COLUMNS:
                placeholders.append("%s::jsonb")
            elif c in UUID_COLUMNS:
                placeholders.append("%s::uuid")
            else:
                placeholders.append("%s")

        template = f"({', '.join(placeholders)})"
        insert_sql = f'INSERT INTO "{DB_SCHEMA}"."{pg_table}" ({col_names}) VALUES %s'

        rows = []
        for _, row in pdf.iterrows():
            cleaned = tuple(
                clean_value(row[c], c, pg_cols_info.get(c, ""))
                for c in common_cols
            )
            rows.append(cleaned)

        try:
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i : i + BATCH_SIZE]
                extras.execute_values(
                    cur, insert_sql, batch,
                    template=template, page_size=BATCH_SIZE
                )
                conn.commit()

            print(f"     \u2705 Pushed {len(rows):,} rows")
            push_summary.append((dbx_table, pg_table, len(rows), "SUCCESS"))
        except Exception as e:
            conn.rollback()
            print(f"     \u274c Insert failed: {e}")
            push_summary.append((dbx_table, pg_table, 0, f"ERROR: {e}"))

    # Re-enable FK constraints
    cur.execute("SET session_replication_role = 'origin';")
    conn.commit()
    print("\n\u2705 FK constraints RE-ENABLED")

    cur.close()
    conn.close()

    print(f"\n--- {db_name} PUSH SUMMARY ---")
    summary_df = pd.DataFrame(push_summary, columns=["databricks_table", "pg_table", "rows_pushed", "status"])
    display(summary_df)
    total = summary_df["rows_pushed"].sum()
    print(f"\u2705 {db_name}: Total rows pushed: {total:,}\n")

# COMMAND ----------

# DBTITLE 1,Map UUID Columns from Master Lookup Tables
# ============================================================
# MAP UUID COLUMNS FROM MASTER LOOKUP TABLES
# NOTE: updates every row of each table (all UPCs), not only UPC_FILTER
# ============================================================
if DRY_RUN:
    raise RuntimeError("DRY_RUN = True: this cell writes to PostgreSQL (UPDATE), so it was not run. "
                       "Check the Dry Run Preview cell, then set DRY_RUN = False in Configuration.")

for db_name in TARGET_DBS:
    print(f"\n{'='*70}")
    print(f"MAPPING UUIDs in: {db_name}")
    print(f"{'='*70}\n")

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=db_name,
        user=DB_USER, password=DB_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Check master tables
    print("Master lookup tables:")
    for mt in MASTER_TABLES:
        try:
            cur.execute(f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."{mt}"')
            count = cur.fetchone()[0]
            status = "\u2705" if count > 0 else "\u26a0\ufe0f  EMPTY"
            print(f"   {status}  {mt:<25s}  {count:>6,} rows")
        except Exception as e:
            print(f"   \u274c  {mt:<25s}  NOT FOUND")

    print()
    map_log = []

    for table in TABLE_LIST:
        cur.execute(f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."{table}"')
        row_count = cur.fetchone()[0]
        if row_count == 0:
            map_log.append((table, 0, 0, "SKIPPED (empty)"))
            continue

        # Map from master_upc_project (project_id + cascaded IDs)
        upc_mapped = 0
        try:
            cur.execute(f'''
                UPDATE "{DB_SCHEMA}"."{table}" t
                SET project_id    = p.id,
                    region_id     = p.region_id,
                    piu_id        = p.piu_id,
                    state_id      = p.state_id,
                    contractor_id = p.contractor_id,
                    ae_ie_id      = p.ae_ie_id
                FROM "{DB_SCHEMA}"."master_upc_project" p
                WHERE t.upc = p.upc
            ''')
            upc_mapped = cur.rowcount
        except Exception as e:
            print(f"   \u274c  {table} UPC mapping: {e}")

        # Map zone_id from master_zone
        zone_mapped = 0
        try:
            cur.execute(f'''
                UPDATE "{DB_SCHEMA}"."{table}" t
                SET zone_id = z.id
                FROM "{DB_SCHEMA}"."master_zone" z
                WHERE t.zone = z.zone_code
            ''')
            zone_mapped = cur.rowcount
        except Exception as e:
            print(f"   \u274c  {table} zone mapping: {e}")

        pct = (upc_mapped / row_count * 100) if row_count > 0 else 0
        status = "\u2705" if upc_mapped > 0 else "\u26a0\ufe0f"
        print(f"   {status}  {table:<35s}  UPC\u2192{upc_mapped:>6,}/{row_count:,} ({pct:.0f}%)  zone\u2192{zone_mapped:>6,}")
        map_log.append((table, upc_mapped, zone_mapped, "OK" if upc_mapped > 0 else "NO MATCH"))

    cur.close()
    conn.close()

    print(f"\n--- {db_name} UUID MAPPING SUMMARY ---")
    log_df = pd.DataFrame(map_log, columns=["table", "upc_mapped", "zone_mapped", "status"])
    display(log_df)
    print(f"\n\u2139\ufe0f  Columns mapped via master_upc_project (by upc):")
    print("   project_id, region_id, piu_id, state_id, contractor_id, ae_ie_id")
    print("   Column mapped via master_zone (by zone): zone_id")

# COMMAND ----------

# DBTITLE 1,Map Asset Type & Set Recurring Defect Flag
# ============================================================
# MAP ASSET TYPE & SET RECURRING DEFECT FLAG
# NOTE: updates every row of each table (all UPCs), not only UPC_FILTER
# ============================================================
if DRY_RUN:
    raise RuntimeError("DRY_RUN = True: this cell writes to PostgreSQL (UPDATE), so it was not run. "
                       "Check the Dry Run Preview cell, then set DRY_RUN = False in Configuration.")

for db_name in TARGET_DBS:
    print(f"\n{'='*70}")
    print(f"MAPPING ASSET TYPE in: {db_name}")
    print(f"{'='*70}\n")

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=db_name,
        user=DB_USER, password=DB_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Check if master_defect_type_group exists
    has_dtg = True
    try:
        cur.execute('''
            SELECT dtg.defect_type, am.id, am.asset_name, am.code
            FROM "public"."master_defect_type_group" dtg
            JOIN "public"."master_asset_mapping" am ON dtg.asset_mapping_id = am.id
            ORDER BY dtg.defect_type
        ''')
        mapping_rows = cur.fetchall()
        for r in mapping_rows:
            print(f"   {r[0]:<15s} -> {str(r[1])[:8]}.. ({r[2]} / {r[3]})")
        print(f"   {len(mapping_rows)} defect type(s) mapped\n")
    except Exception as e:
        has_dtg = False
        conn.rollback()
        print(f"   \u26a0\ufe0f  master_defect_type_group not found \u2014 skipping asset mapping\n")

    map_log = []

    for table in TABLE_LIST:
        cur.execute(f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."{table}"')
        total = cur.fetchone()[0]
        if total == 0:
            map_log.append((table, 0, total, "SKIPPED (empty)"))
            continue

        # Map asset_mapping_id via master_defect_type_group
        mapped = 0
        if has_dtg:
            try:
                cur.execute(f'''
                    UPDATE "{DB_SCHEMA}"."{table}" t
                    SET asset_mapping_id = dtg.asset_mapping_id
                    FROM "{DB_SCHEMA}"."master_defect_type_group" dtg
                    WHERE t.defect_type = dtg.defect_type
                ''')
                mapped = cur.rowcount
            except Exception as e:
                print(f"   \u274c  {table} asset mapping: {e}")

        # Set recurring_defect = false for all rows
        try:
            cur.execute(f'''
                UPDATE "{DB_SCHEMA}"."{table}"
                SET recurring_defect = false
                WHERE recurring_defect IS NULL
            ''')
        except Exception:
            pass

        status = "\u2705" if mapped > 0 else "\u26a0\ufe0f"
        print(f"   {status}  {table:<35s}  asset\u2192{mapped:>6,}/{total:,}")
        map_log.append((table, mapped, total, "OK" if mapped > 0 else "NO MATCH"))

    cur.close()
    conn.close()

    print(f"\n--- {db_name} ASSET MAPPING SUMMARY ---")
    log_df = pd.DataFrame(map_log, columns=["table", "rows_mapped", "total_rows", "status"])
    display(log_df)

# COMMAND ----------

# DBTITLE 1,GPS Geometry Push — segment, parent, child
import uuid as _uuid
from datetime import datetime

# ============================================================
# GPS GEOMETRY PUSH — 3 tables to both databases
# Source: nhai_dhms.bronze_layer.nsv_gps_geometry
# Targets: master_geometry_segment, _parent, _child
# WARNING: TRUNCATES the 3 geometry tables first -> geometry of ALL UPCs is removed.
#          For one survey prefer the DELETE & RE-PUSH (GEOMETRY) cell below.
# ============================================================
if DRY_RUN:
    raise RuntimeError("DRY_RUN = True: this cell writes to PostgreSQL (TRUNCATE + INSERT), so it was not run. "
                       "Check the Dry Run Preview cell, then set DRY_RUN = False in Configuration.")

# Helper: get PG column info (self-contained so this cell doesn't depend on cell 5)
def get_pg_columns(cur, table_name):
    """Return ordered dict: column_name -> udt_name."""
    cur.execute("""
        SELECT column_name, udt_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (DB_SCHEMA, table_name))
    return {row[0]: row[1] for row in cur.fetchall()}

GPS_TABLE = "nhai_dhms.bronze_layer.nsv_gps_geometry"
GEOM_TABLES = ["master_geometry_segment", "master_geometry_parent", "master_geometry_child"]

# UPC filter (slash format — GPS table uses slashes): GPS_UPC_FILTER comes from the Configuration cell

# ---- 1. READ & FILTER GPS DATA ----
gps_spark = spark.table(GPS_TABLE)

filt = None
for f in GPS_UPC_FILTER:
    c = (F.col("upc") == f["upc"]) & (F.col("phase") == f["phase"]) & (F.col("cycle") == f["cycle"])
    filt = c if filt is None else filt | c

gps_pdf = gps_spark.filter(filt).toPandas()
print(f"Filtered GPS rows: {len(gps_pdf):,}")
for f in GPS_UPC_FILTER:
    print(f"  {f['upc']} p{f['phase']}c{f['cycle']}: {len(gps_pdf[gps_pdf['upc']==f['upc']]):,}")

# ---- 2. RENAME COLUMNS to match PG lowercase ----
rename_map = {
    "NH_Number": "nh_number", "Section_Code": "section_code",
    "Direction": "direction", "Start_Chainage": "start_chainage",
    "End_Chainage": "end_chainage", "Start_Latitude": "start_latitude",
    "Start_Longitude": "start_longitude", "End_Latitude": "end_latitude",
    "End_Longitude": "end_longitude", "GPS_Coverage": "gps_coverage",
    "Lane": "lane",
}
gps_r = gps_pdf.rename(columns=rename_map)

now_ts = datetime.utcnow()

# ---- 3. BUILD master_geometry_segment DataFrame ----
seg = gps_r.copy()
seg["id"]            = [str(_uuid.uuid4()) for _ in range(len(seg))]
seg["survey_cycle"]  = seg["cycle"]
seg["source_system"] = "nsv"
seg["created_at"]    = now_ts
seg["updated_at"]    = now_ts
print(f"\nSegment rows to push: {len(seg):,}")

# ---- 4. BUILD master_geometry_parent DataFrame (aggregated) ----
groups = gps_r.groupby(["upc", "nh_number", "section_code"])
parents = []
parent_ids = {}  # (upc, nh, sec) -> parent uuid

for (upc, nh, sec), grp in groups:
    pid = str(_uuid.uuid4())
    parent_ids[(upc, nh, sec)] = pid

    min_row = grp.loc[grp["start_chainage"].idxmin()]
    max_row = grp.loc[grp["end_chainage"].idxmax()]

    parents.append({
        "id": pid,
        "upc": upc,
        "nh_number": nh,
        "section_code": sec,
        "source_system": "nsv",
        "lanes": list(grp["lane"].dropna().unique()),
        "start_chainage": float(grp["start_chainage"].min()),
        "end_chainage":   float(grp["end_chainage"].max()),
        "start_lat":  float(min_row["start_latitude"]),
        "start_long": float(min_row["start_longitude"]),
        "end_lat":    float(max_row["end_latitude"]),
        "end_long":   float(max_row["end_longitude"]),
        "total_records": len(grp),
        "lane_length_surveyed_km": round(
            (grp["end_chainage"].sum() - grp["start_chainage"].sum()) / 1000.0, 3
        ),
        "created_at": now_ts,
        "updated_at": now_ts,
    })

par_df = pd.DataFrame(parents)
print(f"Parent rows to push: {len(par_df):,}")

# ---- 5. BUILD master_geometry_child DataFrame ----
child = gps_r.copy()
child["id"]        = [str(_uuid.uuid4()) for _ in range(len(child))]
child["child_id"]  = [str(_uuid.uuid4()) for _ in range(len(child))]
child["parent_id"] = child.apply(
    lambda r: parent_ids.get((r["upc"], r["nh_number"], r["section_code"])), axis=1
)
child["created_at"] = now_ts
print(f"Child rows to push: {len(child):,}")

# ---- 6. GENERIC PUSH FUNCTION ----
def push_geo_df(pdf, pg_table, cur, conn):
    """Push a DataFrame to a PG geometry table. Returns rows pushed."""
    pg_cols = get_pg_columns(cur, pg_table)
    if not pg_cols:
        print(f"     \u26a0\ufe0f  {pg_table} not found in PG")
        return 0

    common = [c for c in pg_cols if c in pdf.columns]
    push_pdf = pdf[common]

    col_str = ", ".join(f'"{ c}"' for c in common)
    phs = []
    for c in common:
        udt = pg_cols.get(c, "")
        if udt == "jsonb":
            phs.append("%s::jsonb")
        elif udt == "uuid":
            phs.append("%s::uuid")
        else:
            phs.append("%s")

    tpl = f"({', '.join(phs)})"
    sql = f'INSERT INTO "{DB_SCHEMA}"."{pg_table}" ({col_str}) VALUES %s'

    rows = []
    for _, row in push_pdf.iterrows():
        vals = []
        for c in common:
            v = row[c]
            if v is None or (isinstance(v, float) and np.isnan(v)):
                vals.append(None)
            elif isinstance(v, pd._libs.tslibs.nattype.NaTType):
                vals.append(None)
            elif pg_cols.get(c) in ("_text", "_varchar"):
                vals.append(list(v) if isinstance(v, (list, np.ndarray)) else [str(v)])
            elif pg_cols.get(c) == "jsonb":
                vals.append(json.dumps(v) if not isinstance(v, str) else v)
            else:
                vals.append(v)
        rows.append(tuple(vals))

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        extras.execute_values(cur, sql, batch, template=tpl, page_size=BATCH_SIZE)
        conn.commit()

    return len(rows)


# ---- 7. PUSH TO BOTH DATABASES ----
for db_name in TARGET_DBS:
    print(f"\n{'='*70}")
    print(f"\U0001f6f0\ufe0f  GPS GEOMETRY PUSH TO: {db_name}")
    print(f"{'='*70}\n")

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=db_name,
        user=DB_USER, password=DB_PASSWORD
    )
    cur = conn.cursor()
    cur.execute("SET session_replication_role = 'replica';")
    conn.commit()

    # Truncate geometry tables
    for gt in GEOM_TABLES:
        try:
            cur.execute(f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."{gt}"')
            old = cur.fetchone()[0]
            cur.execute(f'TRUNCATE TABLE "{DB_SCHEMA}"."{gt}" CASCADE')
            conn.commit()
            print(f"   \u2705 TRUNCATED {gt} ({old:,} rows)")
        except Exception as e:
            conn.rollback()
            print(f"   \u274c TRUNCATE {gt}: {e}")

    print()

    # Push each table
    for tbl_name, tbl_pdf in [
        ("master_geometry_segment", seg),
        ("master_geometry_parent",  par_df),
        ("master_geometry_child",   child),
    ]:
        try:
            n = push_geo_df(tbl_pdf, tbl_name, cur, conn)
            print(f"   \u2705 {tbl_name:<30s}  {n:>8,} rows pushed")
        except Exception as e:
            conn.rollback()
            print(f"   \u274c {tbl_name}: {e}")

    # Map project_id from master_upc_project
    print(f"\n   Mapping project_id from master_upc_project...")

    # master_geometry_segment: project_id is UUID type
    try:
        cur.execute(f'''
            UPDATE "{DB_SCHEMA}"."master_geometry_segment" t
            SET project_id = p.id::uuid
            FROM "{DB_SCHEMA}"."master_upc_project" p
            WHERE t.upc = p.upc
        ''')
        conn.commit()
        print(f"   \u2705 master_geometry_segment: project_id mapped for {cur.rowcount:,} rows")
    except Exception as e:
        conn.rollback()
        print(f"   \u26a0\ufe0f  master_geometry_segment: {e}")

    # master_geometry_parent: project_id is varchar
    try:
        cur.execute(f'''
            UPDATE "{DB_SCHEMA}"."master_geometry_parent" t
            SET project_id = p.id::varchar
            FROM "{DB_SCHEMA}"."master_upc_project" p
            WHERE t.upc = p.upc
        ''')
        conn.commit()
        print(f"   \u2705 master_geometry_parent: project_id mapped for {cur.rowcount:,} rows")
    except Exception as e:
        conn.rollback()
        print(f"   \u26a0\ufe0f  master_geometry_parent: {e}")

    # master_geometry_child: no upc column — map via parent
    try:
        cur.execute(f'''
            UPDATE "{DB_SCHEMA}"."master_geometry_child" c
            SET parent_id = p.id
            FROM "{DB_SCHEMA}"."master_geometry_parent" p
            WHERE c.parent_id = p.id
              AND p.project_id IS NOT NULL
        ''')
        conn.commit()
        print(f"   \u2705 master_geometry_child: parent linkage verified for {cur.rowcount:,} rows")
    except Exception as e:
        conn.rollback()
        print(f"   \u26a0\ufe0f  master_geometry_child: {e}")

    cur.execute("SET session_replication_role = 'origin';")
    conn.commit()
    cur.close()
    conn.close()

    print(f"\n\u2705 {db_name}: GPS geometry push complete")

# COMMAND ----------

# DBTITLE 1,Validate — Row Counts & Samples (Defect + Geometry)
# ============================================================
# VALIDATE — ROW COUNTS & SAMPLES IN BOTH DATABASES
# ============================================================

for db_name in TARGET_DBS:
    print(f"\n{'='*70}")
    print(f"VALIDATING: {db_name}")
    print(f"{'='*70}\n")

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=db_name,
        user=DB_USER, password=DB_PASSWORD
    )
    cur = conn.cursor()

    ALL_TABLES = TABLE_LIST + ["master_geometry_segment", "master_geometry_parent", "master_geometry_child"]
    results = []

    for table in ALL_TABLES:
        try:
            cur.execute(f'SELECT COUNT(*) FROM "{DB_SCHEMA}"."{table}"')
            pg_count = cur.fetchone()[0]
        except Exception:
            pg_count = -1

        status = "\u2705" if pg_count > 0 else "\u26a0\ufe0f  EMPTY"
        print(f"   {status}  {table:<40s}  PG={pg_count:>8,}")
        results.append({"table": table, "pg_rows": pg_count, "status": status})

    # Show unique UPC/phase/cycle combos pushed
    print(f"\n   Unique UPC/phase/cycle in each table:")
    for table in ALL_TABLES:
        try:
            # Defect tables use upc_code; geometry tables use upc
            upc_col = "upc" if "geometry" in table else "upc_code"
            cur.execute(f'''
                SELECT DISTINCT "{upc_col}", phase, cycle
                FROM "{DB_SCHEMA}"."{table}"
                WHERE "{upc_col}" IS NOT NULL
                ORDER BY "{upc_col}"
            ''')
            combos = cur.fetchall()
            if combos:
                combo_str = "; ".join(f"{r[0]}/p{r[1]}/c{r[2]}" for r in combos)
                print(f"   {table:<40s}  {combo_str}")
        except Exception:
            pass

    # Sample 5 rows from first non-empty table
    for table in ALL_TABLES:
        try:
            cur.execute(f'SELECT * FROM "{DB_SCHEMA}"."{table}" LIMIT 5')
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            if rows:
                print(f"\n   SAMPLE \u2014 {table} ({len(rows)} rows)")
                sample = pd.DataFrame(rows, columns=cols)
                display(sample)
                break  # show sample from first non-empty table only
        except Exception:
            pass

    cur.close()
    conn.close()

    print(f"\n--- {db_name} VALIDATION SUMMARY ---")
    val_df = pd.DataFrame(results)
    display(val_df)
    total = val_df["pg_rows"].sum()
    print(f"\u2705 {db_name}: Total rows = {total:,}")

# COMMAND ----------

# DBTITLE 1,Check Target UPC in PostgreSQL
# ============================================================
# CHECK TARGET UPC (from Configuration) in the target databases
# — Row counts per table
# — parent_id presence in child_10m & child_100m
# ============================================================

# TARGET_UPC_UNDERSCORE / TARGET_UPC_SLASH / TARGET_PHASE / TARGET_CYCLE come from the Configuration cell
print(f"Target: {TARGET_UPC_UNDERSCORE} ({TARGET_UPC_SLASH})  phase={TARGET_PHASE}  cycle={TARGET_CYCLE}")

DEFECT_TABLES = [
    "master_pothole", "master_cracking",
    "master_roughness_parent", "master_roughness_child_10m", "master_roughness_child_100m",
    "master_rutting_parent", "master_rutting_child_10m", "master_rutting_child_100m",
    "master_ravelling_parent", "master_ravelling_child_10m", "master_ravelling_child_100m",
]
GEOM_TABLES = ["master_geometry_segment", "master_geometry_parent", "master_geometry_child"]

for db_name in TARGET_DBS:
    print(f"\n{'='*70}")
    print(f"CHECK UPC in: {db_name}")
    print(f"{'='*70}\n")

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=db_name,
        user=DB_USER, password=DB_PASSWORD
    )
    cur = conn.cursor()

    # --- Defect tables (upc_code column) ---
    print("  DEFECT TABLES (upc_code):")
    for t in DEFECT_TABLES:
        try:
            cur.execute(f'''
                SELECT COUNT(*)
                FROM "{DB_SCHEMA}"."{t}"
                WHERE upc_code = %s AND phase = %s AND cycle = %s
            ''', (TARGET_UPC_UNDERSCORE, TARGET_PHASE, TARGET_CYCLE))
            cnt = cur.fetchone()[0]
            flag = "✅" if cnt > 0 else "⚠️  NONE"
            print(f"    {flag}  {t:<40s}  {cnt:>8,} rows")
        except Exception as e:
            print(f"    ❌  {t:<40s}  ERROR: {e}")
            conn.rollback()

    # --- Check parent_id in child tables ---
    print("\n  PARENT_ID CHECK (child_10m & child_100m):")
    for t in [t for t in DEFECT_TABLES if "child" in t]:
        try:
            cur.execute(f'''
                SELECT
                    COUNT(*) AS total,
                    COUNT(parent_id) AS has_parent,
                    COUNT(*) - COUNT(parent_id) AS missing_parent
                FROM "{DB_SCHEMA}"."{t}"
                WHERE upc_code = %s AND phase = %s AND cycle = %s
            ''', (TARGET_UPC_UNDERSCORE, TARGET_PHASE, TARGET_CYCLE))
            row = cur.fetchone()
            total, has_p, miss_p = row
            pct = (has_p / total * 100) if total > 0 else 0
            flag = "✅" if miss_p == 0 and total > 0 else "⚠️"
            print(f"    {flag}  {t:<40s}  total={total:,}  has_parent={has_p:,}  missing={miss_p:,}  ({pct:.1f}%)")
        except Exception as e:
            print(f"    ❌  {t:<40s}  ERROR: {e}")
            conn.rollback()

    # --- Geometry tables (upc column) ---
    print("\n  GEOMETRY TABLES (upc):")
    for t in GEOM_TABLES:
        try:
            if t == "master_geometry_child":
                # child has no upc column — check via parent
                cur.execute(f'''
                    SELECT COUNT(*)
                    FROM "{DB_SCHEMA}"."master_geometry_child" c
                    JOIN "{DB_SCHEMA}"."master_geometry_parent" p ON c.parent_id::text = p.id::text
                    WHERE p.upc = %s
                ''', (TARGET_UPC_SLASH,))
            else:
                cur.execute(f'''
                    SELECT COUNT(*)
                    FROM "{DB_SCHEMA}"."{t}"
                    WHERE upc = %s
                ''', (TARGET_UPC_SLASH,))
            cnt = cur.fetchone()[0]
            flag = "✅" if cnt > 0 else "⚠️  NONE"
            print(f"    {flag}  {t:<40s}  {cnt:>8,} rows")
        except Exception as e:
            print(f"    ❌  {t:<40s}  ERROR: {e}")
            conn.rollback()

    # --- Check parent_id in geometry child ---
    print("\n  GEOMETRY CHILD parent_id CHECK:")
    try:
        cur.execute(f'''
            SELECT
                COUNT(*) AS total,
                COUNT(parent_id) AS has_parent,
                COUNT(*) - COUNT(parent_id) AS missing_parent
            FROM "{DB_SCHEMA}"."master_geometry_child" c
            JOIN "{DB_SCHEMA}"."master_geometry_parent" p ON c.parent_id::text = p.id::text
            WHERE p.upc = %s
        ''', (TARGET_UPC_SLASH,))
        row = cur.fetchone()
        total, has_p, miss_p = row
        pct = (has_p / total * 100) if total > 0 else 0
        flag = "✅" if miss_p == 0 and total > 0 else "⚠️"
        print(f"    {flag}  master_geometry_child  total={total:,}  has_parent={has_p:,}  missing={miss_p:,}  ({pct:.1f}%)")
    except Exception as e:
        print(f"    ❌  master_geometry_child  ERROR: {e}")
        conn.rollback()

    cur.close()
    conn.close()

print("\n✅ Check complete")

# COMMAND ----------

# DBTITLE 1,DELETE & RE-PUSH — Target UPC Only (Defect Tables)
# ============================================================
# DELETE & RE-PUSH — TARGET UPC ONLY (DEFECT TABLES)
# Target survey comes from the Configuration cell
# ============================================================
if DRY_RUN:
    raise RuntimeError("DRY_RUN = True: this cell writes to PostgreSQL (DELETE + INSERT + UPDATE), so it was not run. "
                       "Check the Dry Run Preview cell, then set DRY_RUN = False in Configuration.")

# --- Utility functions (self-contained) ---
def get_pg_columns(cur, table_name):
    """Return ordered dict: column_name -> udt_name."""
    cur.execute("""
        SELECT column_name, udt_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (DB_SCHEMA, table_name))
    return {row[0]: row[1] for row in cur.fetchall()}

def clean_value(val, col_name, pg_udt):
    """Convert a single Python/pandas value for PostgreSQL insertion."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, pd._libs.tslibs.nattype.NaTType):
        return None
    if col_name in JSONB_COLUMNS:
        if isinstance(val, dict):
            return json.dumps(val)
        if isinstance(val, str):
            return val
        return json.dumps(val)
    if col_name in UUID_COLUMNS:
        s = str(val).strip()
        return s if s and s.lower() != "none" else None
    if pg_udt in ("_text", "_varchar"):
        if isinstance(val, (list, np.ndarray)):
            return list(val)
        if isinstance(val, str):
            try:
                parsed = json.loads(val)
                if isinstance(parsed, list):
                    return parsed
            except Exception:
                pass
            return [val]
        return None
    if pg_udt == "bool":
        if isinstance(val, (bool, np.bool_)):
            return bool(val)
        return val
    return val

TARGET_UPC_CODE = TARGET_UPC_UNDERSCORE  # defect tables (TARGET_PHASE / TARGET_CYCLE from Configuration)

for db_name in TARGET_DBS:
    print(f"\n{'='*70}")
    print(f"\U0001f5d1  DELETE + RE-PUSH defect data in: {db_name}")
    print(f"{'='*70}\n")

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=db_name,
        user=DB_USER, password=DB_PASSWORD
    )
    cur = conn.cursor()
    cur.execute("SET session_replication_role = 'replica';")
    conn.commit()
    print("\u26a0\ufe0f  FK constraints DISABLED\n")

    # ---- STEP 1: DELETE existing rows for this UPC ----
    print("  STEP 1: DELETE existing rows")
    for table in TABLE_LIST:
        try:
            cur.execute(f'''
                DELETE FROM "{DB_SCHEMA}"."{table}"
                WHERE upc_code = %s AND phase = %s AND cycle = %s
            ''', (TARGET_UPC_CODE, TARGET_PHASE, TARGET_CYCLE))
            conn.commit()
            print(f"    \u2705 {table:<40s}  {cur.rowcount:>8,} rows deleted")
        except Exception as e:
            conn.rollback()
            print(f"    \u274c {table}: {e}")

    # ---- STEP 2: RE-PUSH from Databricks ----
    print("\n  STEP 2: RE-PUSH from Databricks")
    filter_cond_single = (
        (F.col("upc_code") == TARGET_UPC_CODE) &
        (F.col("phase") == TARGET_PHASE) &
        (F.col("cycle") == TARGET_CYCLE)
    )

    push_summary = []
    for dbx_table in TABLE_LIST:
        fq_table = f"{DBX_CATALOG}.{DBX_SCHEMA}.{dbx_table}"
        pg_table = dbx_table

        print(f"    \U0001f680 {dbx_table}")

        pg_cols_info = get_pg_columns(cur, pg_table)
        if not pg_cols_info:
            print(f"       \u26a0\ufe0f  PG table not found")
            push_summary.append((dbx_table, 0, "TABLE NOT FOUND"))
            continue

        try:
            spark_df = spark.table(fq_table)
            pdf = spark_df.filter(filter_cond_single).toPandas()
            print(f"       Filtered rows: {len(pdf):,}")
            if len(pdf) == 0:
                push_summary.append((dbx_table, 0, "NO MATCH"))
                continue
        except Exception as e:
            print(f"       \u274c Read failed: {e}")
            push_summary.append((dbx_table, 0, f"ERROR: {e}"))
            continue

        common_cols = [c for c in pg_cols_info.keys() if c in pdf.columns]
        pdf = pdf[common_cols]

        col_names = ", ".join(f'"{ c}"' for c in common_cols)
        placeholders = []
        for c in common_cols:
            if c in JSONB_COLUMNS:
                placeholders.append("%s::jsonb")
            elif c in UUID_COLUMNS:
                placeholders.append("%s::uuid")
            else:
                placeholders.append("%s")

        template = f"({', '.join(placeholders)})"
        insert_sql = f'INSERT INTO "{DB_SCHEMA}"."{pg_table}" ({col_names}) VALUES %s'

        rows = []
        for _, row in pdf.iterrows():
            cleaned = tuple(
                clean_value(row[c], c, pg_cols_info.get(c, ""))
                for c in common_cols
            )
            rows.append(cleaned)

        try:
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i : i + BATCH_SIZE]
                extras.execute_values(
                    cur, insert_sql, batch,
                    template=template, page_size=BATCH_SIZE
                )
                conn.commit()
            print(f"       \u2705 Pushed {len(rows):,} rows")
            push_summary.append((dbx_table, len(rows), "SUCCESS"))
        except Exception as e:
            conn.rollback()
            print(f"       \u274c Insert failed: {e}")
            push_summary.append((dbx_table, 0, f"ERROR: {e}"))

    # ---- STEP 3: UUID MAPPING ----
    print("\n  STEP 3: UUID MAPPING")
    try:
        cur.execute("SET session_replication_role = 'origin';")
        conn.commit()
    except:
        pass
    conn.autocommit = True

    for table in TABLE_LIST:
        try:
            cur.execute(f'''
                UPDATE "{DB_SCHEMA}"."{table}" t
                SET project_id    = p.id,
                    region_id     = p.region_id,
                    piu_id        = p.piu_id,
                    state_id      = p.state_id,
                    contractor_id = p.contractor_id,
                    ae_ie_id      = p.ae_ie_id
                FROM "{DB_SCHEMA}"."master_upc_project" p
                WHERE t.upc = p.upc
                  AND t.upc_code = %s AND t.phase = %s AND t.cycle = %s
            ''', (TARGET_UPC_CODE, TARGET_PHASE, TARGET_CYCLE))
            upc_mapped = cur.rowcount
        except Exception as e:
            upc_mapped = 0
            print(f"    \u274c {table} UPC mapping: {e}")

        try:
            cur.execute(f'''
                UPDATE "{DB_SCHEMA}"."{table}" t
                SET zone_id = z.id
                FROM "{DB_SCHEMA}"."master_zone" z
                WHERE t.zone = z.zone_code
                  AND t.upc_code = %s AND t.phase = %s AND t.cycle = %s
            ''', (TARGET_UPC_CODE, TARGET_PHASE, TARGET_CYCLE))
            zone_mapped = cur.rowcount
        except Exception as e:
            zone_mapped = 0
            print(f"    \u274c {table} zone mapping: {e}")

        print(f"    \u2705 {table:<35s}  UPC\u2192{upc_mapped:>6,}  zone\u2192{zone_mapped:>6,}")

    # ---- STEP 4: ASSET MAPPING + RECURRING DEFECT ----
    print("\n  STEP 4: ASSET MAPPING + RECURRING DEFECT")
    for table in TABLE_LIST:
        try:
            cur.execute(f'''
                UPDATE "{DB_SCHEMA}"."{table}" t
                SET asset_mapping_id = dtg.asset_mapping_id
                FROM "{DB_SCHEMA}"."master_defect_type_group" dtg
                WHERE t.defect_type = dtg.defect_type
                  AND t.upc_code = %s AND t.phase = %s AND t.cycle = %s
            ''', (TARGET_UPC_CODE, TARGET_PHASE, TARGET_CYCLE))
            asset_mapped = cur.rowcount
        except Exception as e:
            asset_mapped = 0
            print(f"    \u274c {table} asset mapping: {e}")

        try:
            cur.execute(f'''
                UPDATE "{DB_SCHEMA}"."{table}"
                SET recurring_defect = false
                WHERE recurring_defect IS NULL
                  AND upc_code = %s AND phase = %s AND cycle = %s
            ''', (TARGET_UPC_CODE, TARGET_PHASE, TARGET_CYCLE))
        except:
            pass

        print(f"    \u2705 {table:<35s}  asset\u2192{asset_mapped:>6,}")

    conn.close()

    print(f"\n--- {db_name} PUSH SUMMARY ---")
    summary_df = pd.DataFrame(push_summary, columns=["table", "rows_pushed", "status"])
    display(summary_df)
    total = summary_df["rows_pushed"].sum()
    print(f"\u2705 {db_name}: Total rows re-pushed: {total:,}")

# COMMAND ----------

# DBTITLE 1,DELETE & RE-PUSH — Target UPC Only (Geometry Tables)
# ============================================================
# DELETE & RE-PUSH — TARGET UPC ONLY (GEOMETRY TABLES)
# Target survey comes from the Configuration cell
# ============================================================
if DRY_RUN:
    raise RuntimeError("DRY_RUN = True: this cell writes to PostgreSQL (DELETE + INSERT + UPDATE), so it was not run. "
                       "Check the Dry Run Preview cell, then set DRY_RUN = False in Configuration.")

import uuid as _uuid
from datetime import datetime

# --- Utility functions (self-contained) ---
def get_pg_columns(cur, table_name):
    """Return ordered dict: column_name -> udt_name."""
    cur.execute("""
        SELECT column_name, udt_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (DB_SCHEMA, table_name))
    return {row[0]: row[1] for row in cur.fetchall()}

def push_geo_df(pdf, pg_table, cur, conn):
    """Push a DataFrame to a PG geometry table. Returns rows pushed."""
    pg_cols = get_pg_columns(cur, pg_table)
    if not pg_cols:
        print(f"     \u26a0\ufe0f  {pg_table} not found in PG")
        return 0
    common = [c for c in pg_cols if c in pdf.columns]
    push_pdf = pdf[common]
    col_str = ", ".join(f'"{ c}"' for c in common)
    phs = []
    for c in common:
        udt = pg_cols.get(c, "")
        if udt == "jsonb":
            phs.append("%s::jsonb")
        elif udt == "uuid":
            phs.append("%s::uuid")
        else:
            phs.append("%s")
    tpl = f"({', '.join(phs)})"
    sql = f'INSERT INTO "{DB_SCHEMA}"."{pg_table}" ({col_str}) VALUES %s'
    rows = []
    for _, row in push_pdf.iterrows():
        vals = []
        for c in common:
            v = row[c]
            if v is None or (isinstance(v, float) and np.isnan(v)):
                vals.append(None)
            elif isinstance(v, pd._libs.tslibs.nattype.NaTType):
                vals.append(None)
            elif pg_cols.get(c) in ("_text", "_varchar"):
                vals.append(list(v) if isinstance(v, (list, np.ndarray)) else [str(v)])
            elif pg_cols.get(c) == "jsonb":
                vals.append(json.dumps(v) if not isinstance(v, str) else v)
            else:
                vals.append(v)
        rows.append(tuple(vals))
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        extras.execute_values(cur, sql, batch, template=tpl, page_size=BATCH_SIZE)
        conn.commit()
    return len(rows)

# TARGET_UPC_SLASH comes from the Configuration cell
TARGET_PHASE_GEO = TARGET_PHASE
TARGET_CYCLE_GEO = TARGET_CYCLE

GPS_TABLE = "nhai_dhms.bronze_layer.nsv_gps_geometry"
GEOM_TABLES_LIST = ["master_geometry_child", "master_geometry_parent", "master_geometry_segment"]

for db_name in TARGET_DBS:
    print(f"\n{'='*70}")
    print(f"\U0001f5d1  DELETE + RE-PUSH geometry in: {db_name}")
    print(f"{'='*70}\n")

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=db_name,
        user=DB_USER, password=DB_PASSWORD
    )
    cur = conn.cursor()
    cur.execute("SET session_replication_role = 'replica';")
    conn.commit()
    print("\u26a0\ufe0f  FK constraints DISABLED\n")

    # ---- STEP 1: DELETE geometry rows for this UPC ----
    print("  STEP 1: DELETE existing geometry rows")

    # Delete child via parent join
    try:
        cur.execute(f'''
            DELETE FROM "{DB_SCHEMA}"."master_geometry_child" c
            USING "{DB_SCHEMA}"."master_geometry_parent" p
            WHERE c.parent_id::text = p.id::text
              AND p.upc = %s
        ''', (TARGET_UPC_SLASH,))
        conn.commit()
        print(f"    \u2705 master_geometry_child   {cur.rowcount:>8,} rows deleted")
    except Exception as e:
        conn.rollback()
        print(f"    \u274c master_geometry_child: {e}")

    # Delete parent & segment directly
    for gt in ["master_geometry_parent", "master_geometry_segment"]:
        try:
            cur.execute(f'''
                DELETE FROM "{DB_SCHEMA}"."{gt}"
                WHERE upc = %s
            ''', (TARGET_UPC_SLASH,))
            conn.commit()
            print(f"    \u2705 {gt:<30s}  {cur.rowcount:>8,} rows deleted")
        except Exception as e:
            conn.rollback()
            print(f"    \u274c {gt}: {e}")

    # ---- STEP 2: READ & FILTER GPS DATA ----
    print("\n  STEP 2: READ GPS data from Databricks")
    gps_spark = spark.table(GPS_TABLE)
    gps_filt = (
        (F.col("upc") == TARGET_UPC_SLASH) &
        (F.col("phase") == TARGET_PHASE_GEO) &
        (F.col("cycle") == TARGET_CYCLE_GEO)
    )
    gps_pdf = gps_spark.filter(gps_filt).toPandas()
    print(f"    Filtered GPS rows: {len(gps_pdf):,}")

    if len(gps_pdf) == 0:
        print(f"    \u26a0\ufe0f  No GPS data for this UPC — skipping")
        cur.execute("SET session_replication_role = 'origin';")
        conn.commit()
        cur.close()
        conn.close()
        continue

    # Rename columns
    rename_map = {
        "NH_Number": "nh_number", "Section_Code": "section_code",
        "Direction": "direction", "Start_Chainage": "start_chainage",
        "End_Chainage": "end_chainage", "Start_Latitude": "start_latitude",
        "Start_Longitude": "start_longitude", "End_Latitude": "end_latitude",
        "End_Longitude": "end_longitude", "GPS_Coverage": "gps_coverage",
        "Lane": "lane",
    }
    gps_r = gps_pdf.rename(columns=rename_map)
    now_ts = datetime.utcnow()

    # ---- BUILD segment ----
    seg = gps_r.copy()
    seg["id"]            = [str(_uuid.uuid4()) for _ in range(len(seg))]
    seg["survey_cycle"]  = seg["cycle"]
    seg["source_system"] = "nsv"
    seg["created_at"]    = now_ts
    seg["updated_at"]    = now_ts

    # ---- BUILD parent ----
    groups = gps_r.groupby(["upc", "nh_number", "section_code"])
    parents = []
    parent_ids = {}
    for (upc, nh, sec), grp in groups:
        pid = str(_uuid.uuid4())
        parent_ids[(upc, nh, sec)] = pid
        min_row = grp.loc[grp["start_chainage"].idxmin()]
        max_row = grp.loc[grp["end_chainage"].idxmax()]
        parents.append({
            "id": pid, "upc": upc, "nh_number": nh, "section_code": sec,
            "source_system": "nsv",
            "lanes": list(grp["lane"].dropna().unique()),
            "start_chainage": float(grp["start_chainage"].min()),
            "end_chainage":   float(grp["end_chainage"].max()),
            "start_lat":  float(min_row["start_latitude"]),
            "start_long": float(min_row["start_longitude"]),
            "end_lat":    float(max_row["end_latitude"]),
            "end_long":   float(max_row["end_longitude"]),
            "total_records": len(grp),
            "lane_length_surveyed_km": round(
                (grp["end_chainage"].sum() - grp["start_chainage"].sum()) / 1000.0, 3
            ),
            "created_at": now_ts, "updated_at": now_ts,
        })
    par_df = pd.DataFrame(parents)

    # ---- BUILD child ----
    child = gps_r.copy()
    child["id"]        = [str(_uuid.uuid4()) for _ in range(len(child))]
    child["child_id"]  = [str(_uuid.uuid4()) for _ in range(len(child))]
    child["parent_id"] = child.apply(
        lambda r: parent_ids.get((r["upc"], r["nh_number"], r["section_code"])), axis=1
    )
    child["created_at"] = now_ts

    print(f"    Segment: {len(seg):,}  Parent: {len(par_df):,}  Child: {len(child):,}")

    # ---- STEP 3: PUSH geometry tables ----
    print("\n  STEP 3: PUSH geometry tables")
    for tbl_name, tbl_pdf in [
        ("master_geometry_segment", seg),
        ("master_geometry_parent",  par_df),
        ("master_geometry_child",   child),
    ]:
        try:
            n = push_geo_df(tbl_pdf, tbl_name, cur, conn)
            print(f"    \u2705 {tbl_name:<30s}  {n:>8,} rows pushed")
        except Exception as e:
            conn.rollback()
            print(f"    \u274c {tbl_name}: {e}")

    # ---- STEP 4: Map project_id ----
    print("\n  STEP 4: MAP project_id")
    conn.autocommit = True
    try:
        cur.execute(f'''
            UPDATE "{DB_SCHEMA}"."master_geometry_segment" t
            SET project_id = p.id::uuid
            FROM "{DB_SCHEMA}"."master_upc_project" p
            WHERE t.upc = p.upc AND t.upc = %s
        ''', (TARGET_UPC_SLASH,))
        print(f"    \u2705 segment: project_id mapped for {cur.rowcount:,} rows")
    except Exception as e:
        print(f"    \u274c segment: {e}")

    try:
        cur.execute(f'''
            UPDATE "{DB_SCHEMA}"."master_geometry_parent" t
            SET project_id = p.id::varchar
            FROM "{DB_SCHEMA}"."master_upc_project" p
            WHERE t.upc = p.upc AND t.upc = %s
        ''', (TARGET_UPC_SLASH,))
        print(f"    \u2705 parent: project_id mapped for {cur.rowcount:,} rows")
    except Exception as e:
        print(f"    \u274c parent: {e}")

    cur.close()
    conn.close()
    print(f"\n\u2705 {db_name}: Geometry re-push complete for {TARGET_UPC_SLASH}")

print(f"\n\u2705 ALL DONE — {TARGET_UPC_SLASH} geometry re-pushed to {', '.join(TARGET_DBS)}")

# COMMAND ----------

# DBTITLE 1,Verify Re-Push — Target UPC
# ============================================================
# VERIFY RE-PUSH — target UPC from the Configuration cell
# Same check as before, re-run to confirm new data is correct
# ============================================================

# TARGET_UPC_UNDERSCORE / TARGET_UPC_SLASH / TARGET_PHASE / TARGET_CYCLE come from the Configuration cell

DEFECT_TABLES = [
    "master_pothole", "master_cracking",
    "master_roughness_parent", "master_roughness_child_10m", "master_roughness_child_100m",
    "master_rutting_parent", "master_rutting_child_10m", "master_rutting_child_100m",
    "master_ravelling_parent", "master_ravelling_child_10m", "master_ravelling_child_100m",
]
GEOM_TABLES = ["master_geometry_segment", "master_geometry_parent", "master_geometry_child"]

for db_name in TARGET_DBS:
    print(f"\n{'='*70}")
    print(f"VERIFY RE-PUSH in: {db_name}")
    print(f"{'='*70}\n")

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=db_name,
        user=DB_USER, password=DB_PASSWORD
    )
    cur = conn.cursor()

    print("  DEFECT TABLES:")
    for t in DEFECT_TABLES:
        try:
            cur.execute(f'''
                SELECT COUNT(*)
                FROM "{DB_SCHEMA}"."{t}"
                WHERE upc_code = %s AND phase = %s AND cycle = %s
            ''', (TARGET_UPC_UNDERSCORE, TARGET_PHASE, TARGET_CYCLE))
            cnt = cur.fetchone()[0]
            flag = "\u2705" if cnt > 0 else "\u26a0\ufe0f  NONE"
            print(f"    {flag}  {t:<40s}  {cnt:>8,} rows")
        except Exception as e:
            print(f"    \u274c  {t}: {e}")
            conn.rollback()

    print("\n  PARENT_ID CHECK (child tables):")
    for t in [t for t in DEFECT_TABLES if "child" in t]:
        try:
            cur.execute(f'''
                SELECT COUNT(*) AS total, COUNT(parent_id) AS has_parent
                FROM "{DB_SCHEMA}"."{t}"
                WHERE upc_code = %s AND phase = %s AND cycle = %s
            ''', (TARGET_UPC_UNDERSCORE, TARGET_PHASE, TARGET_CYCLE))
            total, has_p = cur.fetchone()
            miss = total - has_p
            pct = (has_p / total * 100) if total > 0 else 0
            flag = "\u2705" if miss == 0 and total > 0 else "\u26a0\ufe0f"
            print(f"    {flag}  {t:<40s}  total={total:,}  parent={has_p:,}  missing={miss:,}  ({pct:.1f}%)")
        except Exception as e:
            print(f"    \u274c  {t}: {e}")
            conn.rollback()

    print("\n  GEOMETRY TABLES:")
    for t in GEOM_TABLES:
        try:
            if t == "master_geometry_child":
                cur.execute(f'''
                    SELECT COUNT(*)
                    FROM "{DB_SCHEMA}"."master_geometry_child" c
                    JOIN "{DB_SCHEMA}"."master_geometry_parent" p ON c.parent_id::text = p.id::text
                    WHERE p.upc = %s
                ''', (TARGET_UPC_SLASH,))
            else:
                cur.execute(f'''
                    SELECT COUNT(*) FROM "{DB_SCHEMA}"."{t}" WHERE upc = %s
                ''', (TARGET_UPC_SLASH,))
            cnt = cur.fetchone()[0]
            flag = "\u2705" if cnt > 0 else "\u26a0\ufe0f  NONE"
            print(f"    {flag}  {t:<40s}  {cnt:>8,} rows")
        except Exception as e:
            print(f"    \u274c  {t}: {e}")
            conn.rollback()

    print("\n  GEOMETRY CHILD parent_id CHECK:")
    try:
        cur.execute(f'''
            SELECT COUNT(*) AS total, COUNT(parent_id) AS has_parent
            FROM "{DB_SCHEMA}"."master_geometry_child" c
            JOIN "{DB_SCHEMA}"."master_geometry_parent" p ON c.parent_id::text = p.id::text
            WHERE p.upc = %s
        ''', (TARGET_UPC_SLASH,))
        total, has_p = cur.fetchone()
        miss = total - has_p
        pct = (has_p / total * 100) if total > 0 else 0
        flag = "\u2705" if miss == 0 and total > 0 else "\u26a0\ufe0f"
        print(f"    {flag}  master_geometry_child  total={total:,}  parent={has_p:,}  missing={miss:,}  ({pct:.1f}%)")
    except Exception as e:
        print(f"    \u274c  geometry child: {e}")
        conn.rollback()

    cur.close()
    conn.close()

print("\n\u2705 Verification complete")

# COMMAND ----------

# DBTITLE 1,Sample 10 rows per table — Target UPC
# ============================================================
# PULL 10 ROWS per table for the target UPC from the Configuration cell
# ============================================================

# TARGET_UPC_UNDERSCORE / TARGET_UPC_SLASH / TARGET_PHASE / TARGET_CYCLE come from the Configuration cell

DEFECT_TABLES = [
    "master_pothole", "master_cracking",
    "master_roughness_parent", "master_roughness_child_10m", "master_roughness_child_100m",
    "master_rutting_parent", "master_rutting_child_10m", "master_rutting_child_100m",
    "master_ravelling_parent", "master_ravelling_child_10m", "master_ravelling_child_100m",
]
GEOM_TABLES = ["master_geometry_segment", "master_geometry_parent", "master_geometry_child"]

db_name = TARGET_DBS[0]  # dhms_staging
print(f"Pulling 10-row samples from: {db_name}\n")

conn = psycopg2.connect(
    host=DB_HOST, port=DB_PORT, database=db_name,
    user=DB_USER, password=DB_PASSWORD
)
cur = conn.cursor()

# --- Defect tables ---
for t in DEFECT_TABLES:
    try:
        cur.execute(f'''
            SELECT * FROM "{DB_SCHEMA}"."{t}"
            WHERE upc_code = %s AND phase = %s AND cycle = %s
            LIMIT 10
        ''', (TARGET_UPC_UNDERSCORE, TARGET_PHASE, TARGET_CYCLE))
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        print(f"\n{'='*60}")
        print(f"{t}  ({len(rows)} rows)")
        print(f"{'='*60}")
        if rows:
            display(pd.DataFrame(rows, columns=cols))
        else:
            print("  (no rows)")
    except Exception as e:
        print(f"\n{t}: ERROR — {e}")
        conn.rollback()

# --- Geometry tables ---
for t in GEOM_TABLES:
    try:
        if t == "master_geometry_child":
            cur.execute(f'''
                SELECT c.* FROM "{DB_SCHEMA}"."master_geometry_child" c
                JOIN "{DB_SCHEMA}"."master_geometry_parent" p ON c.parent_id::text = p.id::text
                WHERE p.upc = %s
                LIMIT 10
            ''', (TARGET_UPC_SLASH,))
        else:
            cur.execute(f'''
                SELECT * FROM "{DB_SCHEMA}"."{t}"
                WHERE upc = %s
                LIMIT 10
            ''', (TARGET_UPC_SLASH,))
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        print(f"\n{'='*60}")
        print(f"{t}  ({len(rows)} rows)")
        print(f"{'='*60}")
        if rows:
            display(pd.DataFrame(rows, columns=cols))
        else:
            print("  (no rows)")
    except Exception as e:
        print(f"\n{t}: ERROR — {e}")
        conn.rollback()

cur.close()
conn.close()
print("\n\u2705 Done")