# Databricks notebook source
# DBTITLE 1,DLM Batch Runner
# MAGIC %md
# MAGIC # DLM Batch Runner
# MAGIC
# MAGIC Runs the **(Clone) (Clone) DLM Extraction Notebook_(5_Nov_2026)** once per UPC in `RUN_LIST`, one after another: the next UPC starts only after the previous one has finished (all cells, including Dual Schema Write, Unified Validation, master_chainage and the validation Excel export). Each run's validation PASS / WARN / FAIL / SKIP counts are collected and shown at the end.
# MAGIC
# MAGIC **How to use:** edit `RUN_LIST` in the next cell if needed, then **Run all**. Nothing runs on a schedule; this is started by hand.

# COMMAND ----------

# DBTITLE 1,Run List and Settings
# ═══ RUN LIST — one row per UPC, processed in this order (edit as needed) ═══
import os, json, time
import pandas as pd
from datetime import datetime

_RUN = """
B 1 2 N_09011_01002_RJ
B 1 1 N_01001_03004_RJ
B 1 2 N_01001_03004_RJ
B 1 1 N_01001_03005_RJ
B 1 2 N_01001_03005_RJ
B 1 1 N_01001_03006_RJ
B 1 2 N_01001_03006_RJ
B 1 1 N_01001_03007_RJ
B 1 2 N_01001_03007_RJ
B 1 2 N_01001_03015_RJ
B 1 2 N_04003_10002_RJ
B 1 2 N_04003_10003_RJ
B 1 2 N_04003_10004_RJ
B 1 1 N_04003_10005_RJ
B 1 2 N_04003_10005_RJ
B 1 1 N_04003_10006_RJ
B 1 2 N_04003_10006_RJ
B 1 2 N_04003_10007_RJ
B 1 2 N_04003_10008_RJ
B 1 1 N_04003_10009_RJ
"""
# Zone Phase Cycle UPC -> one dict per survey, in this order
RUN_LIST = [dict(zip(["ZONE", "PHASE", "CYCLE", "UPC"], l.split())) for l in _RUN.strip().splitlines() if l.strip()]

# Extraction notebook run for each UPC (5_Nov_2026 clone in this Batch running folder)
CHILD_NOTEBOOK = "/Users/shivam.raj9226@softsensor.ai/SIlver layer defect data extraction/Batch running/(Clone) (Clone) DLM Extraction Notebook_(5_Nov_2026)"
TIMEOUT_SECONDS = 4 * 60 * 60     # max time per UPC (4 h)
STOP_ON_FAILURE = False           # True = stop the batch at the first failed UPC; False = log it and continue
BRONZE_ROOT = "/Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output"
VALIDATION_TABLE = "nhai_dhms.dlm_kpi.dlm_validation_results"

print(f"{len(RUN_LIST)} UPCs queued -> {CHILD_NOTEBOOK}")
display(pd.DataFrame(RUN_LIST))

# COMMAND ----------

# DBTITLE 1,Pre-flight Check: Bronze Folders Exist
# ═══ PRE-FLIGHT — NSV and NSV_Backup bronze folders must exist for every UPC (both folder styles) ═══
def _upc_root(source, r):
    for p, c in [(r["PHASE"], r["CYCLE"]), (f"Phase {r['PHASE']}", f"Cycle {r['CYCLE']}")]:
        path = f"{BRONZE_ROOT}/Source={source}/Zone={r['ZONE']}/Phase={p}/Cycle={c}/UPC={r['UPC']}"
        if os.path.exists(path): return path
    return None

_pre = pd.DataFrame([{**r, "NSV": _upc_root("NSV", r) is not None, "NSV_Backup": _upc_root("NSV_Backup", r) is not None}
                     for r in RUN_LIST])
_pre["ready"] = _pre["NSV"] & _pre["NSV_Backup"]
display(_pre)
if not _pre["ready"].all():
    print("Not ready (will be skipped):",
          [f"{x.UPC} Z{x.ZONE} P{x.PHASE} C{x.CYCLE}" for x in _pre.loc[~_pre["ready"]].itertuples()])
# ready is checked per survey (zone / phase / cycle / UPC), not per UPC
READY_SURVEYS = {(x.ZONE, x.PHASE, x.CYCLE, x.UPC) for x in _pre.loc[_pre["ready"]].itertuples()}

# COMMAND ----------

# DBTITLE 1,Run Each UPC Sequentially (Extraction + Validation)
# ═══ SEQUENTIAL RUN — next UPC starts only after the previous run has returned ═══
BATCH_RESULTS = []
batch_start = time.time()
for i, r in enumerate(RUN_LIST, 1):
    tag = f"[{i}/{len(RUN_LIST)}] {r['UPC']} (Zone {r['ZONE']} / Phase {r['PHASE']} / Cycle {r['CYCLE']})"
    rec = {**r, "status": None, "minutes": None, "PASS": 0, "WARN": 0, "FAIL": 0, "SKIP": 0, "message": ""}
    if (r["ZONE"], r["PHASE"], r["CYCLE"], r["UPC"]) not in READY_SURVEYS:
        rec.update(status="SKIPPED", message="bronze folder missing")
        BATCH_RESULTS.append(rec); print(f"{tag}: SKIPPED (bronze folder missing)"); continue

    print(f"{tag}: started {datetime.now():%H:%M:%S}")
    t0 = time.time()
    try:
        out = dbutils.notebook.run(CHILD_NOTEBOOK, TIMEOUT_SECONDS, {k: str(v) for k, v in r.items()})
        res = json.loads(out) if out else {}
        val = res.get("validation", {})
        rec.update(status="SUCCESS", **{k: int(val.get(k, 0)) for k in ["PASS", "WARN", "FAIL", "SKIP"]})
        if res.get("fail_checks"):
            rec["message"] = "; ".join(f"{f['section']} | {f['check']} | {f['scope']}" for f in res["fail_checks"])[:500]
    except Exception as e:
        rec.update(status="FAILED", message=str(e).splitlines()[0][:500])
    rec["minutes"] = round((time.time() - t0) / 60, 1)
    BATCH_RESULTS.append(rec)
    print(f"{tag}: {rec['status']} in {rec['minutes']} min | PASS {rec['PASS']} WARN {rec['WARN']} FAIL {rec['FAIL']} SKIP {rec['SKIP']}"
          + (f" | {rec['message']}" if rec['message'] else ""))
    if rec["status"] == "FAILED" and STOP_ON_FAILURE:
        print("STOP_ON_FAILURE=True -> batch stopped"); break

print(f"\nBatch finished in {round((time.time() - batch_start) / 60, 1)} min")

# COMMAND ----------

# DBTITLE 1,Batch Summary and Validation Cross-check
# ═══ SUMMARY — run status per UPC + latest validation counts read back from the validation table ═══
summary = pd.DataFrame(BATCH_RESULTS)
vr = spark.table(VALIDATION_TABLE).toPandas()
vr = vr[vr["upc_code"].isin(summary["UPC"])]
if not vr.empty:
    vr = vr[vr["validation_ts"] == vr.groupby(["upc_code", "phase", "cycle"])["validation_ts"].transform("max")]
    tbl = (vr.pivot_table(index=["upc_code", "phase", "cycle"], columns="status", values="check", aggfunc="count", fill_value=0)
             .add_prefix("table_").reset_index())
    last = vr.groupby(["upc_code", "phase", "cycle"])["validation_ts"].max().rename("last_validated").reset_index()
    tbl = tbl.merge(last, on=["upc_code", "phase", "cycle"])
    summary = summary.merge(tbl, left_on=["UPC", "PHASE", "CYCLE"], right_on=["upc_code", "phase", "cycle"], how="left") \
                     .drop(columns=["upc_code", "phase", "cycle"])
print(summary["status"].value_counts().to_dict())
if (summary.get("FAIL", 0) > 0).any():
    print("UPCs with validation FAILs:", summary.loc[summary["FAIL"] > 0, "UPC"].tolist())
display(summary)

# Failing checks of this batch (from the validation table)
if not vr.empty and (vr["status"] == "FAIL").any():
    display(vr[vr["status"] == "FAIL"][["upc_code", "section", "check", "scope", "checked", "failed", "detail"]])