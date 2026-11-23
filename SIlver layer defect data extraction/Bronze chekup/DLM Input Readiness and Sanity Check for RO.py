# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # DLM Input Readiness Check - O&M UPC List (RO Jaipur)
# MAGIC
# MAGIC Step 1 holds the RO Jaipur list (87 UPCs). For each UPC, this finds every survey (Zone / Phase / Cycle) in bronze and checks whether all the files the DLM Extraction notebook needs are there (NSV D1-D4 and NSV_Backup lane files). One row per survey, with a plain-language comment.

# COMMAND ----------

# DBTITLE 1,Install openpyxl (needed for Excel export)
# MAGIC %pip install openpyxl -q

# COMMAND ----------

# DBTITLE 1,Step 1: UPC list to check
# The 87 UPCs from the O&M Master list - RO Jaipur (S.No order kept)
# Note: S.No 32 is given as N/02001/040011/RJ (6-digit package) - kept as in the list
UPC_LIST_JAIPUR = """
N/03004/01001/RJ
N/02001/05001/RJ
N/08039/01003/RJ
N/09016/01004/RJ
N/09014/01003/RJ
N/08012/01001/RJ
N/07007/01001/RJ
N/07005/01001/RJ
N/04038/03001/RJ
N/04038/02001/RJ
N/04038/01001/RJ
N/09013/01002/RJ
N/09011/01002/RJ
N/09009/02001/RJ
N/01001/03004/RJ
N/01001/03005/RJ
N/01001/03006/RJ
N/01001/03007/RJ
N/01001/03015/RJ
N/04003/10002/RJ
N/04003/10003/RJ
N/04003/10004/RJ
N/04003/10005/RJ
N/04003/10006/RJ
N/04003/10007/RJ
N/04003/10008/RJ
N/04003/10009/RJ
N/04003/10010/RJ
N/09010/01001/RJ
N/02001/02008/RJ
N/02001/03005/RJ
N/02001/040011/RJ
N/05038/01002/RJ
N/05038/02001/RJ
N/08004/04002/RJ
N/08004/05001/RJ
N/08027/01001/RJ
N/09009/01002/RJ
N/08057/02003/RJ
N/08022/01003/RJ
N/08022/02004/RJ
N/08012/02003/RJ
N/08022/02002/RJ
N/07003/01002/RJ
N/07006/01002/RJ
N/08015/03002/RJ
N/04037/01001/RJ
N/08015/01001/RJ
N/04003/12002/RJ
N/04003/12003/RJ
N/04003/12004/RJ
N/04003/12005/RJ
N/04003/12006/RJ
N/04003/12007/RJ
N/04003/12008/RJ
N/04003/12009/RJ
N/04003/11002/RJ
N/04003/11003/RJ
N/04003/11004/RJ
N/04003/11005/RJ
N/04003/11006/RJ
N/04003/11007/RJ
N/08057/01002/RJ
N/08015/02001/RJ
N/02018/03001/RJ
N/02018/02001/RJ
N/05003/02001/RJ
N/08028/02002/RJ
N/08027/02002/RJ
N/08027/02003/RJ
N/09007/01001/RJ
N/09015/01002/RJ
N/01001/03009/RJ
N/01001/03010/RJ
N/01001/03011/RJ
N/01001/03012/RJ
N/01001/03014/RJ
N/02017/04002/RJ
N/06015/01001/RJ
N/06015/01004/RJ
N/02001/06001/RJ
N/02018/01005/RJ
N/03004/03001/RJ
N/02001/07001/RJ
N/02001/08001/RJ
N/02017/03001/RJ
N/02001/09001/RJ
""".split()

REGION = "RO_Jaipur"          # used in the Excel file name
UPC_LIST = [u.strip().upper() for u in UPC_LIST_JAIPUR if u.strip()]
print(REGION, ":", len(UPC_LIST), "UPCs,", len(set(UPC_LIST)), "unique")

# COMMAND ----------

# DBTITLE 1,Step 2: Index NSV and NSV_Backup folders in bronze
import os, re
import pandas as pd
import pyarrow.parquet as pq

BRONZE_ROOT = "/Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output"

def num(v):
    m = re.search(r"\d+", str(v))
    return str(int(m.group())) if m else str(v)

def upc_key(u):
    """N/04006/03001/GJ, N_4006_3001_GJ -> same key (ignores / vs _ and leading zeros)"""
    parts = re.split(r"[/_\-]", str(u).strip().upper())
    return tuple(str(int(p)) if p.isdigit() else p for p in parts if p)

def index_source(source):
    """Every Zone/Phase/Cycle/UPC folder of one source -> list of dicts"""
    rows, sp = [], f"{BRONZE_ROOT}/Source={source}"
    for z in sorted(os.listdir(sp)):
        if not z.startswith("Zone="): continue
        for p in sorted(os.listdir(f"{sp}/{z}")):
            if not p.startswith("Phase="): continue
            for c in sorted(os.listdir(f"{sp}/{z}/{p}")):
                if not c.startswith("Cycle="): continue
                for u in os.listdir(f"{sp}/{z}/{p}/{c}"):
                    if u.startswith("UPC="):
                        rows.append(dict(zone=z[5:].strip().upper(), phase_folder=p[6:], cycle_folder=c[6:],
                                         phase=num(p[6:]), cycle=num(c[6:]), upc_folder=u[4:], key=upc_key(u[4:]),
                                         path=f"{sp}/{z}/{p}/{c}/{u}"))
    return pd.DataFrame(rows)

NSV_IDX = index_source("NSV")
BKP_IDX = index_source("NSV_Backup")
print("NSV folders:", len(NSV_IDX), "| NSV_Backup folders:", len(BKP_IDX))

# Surveys already processed by the DLM notebook
DONE = set()
for q in ["select distinct upc_code, zone, phase, cycle from nhai_dhms.dlm_kpi.dlm_validation_results",
          "select distinct upc_code, zone, phase, cycle from nhai_dhms.dlm_kpi.master_chainage"]:
    try:
        for r in spark.sql(q).collect():
            DONE.add((upc_key(r.upc_code), str(r.zone).upper(), num(r.phase), num(r.cycle)))
    except Exception as e:
        print("could not read:", q, str(e)[:100])
print("Surveys already in dlm_kpi:", len(DONE))

# COMMAND ----------

# DBTITLE 1,Step 3: Check each survey the same way the DLM notebook does
# Same folder / file rules as the DLM Extraction notebook (Configuration + Load sources cells)
def _pn(name):
    try: return int(name.split("=", 1)[1])
    except Exception: return -1

def dates_in(base):
    """All year/month/day delivery folders, newest first"""
    out = []
    if not os.path.exists(base): return out
    for y in [x for x in os.listdir(base) if x.startswith("year=")]:
        for m in [x for x in os.listdir(f"{base}/{y}") if x.startswith("month=")]:
            for d in [x for x in os.listdir(f"{base}/{y}/{m}") if x.startswith("day=")]:
                out.append((_pn(y), _pn(m), _pn(d), f"{y}/{m}/{d}"))
    return [o[3] for o in sorted(out, reverse=True)]

def nice_date(p):
    if not p: return None
    y, m, d = [_pn(x) for x in p.split("/")]
    return f"{d:02d}-{m:02d}-{y}"

def pick_folder(rows):
    """Notebook tries Phase=1/Cycle=1 first, then Phase=Phase 1/Cycle=Cycle 1. Anything else is not found."""
    for r in rows:
        if r["phase_folder"] == r["phase"] and r["cycle_folder"] == r["cycle"]: return r
    for r in rows:
        if r["phase_folder"] == f"Phase {r['phase']}" and r["cycle_folder"] == f"Cycle {r['cycle']}": return r
    return None

def find_deliverable_parent(base):
    if not os.path.exists(base): return base
    children = sorted(os.listdir(base))
    if any(c.startswith("Deliverable=") for c in children): return base
    for child in children:
        cp = os.path.join(base, child)
        if os.path.isdir(cp):
            found = find_deliverable_parent(cp)
            if found != cp: return found
            if any(g.startswith("Deliverable=") for g in os.listdir(cp)): return cp
    return base

def parquets(p):
    return sorted(f for f in os.listdir(p) if f.endswith(".parquet")) if os.path.exists(p) else []

def is_irc_d2(fname, cols):
    if "irc" in fname.lower(): return True
    cols = [str(c).lower() for c in cols if "limit" in str(c).lower()]
    if not cols or any("concession" in c for c in cols): return False
    return any("irc" in c and ("bi" in c or "rut" in c or "roughness" in c or "iri" in c) for c in cols)

DEFECT_SUFFIXES = {
    "Cracking":  ["_cracking.parquet"],
    "Roughness": ["_roughness_iri.parquet", "_roughness.parquet", "_iri.parquet"],
    "Rutting":   ["_rutting.parquet", "_rut.parquet"],
    "Ravelling": ["_raveling.parquet", "_ravelling.parquet"],
}
D3_KEYS = ["crack", "roughness", "rut", "pothole"]
D4_EXCLUDE = ["roughness", "rut,", "crack", "raveling", "ravelling"]
D4_MIN_COLS = {"severity", "area_sq_m", "area", "type_of_defect", "lane_no"}

def check_survey(upc_in, zone, phase, cycle, nsv_rows, bkp_rows):
    upc = "_".join(upc_in.split("/"))
    rec = dict(Zone=zone, Phase=phase, Cycle=cycle, UPC=upc)
    blocked, warn = [], []

    # ---- NSV (D1-D4) ----
    nsv = pick_folder(nsv_rows) if nsv_rows else None
    if nsv_rows and len(nsv_rows) > 1:
        warn.append(f"same survey is saved in {len(nsv_rows)} NSV folders (" + ", ".join(f"Phase={r['phase_folder']}/Cycle={r['cycle_folder']}" for r in nsv_rows) + "); notebook uses the first one")
    if nsv_rows and nsv_rows[0]["upc_folder"] != upc:
        warn.append(f"bronze folder name is {nsv_rows[0]['upc_folder']} (not the same as the list), put that full name in the UPC widget to run")
    if not nsv_rows:
        blocked.append("no NSV folder for this survey (only backup exists)")
    elif nsv is None:
        blocked.append("NSV folder name style is not handled by the notebook: " + nsv_rows[0]["phase_folder"] + "/" + nsv_rows[0]["cycle_folder"])
        nsv = nsv_rows[0]
    rec["Bronze folder"] = f"Phase={nsv['phase_folder']}/Cycle={nsv['cycle_folder']}" if nsv else None

    nsv_dates = dates_in(nsv["path"]) if nsv else []
    rec["NSV date"] = nice_date(nsv_dates[0]) if nsv_dates else None
    rec["NSV deliveries"] = len(nsv_dates)
    # more than one delivery is normal (re-uploads); the notebook always takes the latest, so it is not a warning
    d1 = d2 = d3 = d4 = []
    if nsv:
        base = f"{nsv['path']}/{nsv_dates[0]}" if nsv_dates else nsv["path"]
        dp = find_deliverable_parent(base)
        if not any(x.startswith("Deliverable=") for x in os.listdir(dp)):
            blocked.append("no Deliverable folders inside NSV")
        d1, d2, d3, d4 = [parquets(f"{dp}/Deliverable=Deliverable - {i}") for i in (1, 2, 3, 4)]
    rec["D1 files"] = len(d1)
    pav = [f for f in d1 if "pavementtype" in f.lower()]
    rec["D1 PavementType"] = "Yes" if pav else "No"
    if nsv and not d1: blocked.append("D1 is empty (no road details: pavement, NH, section)")
    elif nsv and not pav: warn.append("D1 has no PavementType file, pavement type will be blank")
    if nsv and not any("lrpmaster" in f.lower() for f in d1) and d1: warn.append("D1 has no LRPMaster file")

    rec["D2 files"] = len(d2)
    ca = False
    if d2:
        dp2 = f"{dp}/Deliverable=Deliverable - 2"
        for f in d2:
            if "roughness,rut,crack,rav" in f.lower(): continue
            try: cols = pq.read_schema(f"{dp2}/{f}").names
            except Exception: continue
            if any("limit" in c.lower() for c in cols) and not is_irc_d2(f, cols): ca = True; break
    rec["D2 CA file"] = "Yes" if ca else "No"
    if nsv and not d2: warn.append("D2 is empty, default thresholds will be used")
    elif nsv and not ca: warn.append("no CA threshold file in D2, default CA thresholds will be used")

    d3_ok = [f for f in d3 if any(k in f.lower() for k in D3_KEYS)]
    rec["D3 files"] = len(d3)
    if nsv and not d3_ok: warn.append("no D3 condition files, road condition columns will be blank")

    rec["D4 files"] = len(d4)
    pot = False
    for f in d4:
        fl = f.lower()
        if "pothole" in fl and not any(k in fl for k in D4_EXCLUDE):
            try:
                if {c.lower() for c in pq.read_schema(f"{dp}/Deliverable=Deliverable - 4/{f}").names} & D4_MIN_COLS: pot = True; break
            except Exception: pass
    rec["D4 pothole"] = "Yes" if pot else "No"

    # ---- NSV_Backup (lane defect values) ----
    bkp = pick_folder(bkp_rows) if bkp_rows else None
    bkp_dates = dates_in(bkp["path"]) if bkp else []
    rec["Backup date"] = nice_date(bkp_dates[0]) if bkp_dates else None
    lanes, found, lane_parents = set(), {k: set() for k in DEFECT_SUFFIXES}, []
    if bkp_dates:
        for dpth, dn, _ in os.walk(f"{bkp['path']}/{bkp_dates[0]}"):
            ln = [d for d in dn if d.startswith("Lane=")]
            if ln:
                lane_parents.append(dpth)
                for l in ln:
                    lname = l[5:].upper().replace("LANE", "").replace("-", "").replace("_", "").replace(" ", "")
                    lanes.add(lname)
                    fs = [f.lower() for f in os.listdir(f"{dpth}/{l}") if f.endswith(".parquet") and "_raw_" not in f.lower()]
                    for k, sfx in DEFECT_SUFFIXES.items():
                        if any(f.endswith(s) for f in fs for s in sfx): found[k].add(lname)
                dn[:] = []
    rec["Backup lanes"] = ",".join(sorted(lanes)) if lanes else None
    have = [k for k in DEFECT_SUFFIXES if found[k]]
    rec["Defect types"] = f"{len(have)} of 4"
    if not bkp_rows: blocked.append("no NSV_Backup folder (defect values come from backup)")
    elif bkp is None: blocked.append("backup folder name style is not handled by the notebook")
    elif not lanes: blocked.append("NSV_Backup has no Lane folders")
    else:
        miss = [k for k in DEFECT_SUFFIXES if not found[k]]
        if miss: warn.append("backup is missing " + ", ".join(miss) + " files")
        for k in have:
            gap = sorted(lanes - found[k])
            if gap: warn.append(f"{k} missing for lane(s) {','.join(gap)}")
        if len(lane_parents) > 1: warn.append(f"backup has {len(lane_parents)} analysis folders (e.g. Asphalt + CC), all will be used")
    if nsv_dates and bkp_dates and nsv_dates[0] != bkp_dates[0]:
        warn.append(f"NSV date {nice_date(nsv_dates[0])} and backup date {nice_date(bkp_dates[0])} do not match")

    rec["Already in dlm_kpi"] = "Yes" if (upc_key(upc), zone, phase, cycle) in DONE else "No"
    if blocked:
        rec["Status"] = "Not ready"
    elif warn:
        rec["Status"] = "Ready (with notes)" + (" - no potholes" if not pot else "")
    else:
        rec["Status"] = "Ready" if pot else "Ready (no potholes)"
    if not pot and not blocked: warn.insert(0, "no pothole file in D4, pothole table will be empty")
    rec["Comments"] = "; ".join(blocked + warn) if (blocked or warn) else "All files present, can be run"
    return rec

# ---- run for every UPC in the list ----
results = []
for upc_in in UPC_LIST:
    k = upc_key(upc_in)
    n = NSV_IDX[NSV_IDX.key == k].to_dict("records") if len(NSV_IDX) else []
    b = BKP_IDX[BKP_IDX.key == k].to_dict("records") if len(BKP_IDX) else []
    if not n and not b:
        # fallback: UPC saved inside a combined folder name, e.g. UPC=N_02017_04002_RJ_N_02017_04003_GJ
        u_ = upc_in.replace("/", "_")
        n = NSV_IDX[NSV_IDX.upc_folder.str.upper().str.contains(u_, regex=False)].to_dict("records")
        b = BKP_IDX[BKP_IDX.upc_folder.str.upper().str.contains(u_, regex=False)].to_dict("records")
    if not n and not b:
        # fallback: same road/package number but a different state code, e.g. list N_02001_01009_HR vs bronze N_02001_01009_DL
        n = NSV_IDX[NSV_IDX.key.map(lambda kk: kk[:3] == k[:3])].to_dict("records")
        b = BKP_IDX[BKP_IDX.key.map(lambda kk: kk[:3] == k[:3])].to_dict("records")
    surveys = sorted({(r["zone"], r["phase"], r["cycle"]) for r in n + b}, key=lambda s: (s[0], int(s[1]) if s[1].isdigit() else 0, int(s[2]) if s[2].isdigit() else 0))
    if not surveys:
        results.append(dict(Zone=None, Phase=None, Cycle=None, UPC=upc_in.replace("/", "_"), **{"Defect types": "0 of 4"},
                            **{"Already in dlm_kpi": "No", "Status": "Not in bronze",
                               "Comments": "UPC not found in NSV or NSV_Backup in any zone"}))
        continue
    for z, p, c in surveys:
        same = lambda r: (r["zone"], r["phase"], r["cycle"]) == (z, p, c)
        results.append(check_survey(upc_in, z, p, c, [r for r in n if same(r)], [r for r in b if same(r)]))

COLS = ["Zone", "Phase", "Cycle", "UPC", "Bronze folder", "NSV date", "NSV deliveries", "D1 files", "D1 PavementType",
        "D2 files", "D2 CA file", "D3 files", "D4 files", "D4 pothole", "Backup date", "Backup lanes",
        "Defect types", "Already in dlm_kpi", "Status", "Comments"]
readiness = pd.DataFrame(results).reindex(columns=COLS)
# S.No of the UPC in the given list, then # per survey
readiness.insert(0, "S.No (list)", readiness["UPC"].map({u.replace("/", "_"): i + 1 for i, u in enumerate(UPC_LIST)}))
readiness = readiness.sort_values(["S.No (list)", "Phase", "Cycle"], na_position="last").reset_index(drop=True)
readiness.insert(0, "#", range(1, len(readiness) + 1))
for c in ["NSV deliveries", "D1 files", "D2 files", "D3 files", "D4 files"]:
    readiness[c] = readiness[c].astype("Int64")
display(readiness)

# COMMAND ----------

# DBTITLE 1,Step 4: Summary counts
s = readiness
print("UPCs in list            :", len(UPC_LIST))
print("UPCs found in bronze    :", s.loc[s.Status != "Not in bronze", "UPC"].nunique())
print("UPCs not in bronze      :", (s.Status == "Not in bronze").sum())
print("Surveys found           :", (s.Status != "Not in bronze").sum())
multi = s[s.Status != "Not in bronze"].groupby("UPC").size()
print("UPCs with >1 survey     :", (multi > 1).sum(), dict(multi[multi > 1]))
print("Already in dlm_kpi      :", (s["Already in dlm_kpi"] == "Yes").sum())
print()
display(s.groupby("Status").size().reset_index(name="surveys"))

# COMMAND ----------

# DBTITLE 1,Step 5: Export readiness table to Excel
# Saves the table as Excel next to this notebook (workspace folder)
from datetime import datetime
import openpyxl  # noqa: F401  (if missing: run %pip install openpyxl first)

OUT_DIR = "/Workspace/Users/shivam.raj9226@softsensor.ai/SIlver layer defect data extraction"
OUT_FILE = f"{OUT_DIR}/DLM_Input_Readiness_{REGION}_{datetime.now():%Y%m%d_%H%M%S}.xlsx"

found = readiness[readiness.Status != "Not in bronze"]
summary = pd.DataFrame([
    ("UPCs in list", len(UPC_LIST)),
    ("UPCs found in bronze", found["UPC"].nunique()),
    ("UPCs not in bronze", (readiness.Status == "Not in bronze").sum()),
    ("Surveys found", len(found)),
    ("UPCs with more than 1 survey", (found.groupby("UPC").size() > 1).sum()),
    ("Surveys already in dlm_kpi", (readiness["Already in dlm_kpi"] == "Yes").sum()),
] + [(f"Status: {k}", v) for k, v in readiness.Status.value_counts().items()], columns=["Item", "Count"])

with pd.ExcelWriter(OUT_FILE, engine="openpyxl") as xw:
    summary.to_excel(xw, sheet_name="summary", index=False)
    readiness.to_excel(xw, sheet_name="readiness", index=False)
    readiness[readiness.Status == "Not in bronze"].to_excel(xw, sheet_name="not_in_bronze", index=False)
    readiness[readiness.Status.str.contains("with notes")].to_excel(xw, sheet_name="needs_attention", index=False)
    for ws in xw.book.worksheets:  # readable column widths
        for col in ws.columns:
            ws.column_dimensions[col[0].column_letter].width = min(max(len(str(c.value or "")) for c in col) + 2, 80)

chk = pd.read_excel(OUT_FILE, sheet_name=None)
print("Saved:", OUT_FILE)
print({k: len(v) for k, v in chk.items()})