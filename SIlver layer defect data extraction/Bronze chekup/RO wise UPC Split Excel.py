# Databricks notebook source
# DBTITLE 1,Install openpyxl
# MAGIC %pip install openpyxl

# COMMAND ----------

# DBTITLE 1,RO-wise UPC split to Excel
import os, shutil
import pandas as pd
from datetime import datetime

BASE = "/Workspace/Users/shivam.raj9226@softsensor.ai/SIlver layer defect data extraction/Bronze chekup"
SRC_CSV = f"{BASE}/Old silver population(886_Project_master).csv"
OUT_DIR = f"{BASE}/RO_based"
OUT_FILE = f"{OUT_DIR}/RO_wise_UPC_886_Project_master.xlsx"

# CSV has Windows characters (e.g. en dash 0x96) -> cp1252
df = pd.read_csv(SRC_CSV, encoding="cp1252")
df["RO"] = df["RO"].astype(str).str.strip()

# Some text cells contain control characters (e.g. \x01) that Excel cannot store -> replace with a space
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
obj_cols = df.select_dtypes(include="object").columns
bad = {c: int(df[c].astype(str).str.contains(ILLEGAL_CHARACTERS_RE).sum()) for c in obj_cols}
print("Cells cleaned of illegal characters:", {c: n for c, n in bad.items() if n})
df[obj_cols] = df[obj_cols].apply(lambda s: s.map(lambda v: ILLEGAL_CHARACTERS_RE.sub(" ", v) if isinstance(v, str) else v))
print("Rows:", len(df), "| Distinct UPC:", df["UPC"].nunique(), "| Distinct RO:", df["RO"].nunique())

# Summary sheet: S.No | RO | Count - UPC + Total Result
summary = (df.groupby("RO")["UPC"].nunique().reset_index(name="Count - UPC").sort_values("RO").reset_index(drop=True))
summary.insert(0, "S.No", range(1, len(summary) + 1))
total = pd.DataFrame([{"S.No": "Total Result", "RO": "", "Count - UPC": int(summary["Count - UPC"].sum())}])
summary_out = pd.concat([summary, total], ignore_index=True)
display(summary_out)

assert summary["Count - UPC"].sum() == df["UPC"].nunique() == len(df), "UPC count mismatch"

# Write to local tmp, then copy into the workspace folder
os.makedirs(OUT_DIR, exist_ok=True)
tmp = "/tmp/RO_wise_UPC.xlsx"
with pd.ExcelWriter(tmp, engine="openpyxl") as xw:
    summary_out.to_excel(xw, sheet_name="Summary", index=False)
    for ro in summary["RO"]:
        part = df[df["RO"] == ro].sort_values("UPC").reset_index(drop=True)
        part.insert(0, "S.No", range(1, len(part) + 1))
        part.to_excel(xw, sheet_name=ro[:31], index=False)
shutil.copy(tmp, OUT_FILE)

# Verify: re-read file, check sheet count and per-sheet row counts vs summary
xl = pd.ExcelFile(OUT_FILE, engine="openpyxl")
chk = pd.DataFrame([(s, len(xl.parse(s))) for s in xl.sheet_names[1:]], columns=["RO", "rows_in_sheet"]).merge(summary[["RO", "Count - UPC"]], on="RO")
print("Saved:", OUT_FILE)
print("Sheets:", len(xl.sheet_names), "(1 Summary +", len(xl.sheet_names) - 1, "RO)")
print("All RO sheet counts match summary:", (chk["rows_in_sheet"] == chk["Count - UPC"]).all(), "| Total rows:", chk["rows_in_sheet"].sum())

# COMMAND ----------

# DBTITLE 1,Step 1: Pick RO and load its UPCs
import os, re
import pandas as pd

# ---- Change this to the RO you want to check (sheet name in the RO-wise workbook) ----
RO_NAME = "RO Jaipur"

BASE = "/Workspace/Users/shivam.raj9226@softsensor.ai/SIlver layer defect data extraction/Bronze chekup"
RO_FILE = f"{BASE}/RO_based/RO_wise_UPC_886_Project_master.xlsx"
TRACKER_FILE = f"{BASE}/NSV_Activity_Tracker_ZoneWise_Standardized_17072026.xlsx"
BRONZE_ROOT = "/Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output"

ro_xl = pd.ExcelFile(RO_FILE, engine="openpyxl")
assert RO_NAME in ro_xl.sheet_names, f"{RO_NAME} not found. Available: {ro_xl.sheet_names[1:]}"
ro_upcs = ro_xl.parse(RO_NAME)[["UPC", "Project Name", "State Name", "PIU", "NSV Zone"]]
ro_upcs["UPC"] = ro_upcs["UPC"].astype(str).str.strip()

def num(v):
    m = re.search(r"\d+", str(v))
    return str(int(m.group())) if m else None

SPLIT_RE = r"[/_\-\s;,]+"

def upc_key(u):
    """Match key = the two number segments only (state code, N/M prefix, / _ - and leading zeros ignored).
    N/01007/04002/UP, N_01007_04002_DL, M_01007_04002_UP-RCW -> ('1007', '4002')
    Safe: no two UPCs in the 886 master list share the same two number segments."""
    parts = [p for p in re.split(SPLIT_RE, str(u).strip().upper()) if p]
    if len(parts) < 3 or not (parts[1].isdigit() and parts[2].isdigit()): return None
    return (str(int(parts[1])), str(int(parts[2])))

def folder_keys(name):
    """Bronze folder can hold 2+ UPCs (N_02017_04002_RJ_N_02017_04003_GJ or 'N_02010_04007_AP; N_02010_04006_AP') -> one key per UPC"""
    parts = [p for p in re.split(SPLIT_RE, str(name).strip().upper()) if p]
    keys = []
    for i in range(len(parts) - 2):
        if parts[i].isalpha() and len(parts[i]) == 1 and parts[i + 1].isdigit() and parts[i + 2].isdigit():
            k = (str(int(parts[i + 1])), str(int(parts[i + 2])))
            if k not in keys: keys.append(k)
    return keys or [upc_key(name)]

assert upc_key("N/01007/04002/UP") == upc_key("N_01007_04002_DL") == upc_key("M_1007_4002_UP-RCW")
assert folder_keys("N_02010_04007_AP; N_02010_04006_AP; N_02010_04005_AP") == [("2010", "4007"), ("2010", "4006"), ("2010", "4005")]
assert folder_keys("N_02017_04002_RJ_N_02017_04003_GJ") == [("2017", "4002"), ("2017", "4003")]

ro_upcs["key"] = ro_upcs["UPC"].map(upc_key)
print(f"{RO_NAME}: {len(ro_upcs)} UPCs ({ro_upcs['UPC'].nunique()} unique)")
display(ro_upcs.drop(columns="key"))

# COMMAND ----------

# DBTITLE 1,Step 2: Find all surveys (Phase/Cycle) in bronze for each UPC
# One survey = one Zone/Phase/Cycle/UPC folder under Source=NSV
# Folders named Phase=1 and Phase=Phase 1 for the same UPC are the same survey -> counted once
def index_source(source):
    rows, sp = [], f"{BRONZE_ROOT}/Source={source}"
    for z in sorted(os.listdir(sp)):
        if not z.startswith("Zone="): continue
        for p in sorted(os.listdir(f"{sp}/{z}")):
            if not p.startswith("Phase="): continue
            for c in sorted(os.listdir(f"{sp}/{z}/{p}")):
                if not c.startswith("Cycle="): continue
                for u in os.listdir(f"{sp}/{z}/{p}/{c}"):
                    if not u.startswith("UPC="): continue
                    for k in folder_keys(u[4:]):
                        rows.append(dict(key=k, zone=z[5:].strip().upper(), phase=num(p[6:]), cycle=num(c[6:]),
                                         bronze_folder=f"{z}/{p}/{c}/{u}"))
    return pd.DataFrame(rows)

nsv_idx = index_source("NSV")
bkp_idx = index_source("NSV_Backup")
print("NSV folders indexed:", len(nsv_idx), "| NSV_Backup folders indexed:", len(bkp_idx))

keys = set(ro_upcs["key"])
hits = nsv_idx[nsv_idx["key"].isin(keys)]
surveys = (hits.groupby(["key", "zone", "phase", "cycle"], as_index=False)
               .agg(bronze_folders=("bronze_folder", lambda s: " | ".join(sorted(set(s))))))
bkp_set = set(map(tuple, bkp_idx[["key", "zone", "phase", "cycle"]].values.tolist()))
surveys["NSV_Backup present"] = ["Yes" if (r.key, r.zone, r.phase, r.cycle) in bkp_set else "No"
                                 for r in surveys.itertuples()]
surveys["Survey"] = "Phase " + surveys["phase"] + " / Cycle " + surveys["cycle"]

survey_list = (ro_upcs.merge(surveys, on="key", how="left")
                     .sort_values(["UPC", "zone", "phase", "cycle"]).reset_index(drop=True))
survey_list["Survey"] = survey_list["Survey"].fillna("Not in bronze")

# Per-UPC view: how many surveys and which ones
per_upc = (survey_list.groupby("UPC", sort=False)
           .apply(lambda g: pd.Series({
               "No. of surveys": int(g["phase"].notna().sum()),
               "Surveys": ", ".join(f"Zone {r.zone} P{r.phase}C{r.cycle}" for r in g.itertuples() if pd.notna(r.phase)) or "Not in bronze"}))
           .reset_index())

print(f"UPCs in {RO_NAME}: {len(ro_upcs)} | found in bronze: {(per_upc['No. of surveys'] > 0).sum()} | not in bronze: {(per_upc['No. of surveys'] == 0).sum()} | total surveys: {per_upc['No. of surveys'].sum()}")
display(survey_list.groupby("Survey").size().reset_index(name="count"))
display(per_upc)
display(survey_list[["UPC", "Project Name", "zone", "phase", "cycle", "Survey", "NSV_Backup present", "bronze_folders"]])

# COMMAND ----------

# DBTITLE 1,Step 3: Check each survey in NSV tracker (As per contract or not)
# Tracker: zone sheets (Zone A..E), header on 2nd row. Scope column tells As per contract / Additional / De-scoped
SCOPE_COL = "Scope Type (Scope / Additional / De-scoped)"
trk_xl = pd.ExcelFile(TRACKER_FILE, engine="openpyxl")
trk = pd.concat([trk_xl.parse(s, header=1).assign(_sheet=s) for s in trk_xl.sheet_names if s.startswith("Zone ")],
                ignore_index=True)
trk = trk[trk["Phase"].notna() | trk["Cycle"].notna()].copy()
trk["zone"] = trk["Zone"].fillna(trk["_sheet"].str[-1]).astype(str).str.strip().str.upper().str[-1]
trk["phase"] = trk["Phase"].map(num)
trk["cycle"] = trk["Cycle"].map(num)
trk["scope_raw"] = trk[SCOPE_COL].astype(str).str.replace(r"\s+", " ", regex=True).str.strip().replace("nan", "")

def scope_class(v):
    s = v.lower()
    if not s: return "Blank in tracker"
    if "as per" in s: return "As per contract"          # As per contract / original NSV contract / NSV Contract / RFP / CA
    if "additional" in s: return "Additional"
    if "descop" in s or "de-scop" in s: return "De-scoped"
    return "Other"                                      # One Time, HAM ..., Project duplicacy ...
trk["scope_class"] = trk["scope_raw"].map(scope_class)
print("Tracker scope values -> class:")
display(trk.groupby(["scope_class", "scope_raw"]).size().reset_index(name="rows"))

# A tracker row matches a UPC by 'UPC code as per O&M master' OR 'UPC as per survey conducted'
t_long = pd.concat([trk.assign(key=trk["UPC code as per O&M master"].map(upc_key), matched_on="O&M master UPC"),
                    trk.assign(key=trk["UPC as per survey conducted"].map(upc_key), matched_on="Survey UPC")])
t_long = t_long[t_long["key"].notna()].drop_duplicates(subset=["_sheet", "S. No.", "key"])

def agg_trk(g):
    return pd.Series({"Tracker scope": " / ".join(sorted(set(g["scope_class"]))),
                      "Tracker scope (raw)": " / ".join(sorted(set(g["scope_raw"]) - {""})),
                      "Tracker rows": len(g),
                      "Tracker sheet / S.No": ", ".join(f"{a} #{int(b) if pd.notna(b) else '?'}" for a, b in zip(g["_sheet"], g["S. No."]))})

exact = t_long.groupby(["key", "zone", "phase", "cycle"]).apply(agg_trk).reset_index()
any_zone = t_long.groupby(["key", "phase", "cycle"]).apply(agg_trk).reset_index()

found = survey_list[survey_list["phase"].notna()].copy()
res = found.merge(exact, on=["key", "zone", "phase", "cycle"], how="left")
res["Tracker match"] = res["Tracker scope"].notna().map({True: "Same zone/phase/cycle", False: None})
# Fallback: same UPC + phase + cycle in another zone sheet
miss = res["Tracker scope"].isna()
fb = res.loc[miss, ["key", "phase", "cycle"]].merge(any_zone, on=["key", "phase", "cycle"], how="left")
for c in ["Tracker scope", "Tracker scope (raw)", "Tracker rows", "Tracker sheet / S.No"]:
    res.loc[miss, c] = fb[c].values
res.loc[miss & res["Tracker scope"].notna(), "Tracker match"] = "Other zone in tracker"
res["Tracker match"] = res["Tracker match"].fillna("Not in tracker")
res["Tracker scope"] = res["Tracker scope"].fillna("Not in tracker")
res["As per contract?"] = res["Tracker scope"].map(lambda s: "Yes" if s == "As per contract" else ("Not in tracker" if s == "Not in tracker" else ("Mixed" if "As per contract" in s else "No")))

contract_check = res[["UPC", "Project Name", "zone", "phase", "cycle", "Survey", "As per contract?", "Tracker scope",
                      "Tracker scope (raw)", "Tracker match", "Tracker rows", "Tracker sheet / S.No", "NSV_Backup present"]]
print(f"{RO_NAME}: {len(contract_check)} bronze surveys checked in tracker")
display(contract_check.groupby(["As per contract?", "Tracker scope"]).size().reset_index(name="surveys"))
display(contract_check)

# COMMAND ----------

# DBTITLE 1,Step 4: All 24 ROs - surveys + contract check to Excel
# Runs Step 2 + Step 3 logic for every RO (uses nsv_idx, bkp_idx, exact, any_zone, upc_key from the cells above)
import shutil
OUT_FILE = f"{BASE}/RO_based/RO_wise_Survey_Contract_Check.xlsx"

all_upcs = pd.concat([ro_xl.parse(s)[["UPC", "Project Name", "State Name", "PIU", "NSV Zone"]].assign(RO=s)
                      for s in ro_xl.sheet_names if s != "Summary"], ignore_index=True)
all_upcs["UPC"] = all_upcs["UPC"].astype(str).str.strip()
all_upcs["key"] = all_upcs["UPC"].map(upc_key)

# Bronze surveys (one row per UPC + Zone + Phase + Cycle)
hits = nsv_idx[nsv_idx["key"].isin(set(all_upcs["key"]))]
sv = hits.groupby(["key", "zone", "phase", "cycle"], as_index=False).agg(
    bronze_folders=("bronze_folder", lambda s: " | ".join(sorted(set(s)))))
bkp_set = set(map(tuple, bkp_idx[["key", "zone", "phase", "cycle"]].values.tolist()))
sv["NSV_Backup present"] = ["Yes" if (r.key, r.zone, r.phase, r.cycle) in bkp_set else "No" for r in sv.itertuples()]

# Tracker check (same zone first, then same UPC + phase + cycle in another zone sheet)
sv = sv.merge(exact, on=["key", "zone", "phase", "cycle"], how="left")
sv["Tracker match"] = sv["Tracker scope"].notna().map({True: "Same zone/phase/cycle", False: None})
miss = sv["Tracker scope"].isna()
fb = sv.loc[miss, ["key", "phase", "cycle"]].merge(any_zone, on=["key", "phase", "cycle"], how="left")
for c in ["Tracker scope", "Tracker scope (raw)", "Tracker rows", "Tracker sheet / S.No"]:
    sv.loc[miss, c] = fb[c].values
sv.loc[miss & sv["Tracker scope"].notna(), "Tracker match"] = "Other zone in tracker"
sv["Tracker match"] = sv["Tracker match"].fillna("Not in tracker")
sv["Tracker scope"] = sv["Tracker scope"].fillna("Not in tracker")
sv["As per contract?"] = sv["Tracker scope"].map(
    lambda s: "Yes" if s == "As per contract" else ("Not in tracker" if s == "Not in tracker" else ("Mixed" if "As per contract" in s else "No")))
sv["Survey"] = "Phase " + sv["phase"] + " / Cycle " + sv["cycle"]

detail = all_upcs.merge(sv, on="key", how="left")
detail["Survey"] = detail["Survey"].fillna("Not in bronze")
for c in ["As per contract?", "Tracker scope", "Tracker match"]:
    detail[c] = detail[c].where(detail["phase"].notna(), "-")
surveys_per_upc = detail.groupby(["RO", "UPC"])["phase"].transform(lambda s: int(s.notna().sum()))
detail["No. of surveys for UPC"] = surveys_per_upc
detail = detail.sort_values(["RO", "UPC", "zone", "phase", "cycle"]).reset_index(drop=True)

COLS = ["UPC", "Project Name", "State Name", "PIU", "NSV Zone", "No. of surveys for UPC", "zone", "phase", "cycle", "Survey",
        "As per contract?", "Tracker scope", "Tracker scope (raw)", "Tracker match", "Tracker rows", "Tracker sheet / S.No",
        "NSV_Backup present", "bronze_folders"]
RENAME = {"zone": "Bronze Zone", "phase": "Phase", "cycle": "Cycle", "bronze_folders": "Bronze folder(s)"}

# ---------------- Summary (one row per RO) ----------------
survey_types = sorted(sv["Survey"].unique(), key=lambda s: tuple(int(x) for x in re.findall(r"\d+", s)))
rows = []
for ro in [s for s in ro_xl.sheet_names if s != "Summary"]:
    d = detail[detail["RO"] == ro]
    s_ = d[d["phase"].notna()]
    n_per_upc = d.groupby("UPC")["phase"].apply(lambda x: x.notna().sum())
    r = {"RO": ro,
         "Total UPCs (master)": d["UPC"].nunique(),
         "UPCs found in bronze": int((n_per_upc > 0).sum()),
         "UPCs NOT in bronze": int((n_per_upc == 0).sum()),
         "UPCs with 1 survey": int((n_per_upc == 1).sum()),
         "UPCs with 2+ surveys": int((n_per_upc >= 2).sum()),
         "Total surveys in bronze": len(s_)}
    for st in survey_types:
        r[st] = int((s_["Survey"] == st).sum())
    r["Surveys As per contract"] = int((s_["As per contract?"] == "Yes").sum())
    r["Surveys Additional"] = int((s_["Tracker scope"] == "Additional").sum())
    r["Surveys De-scoped"] = int((s_["Tracker scope"] == "De-scoped").sum())
    r["Surveys Other / Blank scope"] = int(s_["Tracker scope"].isin(["Other", "Blank in tracker"]).sum())
    r["Surveys Mixed scope (2+ tracker rows)"] = int(s_["Tracker scope"].str.contains(" / ", regex=False).sum())
    r["  of which incl. As per contract"] = int((s_["As per contract?"] == "Mixed").sum())
    r["Surveys NOT in tracker"] = int((s_["Tracker scope"] == "Not in tracker").sum())
    r["Surveys matched in other zone sheet"] = int((s_["Tracker match"] == "Other zone in tracker").sum())
    r["Surveys without NSV_Backup"] = int((s_["NSV_Backup present"] == "No").sum())
    r["% surveys As per contract"] = round(100 * r["Surveys As per contract"] / len(s_), 1) if len(s_) else 0.0
    rows.append(r)

summary = pd.DataFrame(rows)
summary.insert(0, "S.No", range(1, len(summary) + 1))
tot = {c: (summary[c].sum() if c not in ("S.No", "RO", "% surveys As per contract") else "") for c in summary.columns}
tot["S.No"], tot["RO"] = "Total Result", "All ROs"
tot["% surveys As per contract"] = round(100 * tot["Surveys As per contract"] / tot["Total surveys in bronze"], 1)
summary_out = pd.concat([summary, pd.DataFrame([tot])], ignore_index=True)

# Checks: numbers must add up
assert (summary["UPCs found in bronze"] + summary["UPCs NOT in bronze"] == summary["Total UPCs (master)"]).all()
assert (summary[survey_types].sum(axis=1) == summary["Total surveys in bronze"]).all()
scope_cols = ["Surveys As per contract", "Surveys Additional", "Surveys De-scoped", "Surveys Other / Blank scope",
              "Surveys Mixed scope (2+ tracker rows)", "Surveys NOT in tracker"]
assert (summary[scope_cols].sum(axis=1) == summary["Total surveys in bronze"]).all()

notes = pd.DataFrame({"Notes": [
    "One survey = one Zone/Phase/Cycle/UPC folder under bronze Source=NSV (Phase=1 and Phase=Phase 1 folders of same UPC counted once).",
    "UPC match uses only the two number segments (e.g. 01007/04002): state code, N/M prefix, / vs _ , leading zeros and suffix (e.g. -RCW) are ignored. No two master UPCs share these numbers. Combined bronze folders (2+ UPCs in one name, '_' or ';' separated) count for each UPC.",
    "Tracker = NSV_Activity_Tracker_ZoneWise_Standardized_17072026.xlsx, Zone A-E sheets, column 'Scope Type (Scope / Additional / De-scoped)'.",
    "Tracker row matched on 'UPC code as per O&M master' OR 'UPC as per survey conducted' + Zone + Phase + Cycle; if not found, same UPC + Phase + Cycle in another zone sheet.",
    "As per contract = As per contract / As per Contract / As per original NSV contract / As per NSV Contract / As per RFP / As per CA.",
    "Additional = Additional / Additional scope (incl. Original scope of Adharshila / IRSM). De-scoped = De-scoped / Descope / Descoped.",
    "Mixed = survey has 2+ tracker rows with different scope types (e.g. Additional / As per contract). 'of which incl. As per contract' = Mixed rows where one row is As per contract (As per contract? = Mixed).",
    "Scope buckets (As per contract + Additional + De-scoped + Other/Blank + Mixed + NOT in tracker) add up to Total surveys in bronze.",
    "RO sheets: one row per survey; UPCs not in bronze appear once with Survey = 'Not in bronze'."]})

tmp = "/tmp/RO_wise_Survey_Contract_Check.xlsx"
with pd.ExcelWriter(tmp, engine="openpyxl") as xw:
    summary_out.to_excel(xw, sheet_name="Summary", index=False)
    notes.to_excel(xw, sheet_name="Summary", index=False, startrow=len(summary_out) + 3)
    for ro in summary["RO"]:
        part = detail[detail["RO"] == ro][COLS].rename(columns=RENAME).reset_index(drop=True)
        part.insert(0, "S.No", range(1, len(part) + 1))
        part.to_excel(xw, sheet_name=ro[:31], index=False)
shutil.copy(tmp, OUT_FILE)

# Verify saved file
xl_chk = pd.ExcelFile(OUT_FILE, engine="openpyxl")
sheet_rows = {s: len(xl_chk.parse(s)) for s in xl_chk.sheet_names[1:]}
exp_rows = detail.groupby("RO").size().to_dict()
print("Saved:", OUT_FILE)
print("Sheets:", len(xl_chk.sheet_names), "(1 Summary +", len(sheet_rows), "RO)")
print("Every RO sheet row count matches:", all(sheet_rows[r[:31]] == n for r, n in exp_rows.items()))
display(summary_out)

# COMMAND ----------

# DBTITLE 1,Step 5: Reconcile raw bronze folders (1144) vs surveys (960)
sp = f"{BRONZE_ROOT}/Source=NSV"
raw = []
for z in os.listdir(sp):
    if not z.startswith("Zone="): continue
    for p in os.listdir(f"{sp}/{z}"):
        if not p.startswith("Phase="): continue
        for c in os.listdir(f"{sp}/{z}/{p}"):
            if not c.startswith("Cycle="): continue
            raw += [(z[5:], p, c, u[4:]) for u in os.listdir(f"{sp}/{z}/{p}/{c}") if u.startswith("UPC=")]
raw = pd.DataFrame(raw, columns=["zone", "pf", "cf", "folder"])
raw["phase"], raw["cycle"], raw["fkey"] = raw.pf.map(num), raw.cf.map(num), raw.folder.map(upc_key)

u1 = raw.drop_duplicates(["zone", "phase", "cycle", "folder"])
u2 = raw.drop_duplicates(["zone", "phase", "cycle", "fkey"])
ex = u2.assign(key=u2.folder.map(folder_keys)).explode("key").drop_duplicates(["zone", "phase", "cycle", "key"])
inl = ex[ex.key.isin(set(all_upcs.key))]
outl = ex[~ex.key.isin(set(all_upcs.key))]

recon = pd.DataFrame([
    ("1. Raw UPC folders in bronze Source=NSV", len(raw)),
    ("   minus duplicate folders (Phase=1/Cycle=1 and Phase=Phase 1/Cycle=Cycle 1 for same UPC)", -(len(raw) - len(u1))),
    ("   minus same UPC written differently (e.g. N_2004 vs N_02004, _DL vs _UP)", -(len(u1) - len(u2))),
    ("2. Unique survey folders", len(u2)),
    ("   plus extra UPCs inside combined folders (2+ UPCs in one folder name)", len(ex) - len(u2)),
    ("3. Unique UPC + Zone + Phase + Cycle surveys", len(ex)),
    ("   minus surveys of UPCs NOT in the 886 master list", -len(outl)),
    ("4. Surveys of the 886 master UPCs (used in Step 4)", len(inl)),
], columns=["Step", "Count"])
assert len(inl) == detail["phase"].notna().sum()
print(f"UPCs not in master list: {outl.key.nunique()}")
display(recon)
display(outl[["zone", "phase", "cycle", "folder"]].sort_values("folder").reset_index(drop=True))

# COMMAND ----------

# DBTITLE 1,Step 6: Export all 1144 bronze folders with colour-coded reconciliation status
# Every raw bronze NSV folder (1144) with its reconciliation status, colour-coded like the Step 5 table
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font

OUT_1144 = f"{BASE}/RO_based/Bronze_1144_Survey_Folders_Reconciliation.xlsx"
master_map = all_upcs.set_index("key")[["UPC", "RO"]].to_dict("index")

# Rebuild the raw list (same as Step 5); keep the Phase=1 style first so the Phase=Phase 1 copy is the one marked duplicate
f = raw[["zone", "pf", "cf", "folder", "phase", "cycle", "fkey"]].copy()
f["named_style"] = f["pf"].str.contains("Phase ") | f["cf"].str.contains("Cycle ")
f = f.sort_values(["named_style", "zone", "phase", "cycle", "folder"]).reset_index(drop=True)
f["Bronze path"] = "Source=NSV/Zone=" + f.zone + "/" + f.pf + "/" + f.cf + "/UPC=" + f.folder

f["dup_style"] = f.duplicated(["zone", "phase", "cycle", "folder"])
f["dup_spelling"] = False
rest = ~f["dup_style"]
f.loc[rest, "dup_spelling"] = f.loc[rest].duplicated(["zone", "phase", "cycle", "fkey"]).values
kept_first_style = f[~f.dup_style].groupby(["zone", "phase", "cycle", "folder"])["Bronze path"].first()
kept_first_key = f[~f.dup_style & ~f.dup_spelling].groupby(["zone", "phase", "cycle", "fkey"])["Bronze path"].first()

# Survey rows: split combined folders into one row per UPC
kept = f[~f.dup_style & ~f.dup_spelling].copy()
kept["keys"] = kept.folder.map(folder_keys)
sr = kept.explode("keys").rename(columns={"keys": "key"})
sr["Extra UPC from combined folder"] = sr.groupby("Bronze path").cumcount().gt(0).map({True: "Yes", False: "No"})
sr["dup_key"] = sr.duplicated(["zone", "phase", "cycle", "key"])
sr = sr[~sr.dup_key].copy()
sr["Master UPC"] = sr.key.map(lambda k: master_map.get(k, {}).get("UPC"))
sr["RO"] = sr.key.map(lambda k: master_map.get(k, {}).get("RO"))
sr["Status"] = sr["Master UPC"].notna().map({True: "Counted - master UPC survey", False: "Not in 886 master list"})
sr["UPC number key"] = sr.key.map(lambda k: "/".join(k) if isinstance(k, tuple) else None)

# Folder-level status (1144 rows)
per_folder = sr.groupby("Bronze path").agg(n_keys=("key", "size"), n_master=("Master UPC", lambda s: s.notna().sum()),
                                          master_upcs=("Master UPC", lambda s: ", ".join(s.dropna())),
                                          ros=("RO", lambda s: ", ".join(sorted(set(s.dropna())))))
f = f.merge(per_folder, left_on="Bronze path", right_index=True, how="left")

def folder_status(r):
    if r.dup_style: return "Removed - duplicate folder (Phase=Phase N / Cycle=Cycle N copy)"
    if r.dup_spelling: return "Removed - same UPC written differently"
    if r.n_master == r.n_keys: return "Counted - master UPC survey" if r.n_keys == 1 else "Counted - combined folder (all UPCs in master)"
    if r.n_master == 0: return "Not in 886 master list"
    return "Combined folder - partly in master"
f["Status"] = f.apply(folder_status, axis=1)
f["Duplicate of"] = [kept_first_style.get((r.zone, r.phase, r.cycle, r.folder)) if r.dup_style
                     else (kept_first_key.get((r.zone, r.phase, r.cycle, r.fkey)) if r.dup_spelling else None)
                     for r in f.itertuples()]
f["UPCs inside folder"] = f["n_keys"].fillna(0).astype(int)

folders_out = f.rename(columns={"zone": "Zone", "pf": "Phase folder", "cf": "Cycle folder", "phase": "Phase", "cycle": "Cycle",
                                "folder": "UPC folder", "master_upcs": "Master UPC(s)", "ros": "RO(s)"})[
    ["Zone", "Phase folder", "Cycle folder", "Phase", "Cycle", "UPC folder", "Status", "UPCs inside folder",
     "Master UPC(s)", "RO(s)", "Duplicate of", "Bronze path"]].sort_values(["Zone", "Phase", "Cycle", "UPC folder", "Phase folder"]).reset_index(drop=True)
folders_out.insert(0, "S.No", range(1, len(folders_out) + 1))

surveys_out = sr.rename(columns={"zone": "Zone", "phase": "Phase", "cycle": "Cycle", "folder": "UPC folder"})[
    ["Zone", "Phase", "Cycle", "UPC folder", "UPC number key", "Extra UPC from combined folder", "Status", "Master UPC", "RO", "Bronze path"]
].sort_values(["Status", "Zone", "Phase", "Cycle", "UPC folder"]).reset_index(drop=True)
surveys_out.insert(0, "S.No", range(1, len(surveys_out) + 1))

# Reconciliation table (same as Step 5) with colour per line
n_style, n_spell = int(f.dup_style.sum()), int(f.dup_spelling.sum())
n_extra = int((sr["Extra UPC from combined folder"] == "Yes").sum())
n_out, n_in = int((sr.Status == "Not in 886 master list").sum()), int((sr.Status == "Counted - master UPC survey").sum())
recon_out = pd.DataFrame([
    ("1. Raw UPC folders in bronze Source=NSV", len(f), "All_1144_folders", "total"),
    ("   minus duplicate folders (Phase=1 vs Phase=Phase 1 for same UPC)", -n_style, "All_1144_folders", "dup_style"),
    ("   minus same UPC written differently", -n_spell, "All_1144_folders", "dup_spelling"),
    ("2. Unique survey folders", len(kept), "All_1144_folders", "total"),
    ("   plus extra UPCs inside combined folders", n_extra, "Survey_rows", "combined"),
    ("3. Unique UPC + Zone + Phase + Cycle surveys", len(sr), "Survey_rows", "total"),
    ("   minus surveys of UPCs NOT in the 886 master list", -n_out, "Survey_rows", "not_master"),
    ("4. Surveys of the 886 master UPCs", n_in, "Survey_rows", "counted"),
], columns=["Step", "Count", "Where to see rows", "_c"])

# Must match Step 5 numbers
assert len(f) == len(raw) and len(kept) == len(u2) and len(sr) == len(ex) and n_in == len(inl) == detail["phase"].notna().sum()
assert len(kept) + n_extra == len(sr)

FILL = {"dup_style": "F8CBAD", "dup_spelling": "FFE699", "combined": "BDD7EE", "not_master": "F4B084",
        "counted": "C6EFCE", "partial": "D9D2E9", "total": "D9D9D9"}
STATUS_C = {"Removed - duplicate folder (Phase=Phase N / Cycle=Cycle N copy)": "dup_style",
            "Removed - same UPC written differently": "dup_spelling",
            "Counted - master UPC survey": "counted",
            "Counted - combined folder (all UPCs in master)": "combined",
            "Combined folder - partly in master": "partial",
            "Not in 886 master list": "not_master"}
legend = pd.DataFrame([(s, c) for s, c in STATUS_C.items()] + [("Survey_rows: Extra UPC from combined folder = Yes", "combined")],
                      columns=["Colour legend (Status)", "_c"])
status_counts = folders_out.groupby("Status").size().reset_index(name="Folders (of 1144)")

tmp = "/tmp/Bronze_1144_recon.xlsx"
with pd.ExcelWriter(tmp, engine="openpyxl") as xw:
    recon_out.drop(columns="_c").to_excel(xw, sheet_name="Reconciliation", index=False)
    legend.drop(columns="_c").to_excel(xw, sheet_name="Reconciliation", index=False, startrow=len(recon_out) + 3)
    status_counts.to_excel(xw, sheet_name="Reconciliation", index=False, startrow=len(recon_out) + len(legend) + 6)
    folders_out.to_excel(xw, sheet_name="All_1144_folders", index=False)
    surveys_out.to_excel(xw, sheet_name="Survey_rows", index=False)

wb = load_workbook(tmp)
fill = lambda c: PatternFill("solid", start_color=FILL[c], end_color=FILL[c])
for ws in wb.worksheets:
    for cell in ws[1]: cell.font = Font(bold=True)
ws = wb["Reconciliation"]
for i, c in enumerate(recon_out["_c"], start=2):
    for cell in ws[i]: cell.fill = fill(c)
    if c in ("total", "counted"): ws.cell(i, 1).font = ws.cell(i, 2).font = Font(bold=True)
lstart = len(recon_out) + 4
ws.cell(lstart, 1).font = Font(bold=True)
for j, c in enumerate(legend["_c"], start=lstart + 1):
    ws.cell(j, 1).fill = fill(c)
sstart = len(recon_out) + len(legend) + 7
ws.cell(sstart, 1).font = ws.cell(sstart, 2).font = Font(bold=True)
for j, s in enumerate(status_counts["Status"], start=sstart + 1):
    for k in (1, 2): ws.cell(j, k).fill = fill(STATUS_C[s])
ws.column_dimensions["A"].width = 70

for name, df_, scol in [("All_1144_folders", folders_out, "Status"), ("Survey_rows", surveys_out, "Status")]:
    ws = wb[name]
    for i, r in enumerate(df_.itertuples(index=False), start=2):
        c = STATUS_C[getattr(r, scol)]
        if name == "Survey_rows" and c == "counted" and r[df_.columns.get_loc("Extra UPC from combined folder")] == "Yes":
            c = "combined"
        for cell in ws[i]: cell.fill = fill(c)
    ws.freeze_panes = "A2"
wb.save(tmp)
shutil.copy(tmp, OUT_1144)

# Verify saved file
chk = pd.ExcelFile(OUT_1144, engine="openpyxl")
print("Saved:", OUT_1144)
print("Sheets:", chk.sheet_names, "| All_1144_folders rows:", len(chk.parse("All_1144_folders")), "| Survey_rows rows:", len(chk.parse("Survey_rows")))
display(recon_out.drop(columns="_c"))
display(status_counts)