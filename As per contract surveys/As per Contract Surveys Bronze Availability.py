# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # As per Contract Surveys - Bronze Layer Availability
# MAGIC
# MAGIC Takes the "As per contract" surveys from the NSV Activity Tracker zone sheets and checks, zone by zone, whether each Zone / Phase / Cycle / UPC survey exists in the bronze layer (Source=NSV, with NSV_Backup as extra info).

# COMMAND ----------

# DBTITLE 1,Step 0: Install openpyxl
# MAGIC %pip install openpyxl -q

# COMMAND ----------

# DBTITLE 1,Step 1: Open tracker zone sheets and keep As per contract surveys
import re
import pandas as pd

NSV_PATH = "/Workspace/Users/shivam.raj9226@softsensor.ai/As per contract surveys/Input files/NSV_Activity_Tracker_ZoneWise_Standardized (1) (1).xlsx"
ZONE_SHEETS = ['Zone A', 'Zone B', 'Zone C', 'Zone D', 'Zone E']
SCOPE_COL = 'Scope Type (Scope / Additional / De-scoped)'

def norm_num(v):
    """'Phase 1' / 'Phase  3' / 'Cycle 2' / 1.0 -> 1 / 3 / 2 / 1"""
    if pd.isna(v):
        return None
    m = re.search(r'\d+', str(v))
    return int(m.group()) if m else None

def norm_upc(v):
    """N/04006/03001/GJ -> N_04006_03001_GJ"""
    if pd.isna(v):
        return None
    return re.sub(r'[\s/\-]+', '_', str(v).strip().upper()).strip('_') or None

def clean(v):
    return re.sub(r'\s+', ' ', str(v)).strip() if pd.notna(v) else None

nsv_xl = pd.ExcelFile(NSV_PATH)
frames = []
for s in ZONE_SHEETS:
    d = nsv_xl.parse(s, header=1)            # real headers are on row 2 of each zone sheet
    d.columns = [str(c).strip() for c in d.columns]
    d['tracker_sheet'] = s
    d['tracker_row'] = d.index + 3           # Excel row number for easy lookup
    frames.append(d)
tracker = pd.concat(frames, ignore_index=True)
tracker['Scope Type'] = tracker[SCOPE_COL].map(clean)

print('All scope values per zone:')
display(tracker.groupby(['tracker_sheet', 'Scope Type'], dropna=False).size().reset_index(name='rows'))

# 'As per contract' = any scope value starting with 'As per' (As per contract / As per Contract /
# As per original NSV contract / As per NSV Contract / As per RFP in Zone D)
apc = tracker[tracker['Scope Type'].fillna('').str.lower().str.startswith('as per')].copy()
apc['zone']  = apc['Zone'].map(lambda v: clean(v).upper() if pd.notna(v) else None)
apc['phase'] = apc['Phase'].map(norm_num)
apc['cycle'] = apc['Cycle'].map(norm_num)
# a UPC cell can hold several UPCs separated by new lines / commas -> split into a list
split_upcs = lambda v: [u for u in (norm_upc(p) for p in re.split(r'[\n,;]+', str(v))) if u] if pd.notna(v) else []
apc['upc_master_list'] = apc['UPC code as per O&M master'].map(split_upcs)
apc['upc_master'] = apc['upc_master_list'].map(lambda l: l[0] if l else None)
apc['upc_survey'] = apc['UPC as per survey conducted'].map(split_upcs)

print(f"As per contract surveys: {len(apc)}")
display(apc.groupby(['tracker_sheet', 'Scope Type']).size().reset_index(name='surveys'))

# COMMAND ----------

# DBTITLE 1,Step 2: Scan bronze layer (NSV and NSV_Backup) for all zones
BRONZE = "/Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output"

def val(name):
    """'Zone=A/' -> 'A', 'Phase=Phase 1/' -> 'Phase 1'"""
    return name.strip('/').split('=', 1)[-1]

def scan(source):
    recs = []
    for z in dbutils.fs.ls(f"{BRONZE}/Source={source}"):
        for p in dbutils.fs.ls(z.path):
            for c in dbutils.fs.ls(p.path):
                for u in dbutils.fs.ls(c.path):
                    recs.append({'source': source, 'zone': val(z.name).upper(), 'phase': norm_num(val(p.name)),
                                 'cycle': norm_num(val(c.name)), 'upc': norm_upc(val(u.name)),
                                 'bronze_folder': u.path.replace('dbfs:', '')})
    return pd.DataFrame(recs)

bronze_nsv = scan('NSV')
bronze_bkp = scan('NSV_Backup')
print('NSV folders:', len(bronze_nsv), '| NSV_Backup folders:', len(bronze_bkp))
display(bronze_nsv.groupby(['zone', 'phase', 'cycle']).size().reset_index(name='upc_folders'))

# COMMAND ----------

# DBTITLE 1,Step 3: Check each As per contract survey in bronze
def core(u):
    """UPC without state code: N_02001_01009_HR -> N_02001_01009 (catches HR vs DL style differences)"""
    return '_'.join(u.split('_')[:3]) if u else None

nsv_keys = bronze_nsv.groupby(['zone', 'phase', 'cycle', 'upc'])['bronze_folder'].apply(lambda x: ' | '.join(sorted(x))).to_dict()
bkp_keys = set(zip(bronze_bkp.zone, bronze_bkp.phase, bronze_bkp.cycle, bronze_bkp.upc))
nsv_by_upc  = bronze_nsv.groupby('upc').apply(lambda g: sorted({f"{r.zone}/P{r.phase}/C{r.cycle}" for r in g.itertuples()})).to_dict()
nsv_by_core = bronze_nsv.assign(c=bronze_nsv.upc.map(core)).groupby(['zone', 'phase', 'cycle', 'c'])['upc'].apply(lambda x: sorted(set(x))).to_dict()

def check(r):
    cands = list(dict.fromkeys(r.upc_master_list + r.upc_survey))   # master UPCs first, then survey UPCs
    for u in cands:                                    # 1) exact Zone/Phase/Cycle/UPC
        if (r.zone, r.phase, r.cycle, u) in nsv_keys:
            src = 'O&M master UPC' if u in r.upc_master_list else 'UPC as per survey conducted'
            return 'Available', u, nsv_keys[(r.zone, r.phase, r.cycle, u)], f'Matched on {src}', (r.zone, r.phase, r.cycle, u) in bkp_keys
    for u in cands:                                    # 2) same UPC number, different state code
        hit = nsv_by_core.get((r.zone, r.phase, r.cycle, core(u)))
        if hit:
            return 'Available (UPC state code differs)', hit[0], nsv_keys[(r.zone, r.phase, r.cycle, hit[0])], f'Bronze has {", ".join(hit)}', (r.zone, r.phase, r.cycle, hit[0]) in bkp_keys
    for u in cands:                                    # 3) UPC exists but another zone/phase/cycle
        if u in nsv_by_upc:
            return 'Not available (UPC in bronze for other Zone/Phase/Cycle)', u, None, 'Bronze has: ' + ', '.join(nsv_by_upc[u]), False
    if not cands:
        return 'Not available', None, None, 'No UPC in tracker', False
    return 'Not available', cands[0], None, 'UPC not in bronze NSV at all', False

out = apc.apply(lambda r: pd.Series(check(r), index=['Bronze Status', 'Bronze UPC', 'Bronze Folder', 'Bronze Note', 'In NSV_Backup']), axis=1)
res = pd.concat([apc, out], axis=1)

REPORT_COLS = ['tracker_sheet', 'tracker_row', 'Zone', 'Phase', 'Cycle', 'State', 'PIU', 'Project Name',
               'UPC code as per O&M master', 'UPC as per survey conducted', 'NSV Survey Agency Name', 'Scope Type',
               'Bronze Status', 'Bronze UPC', 'In NSV_Backup', 'Bronze Note', 'Bronze Folder']
report = res[REPORT_COLS].rename(columns={'tracker_sheet': 'Tracker Sheet', 'tracker_row': 'Tracker Row'})
print(report['Bronze Status'].value_counts().to_string())
display(report)

# COMMAND ----------

# DBTITLE 1,Step 4: Zone-wise summary and Excel export
summary = (report.assign(Available=report['Bronze Status'].str.startswith('Available'))
           .groupby('Tracker Sheet')
           .agg(As_per_contract_surveys=('Bronze Status', 'size'),
                Available_in_bronze=('Available', 'sum'),
                Also_in_NSV_Backup=('In NSV_Backup', 'sum'))
           .reset_index())
summary['Not_available'] = summary['As_per_contract_surveys'] - summary['Available_in_bronze']
tot = summary.drop(columns='Tracker Sheet').sum()
summary = pd.concat([summary, pd.DataFrame([{'Tracker Sheet': 'GRAND TOTAL', **tot}])], ignore_index=True)
display(summary)

status_by_zone = report.pivot_table(index='Tracker Sheet', columns='Bronze Status', values='Tracker Row', aggfunc='count', fill_value=0)
display(status_by_zone.reset_index())

OUT = "/Workspace/Users/shivam.raj9226@softsensor.ai/As per contract surveys/As_per_contract_bronze_availability.xlsx"
with pd.ExcelWriter(OUT, engine='openpyxl') as w:
    summary.to_excel(w, sheet_name='summary', index=False)
    report.to_excel(w, sheet_name='all_zones', index=False)
    for s in ZONE_SHEETS:
        report[report['Tracker Sheet'] == s].to_excel(w, sheet_name=s, index=False)
    report[~report['Bronze Status'].str.startswith('Available')].to_excel(w, sheet_name='not_available', index=False)
print('Saved:', OUT, '| sheets:', pd.ExcelFile(OUT).sheet_names)