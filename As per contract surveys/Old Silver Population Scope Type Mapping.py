# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Old Silver Population - Scope Type Mapping
# MAGIC
# MAGIC Maps the Scope Type (Scope / Additional / De-scoped) from the NSV Activity Tracker zone sheets onto each RO sheet of the Old silver population file, matched by Zone, UPC, Phase and Cycle.

# COMMAND ----------

# DBTITLE 1,Step 0: Install openpyxl
# MAGIC %pip install openpyxl -q

# COMMAND ----------

# DBTITLE 1,Step 1: Open both Excel files
import pandas as pd

BASE = "/Workspace/Users/shivam.raj9226@softsensor.ai/As per contract surveys/Input files"
NSV_PATH = f"{BASE}/NSV_Activity_Tracker_ZoneWise_Standardized (1) (1).xlsx"
OLD_PATH = f"{BASE}/Old silver population.xlsx"

nsv_xl = pd.ExcelFile(NSV_PATH)
old_xl = pd.ExcelFile(OLD_PATH)

print("NSV tracker sheets:", nsv_xl.sheet_names)
print("Old silver population sheets:", old_xl.sheet_names)

# Read every sheet (header=0) and show shape + columns
nsv_raw = {s: nsv_xl.parse(s) for s in nsv_xl.sheet_names}
old_raw = {s: old_xl.parse(s) for s in old_xl.sheet_names}

for name, sheets in [("NSV", nsv_raw), ("OLD", old_raw)]:
    for s, d in sheets.items():
        print(f"\n[{name}] {s}: {d.shape}")
        print(list(d.columns))

# COMMAND ----------

# DBTITLE 1,Step 2: Build Scope Type lookup from NSV zone sheets
import re

ZONE_SHEETS = ['Zone A', 'Zone B', 'Zone C', 'Zone D', 'Zone E']
RO_SHEETS = ['RO_Gandhinagar', 'RO_Delhi', 'RO_Bhopal', 'RO_Jaipur']
SCOPE_COL = 'Scope Type (Scope / Additional / De-scoped)'

def norm_num(v):
    """'Phase 1' / 'Phase  3' / 1.0 / 'Cycle 2' -> 1 / 3 / 1 / 2"""
    if pd.isna(v):
        return None
    m = re.search(r'\d+', str(v))
    return int(m.group()) if m else None

def norm_upc(v):
    """N/04006/03001/GJ or N_04006_03001_GJ -> N_04006_03001_GJ"""
    if pd.isna(v):
        return None
    s = re.sub(r'[\s/\-]+', '_', str(v).strip().upper())
    return s.strip('_') or None

def clean_scope(v):
    if pd.isna(v):
        return None
    return re.sub(r'\s+', ' ', str(v)).strip() or None

rows = []
for s in ZONE_SHEETS:
    d = nsv_xl.parse(s, header=1)  # row 2 of the sheet holds the real headers
    d.columns = [str(c).strip() for c in d.columns]
    for _, r in d.iterrows():
        zone, ph, cy = str(r['Zone']).strip().upper() if pd.notna(r['Zone']) else None, norm_num(r['Phase']), norm_num(r['Cycle'])
        scope = clean_scope(r[SCOPE_COL])
        # match on both UPC columns; 'UPC as per survey conducted' may hold several UPCs on separate lines
        for src, col in [('O&M master', 'UPC code as per O&M master'), ('survey conducted', 'UPC as per survey conducted')]:
            val = r[col]
            if pd.isna(val):
                continue
            for part in re.split(r'[\n,;]+', str(val)):
                u = norm_upc(part)
                if u:
                    rows.append({'zone': zone, 'phase': ph, 'cycle': cy, 'upc': u, 'scope': scope,
                                 'upc_source': src, 'nsv_sheet': s})

nsv_lookup = pd.DataFrame(rows)
print('NSV lookup rows:', len(nsv_lookup))
display(nsv_lookup.groupby(['nsv_sheet', 'scope'], dropna=False).size().reset_index(name='rows'))

# COMMAND ----------

# DBTITLE 1,Step 3: Map Scope Type onto each RO sheet (after Comments)
def lookup_scope(zone, phase, cycle, upc):
    """Return (scope, match_note). O&M master UPC match is preferred over 'UPC as per survey conducted'."""
    if zone is None or phase is None or cycle is None or upc is None:
        return None, 'No Zone/Phase/Cycle in silver file'
    m = nsv_lookup[(nsv_lookup.zone == zone) & (nsv_lookup.phase == phase) &
                   (nsv_lookup.cycle == cycle) & (nsv_lookup.upc == upc)]
    if m.empty:
        return None, 'Not found in NSV tracker'
    pref = m[m.upc_source == 'O&M master']
    m = pref if not pref.empty else m
    vals = sorted({v for v in m.scope if v})
    note = f"Matched on UPC {m.upc_source.iloc[0]}" + (' (multiple scope values)' if len(vals) > 1 else '')
    return (' | '.join(vals) if vals else 'Blank in tracker'), note

result = {}
for s in RO_SHEETS:
    d = old_raw[s].copy()
    out = [lookup_scope(str(r['Zone']).strip().upper() if pd.notna(r['Zone']) else None,
                        norm_num(r['Phase']), norm_num(r['Cycle']), norm_upc(r['UPC']))
           for _, r in d.iterrows()]
    pos = d.columns.get_loc('Comments') + 1
    d.insert(pos, 'Scope Type', [o[0] for o in out])
    d.insert(pos + 1, 'Scope Match Note', [o[1] for o in out])
    result[s] = d
    print(f"\n{s}: {len(d)} rows")
    print(d['Scope Match Note'].value_counts().to_string())
    print(d['Scope Type'].value_counts(dropna=False).to_string())

display(result['RO_Gandhinagar'][['Zone', 'Phase', 'Cycle', 'UPC', 'Status', 'Comments', 'Scope Type', 'Scope Match Note']].head(10))

# COMMAND ----------

# DBTITLE 1,Step 3b: Combined Scope Type table for all ROs
SHOW_COLS = ['Zone', 'Phase', 'Cycle', 'UPC', 'Status', 'Comments', 'Scope Type', 'Scope Match Note']

# One table with every row of all 4 RO sheets, including the full Comments text
all_ro = pd.concat(
    [result[s][SHOW_COLS].assign(RO=s.replace('RO_', '')) for s in RO_SHEETS],
    ignore_index=True
)[['RO'] + SHOW_COLS]

print('Rows per RO:', all_ro['RO'].value_counts(sort=False).to_dict(), '| Total:', len(all_ro))
display(all_ro)

# Per-RO tables (same columns as Step 3 output)
for s in RO_SHEETS:
    print(f"\n===== {s} ({len(result[s])} rows) =====")
    display(result[s][SHOW_COLS])

# COMMAND ----------

# DBTITLE 1,Step 4: Export updated Old silver population file
OUT_PATH = "/Workspace/Users/shivam.raj9226@softsensor.ai/As per contract surveys/Old silver population_with_scope.xlsx"

# Keep every sheet in the original order; only the 4 RO sheets get the new columns
with pd.ExcelWriter(OUT_PATH, engine='openpyxl') as w:
    for s in old_xl.sheet_names:
        (result[s] if s in result else old_raw[s]).to_excel(w, sheet_name=s, index=False)

# Verify by reading the written file back
chk = pd.ExcelFile(OUT_PATH)
print('Sheets written:', chk.sheet_names)
for s in RO_SHEETS:
    d = chk.parse(s)
    cols = list(d.columns)
    i = cols.index('Comments')
    print(f"{s}: {len(d)} rows | columns after Comments: {cols[i+1:i+3]} | Scope filled: {d['Scope Type'].notna().sum()}")