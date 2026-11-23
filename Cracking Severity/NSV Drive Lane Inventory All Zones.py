# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # NSV Drive Lane Inventory - All Zones
# MAGIC
# MAGIC Reading all Drive Lane inventory Excel files (Zone A–E), checking crack classification per UPC across all zones.

# COMMAND ----------

# DBTITLE 1,Step 1 – Install Required Library
# MAGIC %md
# MAGIC ### Step 1 – Install Required Library
# MAGIC
# MAGIC **Purpose:**  
# MAGIC Installs a supporting library (`openpyxl`) required to read Excel (.xlsx) files within the notebook environment.
# MAGIC
# MAGIC **Important:**  
# MAGIC *This is a one-time setup step. It must run before any Excel file can be loaded in the subsequent steps.*

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install openpyxl -q

# COMMAND ----------

# DBTITLE 1,Step 2 – Load Drive Lane Inventory Data for All Zones
# MAGIC %md
# MAGIC ### Step 2 – Load Drive Lane Inventory Data for All Zones (A–E)
# MAGIC
# MAGIC **Purpose:**  
# MAGIC Reads the NSV Drive Lane inventory Excel files for all five zones (A, B, C, D, E) and combines them into a single consolidated dataset.
# MAGIC
# MAGIC **Input:**  
# MAGIC * Five Excel files located in the `Drive_Lane` folder, one per zone:
# MAGIC   * `NSV_Drive_Lane_Inventory_Zone_A_v5 3.xlsx`
# MAGIC   * `NSV_Drive_Lane_Inventory_Zone_B_v5 3.xlsx`
# MAGIC   * `NSV_Drive_Lane_Inventory_Zone_C_v5 1.xlsx`
# MAGIC   * `NSV_Drive_Lane_Inventory_Zone_D_v5 3.xlsx`
# MAGIC   * `NSV_Drive_Lane_Inventory_Zone_E_v5 3.xlsx`
# MAGIC
# MAGIC **Process:**  
# MAGIC * Each file is read individually, and a `Zone` column (A/B/C/D/E) is added based on the filename.
# MAGIC * All five zone datasets are then merged into one combined dataset.
# MAGIC
# MAGIC **Output:**  
# MAGIC A single consolidated dataset (`df`) containing all Drive Lane inventory records across all zones, with each record tagged by its zone. A preview of the first 10 records and summary counts (total rows, columns, unique UPCs) are displayed.
# MAGIC
# MAGIC **Important:**  
# MAGIC *The number of columns may vary across zone files (e.g., Zone B has 26 columns while Zone D has 45). The combined dataset accommodates all columns, with missing values where a zone file does not contain a particular column.*

# COMMAND ----------

# DBTITLE 1,Read all Zone files
import pandas as pd
import re
import os

folder = "/Workspace/Users/manu.jain9244@softsensor.ai/Silver_Data_RIS_Comparision/Drive_Lane"

files = [
    "NSV_Drive_Lane_Inventory_Zone_A_v5 3.xlsx",
    "NSV_Drive_Lane_Inventory_Zone_B_v5 3.xlsx",
    "NSV_Drive_Lane_Inventory_Zone_C_v5 1.xlsx",
    "NSV_Drive_Lane_Inventory_Zone_D_v5 3.xlsx",
    "NSV_Drive_Lane_Inventory_Zone_E_v5 3.xlsx",
]

all_dfs = []
for f in files:
    path = os.path.join(folder, f)
    # Extract zone letter from filename (Zone_A -> A, Zone_B -> B, etc.)
    zone = re.search(r"Zone_(\w)", f).group(1)
    tmp = pd.read_excel(path, sheet_name=0)
    tmp["Zone"] = zone
    print(f"Zone {zone}: {len(tmp)} rows, {tmp.shape[1]} columns")
    all_dfs.append(tmp)

# Combine all zones
df = pd.concat(all_dfs, ignore_index=True)
print(f"\nCombined: {len(df)} rows x {df.shape[1]} columns")
print(f"Zones: {sorted(df['Zone'].unique())}")
print(f"Unique UPCs: {df['UPC'].nunique()}")
display(df.head(10))

# COMMAND ----------

# DBTITLE 1,Step 3 – Check Crack Classification Presence per UPC and Zone
# MAGIC %md
# MAGIC ### Step 3 – Check Crack Classification Presence per UPC and Zone
# MAGIC
# MAGIC **Purpose:**  
# MAGIC Verifies whether each UPC (road section) in every zone has the required crack classification data sheets present in its inventory file. This is a key data completeness check.
# MAGIC
# MAGIC **Input:**  
# MAGIC Combined Drive Lane inventory dataset from Step 2.
# MAGIC
# MAGIC **Process:**  
# MAGIC * For each inventory record, the sheet names are examined for the presence of four required crack classification types:
# MAGIC   * Longitudinal & Unclassified
# MAGIC   * Alligator & Multiple
# MAGIC   * Transversal Crack
# MAGIC   * Final Crack Rating
# MAGIC * Results are then summarised at the UPC + Zone level — if *any* file under a UPC contains a given classification, it is marked as present.
# MAGIC * A flag (`All_4_Present`) indicates whether all four classifications are available for a given UPC-Zone combination.
# MAGIC
# MAGIC **Output:**  
# MAGIC A summary table (`upc_crack`) showing each UPC-Zone combination, which crack classifications were found, and whether all four are present. Key counts are printed:
# MAGIC * Total UPC-Zone combinations
# MAGIC * UPCs with all 4 classifications
# MAGIC * UPCs with at least 1 classification
# MAGIC * UPCs with no crack classification at all
# MAGIC
# MAGIC **Important:**  
# MAGIC *All four crack classification sheets are expected for complete data. UPCs missing any classification may require follow-up with the survey or data processing team.*

# COMMAND ----------

# DBTITLE 1,Crack classification check per UPC + Zone
# Crack classification sheets to check
crack_classes = [
    "Longitudinal & Unclassified",
    "Alligator & Multiple",
    "Transversal Crack",
    "Final Crack Rating",
    "Cracks (MDB)"
]

# Sheet columns
sheet_cols = [c for c in df.columns if c.startswith("Sheet ")]

# For each row, check which crack classifications are present
for cls in crack_classes:
    df[cls] = df[sheet_cols].apply(lambda row: row.eq(cls).any(), axis=1)

# Summarize per UPC + Zone: for each classification, True if ANY file under that UPC has it
upc_crack = df.groupby(["UPC", "Zone"])[crack_classes].any().reset_index()

# Add a column showing which classifications are present
upc_crack["Classifications_Found"] = upc_crack[crack_classes].apply(
    lambda row: ", ".join([c for c in crack_classes if row[c]]) if row.any() else "None",
    axis=1
)

# Add a column for whether ALL 4 are present
upc_crack["All_4_Present"] = upc_crack[crack_classes].all(axis=1)

print(f"Total unique UPC-Zone combos: {len(upc_crack)}")
print(f"UPCs with ALL 4 crack classifications: {upc_crack['All_4_Present'].sum()}")
print(f"UPCs with at least 1 crack classification: {(upc_crack[crack_classes].any(axis=1)).sum()}")
print(f"UPCs with NO crack classification: {(~upc_crack[crack_classes].any(axis=1)).sum()}")
print()
display(upc_crack)

# COMMAND ----------

# DBTITLE 1,Step 4 – Zone-wise Summary of UPCs with Crack Classifications
# MAGIC %md
# MAGIC ### Step 4 – Zone-wise Summary of UPCs with Crack Classifications
# MAGIC
# MAGIC **Purpose:**  
# MAGIC Provides a zone-level breakdown showing how many UPCs have crack classification data present, helping identify which zones may need attention.
# MAGIC
# MAGIC **Input:**  
# MAGIC Crack classification summary from Step 3 (`upc_crack`).
# MAGIC
# MAGIC **Process:**  
# MAGIC * Filters UPC-Zone records that have at least one crack classification present.
# MAGIC * Counts the total UPCs per zone alongside the count of UPCs with crack classifications.
# MAGIC * A total row is appended for an overall summary.
# MAGIC
# MAGIC **Output:**  
# MAGIC * A zone-wise summary table showing `Total_UPC` and `Count` (UPCs with crack classifications) per zone.
# MAGIC * A detailed list of individual UPC-Zone records along with the specific classifications found.
# MAGIC
# MAGIC **Dependency:**  
# MAGIC *Depends on the crack classification check completed in Step 3.*

# COMMAND ----------

# DBTITLE 1,UPCs with no crack classification (with Zone)
# Filter UPCs with NO crack classification
upc_no_crack = upc_crack[upc_crack["Classifications_Found"] != "None"][["UPC", "Zone", "Classifications_Found"]].reset_index(drop=True)

print(f"UPCs with NO crack classification: {len(upc_no_crack)}")

# Total UPCs per zone
total_upc_zone = upc_crack["Zone"].value_counts().sort_index().reset_index()
total_upc_zone.columns = ["Zone", "Total_UPC"]

# No-crack count per zone
no_crack_zone = upc_no_crack["Zone"].value_counts().sort_index().reset_index()
no_crack_zone.columns = ["Zone", "Count"]

# Merge both
zone_counts = pd.merge(total_upc_zone, no_crack_zone, on="Zone", how="left").fillna(0)
zone_counts["Count"] = zone_counts["Count"].astype(int)

# Add total row
total_row = pd.DataFrame([{"Zone": "Total", "Total_UPC": zone_counts["Total_UPC"].sum(), "Count": zone_counts["Count"].sum()}])
zone_counts = pd.concat([zone_counts, total_row], ignore_index=True)
zone_counts["Total_UPC"] = zone_counts["Total_UPC"].astype(int)
zone_counts["Count"] = zone_counts["Count"].astype(int)
print("\nBreakdown by Zone:")
display(zone_counts)

print(f"\nDetailed list:")
display(upc_no_crack)

# COMMAND ----------

# DBTITLE 1,Step 5 – Validation: Cross-Check Zone-wise Crack Classification Counts
# MAGIC %md
# MAGIC ### Step 5 – Validation: Cross-Check Zone-wise Crack Classification Counts
# MAGIC
# MAGIC **Purpose:**  
# MAGIC Independently verifies the zone-wise summary produced in Step 4 to ensure the numbers are accurate before sharing with stakeholders.
# MAGIC
# MAGIC **Input:**  
# MAGIC Crack classification summary from Step 3 (`upc_crack`) and the combined inventory dataset (`df`).
# MAGIC
# MAGIC **Process:**  
# MAGIC * Recomputes Total UPCs per zone and the count of UPCs with crack classifications directly from the source data.
# MAGIC * Compares the recomputed values against the Step 4 output row by row.
# MAGIC * Flags any mismatches.
# MAGIC
# MAGIC **Output:**  
# MAGIC A validation report confirming whether all counts match, or highlighting specific discrepancies.
# MAGIC
# MAGIC **Important:**  
# MAGIC *This is a data quality gate — the Step 4 summary should not be shared externally until this validation passes.*

# COMMAND ----------

# DBTITLE 1,Validation – Cross-check zone counts
# ── VALIDATION: Cross-check the zone-wise summary from Step 4 ──

print("=" * 65)
print("VALIDATION REPORT: Zone-wise Crack Classification Counts")
print("=" * 65)

# ── 1. Flag the labelling issue in Step 4 ──
print("\n⚠️  LABELLING NOTE (Step 4 code):")
print("   The variable 'upc_no_crack' filters for Classifications_Found != 'None',")
print("   which selects UPCs WITH crack classifications (not without).")
print("   The print statement 'UPCs with NO crack classification: 127' is misleading.")
print("   The 'Count' column in the zone table actually shows UPCs WITH classifications.")
print()

# ── 2. Recompute from source (upc_crack) independently ──
has_any = upc_crack[upc_crack["Classifications_Found"] != "None"]
has_none = upc_crack[upc_crack["Classifications_Found"] == "None"]

print("── Independent recount from upc_crack ──")
print(f"   Total UPC-Zone combos       : {len(upc_crack)}")
print(f"   WITH  crack classification  : {len(has_any)}")
print(f"   WITHOUT crack classification: {len(has_none)}")
print(f"   Sum check (WITH + WITHOUT)  : {len(has_any)} + {len(has_none)} = {len(has_any) + len(has_none)}  "
      f"{'✅ matches total' if len(has_any) + len(has_none) == len(upc_crack) else '❌ MISMATCH'}")
print()

# ── 3. Per-zone recount ──
recount_total = upc_crack.groupby("Zone").size().reset_index(name="Total_UPC_recount")
recount_with  = has_any.groupby("Zone").size().reset_index(name="Count_recount")
validation = pd.merge(recount_total, recount_with, on="Zone", how="left").fillna(0)
validation["Count_recount"] = validation["Count_recount"].astype(int)

# Compare against the Step 4 table (zone_counts, excluding the Total row)
step4 = zone_counts[zone_counts["Zone"] != "Total"].copy()
check = pd.merge(step4, validation, on="Zone", how="outer")
check["Total_Match"] = check["Total_UPC"] == check["Total_UPC_recount"]
check["Count_Match"] = check["Count"] == check["Count_recount"]

print("── Per-zone comparison (Step 4 output vs. independent recount) ──")
display(check)

# ── 4. Overall verdict ──
all_ok = check["Total_Match"].all() and check["Count_Match"].all()
print()
if all_ok:
    print("✅ VALIDATION PASSED: All zone-level counts match the independent recount.")
    print("   The numbers are correct and safe to share.")
    print("   Just note: the 'Count' column represents UPCs WITH crack classifications")
    print("   (not 'without', despite the variable name in Step 4).")
else:
    mismatched = check[~(check["Total_Match"] & check["Count_Match"])]
    print("❌ VALIDATION FAILED: Mismatches found in the following zones:")
    display(mismatched)

# COMMAND ----------

# DBTITLE 1,Step 6 – Check Severity Column in Crack Classification Sheets
# MAGIC %md
# MAGIC ### Step 6 – Check "Severity" Column in Crack Classification Sheets
# MAGIC
# MAGIC **Purpose:**  
# MAGIC For the 127 UPCs that have crack classification sheets, opens the source LCMS Excel files and checks whether the "Severity" column is present in each crack classification sheet.
# MAGIC
# MAGIC **Input:**  
# MAGIC * The 127 UPCs with crack classifications (`upc_no_crack` from Step 4).
# MAGIC * The combined inventory dataset (`df`) which contains `Folder_Path` and `File_Name` for each file.
# MAGIC
# MAGIC **Process:**  
# MAGIC * Identifies the source LCMS files for the 127 UPCs.
# MAGIC * Opens each file and reads the crack classification sheet headers (Longitudinal & Unclassified, Alligator & Multiple, Transversal Crack, Final Crack Rating).
# MAGIC * Checks if "Severity" column is present in each sheet.
# MAGIC
# MAGIC **Output:**  
# MAGIC A zone-wise and UPC-level summary showing which crack sheets have the "Severity" column and which do not.

# COMMAND ----------

# DBTITLE 1,Severity check – Cracks (MDB) priority, fallback Final Crack Rating
# ── Severity column check for 127 UPCs ──
# Uses Classifications_Found (from cell 9) to decide which sheet to inspect:
#   • If "Cracks (MDB)" is listed in Classifications_Found → check Severity there
#   • If "Cracks (MDB)" is NOT listed → fallback to "Final Crack Rating"
# Severity presence is verified from the Sheet_Columns metadata tab.

import os, re
import pandas as pd

# ── 1. Load Sheet_Columns tab from every zone file ──
all_sc = []
for f in files:
    zone = re.search(r"Zone_(\w)", f).group(1)
    path = os.path.join(folder, f)
    sc = pd.read_excel(path, sheet_name="Sheet_Columns")
    sc["Zone"] = zone
    all_sc.append(sc)

sc_df = pd.concat(all_sc, ignore_index=True)

# Extract sheet label (last segment of the Sheet_Name path)
sc_df["Sheet_Label"] = sc_df["Sheet_Name"].apply(
    lambda x: str(x).rsplit("/", 1)[-1] if pd.notna(x) else x
)

# Flag rows whose Columns field contains SEVERITY (case-insensitive)
sc_df["Has_Severity"] = sc_df["Columns"].str.contains(
    r'\bSEVERITY\b', case=False, na=False
)

# ── 2. For each of 127 UPCs, use Classifications_Found to pick sheet ──
results = []

for row in upc_no_crack.itertuples():
    upc_name = row.UPC
    zone     = row.Zone
    clf      = str(row.Classifications_Found)          # e.g. "Longitudinal & Unclassified, Alligator & Multiple, ..."
    sheets_listed = [s.strip() for s in clf.split(",")]  # split into individual sheet names

    # Decide which sheet to check based on Classifications_Found
    if "Cracks (MDB)" in sheets_listed:
        check_sheet = "Cracks (MDB)"
        fallback    = False
    elif "Final Crack Rating" in sheets_listed:
        check_sheet = "Final Crack Rating"
        fallback    = True
    else:
        check_sheet = None
        fallback    = None

    if check_sheet:
        # Look up Sheet_Columns for this UPC + sheet
        match = sc_df[
            (sc_df["UPC"] == upc_name) &
            (sc_df["Zone"] == zone) &
            (sc_df["Sheet_Label"] == check_sheet)
        ]
        has_sev = match["Has_Severity"].any() if len(match) > 0 else False
    else:
        has_sev = False
        check_sheet = "Neither in Classifications"

    results.append({
        "Zone": zone,
        "UPC": upc_name,
        "Classifications_Found": clf,
        "Checked_Sheet": check_sheet,
        "Severity_Present": has_sev,
        "Fallback_Used": fallback,
    })

sev_check = pd.DataFrame(results)

# ── 3. Summary ──
print("=" * 70)
print("SEVERITY CHECK — Based on Classifications_Found per UPC")
print("Priority: Cracks (MDB) → Fallback: Final Crack Rating")
print("=" * 70)
print(f"Total UPCs checked             : {len(sev_check)}")
print(f"Severity FOUND                 : {(sev_check['Severity_Present'] == True).sum()}")
print(f"Severity NOT found             : {(sev_check['Severity_Present'] == False).sum()}")
print(f"Checked via Cracks (MDB)       : {(sev_check['Checked_Sheet'] == 'Cracks (MDB)').sum()}")
print(f"Fallback to Final Crack Rating : {(sev_check['Checked_Sheet'] == 'Final Crack Rating').sum()}")
print(f"Neither sheet in Classifications: {(sev_check['Checked_Sheet'] == 'Neither in Classifications').sum()}")

# Zone-wise breakdown
zone_sev = sev_check.groupby("Zone").agg(
    Total_UPCs=("UPC", "count"),
    Severity_Found=("Severity_Present", "sum"),
    Via_Cracks_MDB=("Checked_Sheet", lambda x: (x == "Cracks (MDB)").sum()),
    Via_Final_Crack_Rating=("Checked_Sheet", lambda x: (x == "Final Crack Rating").sum()),
    Neither=("Checked_Sheet", lambda x: (x == "Neither in Classifications").sum()),
).reset_index()
zone_sev["Severity_Found"] = zone_sev["Severity_Found"].astype(int)

print("\nZone-wise breakdown:")
display(zone_sev)

print("\nDetailed UPC-level results:")
display(sev_check)

# COMMAND ----------

# DBTITLE 1,Load master_upc_project with contractor name mapping
import pandas as pd

# Load master_upc_project
master_path = "/Workspace/Users/shivam.raj9226@softsensor.ai/Cracking Severity/Project/master_upc_project_202610071032.csv"
master_upc_project = pd.read_csv(master_path)

# Load master_contractor for name mapping
contractor_path = "/Workspace/Users/shivam.raj9226@softsensor.ai/Cracking Severity/Project/master_contractor_202610071037.csv"
master_contractor = pd.read_csv(contractor_path)

# Map contractor_id to contractor_name
merged = master_upc_project[['upc', 'contractor_id']].merge(
    master_contractor[['id', 'contractor_name']].rename(columns={'id': 'contractor_id'}),
    on='contractor_id',
    how='left'
)

# Display the result
result = merged[['upc', 'contractor_id', 'contractor_name']]
print(f"Rows: {len(result)}, Matched contractors: {result['contractor_name'].notna().sum()}, Unmatched: {result['contractor_name'].isna().sum()}")
display(result)

# COMMAND ----------

# DBTITLE 1,Contractor mapping for 127 UPCs with crack classifications
# Map the 127 UPCs (from upc_no_crack) to their contractors (from cell 14 result)
mapped_127 = upc_no_crack.merge(
    result,
    left_on='UPC',
    right_on='upc',
    how='left'
).drop(columns=['upc'])

print(f"Total UPCs: {len(mapped_127)}")
print(f"Matched to a contractor: {mapped_127['contractor_name'].notna().sum()}")
print(f"No contractor found: {mapped_127['contractor_name'].isna().sum()}")

# Contractor-wise count
contractor_summary = mapped_127.groupby('contractor_name').agg(
    UPC_Count=('UPC', 'count'),
    UPCs=('UPC', lambda x: ', '.join(sorted(x)))
).sort_values('UPC_Count', ascending=False).reset_index()

print(f"\nUnique contractors for these 127 UPCs: {len(contractor_summary)}")
print("\nContractor-wise breakdown:")
display(contractor_summary)

print("\nFull detail — 127 UPCs with contractor info:")
display(mapped_127[['Zone', 'UPC', 'Classifications_Found', 'contractor_id', 'contractor_name']])