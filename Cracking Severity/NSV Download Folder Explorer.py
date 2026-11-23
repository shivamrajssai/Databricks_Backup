# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # NSV Download Folder Explorer
# MAGIC
# MAGIC Explore the NSV download volume by Zone, Phase, and Cycle. Lists all UPCs with their folder structure and latest file timestamps.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install openpyxl

# COMMAND ----------

# DBTITLE 1,Widget inputs - Zone, Phase, Cycle, UPC
# Create dropdown widgets for Zone, Phase, Cycle and UPC text input
dbutils.widgets.dropdown("zone", "A", ["A", "B", "C", "D", "E"], "Zone")
dbutils.widgets.dropdown("phase", "1", ["1", "2", "3"], "Phase")
dbutils.widgets.dropdown("cycle", "3", ["1", "2", "3"], "Cycle")
dbutils.widgets.text("upc", "N/02006/07001/JK", "UPC")

# Read widget values
zone = dbutils.widgets.get("zone")
phase = dbutils.widgets.get("phase")
cycle = dbutils.widgets.get("cycle")
upc = dbutils.widgets.get("upc")

# Convert UPC from slash format (N/02006/07001/JK) to folder format (N_02006_07001_JK)
upc_folder_name = upc.replace("/", "_")

print(f"Selected: Zone {zone}, Phase {phase}, Cycle {cycle}, UPC {upc}")
print(f"Folder name: {upc_folder_name}")

# COMMAND ----------

# DBTITLE 1,Explore UPC backup folder structure
import os
import pandas as pd
from datetime import datetime

BASE_PATH = "/Volumes/nhai_dhms/nsv/nsv_downloads"

# Build full path to the specific UPC backup folder
backup_path = f"{BASE_PATH}/Zone_{zone}/Phase_{phase}/Cycle_{cycle}/{upc_folder_name}/backup"
print(f"Backup Path: {backup_path}\n")

if not os.path.exists(backup_path):
    print(f"ERROR: Path does not exist \u2014 {backup_path}")
    print("Please check your Zone / Phase / Cycle / UPC selection.")
else:
    # Print full folder tree for backup only
    print(f"Backup folder structure for UPC: {upc} (Zone {zone}, Phase {phase}, Cycle {cycle})")
    print("=" * 100)

    file_details = []
    for root, dirs, files in os.walk(backup_path):
        # Print directory tree
        level = root.replace(backup_path, "").count(os.sep)
        indent = "  " * level
        folder_name = os.path.basename(root)
        print(f"{indent}\U0001f4c1 {folder_name}/")

        sub_indent = "  " * (level + 1)
        for f in sorted(files):
            fpath = os.path.join(root, f)
            size_kb = os.path.getsize(fpath) / 1024
            mtime = os.path.getmtime(fpath)
            dt = datetime.fromtimestamp(mtime)
            rel_path = fpath.replace(backup_path + "/", "")
            print(f"{sub_indent}\U0001f4c4 {f}  ({size_kb:.1f} KB, {dt.strftime('%Y-%m-%d %H:%M')})")

            file_details.append({
                "File": f,
                "Relative_Path": rel_path,
                "Size_KB": round(size_kb, 1),
                "Year": dt.year,
                "Month": dt.month,
                "Day": dt.day,
                "Last_Modified": dt.strftime("%Y-%m-%d %H:%M:%S")
            })

    print("\n" + "=" * 100)
    df_files = pd.DataFrame(file_details)
    if len(df_files) > 0:
        latest_row = df_files.loc[df_files['Last_Modified'].idxmax()]
        print(f"\nTotal files: {len(df_files)}")
        print(f"Latest file: {latest_row['File']} (modified {latest_row['Last_Modified']})")
        print(f"Latest Year: {latest_row['Year']}, Month: {latest_row['Month']}, Day: {latest_row['Day']}")
        print()
        display(df_files)
    else:
        print("No files found in this UPC folder.")

# COMMAND ----------

# DBTITLE 1,Check crack classification sheets in L1 lane files
import os
import pandas as pd
import openpyxl

BASE_PATH = "/Volumes/nhai_dhms/nsv/nsv_downloads"

# Keywords to match any crack-related sheet (case-insensitive)
CRACK_KEYWORDS = [
    "crack", "longitudinal", "unclassified", "alligator", "multiple",
    "transversal", "faulting", "spalled", "mdb", "crack rating"
]

def is_crack_sheet(sheet_name):
    """Return True if sheet name matches any crack-related keyword."""
    name_lower = sheet_name.lower().strip()
    return any(kw in name_lower for kw in CRACK_KEYWORDS)

# Build path to backup folder
backup_path = f"{BASE_PATH}/Zone_{zone}/Phase_{phase}/Cycle_{cycle}/{upc_folder_name}/backup"
print(f"Checking: {backup_path}\n")

if not os.path.exists(backup_path):
    print(f"ERROR: Path does not exist -- {backup_path}")
    print("Please check your Zone / Phase / Cycle / UPC selection.")
else:
    # Find only L1 .xlsx files in backup
    xlsx_files = []
    for root, dirs, files in os.walk(backup_path):
        for f in sorted(files):
            if f.endswith(".xlsx") and "L1" in f:
                xlsx_files.append(os.path.join(root, f))

    print(f"Total L1 Excel files found: {len(xlsx_files)}")
    print(f"Crack keywords searched: {CRACK_KEYWORDS}\n")
    print("=" * 120)

    results = []
    for fpath in xlsx_files:
        rel_path = fpath.replace(backup_path + "/", "")
        fname = os.path.basename(fpath)
        subfolder = os.path.dirname(rel_path)

        # Read sheet names from the Excel file
        try:
            wb = openpyxl.load_workbook(fpath, read_only=True, data_only=True)
            sheet_names = wb.sheetnames
            wb.close()
        except Exception as e:
            sheet_names = []
            print(f"  ERROR reading {rel_path}: {e}")

        # Find all crack-related sheets by keyword matching
        found_crack_sheets = [s for s in sheet_names if is_crack_sheet(s)]

        results.append({
            "Subfolder": subfolder,
            "File": fname,
            "Total_Sheets": len(sheet_names),
            "All_Sheets": ", ".join(sheet_names),
            "Crack_Sheets_Found": len(found_crack_sheets),
            "Crack_Sheet_Names": ", ".join(found_crack_sheets) if found_crack_sheets else "None"
        })

    df_check = pd.DataFrame(results)

    # Summary
    print(f"\nUPC: {upc} | Zone {zone}, Phase {phase}, Cycle {cycle}")
    print(f"L1 files checked: {len(df_check)}")
    print("=" * 120)

    for _, r in df_check.iterrows():
        if r["Crack_Sheets_Found"] > 0:
            print(f"\n{r['Subfolder']}/{r['File']} -- {r['Crack_Sheets_Found']} crack sheet(s) FOUND:")
            print(f"  {r['Crack_Sheet_Names']}")
        else:
            print(f"\n{r['Subfolder']}/{r['File']} -- No crack sheets found")
            print(f"  (sheets present: {r['All_Sheets']})")

    print("\n" + "=" * 120)
    display(df_check)

# COMMAND ----------

# DBTITLE 1,Open target crack sheets from qualifying L1 files
# Only look for these 6 specific sheets
TARGET_SHEETS = [
    "Longitudinal & Unclassified",
    "Alligator & Multiple",
    "Transversal Crack",
    "Final Crack Rating",
    "Cracks (MDB)",
    "Cracks"
]

# Gate: file must have at least one of the 3 classification sheets
GATE_SHEETS = ["Longitudinal & Unclassified", "Alligator & Multiple", "Transversal Crack"]

crack_sheet_data = {}  # {key: DataFrame}

for _, row in df_check.iterrows():
    if row["Crack_Sheets_Found"] == 0:
        continue

    found_sheets = [s.strip() for s in row["Crack_Sheet_Names"].split(",")]
    # Check if any of the gate classification sheets are present
    has_classification = any(s in GATE_SHEETS for s in found_sheets)

    if not has_classification:
        print(f"SKIP: {row['Subfolder']}/{row['File']} -- no classification sheets found: {found_sheets}")
        continue

    # File qualifies -- load only the 6 target sheets that exist in this file
    fpath = os.path.join(backup_path, row["Subfolder"], row["File"])
    sheets_to_load = [s for s in TARGET_SHEETS if s in found_sheets]
    print(f"Opening: {row['Subfolder']}/{row['File']}")
    for sheet_name in sheets_to_load:
        try:
            df_sheet = pd.read_excel(fpath, sheet_name=sheet_name)
            key = f"{row['Subfolder']}/{row['File']} -> {sheet_name}"
            crack_sheet_data[key] = df_sheet
            print(f"  Loaded sheet '{sheet_name}': {df_sheet.shape[0]} rows x {df_sheet.shape[1]} columns")
        except Exception as e:
            print(f"  ERROR loading sheet '{sheet_name}': {e}")

print(f"\nTotal crack sheets loaded: {len(crack_sheet_data)}")

# COMMAND ----------

# DBTITLE 1,Extract schema and save to output Excel
# Extract full schema for each loaded crack sheet
# One output sheet per crack sheet name (no splitting)
import os
import pandas as pd

# Create output folder
output_dir = "/Workspace/Users/shivam.raj9226@softsensor.ai/Cracking Severity/Project/output"
os.makedirs(output_dir, exist_ok=True)

# Build output file name: Zone_Phase_Cycle_UPC.xlsx
output_filename = f"Zone_{zone}_Phase_{phase}_Cycle_{cycle}_{upc_folder_name}.xlsx"
output_path = os.path.join(output_dir, output_filename)

schema_dfs = {}  # {sheet_name: schema_df}

for key, df_sheet in crack_sheet_data.items():
    sheet_name = key.split(" -> ")[1]
    # Use full name, truncate to 31 chars for Excel limit
    output_label = sheet_name[:31]

    print("=" * 120)
    print(f"SHEET: {key}")
    print(f"Total: {df_sheet.shape[0]} rows x {df_sheet.shape[1]} columns")
    print("-" * 120)

    columns = list(df_sheet.columns)
    schema_rows = []
    for c in columns:
        non_null = int(df_sheet[c].notna().sum())
        null_ct = int(df_sheet[c].isna().sum())
        nn = df_sheet[c].dropna()
        sample = str(nn.iloc[0])[:80] if len(nn) > 0 else "(all null)"
        schema_rows.append({
            "Column_Name": str(c),
            "Dtype": str(df_sheet[c].dtype),
            "Non_Null_Count": non_null,
            "Null_Count": null_ct,
            "Sample_Value": sample
        })

    df_schema = pd.DataFrame(schema_rows)
    schema_dfs[output_label] = df_schema

    print(f"Output sheet: '{output_label}'")
    display(df_schema)
    print()

# Write all schemas to a single Excel file
if schema_dfs:
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sname, df_schema in schema_dfs.items():
            df_schema.to_excel(writer, sheet_name=sname, index=False)

    print("=" * 120)
    print(f"Output saved to: {output_path}")
    print(f"Sheets written ({len(schema_dfs)}): {list(schema_dfs.keys())}")
else:
    print("=" * 120)
    print("No crack classification sheets found -- no output file created.")
    print("Check that cell 6 loaded at least one qualifying sheet.")

# COMMAND ----------

# DBTITLE 1,Compare schemas across output files and save report
import os
import pandas as pd
from collections import defaultdict

output_dir = "/Workspace/Users/shivam.raj9226@softsensor.ai/Cracking Severity/Project/output"

TARGET_SHEETS = [
    "Longitudinal & Unclassified",
    "Alligator & Multiple",
    "Transversal Crack",
    "Final Crack Rating",
    "Cracks (MDB)",
    "Cracks"
]

# Read all output xlsx files
xlsx_files = sorted([f for f in os.listdir(output_dir) if f.endswith(".xlsx")])
print(f"Output files found: {len(xlsx_files)}")
for f in xlsx_files:
    print(f"  {f}")
print()

# Collect schemas: {sheet_name: {file_name: [list of column names]}}
schemas_by_sheet = defaultdict(dict)

for fname in xlsx_files:
    fpath = os.path.join(output_dir, fname)
    try:
        xls = pd.ExcelFile(fpath, engine="openpyxl")
        for sheet in xls.sheet_names:
            if sheet in TARGET_SHEETS:
                df = pd.read_excel(xls, sheet_name=sheet)
                if "Column_Name" in df.columns:
                    col_list = df["Column_Name"].tolist()
                else:
                    col_list = list(df.columns)
                schemas_by_sheet[sheet][fname] = col_list
        xls.close()
    except Exception as e:
        print(f"  SKIP (corrupt/invalid): {fname} -- {e}")

# Compare schemas for each sheet type
for sheet_name in TARGET_SHEETS:
    file_schemas = schemas_by_sheet.get(sheet_name, {})
    if not file_schemas:
        print(f"{'=' * 120}")
        print(f"SHEET: {sheet_name}")
        print(f"  Not found in any output file.")
        print()
        continue

    print(f"{'=' * 120}")
    print(f"SHEET: {sheet_name}")
    print(f"Found in {len(file_schemas)} file(s)")
    print(f"{'-' * 120}")

    files = list(file_schemas.keys())
    all_match = True
    reference_cols = file_schemas[files[0]]

    if len(files) == 1:
        print(f"  Only 1 file has this sheet: {files[0]}")
        print(f"  Columns ({len(reference_cols)}): {reference_cols}")
        print()
        continue

    # Compare each file against the first file as reference
    comparison_rows = []
    for i, fname in enumerate(files):
        cols = file_schemas[fname]
        if cols != reference_cols:
            all_match = False
            missing = [c for c in reference_cols if c not in cols]
            extra = [c for c in cols if c not in reference_cols]
            comparison_rows.append({
                "File": fname,
                "Column_Count": len(cols),
                "Match": "DIFFERENT",
                "Missing_vs_Reference": ", ".join(missing) if missing else "None",
                "Extra_vs_Reference": ", ".join(extra) if extra else "None"
            })
        else:
            comparison_rows.append({
                "File": fname,
                "Column_Count": len(cols),
                "Match": "IDENTICAL" if i > 0 else "REFERENCE",
                "Missing_vs_Reference": "None",
                "Extra_vs_Reference": "None"
            })

    df_compare = pd.DataFrame(comparison_rows)

    if all_match:
        print(f"  ALL {len(files)} FILES HAVE IDENTICAL SCHEMA")
        print(f"  Columns ({len(reference_cols)}): {reference_cols}")
    else:
        print(f"  SCHEMA DIFFERENCES DETECTED")

    display(df_compare)
    print()

# Save comparison report to Excel
report_path = os.path.join(output_dir, "Cracking_Analysis_Report.xlsx")
with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
    # Summary sheet: which files have which sheets
    summary_rows = []
    for fname in xlsx_files:
        row = {"File": fname}
        for sheet in TARGET_SHEETS:
            row[sheet] = "Yes" if fname in schemas_by_sheet.get(sheet, {}) else "-"
        summary_rows.append(row)
    if summary_rows:
        pd.DataFrame(summary_rows).to_excel(writer, sheet_name="File Summary", index=False)

    # One sheet per crack type with the comparison table
    for sheet_name in TARGET_SHEETS:
        file_schemas = schemas_by_sheet.get(sheet_name, {})
        if not file_schemas:
            continue

        files = list(file_schemas.keys())
        reference_cols = file_schemas[files[0]]

        comp_rows = []
        for i, fname in enumerate(files):
            cols = file_schemas[fname]
            missing = [c for c in reference_cols if c not in cols]
            extra = [c for c in cols if c not in reference_cols]
            comp_rows.append({
                "File": fname,
                "Column_Count": len(cols),
                "Match": "REFERENCE" if i == 0 else ("IDENTICAL" if cols == reference_cols else "DIFFERENT"),
                "Missing_vs_Reference": ", ".join(missing) if missing else "None",
                "Extra_vs_Reference": ", ".join(extra) if extra else "None",
                "Columns": ", ".join(str(c) for c in cols)
            })
        label = sheet_name[:31]
        pd.DataFrame(comp_rows).to_excel(writer, sheet_name=label, index=False)

print("=" * 120)
print(f"Report saved to: {report_path}")