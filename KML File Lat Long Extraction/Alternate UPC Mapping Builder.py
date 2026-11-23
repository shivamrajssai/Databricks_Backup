# Databricks notebook source
# DBTITLE 1,Alternate UPC Extraction — Overview
# MAGIC %md
# MAGIC # Alternate UPC Extraction — Consolidated Master
# MAGIC
# MAGIC Builds a unified **UPC → Alternate UPC** mapping from the Consolidated Master (Ver 7) workbook.
# MAGIC
# MAGIC | Source | Sheet(s) | Role |
# MAGIC | --- | --- | --- |
# MAGIC | Zone Sheets (A–E) | `Zone A` … `Zone E`, Cols H–I | **Master UPC list** (887 UPCs) + 1 Alternate each |
# MAGIC | Consolidated Master | `Consolidated Master_Cleaned`, Cols Z–AD | Up to 5 Alternate UPCs per UPC (reference) |
# MAGIC
# MAGIC **Final output:** 158 UPCs that have ≥1 distinct Alternate UPC, each alternate in its own column.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install openpyxl

# COMMAND ----------

# DBTITLE 1,Step 1 — Load All Source Data
# MAGIC %md
# MAGIC ## Step 1 — Load All Source Data
# MAGIC
# MAGIC **Purpose:**
# MAGIC Reads both data sources from the Consolidated Master workbook in a single step — the Zone Sheets (master UPC list) and the Consolidated Master sheet (alternate UPC reference).
# MAGIC
# MAGIC **Input:**
# MAGIC - `20260519_Consolidated Master_Ver7.xlsx`
# MAGIC   - Zone A–E sheets → Columns H (UPC) and I (Alternate UPC)
# MAGIC   - Consolidated Master_Cleaned sheet → Column A (Zone), I (UPC), Z–AD (Alternate UPCs)
# MAGIC
# MAGIC **Process:**
# MAGIC - Each Zone sheet is read, tagged with its zone letter, cleaned, and appended into a single master list.
# MAGIC - The Consolidated Master sheet is read separately for its 5 Alternate UPC columns and deduplicated on Zone + UPC.
# MAGIC - Duplicate UPCs within a zone are flagged.
# MAGIC
# MAGIC **Output:**
# MAGIC - `zone_upc_df` — 887 UPC records (the authoritative master list)
# MAGIC - `df_master` — deduplicated Alternate UPC reference table
# MAGIC
# MAGIC **Important:**
# MAGIC *Zone D has the header at row 3 instead of row 2. This is handled automatically.*

# COMMAND ----------

# DBTITLE 1,Load Zone sheets + Consolidated Master
import pandas as pd
import re

FILE_PATH = "/Workspace/Users/shivam.raj9226@softsensor.ai/KML File Lat Long Extraction/Input/20260519_Consolidated Master_Ver7.xlsx"

# ══ Helper functions (reused across steps) ══
NULLS = {"nan", "None", "Not Found", "", "-", "0"}
ALT_COLS = ["Alternate UPC", "Alternate UPC 1", "Alternate UPC 2", "Alternate UPC 3", "Alternate UPC 4"]

def clean_col(series):
    """Strip whitespace and normalize null-like values to None."""
    return series.astype(str).str.strip().apply(lambda x: None if x in NULLS else x)

def split_multi_upc(val):
    """Split a cell that may contain multiple UPCs (newline, comma, &)."""
    parts = re.split(r'\s*[,\n]\s*(?:&\s*)?|\s+&\s+', str(val).strip())
    return [p.strip() for p in parts if p.strip() and p.strip() not in NULLS]

# ════════════════════════════════════════════════════════════
# 1. Zone Sheets (A–E) → Master UPC list
# ════════════════════════════════════════════════════════════
ZONE_HEADERS = {"Zone A": 1, "Zone B": 1, "Zone C": 1, "Zone D": 2, "Zone E": 1}

zone_dfs = []
for sheet, hdr in ZONE_HEADERS.items():
    z = pd.read_excel(FILE_PATH, sheet_name=sheet, header=hdr, usecols="H:I", engine="openpyxl")
    z.columns = ["UPC", "Alternate_UPC"]
    z["Zone"] = sheet[-1]
    for c in ["UPC", "Alternate_UPC"]:
        z[c] = clean_col(z[c])
    z = z.dropna(subset=["UPC", "Alternate_UPC"], how="all")
    zone_dfs.append(z)
    print(f"  {sheet}: {len(z)} rows")

zone_upc_df = pd.concat(zone_dfs, ignore_index=True)
zone_upc_df.insert(0, "SerialNo", range(1, len(zone_upc_df) + 1))
zone_upc_df = zone_upc_df[["SerialNo", "Zone", "UPC", "Alternate_UPC"]]

# ════════════════════════════════════════════════════════════
# 2. Consolidated Master_Cleaned → Alternate UPC reference
# ════════════════════════════════════════════════════════════
df_master = pd.read_excel(FILE_PATH, sheet_name="Consolidated Master_Cleaned", header=1, engine="openpyxl")
df_master = df_master[["Zone", "UPC"] + ALT_COLS].copy()
for c in ["UPC"] + ALT_COLS:
    df_master[c] = clean_col(df_master[c])
df_master = df_master.dropna(subset=["Zone", "UPC"], how="any")
df_master = df_master.drop_duplicates(subset=["Zone", "UPC"], keep="first")

# ══ Summary & Validation ══
print(f"\n{'='*60}")
print(f"LOADED DATA SUMMARY")
print(f"{'='*60}")
print(f"  Zone sheets (master UPC list) : {len(zone_upc_df)} UPCs")
print(f"  Consolidated Master (alt ref) : {len(df_master)} unique records")
print(f"\n  Zone breakdown (master list):")
for zn, cnt in zone_upc_df["Zone"].value_counts().sort_index().items():
    print(f"    Zone {zn}: {cnt}")

print(f"\n  Duplicate UPC check (per zone):")
dup_found = False
for zn in sorted(zone_upc_df["Zone"].unique()):
    dups = zone_upc_df[zone_upc_df["Zone"] == zn]["UPC"].dropna()
    dup_vals = dups[dups.duplicated(keep=False)]
    if dup_vals.nunique():
        dup_found = True
        print(f"    Zone {zn}: \u26a0\ufe0f {dup_vals.nunique()} duplicate(s)")
        for upc_val, cnt in dup_vals.value_counts().items():
            print(f"      {upc_val} (\u00d7{cnt})")
if not dup_found:
    print(f"    \u2705 No duplicates found")
print(f"{'='*60}")

display(zone_upc_df)

# COMMAND ----------

# DBTITLE 1,Step 2 — Merge and Consolidate Alternate UPCs
# MAGIC %md
# MAGIC ## Step 2 — Merge and Consolidate Alternate UPCs
# MAGIC
# MAGIC **Purpose:**
# MAGIC Merges the master UPC list (887 records from Zone sheets) with the Alternate UPC reference from the Consolidated Master sheet, producing a single dataset where each UPC has all its distinct Alternate UPCs.
# MAGIC
# MAGIC **Input:**
# MAGIC - `zone_upc_df` — 887 UPCs from Step 1
# MAGIC - `df_master` — Alternate UPC reference from Step 1
# MAGIC
# MAGIC **Process:**
# MAGIC - Zone UPC list is left-joined with the Consolidated Master reference on Zone + UPC.
# MAGIC - All Alternate UPC values from both sources (6 columns total) are gathered per UPC.
# MAGIC - Multi-UPC cells (separated by newlines, commas, or `&`) are split into individual values.
# MAGIC - Duplicates and self-references (Alternate = main UPC) are removed.
# MAGIC - Values exceeding 16 characters (suffixes like `-RCW`, `-DPR`) are flagged for review.
# MAGIC
# MAGIC **Output:**
# MAGIC - `merged_df` — 887 UPC records with consolidated distinct Alternate UPCs and count.

# COMMAND ----------

# DBTITLE 1,Merge zone UPCs with Consolidated Master alternates
# ════════════════════════════════════════════════════════════
# Merge zone UPC list with Consolidated Master alternate UPCs
# ════════════════════════════════════════════════════════════
merged = zone_upc_df.merge(
    df_master[["Zone", "UPC"] + ALT_COLS],
    on=["Zone", "UPC"],
    how="left",
)

# Consolidate all alternate UPC sources into a distinct set per row
# Sources: Alternate_UPC (zone sheet Col I) + 5 cols from Consolidated Master
all_alt_sources = ["Alternate_UPC"] + ALT_COLS

# Only accept values that look like a UPC (N/ or M/ prefix)
upc_format = re.compile(r'^[NM]/')

results = []
skipped_non_upc = set()
for _, row in merged.iterrows():
    alt_set = set()
    for col in all_alt_sources:
        val = row.get(col)
        if pd.notna(val) and str(val).strip() not in NULLS:
            for v in split_multi_upc(val):
                if upc_format.match(v):
                    alt_set.add(v)
                else:
                    skipped_non_upc.add(v)
    # Remove self-reference (Alternate = main UPC)
    alt_set.discard(str(row["UPC"]).strip() if pd.notna(row["UPC"]) else "")
    alt_list = sorted(alt_set)
    results.append({
        "SerialNo": row["SerialNo"], "Zone": row["Zone"], "UPC": row["UPC"],
        "Distinct_Alternate_UPCs": ", ".join(alt_list) if alt_list else None,
        "Alternate_UPC_Count": len(alt_list),
    })

merged_df = pd.DataFrame(results)

# ══ Summary ══
total = len(merged_df)
with_alt = int((merged_df["Alternate_UPC_Count"] > 0).sum())
print(f"Total UPCs: {total}")
print(f"  With \u22651 Alternate UPC : {with_alt}")
print(f"  Without Alternate UPC: {total - with_alt}")
print(f"  Total distinct alternates: {int(merged_df['Alternate_UPC_Count'].sum())}")

print(f"\n--- Per-Zone Breakdown ---")
zone_summary = merged_df.groupby("Zone").agg(
    Total_UPCs=("SerialNo", "count"),
    With_Alternate=("Alternate_UPC_Count", lambda x: int((x > 0).sum())),
    Total_Alternates=("Alternate_UPC_Count", "sum"),
).reset_index()
display(zone_summary)

# ══ Data quality: flag values > 16 chars ══
all_alts = merged_df["Distinct_Alternate_UPCs"].dropna().str.split(", ").explode()
long_vals = all_alts[all_alts.str.len() > 16]
if len(long_vals):
    print(f"\n\u26a0\ufe0f {long_vals.nunique()} Alternate UPC value(s) > 16 chars (suffixes/annotations):")
    for v in long_vals.unique():
        print(f"    {v} (len={len(v)})")
else:
    print(f"\n\u2705 All Alternate UPC values \u226416 characters")

# Report rejected non-UPC values
if skipped_non_upc:
    print(f"\n\u26a0\ufe0f Skipped {len(skipped_non_upc)} non-UPC value(s): {sorted(skipped_non_upc)}")

display(merged_df)

# COMMAND ----------

# DBTITLE 1,Step 3 — Final Alternate UPC Listing
# MAGIC %md
# MAGIC ## Step 3 — Final Alternate UPC Listing
# MAGIC
# MAGIC **Purpose:**
# MAGIC Produces the final filtered output containing only UPCs that have at least one Alternate UPC, with each Alternate UPC in its own separate column.
# MAGIC
# MAGIC **Input:**
# MAGIC - `merged_df` — 887 UPC records from Step 2.
# MAGIC
# MAGIC **Process:**
# MAGIC - UPCs with zero Alternate UPCs are excluded.
# MAGIC - The consolidated Alternate UPC string is expanded so each value occupies its own column (`Alt_UPC_1`, `Alt_UPC_2`, etc.).
# MAGIC
# MAGIC **Output:**
# MAGIC - `alt_upc_final` — 158 UPC records with SerialNo, Zone, UPC, Alternate_UPC_Count, and individual Alternate UPC columns.
# MAGIC
# MAGIC **Important:**
# MAGIC *This is the final consumable output. Only UPCs with ≥1 Alternate are included.*

# COMMAND ----------

# DBTITLE 1,Final output — UPCs with Alternates in separate columns
# ════════════════════════════════════════════════════════════
# Filter to UPCs with ≥1 Alternate, expand to individual columns
# ════════════════════════════════════════════════════════════
alt_only = merged_df[merged_df["Alternate_UPC_Count"] > 0].copy()

# Expand comma-separated alternates into individual columns
alt_expanded = alt_only["Distinct_Alternate_UPCs"].str.split(", ", expand=True)
alt_expanded.columns = [f"Alt_UPC_{i+1}" for i in range(alt_expanded.shape[1])]

alt_upc_final = pd.concat(
    [alt_only[["Zone", "UPC", "Alternate_UPC_Count"]].reset_index(drop=True),
     alt_expanded.reset_index(drop=True)],
    axis=1,
)
alt_upc_final.insert(0, "SerialNo", range(1, len(alt_upc_final) + 1))

# ══ Summary ══
print(f"UPCs with \u22651 Alternate UPC: {len(alt_upc_final)}")
print(f"Max alternates per UPC: {alt_upc_final['Alternate_UPC_Count'].max()}")
print(f"\n--- Distribution ---")
for cnt, n in alt_upc_final["Alternate_UPC_Count"].value_counts().sort_index().items():
    print(f"  {cnt} alternate(s): {n} UPCs")

print(f"\n--- Per-Zone ---")
display(alt_upc_final.groupby("Zone").size().reset_index(name="Count"))

display(alt_upc_final)

# COMMAND ----------

# DBTITLE 1,Step 4 — Comprehensive Validation
# MAGIC %md
# MAGIC ## Step 4 — Comprehensive Validation
# MAGIC
# MAGIC **Purpose:**
# MAGIC Runs a full suite of data-integrity checks across every stage of the pipeline to catch errors, leaks, or inconsistencies.
# MAGIC
# MAGIC **Checks performed:**
# MAGIC 1. **Row count integrity** — zone_upc_df (887) → merged_df (887) → alt_upc_final (filtered)
# MAGIC 2. **No duplicate UPCs per zone** in the final outputs
# MAGIC 3. **No null / placeholder leaks** — values like `0`, `None`, `-`, `nan` must not appear as Alternate UPCs
# MAGIC 4. **No self-references** — Alternate UPC must not equal the main UPC on the same row
# MAGIC 5. **No multi-UPC cells** — each Alt_UPC column must contain exactly one UPC
# MAGIC 6. **UPC format validation** — all UPC and Alternate UPC values should match the expected `N/XXXXX/XXXXX/XX` pattern
# MAGIC 7. **Alt_UPC_Count accuracy** — the count column must match the actual number of populated Alt_UPC columns
# MAGIC 8. **Cross-source coverage** — how many zone-sheet UPCs were found in the Consolidated Master
# MAGIC 9. **Orphan check** — Alternate UPCs that don't exist as main UPCs in the master list

# COMMAND ----------

# DBTITLE 1,Run full validation suite
import re

passed, failed, warned = 0, 0, 0

def check(name, condition, detail=""):
    global passed, failed
    if condition:
        print(f"  \u2705 {name}")
        passed += 1
    else:
        print(f"  \u274c {name}")
        if detail:
            print(f"     {detail}")
        failed += 1

def warn(name, detail):
    global warned
    print(f"  \u26a0\ufe0f {name}")
    if detail:
        print(f"     {detail}")
    warned += 1

print("=" * 65)
print("VALIDATION SUITE")
print("=" * 65)

# ── 1. Row count integrity ──
print("\n1. Row Count Integrity")
check("zone_upc_df has 887 rows", len(zone_upc_df) == 887, f"Got {len(zone_upc_df)}")
check("merged_df has 887 rows (no row inflation)", len(merged_df) == 887, f"Got {len(merged_df)}")
check("alt_upc_final is a strict subset of merged_df", len(alt_upc_final) <= len(merged_df))
expected_alt = int((merged_df["Alternate_UPC_Count"] > 0).sum())
check(f"alt_upc_final count matches merged_df filter ({expected_alt})",
      len(alt_upc_final) == expected_alt, f"Got {len(alt_upc_final)}")

# ── 2. No duplicate UPCs per zone ──
print("\n2. Duplicate UPCs Per Zone")
for label, df_chk in [("merged_df", merged_df), ("alt_upc_final", alt_upc_final)]:
    dup = df_chk.groupby(["Zone", "UPC"]).size()
    dup_count = int((dup > 1).sum())
    check(f"No duplicate Zone+UPC in {label}", dup_count == 0,
          f"{dup_count} duplicate(s) found")

# ── 3. No null / placeholder leaks ──
print("\n3. No Null / Placeholder Leaks in Alternates")
alt_cols_final = [c for c in alt_upc_final.columns if c.startswith("Alt_UPC_")]
all_alt_vals = []
for c in alt_cols_final:
    all_alt_vals.extend(alt_upc_final[c].dropna().astype(str).tolist())

placeholders = {"0", "nan", "None", "null", "-", "", "Not Found"}
leaked = [v for v in all_alt_vals if v.strip() in placeholders]
check(f"No placeholder values in Alt_UPC columns", len(leaked) == 0,
      f"{len(leaked)} placeholder(s) found: {set(leaked)}")

# ── 4. No self-references ──
print("\n4. No Self-References (Alternate = Main UPC)")
self_refs = 0
for _, row in alt_upc_final.iterrows():
    upc = str(row["UPC"]).strip()
    for c in alt_cols_final:
        if pd.notna(row[c]) and str(row[c]).strip() == upc:
            self_refs += 1
check("No Alternate UPC equals its own main UPC", self_refs == 0,
      f"{self_refs} self-reference(s) found")

# ── 5. No multi-UPC cells ──
print("\n5. No Multi-UPC Values in Single Cells")
upc_pat = re.compile(r'[NM]/\d+/\d+/[A-Z]{2}')
multi = 0
for _, row in alt_upc_final.iterrows():
    for c in alt_cols_final:
        v = row[c]
        if pd.notna(v) and len(upc_pat.findall(str(v))) > 1:
            multi += 1
check("Each Alt_UPC cell contains at most 1 UPC", multi == 0,
      f"{multi} cell(s) with multiple UPCs")

# ── 6. UPC format validation ──
print("\n6. UPC Format Validation")
fmt_pat = re.compile(r'^[NM]/\d+/\d+/[A-Z]{2}[\w-]*$')

# Main UPCs
bad_main = merged_df["UPC"].dropna().apply(lambda x: not bool(fmt_pat.match(str(x).strip())))
bad_main_count = int(bad_main.sum())
if bad_main_count:
    bad_vals = merged_df.loc[bad_main[bad_main].index, "UPC"].unique()[:5]
    warn(f"{bad_main_count} main UPC(s) don't match expected format",
         f"Examples: {list(bad_vals)}")
else:
    check("All main UPCs match expected format", True)

# Alternate UPCs
bad_alt = [v for v in all_alt_vals if not fmt_pat.match(v.strip())]
if bad_alt:
    warn(f"{len(bad_alt)} Alternate UPC value(s) don't match expected format",
         f"Examples: {list(set(bad_alt))[:5]}")
else:
    check("All Alternate UPCs match expected format", True)

# ── 7. Alt_UPC_Count accuracy ──
print("\n7. Alt_UPC_Count Matches Actual Columns")
mismatches = 0
for _, row in alt_upc_final.iterrows():
    actual = sum(1 for c in alt_cols_final if pd.notna(row[c]) and str(row[c]).strip())
    if actual != row["Alternate_UPC_Count"]:
        mismatches += 1
check("Alternate_UPC_Count matches populated Alt_UPC columns", mismatches == 0,
      f"{mismatches} mismatch(es)")

# ── 8. Cross-source coverage ──
print("\n8. Cross-Source Coverage (Zone Sheets vs Consolidated Master)")
zone_keys = set(zip(zone_upc_df["Zone"], zone_upc_df["UPC"].fillna("")))
master_keys = set(zip(df_master["Zone"], df_master["UPC"].fillna("")))
matched = zone_keys & master_keys
unmatched = zone_keys - master_keys
match_pct = len(matched) / len(zone_keys) * 100 if zone_keys else 0
print(f"  \u2139\ufe0f {len(matched)}/{len(zone_keys)} zone UPCs found in Consolidated Master ({match_pct:.1f}%)")
if unmatched:
    samples = sorted(unmatched)[:5]
    warn(f"{len(unmatched)} zone UPC(s) NOT in Consolidated Master",
         f"Samples: {samples}")

# ── 9. Orphan Alternates ──
print("\n9. Orphan Alternate UPCs (not in master list)")
all_main_upcs = set(merged_df["UPC"].dropna().astype(str).str.strip())
orphans = set()
for v in all_alt_vals:
    v_clean = v.strip().rstrip("-RCW").rstrip("-DPR")  # strip known suffixes for matching
    if v.strip() not in all_main_upcs:
        orphans.add(v.strip())

if orphans:
    warn(f"{len(orphans)} Alternate UPC(s) not found as main UPCs in the 887 master list",
         f"(This is informational — alternates may legitimately reference UPCs outside the zone sheets)")
else:
    check("All Alternate UPCs also exist as main UPCs", True)

# ══ Final Score ══
print(f"\n{'='*65}")
print(f"RESULTS: {passed} passed, {failed} failed, {warned} warning(s)")
if failed == 0:
    print(f"\u2705 All critical checks passed.")
else:
    print(f"\u274c {failed} critical check(s) failed — review above.")
print(f"{'='*65}")

# COMMAND ----------

# DBTITLE 1,Match kml_333 UPCs against Alternate UPC mapping (cell 8)
import pandas as pd

# ══ Load kml_333.xlsx (keep as-is) ══
kml_path = "/Workspace/Users/shivam.raj9226@softsensor.ai/KML File Lat Long Extraction/Input/kml_333.xlsx"
kml_df = pd.read_excel(kml_path, engine="openpyxl")
print(f"Loaded kml_333: {len(kml_df)} rows")

# ══ Build lookup from alt_upc_final (cell 8 output) ══
# For each UPC in alt_upc_final, collect which Alt_UPC columns have values
alt_cols = [c for c in alt_upc_final.columns if c.startswith("Alt_UPC_")]

# Create lookup: (Zone, UPC) → {alt_upc_values, matched_columns}
lookup = {}
for _, row in alt_upc_final.iterrows():
    key = (str(row["Zone"]).strip(), str(row["UPC"]).strip())
    alts = []
    cols_matched = []
    for c in alt_cols:
        if pd.notna(row[c]) and str(row[c]).strip():
            alts.append(str(row[c]).strip())
            cols_matched.append(c)
    lookup[key] = {"alts": alts, "cols": cols_matched}

# ══ Also build reverse lookup: if a 333 UPC appears as an Alternate ══
# (Zone, Alt_UPC) → {main_upc, matched_column}
reverse_lookup = {}
for _, row in alt_upc_final.iterrows():
    zone = str(row["Zone"]).strip()
    main_upc = str(row["UPC"]).strip()
    for c in alt_cols:
        if pd.notna(row[c]) and str(row[c]).strip():
            alt_val = str(row[c]).strip()
            rkey = (zone, alt_val)
            if rkey not in reverse_lookup:
                reverse_lookup[rkey] = []
            reverse_lookup[rkey].append({"main_upc": main_upc, "col": c})

# ══ Match each kml_333 UPC against both lookups ══
match_results = []
for _, row in kml_df.iterrows():
    zone = str(row["Zone"]).strip() if pd.notna(row["Zone"]) else ""
    upc = str(row["UPC"]).strip() if pd.notna(row["UPC"]) else ""
    key = (zone, upc)

    alt_match = None
    source = None

    # Check 1: Is this UPC a main UPC in alt_upc_final? (it HAS alternates)
    if key in lookup:
        info = lookup[key]
        alt_match = ", ".join(info["alts"])
        source = f"Main UPC match → {', '.join(info['cols'])}"

    # Check 2: Is this UPC listed as an Alternate for some other UPC?
    if key in reverse_lookup:
        for entry in reverse_lookup[key]:
            rev_info = f"Alt of {entry['main_upc']} (found in {entry['col']})"
            if alt_match:
                alt_match += f" | {entry['main_upc']}"
                source += f" + {rev_info}"
            else:
                alt_match = entry["main_upc"]
                source = rev_info

    match_results.append({
        "Alt_UPC_Match": alt_match if alt_match else "No Match",
        "Match_Source": source if source else None,
    })

# ══ Add columns F (Alt_UPC_Match) and G (Match_Source) to kml_df ══
kml_matched = kml_df.copy()
kml_matched["Alt_UPC_Match"] = [r["Alt_UPC_Match"] for r in match_results]
kml_matched["Match_Source"] = [r["Match_Source"] for r in match_results]

# ══ Summary ══
total = len(kml_matched)
matched = int((kml_matched["Alt_UPC_Match"] != "No Match").sum())
unmatched = total - matched

print(f"\n{'='*65}")
print(f"KML 333 → ALTERNATE UPC MATCH RESULTS")
print(f"{'='*65}")
print(f"  Total UPCs      : {total}")
print(f"  Matched         : {matched}")
print(f"  No Match        : {unmatched}")

print(f"\n--- Per-Zone ---")
zone_match = kml_matched.groupby("Zone").agg(
    Total=("UPC", "count"),
    Matched=("Alt_UPC_Match", lambda x: int((x != "No Match").sum())),
    No_Match=("Alt_UPC_Match", lambda x: int((x == "No Match").sum())),
).reset_index()
display(zone_match)

print(f"\n--- Full List (333 rows) ---")
display(kml_matched)