# Databricks notebook source
# DBTITLE 1,MOM Drive Folder Checker
# MAGIC %md
# MAGIC # MOM Drive Folder Checker
# MAGIC
# MAGIC Enter a row number (2–27) in the widget above, then run the cells to see what's inside that row's Google Drive folder.

# COMMAND ----------

# DBTITLE 1,Step 0: Install Required Libraries
# MAGIC %pip install openpyxl google-auth-oauthlib google-api-python-client

# COMMAND ----------

# DBTITLE 1,Imports
import re
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse
from openpyxl import load_workbook
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# COMMAND ----------

# DBTITLE 1,Config & Load Workbook
BASE_DIR = Path('/Workspace/Users/shivam.raj9226@softsensor.ai/New structure cheking 2026-10-14 10:48:19')
CLIENT_SECRET_FILE = BASE_DIR / 'credentials' / 'client_secret_2_1069911700212-9kponnktv21g55ngdkkkb4n6m00h8bfm.apps.googleusercontent.com.json'
TOKEN_FILE = BASE_DIR / 'credentials' / 'token.json'
WORKBOOK_FILE = BASE_DIR / 'data' / 'MOM.xlsx'
SCOPES = ['https://www.googleapis.com/auth/drive.readonly', 'https://www.googleapis.com/auth/spreadsheets.readonly']

# Load workbook once
wb = load_workbook(WORKBOOK_FILE, data_only=False)
ws = wb.active
print(f"Loaded '{ws.title}': {ws.max_row} rows, {ws.max_column} cols")

# COMMAND ----------

# DBTITLE 1,Row Number Widget
dbutils.widgets.text("row_num", "2", "Row Number (2-27)")

# COMMAND ----------

# DBTITLE 1,Check Drive Folder for Selected Row
# --- Get row number from widget ---
row_num = int(dbutils.widgets.get("row_num"))
assert 2 <= row_num <= ws.max_row, f"Row must be between 2 and {ws.max_row}"

# --- Read URL from column K (index 11) ---
cell = ws.cell(row_num, 11)
url = None
if cell.hyperlink and cell.hyperlink.target:
    url = cell.hyperlink.target.strip()
elif isinstance(cell.value, str) and cell.value.strip().lower().startswith('http'):
    url = cell.value.strip()

# Decode Outlook SafeLinks if needed
if url:
    parsed = urlparse(url)
    if parsed.netloc.lower().endswith('safelinks.protection.outlook.com'):
        encoded = parse_qs(parsed.query).get('url', [None])[0]
        if encoded:
            url = unquote(encoded)

assert url, f"No URL found in column K of row {row_num}"

# --- Detect SharePoint / OneDrive URLs ---
is_sharepoint = 'sharepoint' in url.lower() or 'onedrive' in url.lower()

import time
all_names = []
all_drive_files = []

if is_sharepoint:
    folder_id = None
    service = None
    print(f"Row {row_num} | URL: {url}")
    print(f"\n⚠️  This is a SharePoint/OneDrive folder — cannot list contents via Google Drive API.")
    print("   Downstream cells will flag manual verification steps.")
else:
    # --- Extract folder ID ---
    match = re.search(r'/folders/([A-Za-z0-9_-]+)', url)
    assert match, f"No Google Drive folder ID found in URL: {url}"
    folder_id = match.group(1)

    print(f"Row {row_num} | URL: {url}")
    print(f"Folder ID: {folder_id}")

    # --- Authenticate with Google Drive ---
    creds = None
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        TOKEN_FILE.write_text(creds.to_json())
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRET_FILE), SCOPES)
        creds = flow.run_local_server(port=0)
        TOKEN_FILE.write_text(creds.to_json())
    service = build('drive', 'v3', credentials=creds)
    print("Drive authenticated.\n")

    # --- Recursively list folder contents ---
    def list_drive(fid, indent=0):
        for attempt in range(4):
            try:
                resp = service.files().list(
                    q=f"'{fid}' in parents and trashed=false",
                    fields='files(id, name, mimeType)',
                    pageSize=1000, supportsAllDrives=True, includeItemsFromAllDrives=True
                ).execute()
                break
            except Exception as e:
                if attempt < 3 and ('500' in str(e) or '503' in str(e) or 'Internal' in str(e)):
                    time.sleep(2 ** attempt)
                else:
                    raise
        for f in resp.get('files', []):
            is_folder = f['mimeType'] == 'application/vnd.google-apps.folder'
            icon = '📁' if is_folder else '📄'
            all_names.append(f['name'])
            all_drive_files.append(f)
            print(f"{'  ' * indent}{icon} {f['name']}")
            if is_folder:
                list_drive(f['id'], indent + 1)

    print(f"Contents of Drive folder for row {row_num}:")
    print("=" * 50)
    list_drive(folder_id)

# COMMAND ----------

# DBTITLE 1,Deliverable Completeness Check
# Handle SharePoint URLs where folder contents couldn't be listed
if is_sharepoint:
    print(f"Deliverable check for row {row_num}")
    print("=" * 50)
    print(f"\n⚠️  SharePoint/OneDrive folder — cannot verify deliverables automatically.")
    print(f"   URL: {url}")
    print(f"\n   ➡️  Please manually verify the following are present:")
    print(f"   D1: LRMS, RIS, Visual Condition")
    print(f"   D2: Comparison as per CA/IRC")
    print(f"   D4: Defect Report DHMS, CRRI PCI, NSV Individual Cracks, NSV NHAI ONE")
    print("\n" + "=" * 50)
else:
    # Substring-based check: each expected item maps to keywords that should appear in at least one file/folder name
    expected = {
        "D1": {
            "LRMS": ["lrms"],
            "RIS": ["ris"],
            "Visual Condition": ["visual condition", "visual_condition", "visual-condition"],
        },
        "D2": {
            "Comparison as per CA/IRC": ["comparison", "ca/irc", "ca irc", "cairc"],
        },
        "D4": {
            "Defect Report DHMS": ["defect report", "defect_report", "dhms"],
            "CRRI PCI": ["crri", "pci"],
            "NSV Individual Cracks": ["nsv cracks", "nsv_cracks", "individual crack"],
            "NSV NHAI ONE": ["nsv nhai", "nhai one", "nhai_one"],
        },
    }

    lower_names = [n.lower() for n in all_names]

    print(f"Deliverable check for row {row_num}")
    print("=" * 50)
    all_ok = True
    for deliverable, items in expected.items():
        print(f"\n{deliverable}:")
        for label, keywords in items.items():
            found = any(kw in name for name in lower_names for kw in keywords)
            if found:
                print(f"  ✅ {label}")
            else:
                print(f"  ❌ {label} — MISSING")
                all_ok = False

    print("\n" + "=" * 50)
    print("✅ All deliverables present!" if all_ok else "⚠️ Some deliverables are missing — see above.")

# COMMAND ----------

# DBTITLE 1,D2 Deep Analysis — CA & IRC Sheets
import io

# --- SharePoint / OneDrive check ---
is_sharepoint = 'sharepoint' in url.lower() or 'onedrive' in url.lower()

print(f"D2 Deep Analysis — Row {row_num}")
print("=" * 60)

if is_sharepoint:
    print(f"\n⚠️  Data is on SharePoint/OneDrive — cannot inspect via Drive API")
    print(f"   URL: {url}")
    print("   ➡️  Manually verify: D2 comparison file has 'Comparison as per CA' and 'Comparison as per IRC' sheets with data")
else:
    required_sheets = ["comparison as per ca", "comparison as per irc"]
    key_measurements = {
        "Roughness": ["roughness", "iri", "bi", "ride"],
        "Rutting/Rut Depth": ["rutting", "rut ", "rut_", "rut("],
        "Cracking": ["crack", "cracking"],
        "Pothole": ["pothole", "pot hole"],
    }

    def find_all(fid):
        out = []
        resp = service.files().list(
            q=f"'{fid}' in parents and trashed=false",
            fields='files(id, name, mimeType)',
            pageSize=1000, supportsAllDrives=True, includeItemsFromAllDrives=True
        ).execute()
        for f in resp.get('files', []):
            out.append(f)
            if f['mimeType'] == 'application/vnd.google-apps.folder':
                out.extend(find_all(f['id']))
        return out

    all_drive_files = find_all(folder_id)
    d2_excels = [f for f in all_drive_files
                 if any(f['name'].lower().endswith(e) for e in ('.xlsx', '.xlsm', '.xls'))
                 and ('comparison' in f['name'].lower() or 'd2' in f['name'].lower())]

    if not d2_excels:
        print("\n❌ No D2 comparison Excel file found")
    else:
        for d2f in d2_excels:
            print(f"\n📄 {d2f['name']}")
            raw_bytes = service.files().get_media(fileId=d2f['id'], supportsAllDrives=True).execute()
            wb_raw = load_workbook(io.BytesIO(raw_bytes), data_only=False, read_only=True)
            wb_val = load_workbook(io.BytesIO(raw_bytes), data_only=True, read_only=True)
            sheet_map = {s.lower(): s for s in wb_raw.sheetnames}

            sheet_results = {}  # for CA vs IRC cross-check

            for req in required_sheets:
                actual = sheet_map.get(req)
                label = req.replace("comparison as per ", "").upper()
                print(f"\n   {'─'*50}")
                print(f"   📋 {label} Sheet: '{actual or req}'")

                if not actual:
                    print(f"   ❌ NOT FOUND")
                    sheet_results[label] = None
                    continue

                rows_raw = list(wb_raw[actual].iter_rows(values_only=True))
                rows_val = list(wb_val[actual].iter_rows(values_only=True))

                # --- Smart multi-row header detection ---
                ncols = max(len(r) for r in rows_val) if rows_val else 0

                # Find chainage column in header rows
                ch_col_guess = None
                for i, row in enumerate(rows_val[:6]):
                    for j, c in enumerate(row):
                        if c is not None and 'chainage' in str(c).strip().lower():
                            ch_col_guess = j
                            break
                    if ch_col_guess is not None:
                        break

                # Find first row where chainage col has a number → that's data start
                data_start = None
                if ch_col_guess is not None:
                    for i, row in enumerate(rows_val):
                        v = row[ch_col_guess] if ch_col_guess < len(row) else None
                        if v is not None:
                            try:
                                float(v)
                                data_start = i
                                break
                            except (ValueError, TypeError):
                                pass

                if data_start is None or data_start < 1:
                    # Fallback: first non-empty row is header
                    for i, rv in enumerate(rows_val):
                        if any(c is not None for c in rv):
                            data_start = i + 1
                            break
                    if data_start is None:
                        continue

                # Build composite headers from all header rows (forward-fill + combine)
                filled_rows = []
                for i in range(data_start):
                    row_filled = []
                    last_val = None
                    for j in range(ncols):
                        v = rows_val[i][j] if j < len(rows_val[i]) else None
                        if v is not None and str(v).strip():
                            last_val = str(v).strip()
                            row_filled.append(last_val)
                        else:
                            row_filled.append(last_val)
                    filled_rows.append(row_filled)

                headers = []
                for j in range(ncols):
                    parts = []
                    seen = set()
                    for i in range(len(filled_rows)):
                        v = filled_rows[i][j]
                        if v and v not in seen:
                            seen.add(v)
                            parts.append(v)
                    headers.append((' | '.join(parts)).lower() if parts else '')

                # Count real vs formula data rows from data_start
                real_count, formula_count = 0, 0
                real_data_rows = []
                raw_data_rows = []
                for rv, rr in zip(rows_val[data_start:], rows_raw[data_start:]):
                    if not any(c is not None for c in rv) and not any(c is not None for c in rr):
                        continue
                    non_none = [c for c in rr if c is not None]
                    if non_none and all(str(c).startswith('=') for c in non_none):
                        formula_count += 1
                    else:
                        real_count += 1
                        real_data_rows.append(rv)
                        raw_data_rows.append(rr)

                if real_count == 0:
                    print(f"   ❌ EMPTY — 0 real data rows ({formula_count} formula rows)")
                    sheet_results[label] = {"rows": 0, "meas": {m: "empty" for m in key_measurements}}
                    continue

                # Detect chainage column & step size
                # Try header match first, then scan first 10 cols for sequential numbers
                ch_idx = None
                for j, h in enumerate(headers):
                    if any(kw in h for kw in ['chainage','chain','from km','to km','start km','end km','section']):
                        ch_idx = j; break
                # Fallback: find column with most sequential increasing values in first 10 cols
                if ch_idx is None and real_data_rows:
                    best_j, best_seq = None, 0
                    for j in range(min(10, len(headers))):
                        nums = []
                        for r in real_data_rows[:50]:
                            v = r[j] if j < len(r) else None
                            if v is not None:
                                try: nums.append(float(v))
                                except: pass
                        if len(nums) >= 5:
                            inc = sum(1 for i in range(len(nums)-1) if nums[i+1] > nums[i])
                            if inc > best_seq: best_seq = inc; best_j = j
                    if best_j is not None and best_seq >= 4: ch_idx = best_j
                ch_step = None
                if ch_idx is not None:
                    ch_vals = []
                    for r in real_data_rows:
                        v = r[ch_idx] if ch_idx < len(r) else None
                        if v is not None:
                            try: ch_vals.append(float(v))
                            except: pass
                    if len(ch_vals) >= 2:
                        ch_s = sorted(ch_vals)
                        dfs = [round(ch_s[i+1]-ch_s[i], 4) for i in range(len(ch_s)-1) if ch_s[i+1]-ch_s[i] > 0]
                        if dfs:
                            ch_step = max(set(dfs), key=dfs.count)

                step_info = f", chainage step: {ch_step}m" if ch_step else ""
                print(f"   ✅ {real_count} real data rows + {formula_count} formula rows (header rows 1–{data_start}{step_info})")

                # Check all non-empty columns for data coverage
                all_col_stats = []
                for j, h in enumerate(headers):
                    if not h: continue
                    filled = sum(1 for r in real_data_rows
                                 if (r[j] if j < len(r) else None) is not None
                                 and str(r[j] if j < len(r) else '').strip() != '')
                    # If no cached values, check raw rows for formulas
                    if filled == 0 and raw_data_rows:
                        filled = sum(1 for r in raw_data_rows
                                     if (r[j] if j < len(r) else None) is not None
                                     and str(r[j] if j < len(r) else '').strip() != '')
                    all_col_stats.append((h, filled, real_count))

                # Match key measurements
                meas_status = {}
                matched_cols = set()
                print(f"   Key measurements:")
                for mname, keywords in key_measurements.items():
                    cols = [(h, f, t) for h, f, t in all_col_stats if any(kw in h for kw in keywords)]
                    if not cols:
                        meas_status[mname] = "no_columns"
                        continue
                    matched_cols.update(h for h, _, _ in cols)
                    # Aggregate: at least one col should have data
                    best_fill = max(f for _, f, _ in cols)
                    best_pct = best_fill * 100 // real_count if real_count else 0
                    if best_fill == 0:
                        print(f"      ❌ {mname} — columns exist but ALL EMPTY")
                        meas_status[mname] = "empty"
                    elif best_pct < 80:
                        print(f"      ⚠️  {mname} — best {best_fill}/{real_count} rows ({best_pct}%)")
                        meas_status[mname] = f"partial({best_pct}%)"
                    else:
                        print(f"      ✅ {mname} — {best_fill}/{real_count} rows ({best_pct}%)")
                        meas_status[mname] = "ok"

                no_match = [m for m, s in meas_status.items() if s == "no_columns"]
                if no_match:
                    print(f"      ⚠️  Not found: {', '.join(no_match)}")
                    # Show actual column headers for manual review
                    unmatched = [(h, f, t) for h, f, t in all_col_stats if h not in matched_cols and f > 0]
                    if unmatched:
                        print(f"      Columns with data (may contain the above):")
                        for h, f, t in unmatched[:15]:
                            print(f"         • '{h}' — {f}/{t} rows")

                sheet_results[label] = {"rows": real_count, "meas": meas_status, "step": ch_step}

            # --- CA vs IRC Cross-Check ---
            print(f"\n   {'='*50}")
            print(f"   🔍 CA vs IRC Cross-Check:")
            ca, irc = sheet_results.get("CA"), sheet_results.get("IRC")
            if ca is None and irc is None:
                print(f"      ❌ Both sheets missing")
            elif ca is None:
                print(f"      ❌ CA sheet missing, IRC available ({irc['rows']} rows)")
            elif irc is None:
                print(f"      ❌ IRC sheet missing, CA available ({ca['rows']} rows)")
            elif ca["rows"] == 0 and irc["rows"] == 0:
                print(f"      ❌ Both sheets empty")
            elif ca["rows"] == 0:
                print(f"      ⚠️  CA is EMPTY but IRC has {irc['rows']} rows")
            elif irc["rows"] == 0:
                print(f"      ⚠️  IRC is EMPTY but CA has {ca['rows']} rows")
            else:
                ca_step, irc_step = ca.get('step'), irc.get('step')
                if ca["rows"] != irc["rows"]:
                    if ca_step and irc_step and ca_step != irc_step:
                        print(f"      ℹ️  Row count differs: CA={ca['rows']} (step {ca_step}m), IRC={irc['rows']} (step {irc_step}m) — different chainage granularity, expected")
                    else:
                        print(f"      ⚠️  Row mismatch: CA={ca['rows']}, IRC={irc['rows']}")
                else:
                    print(f"      ✅ Both sheets: {ca['rows']} data rows")
                if ca_step and irc_step and ca_step != irc_step:
                    print(f"      ℹ️  Chainage steps: CA={ca_step}m, IRC={irc_step}m")
                # Compare measurements
                issues = []
                for m in key_measurements:
                    cs, irs = ca["meas"].get(m, "missing"), irc["meas"].get(m, "missing")
                    if cs != irs:
                        issues.append(f"{m}: CA={cs}, IRC={irs}")
                if issues:
                    print(f"      ⚠️  Differences:")
                    for iss in issues:
                        print(f"         • {iss}")
                else:
                    print(f"      ✅ Same measurement coverage in both sheets")

            wb_raw.close()
            wb_val.close()

# COMMAND ----------

# DBTITLE 1,D4 Deep Analysis — Defect Report DHMS + NSV Cracks
import io

# --- SharePoint check ---
is_sharepoint = 'sharepoint' in url.lower() or 'onedrive' in url.lower()

print(f"D4 Deep Analysis — Row {row_num}")
print("=" * 60)

if is_sharepoint:
    print(f"\n⚠️  Data is on SharePoint/OneDrive — cannot inspect via Drive API")
    print(f"   URL: {url}")
    print("   ➡️  Manually verify: Defect Report DHMS, NSV Cracks, CRRI PCI, NSV NHAI ONE")
elif not all_drive_files:
    print("\n❌ No files found in Drive folder")
else:
    # --- Find the Defect Report DHMS file ---
    d4_excels = [f for f in all_drive_files
                 if any(f['name'].lower().endswith(e) for e in ('.xlsx', '.xlsm', '.xls'))
                 and 'defect' in f['name'].lower() and 'dhms' in f['name'].lower()]

    # Also find NSV Cracks file (for cross-check if cracking missing in DHMS)
    nsv_cracks_files = [f for f in all_drive_files
                        if any(f['name'].lower().endswith(e) for e in ('.xlsx', '.xlsm', '.xls'))
                        and 'nsv' in f['name'].lower() and 'crack' in f['name'].lower()]

    if not d4_excels:
        print("\n❌ Defect Report DHMS file NOT FOUND")

    for d4f in d4_excels:
        print(f"\n✅ Found: {d4f['name']}")
        raw = service.files().get_media(fileId=d4f['id'], supportsAllDrives=True).execute()
        wb_d4_raw = load_workbook(io.BytesIO(raw), data_only=False, read_only=True)
        wb_d4 = load_workbook(io.BytesIO(raw), data_only=True, read_only=True)

        for sname in wb_d4.sheetnames:
            ws_d4 = wb_d4[sname]
            ws_d4_raw = wb_d4_raw[sname]
            all_rows = list(ws_d4.iter_rows(values_only=True))     # cached values
            raw_rows = list(ws_d4_raw.iter_rows(values_only=True)) # formulas visible
            if not all_rows:
                continue

            ncols = max(len(r) for r in all_rows)

            # --- Smart header detection: find where numeric data starts ---
            # Look for the Chainage column first (always present)
            ch_col_guess = None
            for i, row in enumerate(all_rows[:5]):
                for j, c in enumerate(row):
                    if c is not None and 'chainage' in str(c).strip().lower():
                        ch_col_guess = j
                        break
                if ch_col_guess is not None:
                    break

            # Find first row where chainage col has a number → that's data start
            data_start = None
            if ch_col_guess is not None:
                for i, row in enumerate(all_rows):
                    v = row[ch_col_guess] if ch_col_guess < len(row) else None
                    if v is not None:
                        try:
                            float(v)
                            data_start = i
                            break
                        except (ValueError, TypeError):
                            pass

            if data_start is None or data_start < 1:
                continue

            # Build composite headers with forward-fill for merged cells
            # Step 1: forward-fill each header row (merged cells appear as value + blanks)
            filled_rows = []
            for i in range(data_start):
                row_filled = []
                last_val = None
                for j in range(ncols):
                    v = all_rows[i][j] if j < len(all_rows[i]) else None
                    if v is not None and str(v).strip():
                        last_val = str(v).strip()
                        row_filled.append(last_val)
                    else:
                        row_filled.append(last_val)  # propagate merged cell value
                filled_rows.append(row_filled)

            # Step 2: combine filled rows top-to-bottom per column
            headers = []
            for j in range(ncols):
                parts = []
                seen = set()
                for i in range(len(filled_rows)):
                    v = filled_rows[i][j]
                    if v and v not in seen:
                        seen.add(v)
                        parts.append(v)
                headers.append(' | '.join(parts) if parts else '')

            headers_lower = [h.lower() for h in headers]

            # Separate real data rows from formula-only rows
            data_rows_all = list(zip(all_rows[data_start:], raw_rows[data_start:]))
            data_rows = []       # value tuples (for analysis)
            raw_data_rows = []   # raw tuples (formulas visible)
            formula_row_count = 0
            for val_row, raw_row in data_rows_all:
                if not any(c is not None for c in val_row) and not any(c is not None for c in raw_row):
                    continue
                # Check raw_row: if every non-None cell is a formula, skip it
                non_none_raw = [c for c in raw_row if c is not None]
                if non_none_raw and all(str(c).startswith('=') for c in non_none_raw):
                    formula_row_count += 1
                else:
                    data_rows.append(val_row)
                    raw_data_rows.append(raw_row)

            if not data_rows:
                continue

            print(f"\n{'─'*60}")
            print(f"Sheet: '{sname}' | Header rows: 1–{data_start} | {len(data_rows)} real data rows + {formula_row_count} formula rows | {len([h for h in headers if h])} columns")

            # --- CHECK 1: 10m chainage data ---
            ch_col = None
            for j, h in enumerate(headers_lower):
                if 'chainage' in h:
                    ch_col = j
                    break

            print(f"\n  📏 10m Chainage Check:")
            if ch_col is None:
                print(f"     ⚠️  No chainage column found")
            else:
                print(f"     Column: '{headers[ch_col]}'")
                ch_vals = []
                for r in data_rows:
                    v = r[ch_col] if ch_col < len(r) else None
                    if v is not None:
                        try:
                            ch_vals.append(float(v))
                        except (ValueError, TypeError):
                            pass

                if not ch_vals:
                    print(f"     ❌ No numeric chainage values")
                else:
                    ch_sorted = sorted(ch_vals)
                    start, end = ch_sorted[0], ch_sorted[-1]
                    print(f"     Range: {start} → {end} | Points: {len(ch_vals)}")

                    diffs = [round(ch_sorted[i+1] - ch_sorted[i], 4) for i in range(len(ch_sorted)-1)]
                    most_common = max(set(diffs), key=diffs.count) if diffs else 0
                    expected = round((end - start) / most_common) + 1 if most_common > 0 else 0
                    gaps = [(ch_sorted[i], ch_sorted[i+1], d) for i, d in enumerate(diffs) if abs(d - most_common) > 0.001]

                    print(f"     Step size: {most_common} | Expected: {expected} | Actual: {len(ch_vals)}")
                    if not gaps:
                        print(f"     ✅ Continuous, no gaps")
                    else:
                        print(f"     ❌ {len(gaps)} gap(s):")
                        for frm, to, d in gaps[:10]:
                            print(f"        {frm} → {to} (jump={d})")
                        if len(gaps) > 10:
                            print(f"        ...and {len(gaps)-10} more")

            # --- CHECK 2: Per-measurement lane coverage ---
            print(f"\n  📊 Measurement × Lane Coverage:")
            lanes = ['L4','L3','L2','L1','R1','R2','R3','R4']
            main_lanes = {'L3','L2','L1','R1','R2','R3'}
            meas_groups = {
                "Roughness BI":   "roughness bi",
                "Roughness IRI":  "roughness iri",
                "Rut Depth":      "rut depth",
                "Cracking (%)":   "cracking lane area (%)",
                "Cracking (sq.m)":"cracking lane area (sq",
                "Raveling":       "raveling",
                "Patching":       "patching",
                "Potholes":       "potholes",
                "VC Roughness CA": "visual condition - roughness (ca)",
                "VC Roughness IRC":"visual condition - roughness (irc)",
                "VC Rutting CA":   "visual condition - rutting (ca)",
                "VC Rutting IRC":  "visual condition - rutting (irc)",
                "VC Pothole CA":   "visual condition - pothole (ca)",
                "VC Pothole IRC":  "visual condition - pothole (irc)",
                "VC Cracks CA":    "visual condition - cracks (ca)",
                "VC Cracks IRC":   "visual condition - cracks (irc)",
            }

            # Build col fill stats — check raw formulas when cached values are missing
            col_fill = {}  # col_idx -> (filled, total)
            for j, h in enumerate(headers):
                if not h: continue
                vals = [r[j] if j < len(r) else None for r in data_rows]
                filled = sum(1 for v in vals if v is not None and str(v).strip() != '')
                # If no cached values, check raw rows for formulas (openpyxl data_only=True
                # returns None when formula results weren't cached at save time)
                if filled == 0 and raw_data_rows:
                    raws = [r[j] if j < len(r) else None for r in raw_data_rows]
                    filled = sum(1 for v in raws if v is not None and str(v).strip() != '')
                col_fill[j] = (filled, len(vals))

            # Map each header to its lane
            def get_lane(h):
                hu = h.upper()
                for ln in lanes:
                    if f'| {ln}' in hu or hu.endswith(f'| {ln}') or f'| {ln} |' in hu:
                        return ln
                return None

            cracking_missing_in_main = False
            critical_issues = []
            empty_lane_set = set()  # track which lanes are entirely empty

            # First pass: detect entirely empty lanes
            for ln in lanes:
                ln_cols = [j for j, h in enumerate(headers) if get_lane(h) == ln and h]
                if ln_cols and all(col_fill.get(j, (0,1))[0] == 0 for j in ln_cols):
                    empty_lane_set.add(ln)

            active_main = main_lanes - empty_lane_set

            for mname, mkey in meas_groups.items():
                # Find columns for this measurement across all lanes
                m_cols = [(j, h) for j, h in enumerate(headers) if mkey in h.lower()]
                if not m_cols:
                    print(f"     ❌ {mname} — no columns found")
                    if 'crack' in mname.lower():
                        cracking_missing_in_main = True
                    continue

                lanes_ok, lanes_empty, lanes_partial = [], [], []
                for j, h in m_cols:
                    ln = get_lane(h) or '?'
                    if ln in empty_lane_set:
                        continue  # skip entirely empty lanes (L4/R4)
                    filled, total = col_fill.get(j, (0, 1))
                    if filled == 0:
                        lanes_empty.append(ln)
                    elif filled == total:
                        lanes_ok.append(ln)
                    else:
                        lanes_partial.append((ln, filled, total))

                if not lanes_ok and not lanes_partial:
                    if lanes_empty:
                        print(f"     ❌ {mname} — EMPTY in {', '.join(sorted(lanes_empty))}")
                    else:
                        print(f"     ❌ {mname} — no columns found")
                    if 'crack' in mname.lower():
                        cracking_missing_in_main = True
                    for ln in lanes_empty:
                        if ln in active_main:
                            critical_issues.append(f"{ln}: {mname}")
                elif lanes_empty:
                    empty_main = [ln for ln in lanes_empty if ln in main_lanes]
                    print(f"     ⚠️  {mname} — OK: {','.join(lanes_ok)} | EMPTY: {','.join(lanes_empty)}")
                    if empty_main:
                        for ln in empty_main:
                            critical_issues.append(f"{ln}: {mname}")
                        if 'crack' in mname.lower():
                            cracking_missing_in_main = True
                else:
                    print(f"     ✅ {mname} — {','.join(lanes_ok)}")

            # Report empty lanes
            if empty_lane_set:
                print(f"\n     ⚪ Skipped lanes (no data, likely don't exist): {', '.join(sorted(empty_lane_set))}")
                print(f"     📋 Active main lanes: {', '.join(sorted(active_main))}")

            # --- CHECK 3: NSV Cracks cross-check if cracking missing ---
            if cracking_missing_in_main and nsv_cracks_files:
                print(f"\n  🔍 Cracking missing in DHMS — checking NSV Cracks file...")
                for nf in nsv_cracks_files:
                    print(f"     📄 {nf['name']}")
                    try:
                        nraw = service.files().get_media(fileId=nf['id'], supportsAllDrives=True).execute()
                        wb_nsv_r = load_workbook(io.BytesIO(nraw), data_only=False, read_only=True)
                        wb_nsv_v = load_workbook(io.BytesIO(nraw), data_only=True, read_only=True)
                        for ns in wb_nsv_v.sheetnames:
                            nsv_rows_r = list(wb_nsv_r[ns].iter_rows(values_only=True))
                            nsv_rows_v = list(wb_nsv_v[ns].iter_rows(values_only=True))
                            real_nsv = sum(1 for rr, rv in zip(nsv_rows_r, nsv_rows_v)
                                           if any(c is not None for c in rv)
                                           and not all(str(c).startswith('=') for c in rr if c is not None))
                            if real_nsv > 1:
                                print(f"        ✅ Sheet '{ns}' — {real_nsv - 1} real data rows (cracking data available here)")
                            elif real_nsv <= 1:
                                print(f"        ❌ Sheet '{ns}' — empty")
                        wb_nsv_r.close()
                        wb_nsv_v.close()
                    except Exception as e:
                        print(f"        ⚠️  Could not read: {e}")
            elif cracking_missing_in_main:
                print(f"\n  ❌ Cracking missing in DHMS and NO NSV Cracks file found in D4!")

            # Final verdict
            print(f"\n  {'='*50}")
            if critical_issues:
                active_str = ', '.join(sorted(active_main))
                print(f"  🚨 CRITICAL — Data missing in active lanes ({active_str}):")
                for c in sorted(set(critical_issues)):
                    print(f"     • {c}")
                if cracking_missing_in_main and nsv_cracks_files:
                    print(f"     ℹ️  Cracking data available in NSV Cracks file (see above)")
            else:
                print(f"  ✅ All main lanes (L1-L3, R1-R3) look good.")
                if empty_lane_set:
                    print(f"     ({', '.join(sorted(empty_lane_set))} empty — lanes don't exist for this stretch)")

        wb_d4.close()
        wb_d4_raw.close()

# COMMAND ----------

# DBTITLE 1,SOP Validation Report — Combined Summary
import io

# ==============================================================
# SOP – Deliverable Validation (combines cells 7, 8, 9)
# ==============================================================
comments = []
status = "Complete"
is_sp = 'sharepoint' in url.lower() or 'onedrive' in url.lower()

# Read UPC from the workbook for context
upc_cell = ws.cell(row_num, 6)
upc = str(upc_cell.value).strip() if upc_cell.value else f"Row {row_num}"

# ---- STEP 1: Access check ----
if is_sp:
    status = "No access"
    comments.append("Link is SharePoint/OneDrive — cannot inspect via API")

if not is_sp:
    # ---- STEP 2: Deliverable presence (D1, D2, D4) ----
    lower_names = [n.lower() for n in all_names]
    expected = {
        "D1": {"LRMS": ["lrms"], "RIS": ["ris"], "Visual Condition": ["visual condition", "visual_condition", "visual-condition"]},
        "D2": {"Comparison as per CA/IRC": ["comparison", "ca/irc", "ca irc"]},
        "D4": {
            "Defect Report DHMS": ["defect report", "dhms"],
            "CRRI PCI": ["crri", "pci"],
            "NSV Individual Cracks": ["nsv cracks", "individual crack"],
            "NSV NHAI ONE": ["nsv nhai", "nhai one"],
        },
    }
    for deliv, items in expected.items():
        for label, kws in items.items():
            if not any(kw in nm for nm in lower_names for kw in kws):
                comments.append(f"{deliv} – {label} missing")
                if 'comparison' in label.lower() or 'defect report' in label.lower():
                    status = "Missing"

    # ---- STEP 3: D2 CA/IRC sheet check ----
    d2_files = [f for f in all_drive_files
                if any(f['name'].lower().endswith(e) for e in ('.xlsx','.xlsm','.xls'))
                and ('comparison' in f['name'].lower() or 'd2' in f['name'].lower())]
    if d2_files:
        try:
            d2_raw = service.files().get_media(fileId=d2_files[0]['id'], supportsAllDrives=True).execute()
            wb2_raw = load_workbook(io.BytesIO(d2_raw), data_only=False, read_only=True)
            wb2_val = load_workbook(io.BytesIO(d2_raw), data_only=True, read_only=True)
            smap = {s.lower(): s for s in wb2_raw.sheetnames}
            d2_info = {}  # {lbl: {rows, step}}
            for req, lbl in [("comparison as per ca","CA"),("comparison as per irc","IRC")]:
                actual = smap.get(req)
                if not actual:
                    comments.append(f"D2 – {lbl} comparison sheet missing")
                    if status == "Complete": status = "Partial"
                    d2_info[lbl] = {"rows": 0, "step": None}
                    continue
                rows_raw = list(wb2_raw[actual].iter_rows(values_only=True))
                rows_val = list(wb2_val[actual].iter_rows(values_only=True))
                # Smart multi-row header: find chainage col → data start
                ch_j = None
                for i_h, row in enumerate(rows_val[:6]):
                    for j_h, c in enumerate(row):
                        if c is not None and 'chainage' in str(c).strip().lower():
                            ch_j = j_h; break
                    if ch_j is not None: break
                ds = None
                if ch_j is not None:
                    for i_h, row in enumerate(rows_val):
                        v = row[ch_j] if ch_j < len(row) else None
                        if v is not None:
                            try: float(v); ds = i_h; break
                            except: pass
                if ds is None or ds < 1:
                    for i_h, rv in enumerate(rows_val):
                        if any(c is not None for c in rv): ds = i_h + 1; break
                if ds is None:
                    d2_info[lbl] = {"rows": 0, "step": None}; continue
                # Count real data rows from data_start
                real, real_vals = 0, []
                for rv, rr in zip(rows_val[ds:], rows_raw[ds:]):
                    if not any(c is not None for c in rv) and not any(c is not None for c in rr): continue
                    nn = [c for c in rr if c is not None]
                    if nn and all(str(c).startswith('=') for c in nn): continue
                    real += 1; real_vals.append(rv)
                # Detect chainage step
                ch_step = None
                if real_vals:
                    if ch_j is None:
                        best_j, best_seq = None, 0
                        ncols_d2 = max((len(r) for r in real_vals), default=0)
                        for j in range(min(10, ncols_d2)):
                            nums = []
                            for r in real_vals[:50]:
                                v = r[j] if j < len(r) else None
                                if v is not None:
                                    try: nums.append(float(v))
                                    except: pass
                            if len(nums) >= 5:
                                inc = sum(1 for i in range(len(nums)-1) if nums[i+1] > nums[i])
                                if inc > best_seq: best_seq = inc; best_j = j
                        if best_j is not None and best_seq >= 4: ch_j = best_j
                    if ch_j is not None:
                        cvs = []
                        for r in real_vals:
                            v = r[ch_j] if ch_j < len(r) else None
                            if v is not None:
                                try: cvs.append(float(v))
                                except: pass
                        if len(cvs) >= 2:
                            cs = sorted(cvs)
                            dfs = [round(cs[i+1]-cs[i],4) for i in range(len(cs)-1) if cs[i+1]-cs[i]>0]
                            if dfs: ch_step = max(set(dfs), key=dfs.count)
                d2_info[lbl] = {"rows": real, "step": ch_step}
                if real == 0:
                    comments.append(f"D2 – {lbl} comparison sheet empty")
                    if status == "Complete": status = "Partial"
            # Cross-check CA vs IRC
            ca_i, irc_i = d2_info.get("CA",{}), d2_info.get("IRC",{})
            ca_r, irc_r = ca_i.get("rows",0), irc_i.get("rows",0)
            ca_st, irc_st = ca_i.get("step"), irc_i.get("step")
            if ca_r > 0 and irc_r > 0 and ca_r != irc_r:
                if ca_st and irc_st and ca_st != irc_st:
                    comments.append(f"D2 – CA/IRC different granularity: CA={ca_r} rows (step {ca_st}m), IRC={irc_r} rows (step {irc_st}m)")
                else:
                    comments.append(f"D2 – CA/IRC row mismatch (CA={ca_r}, IRC={irc_r})")
                    if status == "Complete": status = "Partial"
            elif ca_r > 0 and irc_r == 0:
                comments.append(f"D2 – IRC empty but CA has {ca_r} rows")
            elif irc_r > 0 and ca_r == 0:
                comments.append(f"D2 – CA empty but IRC has {irc_r} rows")
            wb2_raw.close(); wb2_val.close()
        except Exception as e:
            comments.append(f"D2 – could not read: {e}")

    # ---- STEP 4-6: D4 DHMS — 10m data + defect columns ----
    d4_files = [f for f in all_drive_files
                if any(f['name'].lower().endswith(e) for e in ('.xlsx','.xlsm','.xls'))
                and 'defect' in f['name'].lower() and 'dhms' in f['name'].lower()]
    nsv_files = [f for f in all_drive_files
                 if any(f['name'].lower().endswith(e) for e in ('.xlsx','.xlsm','.xls'))
                 and 'nsv' in f['name'].lower() and 'crack' in f['name'].lower()]

    if not d4_files:
        status = "Missing"
        comments.append("D4 – Defect Report DHMS not found")
    else:
        try:
            d4_raw = service.files().get_media(fileId=d4_files[0]['id'], supportsAllDrives=True).execute()
            wb4v = load_workbook(io.BytesIO(d4_raw), data_only=True, read_only=True)
            wb4r = load_workbook(io.BytesIO(d4_raw), data_only=False, read_only=True)

            for sn in wb4v.sheetnames:
                rows_v = list(wb4v[sn].iter_rows(values_only=True))
                rows_r = list(wb4r[sn].iter_rows(values_only=True))
                if not rows_v: continue
                nc = max(len(r) for r in rows_v)

                # Find chainage col & data start
                chc = None
                for i, row in enumerate(rows_v[:5]):
                    for j, c in enumerate(row):
                        if c is not None and 'chainage' in str(c).lower(): chc = j; break
                    if chc is not None: break
                ds = None
                if chc is not None:
                    for i, row in enumerate(rows_v):
                        v = row[chc] if chc < len(row) else None
                        if v is not None:
                            try: float(v); ds = i; break
                            except: pass
                if ds is None or ds < 1: continue

                # Forward-fill headers
                frows = []
                for i in range(ds):
                    fr, lv = [], None
                    for j in range(nc):
                        v = rows_v[i][j] if j < len(rows_v[i]) else None
                        if v is not None and str(v).strip(): lv = str(v).strip(); fr.append(lv)
                        else: fr.append(lv)
                    frows.append(fr)
                hdrs = []
                for j in range(nc):
                    pts, seen = [], set()
                    for i in range(len(frows)):
                        v = frows[i][j]
                        if v and v not in seen: seen.add(v); pts.append(v)
                    hdrs.append(' | '.join(pts) if pts else '')

                # Real data rows only
                data = []
                raw_data = []
                for vr, rr in zip(rows_v[ds:], rows_r[ds:]):
                    if not any(c is not None for c in vr) and not any(c is not None for c in rr): continue
                    nn = [c for c in rr if c is not None]
                    if nn and all(str(c).startswith('=') for c in nn): continue
                    data.append(vr)
                    raw_data.append(rr)
                if not data: continue

                # --- 10m chainage gaps ---
                chv = []
                for r in data:
                    v = r[chc] if chc < len(r) else None
                    if v is not None:
                        try: chv.append(float(v))
                        except: pass
                if not chv:
                    comments.append("D4 – 10m data missing")
                    if status == "Complete": status = "Partial"
                else:
                    cs = sorted(chv)
                    dfs = [round(cs[i+1]-cs[i],4) for i in range(len(cs)-1)]
                    if dfs:
                        mc = max(set(dfs), key=dfs.count)
                        gps = [(cs[i],cs[i+1]) for i,d in enumerate(dfs) if abs(d-mc)>0.001]
                        if gps:
                            gs = "; ".join(f"{a}→{b}" for a,b in gps[:3])
                            comments.append(f"D4 – 10m chainage gaps: {gs}{'...' if len(gps)>3 else ''}")
                            if status == "Complete": status = "Partial"

                # --- Measurement x lane ---
                lns = ['L4','L3','L2','L1','R1','R2','R3','R4']
                main_lns = {'L3','L2','L1','R1','R2','R3'}
                def glane(h):
                    hu = h.upper()
                    for ln in lns:
                        if f'| {ln}' in hu or hu.endswith(f'| {ln}') or f'| {ln} |' in hu: return ln
                    return None
                cfill = {}
                for j, h in enumerate(hdrs):
                    if not h: continue
                    vs = [r[j] if j < len(r) else None for r in data]
                    filled = sum(1 for v in vs if v is not None and str(v).strip() != '')
                    # If no cached values, check raw rows for formulas
                    if filled == 0 and raw_data:
                        rvs = [r[j] if j < len(r) else None for r in raw_data]
                        filled = sum(1 for v in rvs if v is not None and str(v).strip() != '')
                    cfill[j] = filled
                elanes = set()
                for ln in lns:
                    lc = [j for j, h in enumerate(hdrs) if glane(h)==ln and h]
                    if lc and all(cfill.get(j,0)==0 for j in lc): elanes.add(ln)

                mgs = {
                    "Roughness BI":"roughness bi", "Roughness IRI":"roughness iri",
                    "Rut Depth":"rut depth",
                    "Cracking Area %":"cracking lane area (%)", "Cracking Area sq.m":"cracking lane area (sq",
                    "Raveling":"raveling", "Patching":"patching", "Potholes":"potholes",
                    "VC Roughness CA":"visual condition - roughness (ca)", "VC Roughness IRC":"visual condition - roughness (irc)",
                    "VC Rutting CA":"visual condition - rutting (ca)", "VC Rutting IRC":"visual condition - rutting (irc)",
                    "VC Pothole CA":"visual condition - pothole (ca)", "VC Pothole IRC":"visual condition - pothole (irc)",
                    "VC Cracks CA":"visual condition - cracks (ca)", "VC Cracks IRC":"visual condition - cracks (irc)",
                }
                empty_m = []
                for mn, mk in mgs.items():
                    mc = [(j,h) for j,h in enumerate(hdrs) if mk in h.lower()]
                    if not mc: continue
                    has = any(cfill.get(j,0)>0 for j,h in mc if glane(h) not in elanes)
                    if not has: empty_m.append(mn)

                if empty_m:
                    crack_m = [m for m in empty_m if 'crack' in m.lower()]
                    other_m = [m for m in empty_m if 'crack' not in m.lower()]
                    if crack_m and nsv_files:
                        comments.append(f"D4 – 10m: {', '.join(crack_m)} empty in DHMS (available in NSV Cracks file)")
                    elif crack_m:
                        comments.append(f"D4 – 10m: {', '.join(crack_m)} empty")
                    if other_m:
                        comments.append(f"D4 – 10m: {', '.join(other_m)} empty")
                    if status == "Complete": status = "Partial"

            wb4v.close(); wb4r.close()
        except Exception as e:
            comments.append(f"D4 – could not read: {e}")

# ==============================================================
# FINAL SOP OUTPUT
# ==============================================================
comment_str = ' ; '.join(comments) if comments else 'All checks passed'

print(f"{'='*70}")
print(f"  SOP DELIVERABLE VALIDATION REPORT")
print(f"{'='*70}")
print(f"  Row:     {row_num}")
print(f"  UPC:     {upc}")
print(f"  URL:     {url[:80]}{'...' if len(url)>80 else ''}")
print(f"{'─'*70}")
print(f"  Status:  {status}")
print(f"{'─'*70}")
print(f"  Comment:")
if comments:
    for c in comments:
        print(f"    • {c}")
else:
    print(f"    All checks passed")
print(f"{'─'*70}")
print(f"  Tracker: {comment_str}")
print(f"{'='*70}")
print(f"\n  Status legend:")
print(f"    Complete – All checks passed")
print(f"    Partial  – Files present; some 10m data or defect columns empty")
print(f"    Missing  – D2 comparison or D4 Defect Report DHMS not present")
print(f"    No access – Link broken or access denied")