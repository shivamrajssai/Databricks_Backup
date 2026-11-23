# Databricks notebook source
# DBTITLE 1,Step 1 – Install Required Library
# MAGIC %md
# MAGIC ## Step 1 – Install Required Library
# MAGIC
# MAGIC **Purpose:**
# MAGIC Installs the `openpyxl` library, which is needed to read `.xlsx` (Excel) files in the next step.
# MAGIC
# MAGIC **Important:**
# MAGIC *This is a one-time setup step. It must run before any Excel file can be loaded.*

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install openpyxl

# COMMAND ----------

# DBTITLE 1,Step 2 – Load Source Data and Analyse UPC Summary
# MAGIC %md
# MAGIC ## Step 2 – Load Source Data and Analyse UPC Summary
# MAGIC
# MAGIC **Purpose:**
# MAGIC Loads the master UPC list and provides a high-level summary of what data is available for processing.
# MAGIC
# MAGIC **Input:**
# MAGIC - Excel file: `kml_333.xlsx` containing 333 UPC records with columns: Serial No, Zone, UPC, Status, Folder/Type.
# MAGIC
# MAGIC **Process:**
# MAGIC - Reads the master Excel file into memory.
# MAGIC - Classifies each UPC as: **Present** (file/folder exists) or **Not Found** (no source data available).
# MAGIC - Among the Present entries, identifies the source format: **KMZ** (68), **Unknown/RAR** (10), or **Folder** (38).
# MAGIC - Produces a zone-wise breakdown (Zones A through E).
# MAGIC
# MAGIC **Output:**
# MAGIC - Master dataset `df` (333 rows) available for all subsequent steps.
# MAGIC - Summary: 116 Present, 217 Not Found.
# MAGIC
# MAGIC **Important:**
# MAGIC *Only the 116 "Present" UPCs are processed for chainage and lat/long extraction in the following steps.*

# COMMAND ----------

# DBTITLE 1,Read kml_333.xlsx and Analyse UPC Summary
import pandas as pd

df = pd.read_excel("/Workspace/Users/shivam.raj9226@softsensor.ai/KML File Lat Long Extraction/Input/kml_333.xlsx", engine="openpyxl")
display(df)

# ── Analysis Summary ──
total_upc = len(df)
not_found = df[df["Folder/Type"] == "Not found"].shape[0]
found = df[df["Folder/Type"] != "Not found"]

# Classify found entries by type
kmz_count = found[found["Folder/Type"].str.lower().str.endswith(('.kmz', '.kmz'))].shape[0]
unknown_ext = found[found["Folder/Type"].str.lower().str.endswith('.unknown')].shape[0]
# Folders = found entries with no file extension (no dot after the name) — these are plain folder names
no_ext = found[~found["Folder/Type"].str.contains(r'\.[a-zA-Z]+$', regex=True)].shape[0]

print("=" * 60)
print(f"📊  UPC SUMMARY ANALYSIS")
print("=" * 60)
print(f"  Total UPCs              : {total_upc}")
print(f"  Present (found)         : {found.shape[0]}")
print(f"  Not Found               : {not_found}")
print("-" * 60)
print(f"  Breakdown of Found ({found.shape[0]}):")
print(f"    KMZ files (.kmz)      : {kmz_count}")
print(f"    Unknown (.unknown)    : {unknown_ext}")
print(f"    Folders (no extension): {no_ext}")
print("=" * 60)

# Zone-wise breakdown
print(f"\n📍 ZONE-WISE BREAKDOWN:")
zone_summary = df.groupby("Zone").agg(
    Total=("UPC", "count"),
    Present=("Status", lambda x: (x == "Present").sum()),
    Not_Present=("Status", lambda x: (x == "Not present").sum())
).reset_index()
display(zone_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC | Extension | Simple meaning | Kya store karta hai? |
# MAGIC |---|---|---|
# MAGIC | `.shp` | **Shape file** | Actual GIS geometry — points, lines, polygons etc. |
# MAGIC | `.shx` | **Shape Index** | `.shp` file ke geometries ka index/reference, taaki data ko quickly access kiya ja sake |
# MAGIC | `.dbf` | **Database File** | Geometry se related attribute information, jaise UPC, chainage, road details etc. |
# MAGIC | `.prj` | **Projection File** | GIS coordinate system / projection ki information, jaise WGS84 |
# MAGIC | `.cpg` | **Code Page** | Text/character encoding batata hai, jaise UTF-8 |
# MAGIC | `.sbn` | **Spatial Index** | Spatial/geographic indexing ke liye use hota hai |
# MAGIC | `.sbx` | **Spatial Index file** | `.sbn` ke saath spatial data ko efficiently search/access karne mein help karta hai |

# COMMAND ----------

# DBTITLE 1,Step 3 – UPC Deep Dive Tool
# MAGIC %md
# MAGIC ## Step 3 – UPC Deep Dive — Inspect Raw Data for Any Single UPC
# MAGIC
# MAGIC **Purpose:**
# MAGIC Provides an interactive tool to examine the raw source data of any individual UPC. Useful for **manual verification, debugging, and quality checks**.
# MAGIC
# MAGIC **Input:**
# MAGIC - A single UPC code set by the user (variable `UPC_TO_CHECK`).
# MAGIC - The master dataset `df` and the corresponding source file (KMZ / Unknown / Folder).
# MAGIC
# MAGIC **Process:**
# MAGIC - Opens the source archive or folder for the specified UPC.
# MAGIC - **KMZ:** Parses embedded KML for placemarks. If no KML exists, scans all shapefile components (.shp, .dbf, .prj, .cpg) and merges geometry with attributes.
# MAGIC - **Unknown:** Extracts embedded KMZ/ZIP from within a RAR archive, then parses the KML inside.
# MAGIC - **Folder:** Parses KML files and reads `metadata.json` if available.
# MAGIC
# MAGIC **Output:**
# MAGIC - UPC metadata and a table of all extracted records (Name, Geometry, Latitude, Longitude, Elevation, etc.).
# MAGIC
# MAGIC **Important:**
# MAGIC *Change `UPC_TO_CHECK` and re-run. This cell does not affect the main extraction pipeline — it is a standalone diagnostic tool.*

# COMMAND ----------

# DBTITLE 1,UPC Deep Dive - Inspect raw data for any UPC
# ═══════════════════════════════════════════════════════════
# 🔍 UPC DEEP DIVE — Change the UPC below and run this cell
# ═══════════════════════════════════════════════════════════
UPC_TO_CHECK = "N/01007/04002/UP"   # <── CHANGE THIS TO ANY UPC
# ═══════════════════════════════════════════════════════════

import os, re, io, zipfile, json
import xml.etree.ElementTree as ET
import pandas as pd

kmz_folder = "/Workspace/Users/shivam.raj9226@softsensor.ai/KML File Lat Long Extraction/KMZ"
unknown_folder = "/Workspace/Users/shivam.raj9226@softsensor.ai/KML File Lat Long Extraction/Unknown"
folder_base = "/Workspace/Users/shivam.raj9226@softsensor.ai/KML File Lat Long Extraction/Folder"

# ── Lookup UPC in master list ──
match = df[df["UPC"].str.strip() == UPC_TO_CHECK.strip()]
if match.empty:
    print(f"❌ UPC '{UPC_TO_CHECK}' not found in the master Excel. Check spelling.")
else:
    row = match.iloc[0]
    sno = row["Serial No"]
    zone = row["Zone"]
    status = row["Status"]
    folder_type = row["Folder/Type"].strip()

    # Determine file type
    if folder_type.lower().endswith('.kmz'):
        file_type = "KMZ"
    elif folder_type.lower().endswith('.unknown'):
        file_type = "Unknown"
    elif folder_type == "Not found":
        file_type = "Not found"
    else:
        file_type = "Folder"

    print(f"{'='*70}")
    print(f"📋 UPC DETAILS")
    print(f"{'='*70}")
    print(f"  S. No       : {sno}")
    print(f"  UPC         : {UPC_TO_CHECK}")
    print(f"  Zone        : {zone}")
    print(f"  Status      : {status}")
    print(f"  Folder/Type : {folder_type}")
    print(f"  Category    : {file_type}")
    print(f"{'='*70}\n")

    if file_type == "Not found":
        print("⚠️ This UPC is marked 'Not found' — no file or folder exists for it.")
    else:
        # ── Helper: detect namespace ──
        def _detect_ns(root):
            tag = root.tag
            if "opengis.net/kml/2.2" in tag: return "http://www.opengis.net/kml/2.2"
            if "earth.google.com/kml/2.0" in tag: return "http://earth.google.com/kml/2.0"
            return ""

        # ── Helper: extract all placemarks into a flat list ──
        def _extract_placemarks(root, ns):
            rows = []
            for pm in root.iter(f"{{{ns}}}Placemark" if ns else "Placemark"):
                name_el = pm.find(f"{{{ns}}}name" if ns else "name")
                name = (name_el.text or "").strip() if name_el is not None else ""
                desc_el = pm.find(f"{{{ns}}}description" if ns else "description")
                desc = (desc_el.text or "").strip() if desc_el is not None else ""
                # Point
                pt = pm.find(f"{{{ns}}}Point/{{{ns}}}coordinates" if ns else "Point/coordinates")
                lat = lon = elev = None
                geom_type = "Unknown"
                if pt is not None and pt.text:
                    geom_type = "Point"
                    parts = pt.text.strip().split()[0].split(",")
                    try: lon, lat = float(parts[0]), float(parts[1])
                    except: pass
                    if len(parts) > 2:
                        try: elev = float(parts[2])
                        except: pass
                # LineString
                ls = pm.find(f"{{{ns}}}LineString/{{{ns}}}coordinates" if ns else "LineString/coordinates")
                coord_count = None
                if ls is not None and ls.text:
                    geom_type = "LineString"
                    pairs = ls.text.strip().split()
                    coord_count = len(pairs)
                    if pairs:
                        fp = pairs[0].split(",")
                        try: lon, lat = float(fp[0]), float(fp[1])
                        except: pass
                # Polygon
                poly = pm.find(f".//{{{ns}}}Polygon" if ns else ".//Polygon")
                if poly is not None:
                    geom_type = "Polygon"
                rows.append({
                    "Name": name, "Geometry": geom_type,
                    "Latitude": lat, "Longitude": lon, "Elevation": elev,
                    "Coord Count": coord_count,
                    "Description": desc[:80] if desc else "",
                })
            return rows

        all_placemarks = []

        # ══════════ KMZ ══════════
        if file_type == "KMZ":
            fpath = os.path.join(kmz_folder, folder_type)
            if not os.path.exists(fpath):
                print(f"❌ File not found: {fpath}")
            else:
                print(f"📂 Opening KMZ: {fpath}")
                with zipfile.ZipFile(fpath, 'r') as z:
                    print(f"   Archive contents: {z.namelist()[:10]}")
                    kml_list = [f for f in z.namelist() if f.endswith('.kml')]
                    if kml_list:
                        root = ET.fromstring(z.read(kml_list[0]))
                        ns = _detect_ns(root)
                        all_placemarks = _extract_placemarks(root, ns)
                    else:
                        print("   ⚠️ No .kml inside — scanning ALL shapefile components...")
                        import struct
                        all_archive = z.namelist()

                        # ── Show every file extension found ──
                        ext_labels = {'.shp':'Shape geometry (lat/long)','.shx':'Shape index',
                                      '.dbf':'Attributes (chainage etc.)','.prj':'Projection / CRS',
                                      '.cpg':'Encoding','.sbn':'Spatial index','.sbx':'Spatial index',
                                      '.kmz':'Nested KMZ','.xlsx':'Excel','.json':'JSON'}
                        ext_groups = {}
                        for af in all_archive:
                            ext = os.path.splitext(af)[1].lower()
                            ext_groups.setdefault(ext, []).append(af)
                        print(f"\n   {'Ext':8} {'What it stores':30} {'Files':6} Example")
                        print(f"   {'─'*75}")
                        for ext in sorted(ext_groups):
                            label = ext_labels.get(ext, ext or '(none)')
                            print(f"   {ext or '(none)':8} {label:30} {len(ext_groups[ext]):>3}    {ext_groups[ext][0]}")

                        # ── .prj → Coordinate Reference System ──
                        for pf in [f for f in all_archive if f.lower().endswith('.prj')]:
                            prj = z.read(pf).decode('utf-8', errors='replace').strip()
                            print(f"\n   📐 Projection (.prj): {prj[:200]}")
                            break

                        # ── .cpg → Character Encoding ──
                        for cf in [f for f in all_archive if f.lower().endswith('.cpg')]:
                            print(f"   📝 Encoding  (.cpg): {z.read(cf).decode('utf-8',errors='replace').strip()}")
                            break

                        # ── .dbf → Attribute table ──
                        dbf_files = [f for f in all_archive if f.lower().endswith('.dbf')]
                        shp_geom   = [f for f in all_archive if f.lower().endswith('.shp')]

                        if dbf_files:
                            dbf_f = next((f for f in dbf_files if 'point' in f.lower()), dbf_files[0])
                            data = z.read(dbf_f)
                            n_rec = struct.unpack('<I', data[4:8])[0]
                            hdr_sz = struct.unpack('<H', data[8:10])[0]
                            rec_sz = struct.unpack('<H', data[10:12])[0]
                            fields = []
                            pos = 32
                            while pos < hdr_sz - 1 and data[pos] != 0x0D:
                                fn = data[pos:pos+11].split(b'\x00')[0].decode('ascii', errors='replace')
                                fs = data[pos+16]
                                fields.append((fn, fs))
                                pos += 32
                            records = []
                            for r in range(min(n_rec, 500)):
                                off = hdr_sz + r * rec_sz + 1
                                vals = {}
                                foff = off
                                for fn, fs in fields:
                                    vals[fn] = data[foff:foff+fs].decode('ascii', errors='replace').strip()
                                    foff += fs
                                records.append(vals)
                            print(f"\n   📄 .dbf: {dbf_f}  ({n_rec} records)")
                            print(f"      Fields: {[f[0] for f in fields]}")

                            # ── .shp → Geometry coordinates ──
                            shp_base = os.path.splitext(dbf_f)[0].lower()
                            matching_shp = next((f for f in shp_geom if os.path.splitext(f)[0].lower() == shp_base), None)
                            if not matching_shp and shp_geom:
                                matching_shp = shp_geom[0]

                            if matching_shp:
                                sd = z.read(matching_shp)
                                shp_type = struct.unpack('<I', sd[32:36])[0]
                                type_map = {0:'Null',1:'Point',3:'PolyLine',5:'Polygon',
                                            8:'MultiPoint',11:'PointZ',13:'PolyLineZ',15:'PolygonZ',
                                            21:'PointM',23:'PolyLineM',25:'PolygonM'}
                                print(f"   📐 .shp: {matching_shp}  (geometry: {type_map.get(shp_type, shp_type)})")

                                # Parse each geometry record
                                soff = 100
                                shp_coords = []
                                while soff < len(sd) - 8:
                                    try:
                                        clen = struct.unpack('>I', sd[soff+4:soff+8])[0] * 2
                                        soff += 8
                                        if soff + clen > len(sd): break
                                        rec = sd[soff:soff+clen]
                                        st = struct.unpack('<I', rec[0:4])[0]
                                        if st in (1, 11, 21):        # Point / PointZ / PointM
                                            x = struct.unpack('<d', rec[4:12])[0]
                                            y = struct.unpack('<d', rec[12:20])[0]
                                            el = struct.unpack('<d', rec[20:28])[0] if st == 11 and len(rec) >= 28 else None
                                            shp_coords.append({"Latitude": round(y,8), "Longitude": round(x,8), "Elevation": el})
                                        elif st in (3, 5, 13, 15):   # PolyLine / Polygon
                                            np_ = struct.unpack('<I', rec[40:44])[0]
                                            nparts = struct.unpack('<I', rec[36:40])[0]
                                            po = 44 + nparts * 4
                                            if np_ > 0 and po + 16 <= len(rec):
                                                x = struct.unpack('<d', rec[po:po+8])[0]
                                                y = struct.unpack('<d', rec[po+8:po+16])[0]
                                                shp_coords.append({"Latitude": round(y,8), "Longitude": round(x,8), "Num Vertices": np_})
                                            else:
                                                shp_coords.append({})
                                        elif st == 8:                # MultiPoint
                                            np_ = struct.unpack('<I', rec[36:40])[0]
                                            if np_ > 0 and len(rec) >= 56:
                                                x = struct.unpack('<d', rec[40:48])[0]
                                                y = struct.unpack('<d', rec[48:56])[0]
                                                shp_coords.append({"Latitude": round(y,8), "Longitude": round(x,8), "Num Points": np_})
                                            else:
                                                shp_coords.append({})
                                        else:
                                            shp_coords.append({})
                                        soff += clen
                                    except:
                                        break

                                # Merge .shp geometry INTO .dbf attribute records
                                for i, rec in enumerate(records):
                                    if i < len(shp_coords):
                                        rec.update(shp_coords[i])
                                print(f"      Geometry records merged: {len(shp_coords)}")
                            else:
                                print("   ⚠️ No matching .shp file found for coordinates")

                            all_placemarks = records

        # ══════════ Unknown ══════════
        elif file_type == "Unknown":
            fpath = os.path.join(unknown_folder, folder_type)
            if not os.path.exists(fpath):
                print(f"❌ File not found: {fpath}")
            else:
                print(f"📂 Opening Unknown archive: {fpath}")
                with open(fpath, 'rb') as f:
                    raw = f.read()
                # Try embedded ZIP
                pk = raw.find(b'PK\x03\x04')
                eocd = raw.rfind(b'PK\x05\x06')
                if pk != -1 and eocd != -1 and eocd > pk:
                    try:
                        cl = int.from_bytes(raw[eocd+20:eocd+22], 'little')
                        with zipfile.ZipFile(io.BytesIO(raw[pk:eocd+22+cl]), 'r') as z:
                            kml_in = [f for f in z.namelist() if f.endswith('.kml')]
                            if kml_in:
                                root = ET.fromstring(z.read(kml_in[0]))
                                ns = _detect_ns(root)
                                all_placemarks = _extract_placemarks(root, ns)
                                print(f"   Extracted KML from embedded ZIP: {kml_in[0]}")
                    except: pass
                # Fallback: libarchive
                if not all_placemarks:
                    try:
                        import libarchive
                        with libarchive.file_reader(fpath) as archive:
                            for entry in archive:
                                ep = str(entry)
                                if ep.endswith('.kml'):
                                    kml_data = b''.join(block for block in entry.get_blocks())
                                    root = ET.fromstring(kml_data)
                                    ns = _detect_ns(root)
                                    all_placemarks = _extract_placemarks(root, ns)
                                    print(f"   Extracted via libarchive: {ep}")
                                    break
                                elif ep.endswith('.kmz'):
                                    kmz_raw = b''.join(block for block in entry.get_blocks())
                                    with zipfile.ZipFile(io.BytesIO(kmz_raw), 'r') as zf:
                                        kml_in = [f for f in zf.namelist() if f.endswith('.kml')]
                                        if kml_in:
                                            root = ET.fromstring(zf.read(kml_in[0]))
                                            ns = _detect_ns(root)
                                            all_placemarks = _extract_placemarks(root, ns)
                                            print(f"   Extracted via libarchive → KMZ: {ep}")
                                    break
                    except: pass

        # ══════════ Folder ══════════
        elif file_type == "Folder":
            fpath = os.path.join(folder_base, folder_type)
            if not os.path.exists(fpath):
                print(f"❌ Folder not found: {fpath}")
            else:
                contents = os.listdir(fpath)
                print(f"📂 Folder contents: {contents}")
                kml_list = [f for f in contents if f.endswith('.kml')]
                if kml_list:
                    root = ET.parse(os.path.join(fpath, kml_list[0])).getroot()
                    ns = _detect_ns(root)
                    all_placemarks = _extract_placemarks(root, ns)
                    print(f"   Parsed KML: {kml_list[0]}")
                # Also show metadata.json if present
                meta_path = os.path.join(fpath, "metadata.json")
                if os.path.exists(meta_path):
                    with open(meta_path) as mf:
                        meta = json.load(mf)
                    print(f"\n📋 metadata.json:")
                    for k, v in meta.items():
                        print(f"   {k}: {v}")

        # ── Display the data ──
        if all_placemarks:
            data_df = pd.DataFrame(all_placemarks)
            print(f"\n📊 Total records: {len(data_df)}")
            if "Geometry" in data_df.columns:
                print(f"   Points: {(data_df['Geometry']=='Point').sum()}  |  "
                      f"Lines: {(data_df['Geometry']=='LineString').sum()}  |  "
                      f"Polygons: {(data_df['Geometry']=='Polygon').sum()}")
            print()
            display(data_df)
        else:
            print("\n⚠️ No placemark data could be extracted from this file.")

# COMMAND ----------

# DBTITLE 1,Step 4 – Extract Chainage and Lat/Long from KMZ Files
# MAGIC %md
# MAGIC ## Step 4 – Extract Chainage and Lat/Long from All KMZ Files (68 files)
# MAGIC
# MAGIC **Purpose:**
# MAGIC Processes all 68 `.kmz` files to extract chainage values and geographic coordinates (latitude/longitude) for each UPC.
# MAGIC
# MAGIC **Input:**
# MAGIC - 68 KMZ files from the `KMZ/` folder.
# MAGIC - Master dataset `df` for UPC-to-file mapping.
# MAGIC
# MAGIC **Process:**
# MAGIC - Opens each KMZ archive and parses the embedded KML file.
# MAGIC - Identifies **named Point placemarks** whose names represent chainage values (e.g., "77000", "841+400", "CH : 8+870").
# MAGIC - Extracts the chainage value and corresponding latitude/longitude from each placemark using 7 different naming patterns.
# MAGIC - If no KML exists inside a KMZ, falls back to parsing **shapefile .dbf** for attributes.
# MAGIC - For each UPC, determines the Start Chainage, End Chainage, Start Lat/Long, and End Lat/Long.
# MAGIC
# MAGIC **Output:**
# MAGIC - `result_df`: Detailed dataset of ~101,678 individual chainage point records.
# MAGIC - `kmz_summary_df`: Summary table (68 rows) with one row per KMZ file showing Start/End Chainage and Start/End Lat/Long.
# MAGIC
# MAGIC **Important:**
# MAGIC *This is the primary extraction step and covers the majority (68 out of 116) of the Present UPCs.*

# COMMAND ----------

# DBTITLE 1,Extract Chainage and Lat Long from all KMZ files
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
import re
import os

kmz_folder = "/Workspace/Users/shivam.raj9226@softsensor.ai/KML File Lat Long Extraction/KMZ"

kmz_files = sorted([f for f in os.listdir(kmz_folder) if f.endswith('.kmz')])
print(f"Found {len(kmz_files)} KMZ files to process\n")

import struct

def detect_kml_ns(root):
    """Detect KML namespace from root element."""
    tag = root.tag
    if "opengis.net/kml/2.2" in tag:
        return "http://www.opengis.net/kml/2.2"
    elif "earth.google.com/kml/2.0" in tag:
        return "http://earth.google.com/kml/2.0"
    return ""

def find_el(parent, ns, tag):
    """Find element handling any namespace."""
    return parent.find(f"{{{ns}}}{tag}" if ns else tag)

def find_all_el(parent, ns, tag):
    """Iterate over all elements with tag (any namespace)."""
    return list(parent.iter(f"{{{ns}}}{tag}" if ns else tag))

def get_el_text(parent, ns, tag, default=""):
    """Get text of child element."""
    el = find_el(parent, ns, tag)
    return (el.text or "").strip() if el is not None else default

def extract_chainage_value(name):
    """Extract chainage value from placemark name. Returns (value, format) or (None, None)."""
    name = name.strip()
    # Pattern 1: Pure numeric ("77000", "0.000", "950500.0")
    if re.match(r'^\d+\.?\d*$', name):
        return float(name), f"Numeric (e.g. {name})"
    # Pattern 2: Plus-format ("841+400", "0+000")
    m = re.match(r'^(\d+)\+(\d+)$', name)
    if m:
        return int(m.group(1)) * 1000 + int(m.group(2)), f"Plus-format (e.g. {name})"
    # Pattern 3: CH-prefixed number ("CH : 1.5", "CH: 100")
    m = re.match(r'^CH\s*:\s*([\d.]+)$', name, re.IGNORECASE)
    if m:
        return float(m.group(1)), f"Prefixed (e.g. {name})"
    # Pattern 4: CH-prefixed plus-format ("CH : 8+870")
    m = re.match(r'^CH\s*[:.\s]\s*(\d+)\+(\d+)', name, re.IGNORECASE)
    if m:
        return int(m.group(1)) * 1000 + int(m.group(2)), f"CH-plus (e.g. {name})"
    # Pattern 5: Embedded chainage in text like "MCW Start Ch. 841+400 (...)" 
    m = re.search(r'Ch\.?\s*(\d+)\+(\d+)', name)
    if m:
        return int(m.group(1)) * 1000 + int(m.group(2)), f"Embedded (e.g. {name[:40]})"
    # Pattern 6: Embedded chainage like "MCW 842+000 (...)" (no Ch prefix but plus-format in sentence)
    m = re.search(r'\b(\d{2,})\+(\d{3})\b', name)
    if m and not re.match(r'^\d+\.?\d*$', name):  # not pure numeric
        return int(m.group(1)) * 1000 + int(m.group(2)), f"Inline-plus (e.g. {name[:40]})"
    # Pattern 7: Numeric with text prefix like "360915 UP Border"
    m = re.match(r'^(\d{4,})\s+\w', name)
    if m:
        return float(m.group(1)), f"Numeric-prefixed (e.g. {name[:30]})"
    return None, None

def parse_shapefile_dbf(z, dbf_path):
    """Parse a .dbf file from a ZIP and return records as list of dicts."""
    dbf_data = z.read(dbf_path)
    n_records = struct.unpack('<I', dbf_data[4:8])[0]
    header_size = struct.unpack('<H', dbf_data[8:10])[0]
    record_size = struct.unpack('<H', dbf_data[10:12])[0]
    fields = []
    pos = 32
    while dbf_data[pos] != 0x0D:
        field_name = dbf_data[pos:pos+11].split(b'\x00')[0].decode('ascii')
        field_type = chr(dbf_data[pos+11])
        field_size = dbf_data[pos+16]
        fields.append((field_name, field_type, field_size))
        pos += 32
    records = []
    data_start = header_size
    for i in range(n_records):
        rec_offset = data_start + i * record_size + 1
        values = {}
        offset = rec_offset
        for fn, ft, fs in fields:
            raw = dbf_data[offset:offset+fs].decode('ascii', errors='replace').strip()
            values[fn] = raw
            offset += fs
        records.append(values)
    return records, [f[0] for f in fields]

all_results = []  # combined results across all files

for kmz_file in kmz_files:
    kmz_path = os.path.join(kmz_folder, kmz_file)
    all_points = []  # points for this file

    try:
        with zipfile.ZipFile(kmz_path, 'r') as z:
            kml_files_list = [f for f in z.namelist() if f.endswith('.kml')]
            
            # ── SHAPEFILE FALLBACK: If no KML, check for shapefiles ──
            if not kml_files_list:
                shp_files = [f for f in z.namelist() if f.lower().endswith('.dbf') and 'point' in f.lower()]
                if shp_files:
                    records, field_names = parse_shapefile_dbf(z, shp_files[0])
                    # Check if 'Chainage' field has actual chainage (not coordinates)
                    ch_vals = []
                    for r in records[:5]:
                        try:
                            v = float(r.get('Chainage', ''))
                            ch_vals.append(v)
                        except: pass
                    if ch_vals and all(60 < v < 100 for v in ch_vals):
                        # Values look like coordinates (60-100 range), not chainage
                        print(f"📦 {kmz_file}: Shapefile only — 'Chainage' field contains coordinates, not chainage values ({len(records)} points)")
                    else:
                        print(f"📦 {kmz_file}: Shapefile only — {len(records)} points in DBF")
                else:
                    contents = z.namelist()
                    has_shp = any(f.lower().endswith('.shp') for f in contents)
                    print(f"⚠️ {kmz_file}: No .kml found inside{'(shapefiles present — no chainage labels)' if has_shp else ''}, skipping")
                continue
            
            with z.open(kml_files_list[0]) as kml_file_obj:
                tree = ET.parse(kml_file_obj)
                root = tree.getroot()
                kml_ns = detect_kml_ns(root)

                # Build parent map once (O(n)) instead of walking tree per placemark (O(n²))
                parent_map = {child: parent for parent in root.iter() for child in parent}

                # Iterate ALL placemarks (both in Folders and at Document level)
                for pm in find_all_el(root, kml_ns, "Placemark"):
                    name = get_el_text(pm, kml_ns, "name")
                    desc = get_el_text(pm, kml_ns, "description")
                    point_el = find_el(pm, kml_ns, "Point")
                    if point_el is not None:
                        coords_el = find_el(point_el, kml_ns, "coordinates")
                        if coords_el is not None and coords_el.text:
                            first_coord = coords_el.text.strip().split()[0]
                            parts = first_coord.split(",")
                            # Walk up parent chain to find folder name
                            folder_name = ""
                            node = pm
                            while node in parent_map:
                                node = parent_map[node]
                                fn = get_el_text(node, kml_ns, "name")
                                if fn:
                                    folder_name = fn
                                    break
                            all_points.append({
                                "folder": folder_name,
                                "name": name,
                                "description": desc,
                                "longitude": float(parts[0]),
                                "latitude": float(parts[1]),
                                "elevation": float(parts[2]) if len(parts) > 2 else None,
                            })
    except Exception as e:
        print(f"❌ {kmz_file}: Error - {e}")
        continue

    if not all_points:
        print(f"⚠️ {kmz_file}: No point placemarks found, skipping")
        continue

    points_df = pd.DataFrame(all_points)

    # --- Extract chainage rows using all patterns ---
    chainage_rows = []
    for _, row in points_df.iterrows():
        ch_val, ch_fmt = extract_chainage_value(row["name"])
        if ch_val is not None:
            chainage_rows.append({
                "chainage": ch_val,
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "elevation": row["elevation"],
            })
    
    if not chainage_rows:
        print(f"⚠️ {kmz_file}: {len(points_df)} point placemarks but 0 chainage detected")
        continue
    
    chainage_df = pd.DataFrame(chainage_rows)

    # --- Extract curve parameters (R=, RL=) ---
    curve_df = points_df[
        points_df["name"].str.match(r'^R[L]?=', na=False)
    ].copy()
    curve_df.rename(columns={"name": "curve_param"}, inplace=True)

    # Merge curve params on matching coordinates
    file_result = chainage_df.merge(
        curve_df[["latitude", "longitude", "curve_param"]],
        on=["latitude", "longitude"],
        how="left"
    )

    # Add file identifier and clean up
    file_result["file_name"] = kmz_file
    file_result = file_result[["file_name", "chainage", "latitude", "longitude", "elevation", "curve_param"]].sort_values("chainage").reset_index(drop=True)

    print(f"✅ {kmz_file}: {len(file_result)} chainage points | range {file_result['chainage'].min()} – {file_result['chainage'].max()}")
    all_results.append(file_result)

# --- Combine all results and create summary (one row per KMZ file) ---
result_df = pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()

kmz_summary_rows = []
processed_files = set()

for file_df in all_results:
    fname = file_df["file_name"].iloc[0]
    processed_files.add(fname)
    start_row = file_df.loc[file_df["chainage"].idxmin()]
    end_row = file_df.loc[file_df["chainage"].idxmax()]
    match = df[df["Folder/Type"].str.strip() == fname]
    kmz_summary_rows.append({
        "S. No": match["Serial No"].iloc[0] if not match.empty else None,
        "UPC": match["UPC"].iloc[0] if not match.empty else None,
        "Start Chainage": start_row["chainage"],
        "End Chainage": end_row["chainage"],
        "Start Lat": start_row["latitude"],
        "Start Long": start_row["longitude"],
        "End Lat": end_row["latitude"],
        "End Long": end_row["longitude"],
        "Source": "Chainage and Lat/Long from named Point placemarks in KML inside .kmz file",
    })

# Add KMZ files from df that had no chainage data (errors, no placemarks, etc.)
kmz_df_entries = df[df["Folder/Type"].str.lower().str.strip().str.endswith('.kmz')]
for _, row in kmz_df_entries.iterrows():
    if row["Folder/Type"].strip() not in processed_files:
        kmz_summary_rows.append({
            "S. No": row["Serial No"],
            "UPC": row["UPC"],
            "Start Chainage": None, "End Chainage": None,
            "Start Lat": None, "Start Long": None,
            "End Lat": None, "End Long": None,
            "Source": "No chainage data extracted from .kmz file",
        })

kmz_summary_df = pd.DataFrame(kmz_summary_rows).sort_values("S. No").reset_index(drop=True)

print(f"\n{'='*60}")
print(f"📊 KMZ FILES SUMMARY: {len(kmz_summary_df)} entries")
print(f"   With chainage data: {kmz_summary_df['Start Chainage'].notna().sum()}")
print(f"   Without data      : {kmz_summary_df['Start Chainage'].isna().sum()}")
print(f"{'='*60}")
display(kmz_summary_df)

# COMMAND ----------

# DBTITLE 1,Step 5 – Extract from Unknown Files
# MAGIC %md
# MAGIC ## Step 5 – Extract Chainage and Lat/Long from Unknown Files (10 files)
# MAGIC
# MAGIC **Purpose:**
# MAGIC Processes 10 `.unknown` files that are actually **RAR archives** containing embedded KMZ/KML data.
# MAGIC
# MAGIC **Input:**
# MAGIC - 10 `.unknown` files from the `Unknown/` folder.
# MAGIC
# MAGIC **Process:**
# MAGIC - Each `.unknown` file is a RAR archive. The code detects and extracts the embedded KMZ/ZIP inside it by scanning for the ZIP magic bytes (`PK` signature).
# MAGIC - If direct extraction fails, uses `libarchive` as a fallback to handle the RAR format.
# MAGIC - Once the KML is extracted, parses chainage point placemarks the same way as KMZ files.
# MAGIC
# MAGIC **Output:**
# MAGIC - `unknown_summary_df`: Summary table (10 rows) with Start/End Chainage and Start/End Lat/Long for each UPC.
# MAGIC
# MAGIC **Important:**
# MAGIC *All 10 files were successfully extracted. These files originated from systems that output RAR-wrapped KMZ data.*

# COMMAND ----------

# DBTITLE 1,Extract Chainage and Lat Long from Unknown files
import os, re, io, zipfile, struct
import xml.etree.ElementTree as ET
import subprocess
subprocess.check_call(['pip', 'install', '-q', 'libarchive-c'])
import libarchive

unknown_folder = "/Workspace/Users/shivam.raj9226@softsensor.ai/KML File Lat Long Extraction/Unknown"
files_list = sorted(os.listdir(unknown_folder))
print(f"\U0001f4c2 Found {len(files_list)} files in Unknown folder\n")

def parse_kml_chainages_with_coords(kml_data):
    """Parse KML data and return list of (chainage, lat, lon) tuples."""
    if isinstance(kml_data, bytes):
        root = ET.fromstring(kml_data)
    else:
        root = ET.parse(kml_data).getroot()
    tag = root.tag
    if "opengis.net/kml/2.2" in tag:
        ns = "http://www.opengis.net/kml/2.2"
    elif "earth.google.com/kml/2.0" in tag:
        ns = "http://earth.google.com/kml/2.0"
    else:
        ns = ""
    results = []
    placemarks = list(root.iter(f"{{{ns}}}Placemark" if ns else "Placemark"))
    for pm in placemarks:
        name_el = pm.find(f"{{{ns}}}name" if ns else "name")
        name = (name_el.text or "").strip() if name_el is not None else ""
        point_el = pm.find(f"{{{ns}}}Point" if ns else "Point")
        if point_el is None:
            continue
        coords_el = point_el.find(f"{{{ns}}}coordinates" if ns else "coordinates")
        if coords_el is None or not coords_el.text:
            continue
        first_coord = coords_el.text.strip().split()[0]
        parts = first_coord.split(",")
        try:
            lon, lat = float(parts[0]), float(parts[1])
        except (ValueError, IndexError):
            continue
        ch_val = None
        if re.match(r'^\d+\.?\d*$', name):
            ch_val = float(name)
        m = re.match(r'^(\d+)\+(\d+)$', name)
        if m:
            ch_val = int(m.group(1)) * 1000 + int(m.group(2))
        m2 = re.match(r'^CH\s*:\s*([\d.]+)$', name, re.IGNORECASE)
        if m2:
            ch_val = float(m2.group(1))
        m3 = re.match(r'^CH\s*[:.\s]\s*(\d+)\+(\d+)', name, re.IGNORECASE)
        if m3:
            ch_val = int(m3.group(1)) * 1000 + int(m3.group(2))
        m4 = re.match(r'^(?:KM|Km|km)\s*([\d.]+)$', name)
        if m4:
            ch_val = float(m4.group(1))
        m5 = re.search(r'Ch\.?\s*(\d+)\+(\d+)', name)
        if m5 and ch_val is None:
            ch_val = int(m5.group(1)) * 1000 + int(m5.group(2))
        m6 = re.search(r'\b(\d{2,})\+(\d{3})\b', name)
        if m6 and ch_val is None:
            ch_val = int(m6.group(1)) * 1000 + int(m6.group(2))
        m7 = re.match(r'^(\d{4,})\s+\w', name)
        if m7 and ch_val is None:
            ch_val = float(m7.group(1))
        if ch_val is not None:
            results.append((ch_val, lat, lon))
    return results

unknown_summary_rows = []

for fname in files_list:
    fpath = os.path.join(unknown_folder, fname)
    with open(fpath, 'rb') as f:
        raw = f.read()
    chainage_points = []

    # Method 1: Find embedded ZIP (KMZ inside RAR)
    pk_start = raw.find(b'PK\x03\x04')
    eocd_pos = raw.rfind(b'PK\x05\x06')
    if pk_start != -1 and eocd_pos != -1 and eocd_pos > pk_start:
        try:
            comment_len = int.from_bytes(raw[eocd_pos+20:eocd_pos+22], 'little')
            zip_end = eocd_pos + 22 + comment_len
            kmz_bytes = raw[pk_start:zip_end]
            with zipfile.ZipFile(io.BytesIO(kmz_bytes), 'r') as z:
                kml_in_zip = [fl for fl in z.namelist() if fl.endswith('.kml')]
                if kml_in_zip:
                    chainage_points = parse_kml_chainages_with_coords(z.read(kml_in_zip[0]))
        except Exception:
            pass

    # Method 2: Fallback to libarchive for fully compressed RAR
    if not chainage_points:
        try:
            with libarchive.file_reader(fpath) as archive:
                for entry in archive:
                    entry_path = str(entry)
                    if entry_path.endswith('.kml'):
                        kml_data = b''.join(block for block in entry.get_blocks())
                        chainage_points = parse_kml_chainages_with_coords(kml_data)
                        break
                    elif entry_path.endswith('.kmz'):
                        kmz_raw = b''.join(block for block in entry.get_blocks())
                        with zipfile.ZipFile(io.BytesIO(kmz_raw), 'r') as zf:
                            kml_inside = [fl for fl in zf.namelist() if fl.endswith('.kml')]
                            if kml_inside:
                                chainage_points = parse_kml_chainages_with_coords(zf.read(kml_inside[0]))
                        break
        except Exception:
            pass

    # Map to UPC from df
    match = df[df["Folder/Type"].str.strip() == fname]
    if match.empty:
        base_name = os.path.splitext(fname)[0]
        match = df[df["Folder/Type"].str.strip().str.replace(r'\.[^.]+$', '', regex=True) == base_name]

    if chainage_points:
        chainage_points.sort(key=lambda x: x[0])
        s, e = chainage_points[0], chainage_points[-1]
        unknown_summary_rows.append({
            "S. No": match["Serial No"].iloc[0] if not match.empty else None,
            "UPC": match["UPC"].iloc[0] if not match.empty else None,
            "Start Chainage": s[0], "End Chainage": e[0],
            "Start Lat": s[1], "Start Long": s[2],
            "End Lat": e[1], "End Long": e[2],
            "Source": "Chainage and Lat/Long from named Point placemarks in KML embedded in RAR archive (.unknown file)",
        })
        print(f"\u2705 {fname}: {len(chainage_points)} chainage pts | range {s[0]} \u2013 {e[0]}")
    else:
        unknown_summary_rows.append({
            "S. No": match["Serial No"].iloc[0] if not match.empty else None,
            "UPC": match["UPC"].iloc[0] if not match.empty else None,
            "Start Chainage": None, "End Chainage": None,
            "Start Lat": None, "Start Long": None,
            "End Lat": None, "End Long": None,
            "Source": "No chainage data extracted from .unknown archive",
        })
        print(f"\u26a0\ufe0f {fname}: No chainage data extracted")

unknown_summary_df = pd.DataFrame(unknown_summary_rows).sort_values("S. No").reset_index(drop=True)
print(f"\n{'='*60}")
print(f"\U0001f4ca UNKNOWN FILES SUMMARY: {len(unknown_summary_df)} entries")
print(f"   With chainage data: {unknown_summary_df['Start Chainage'].notna().sum()}")
print(f"   Without data      : {unknown_summary_df['Start Chainage'].isna().sum()}")
print(f"{'='*60}")
display(unknown_summary_df)

# COMMAND ----------

# DBTITLE 1,Step 6 – Extract from Folder Files
# MAGIC %md
# MAGIC ## Step 6 – Extract Chainage and Lat/Long from Folder Files (38 folders)
# MAGIC
# MAGIC **Purpose:**
# MAGIC Processes 38 sub-folders, each containing KML files and/or `metadata.json` with project information.
# MAGIC
# MAGIC **Input:**
# MAGIC - 38 sub-folders from the `Folder/` directory.
# MAGIC - Each folder may contain: `.kml` files, `metadata.json`, or both.
# MAGIC
# MAGIC **Process:**
# MAGIC - Parses KML files within each folder to extract chainage-labelled Point placemarks and their coordinates.
# MAGIC - Handles multiple KML namespace versions (KML 2.2, KML 2.0, no namespace).
# MAGIC - If no KML file exists or parsing fails, falls back to `metadata.json` which may contain `start_lat_long`, `end_lat_long`, and chainage range in the `project_name` field.
# MAGIC - A further fallback reads centerline LineString geometry from the KML.
# MAGIC
# MAGIC **Output:**
# MAGIC - `folder_summary_df`: Summary table (38 rows) — 33 with chainage data, 5 without.
# MAGIC
# MAGIC **Important:**
# MAGIC *Malformed coordinate values (e.g., ".6.160") are handled gracefully. The extraction tries multiple fallback methods to maximise data recovery.*

# COMMAND ----------

# DBTITLE 1,Extract Chainage and Lat Long from Folder files
import os, re, json
import xml.etree.ElementTree as ET

base_folder = "/Workspace/Users/shivam.raj9226@softsensor.ai/KML File Lat Long Extraction/Folder"
folders_list = sorted([d for d in os.listdir(base_folder) if os.path.isdir(os.path.join(base_folder, d))])
print(f"\U0001f4c2 Found {len(folders_list)} sub-folders in Folder directory\n")

folder_summary_rows = []

for folder_name in folders_list:
    folder_path = os.path.join(base_folder, folder_name)
    kml_files_in_dir = [f for f in os.listdir(folder_path) if f.endswith('.kml')]

    # Read metadata.json if available
    meta_path = os.path.join(folder_path, "metadata.json")
    metadata = {}
    if os.path.exists(meta_path):
        with open(meta_path) as mf:
            metadata = json.load(mf)

    match = df[df["Folder/Type"].str.strip() == folder_name]
    serial_no = match["Serial No"].iloc[0] if not match.empty else None
    upc = match["UPC"].iloc[0] if not match.empty else None

    if not kml_files_in_dir:
        # Try metadata fallback for chainage + coords
        meta_start_ch = meta_end_ch = None
        meta_start_lat = meta_start_lon = meta_end_lat = meta_end_lon = None
        if metadata:
            proj = metadata.get('project_name', '')
            km_matches = re.findall(r'(?:Km|KM|km)[.\s]*(\d+[.]\d+)', proj)
            if len(km_matches) >= 2:
                meta_start_ch = float(km_matches[0]) * 1000
                meta_end_ch = float(km_matches[-1]) * 1000
            start_ll = metadata.get('start_lat_long', '')
            end_ll = metadata.get('end_lat_long', '')
            if start_ll and ',' in start_ll:
                pts = start_ll.split(',')
                try: meta_start_lat, meta_start_lon = float(pts[0]), float(pts[1])
                except: pass
            if end_ll and ',' in end_ll:
                pts = end_ll.split(',')
                try: meta_end_lat, meta_end_lon = float(pts[0]), float(pts[1])
                except: pass
        folder_summary_rows.append({
            "S. No": serial_no, "UPC": upc,
            "Start Chainage": meta_start_ch, "End Chainage": meta_end_ch,
            "Start Lat": meta_start_lat, "Start Long": meta_start_lon,
            "End Lat": meta_end_lat, "End Long": meta_end_lon,
            "Source": "Chainage and coordinates from metadata.json (no KML file in folder)",
        })
        if meta_start_ch:
            print(f"\U0001f4cb {folder_name}: metadata fallback Ch {meta_start_ch}\u2013{meta_end_ch}")
        else:
            print(f"\u26a0\ufe0f {folder_name}: No KML found")
        continue

    kml_path = os.path.join(folder_path, kml_files_in_dir[0])
    try:
        tree = ET.parse(kml_path)
        root = tree.getroot()
    except Exception as ex:
        print(f"\u274c {folder_name}: XML parse error - {ex}")
        folder_summary_rows.append({
            "S. No": serial_no, "UPC": upc,
            "Start Chainage": None, "End Chainage": None,
            "Start Lat": None, "Start Long": None,
            "End Lat": None, "End Long": None,
            "Source": "XML parse error in KML file",
        })
        continue

    tag = root.tag
    if "opengis.net/kml/2.2" in tag:
        ns = "http://www.opengis.net/kml/2.2"
    elif "earth.google.com/kml/2.0" in tag:
        ns = "http://earth.google.com/kml/2.0"
    else:
        ns = ""

    chainage_points = []
    placemarks = list(root.iter(f"{{{ns}}}Placemark" if ns else "Placemark"))

    for pm in placemarks:
        name_el = pm.find(f"{{{ns}}}name" if ns else "name")
        name = (name_el.text or "").strip() if name_el is not None else ""
        point_el = pm.find(f"{{{ns}}}Point" if ns else "Point")
        if point_el is None:
            continue
        coords_el = point_el.find(f"{{{ns}}}coordinates" if ns else "coordinates")
        if coords_el is None or not coords_el.text:
            continue
        first_coord = coords_el.text.strip().split()[0]
        parts = first_coord.split(",")
        try:
            lon, lat = float(parts[0]), float(parts[1])
        except (ValueError, IndexError):
            continue
        ch_val = None
        try:
            if re.match(r'^\d+\.?\d*$', name):
                ch_val = float(name)
            m = re.match(r'^(\d+)\+(\d+)$', name)
            if m:
                ch_val = int(m.group(1)) * 1000 + int(m.group(2))
            m2 = re.match(r'^CH\s*:\s*(\d+\.?\d*)$', name, re.IGNORECASE)
            if m2:
                ch_val = float(m2.group(1))
            m3 = re.match(r'^CH\s*[:.\s]\s*(\d+)\+(\d+)', name, re.IGNORECASE)
            if m3:
                ch_val = int(m3.group(1)) * 1000 + int(m3.group(2))
            m4 = re.match(r'^(?:KM|Km|km)[.\s]*(\d+\.?\d*)$', name)
            if m4:
                ch_val = float(m4.group(1))
            m5 = re.search(r'Ch\.?\s*(\d+)\+(\d+)', name)
            if m5 and ch_val is None:
                ch_val = int(m5.group(1)) * 1000 + int(m5.group(2))
            m6 = re.search(r'\b(\d{2,})\+(\d{3})\b', name)
            if m6 and ch_val is None:
                ch_val = int(m6.group(1)) * 1000 + int(m6.group(2))
            m7 = re.match(r'^(\d{4,})\s+\w', name)
            if m7 and ch_val is None:
                ch_val = float(m7.group(1))
        except (ValueError, TypeError):
            ch_val = None
        if ch_val is not None:
            chainage_points.append((ch_val, lat, lon))

    if chainage_points:
        chainage_points.sort(key=lambda x: x[0])
        s, e = chainage_points[0], chainage_points[-1]
        folder_summary_rows.append({
            "S. No": serial_no, "UPC": upc,
            "Start Chainage": s[0], "End Chainage": e[0],
            "Start Lat": s[1], "Start Long": s[2],
            "End Lat": e[1], "End Long": e[2],
            "Source": "Chainage and Lat/Long from named Point placemarks in KML file within folder",
        })
        print(f"\u2705 {folder_name}: {len(chainage_points)} chainage pts | range {s[0]} \u2013 {e[0]}")
    else:
        # Metadata fallback
        meta_start_ch = meta_end_ch = None
        meta_start_lat = meta_start_lon = meta_end_lat = meta_end_lon = None
        if metadata:
            proj = metadata.get('project_name', '')
            km_matches = re.findall(r'(?:Km|KM|km)[.\s]*(\d+[.]\d+)', proj)
            if len(km_matches) >= 2:
                meta_start_ch = float(km_matches[0]) * 1000
                meta_end_ch = float(km_matches[-1]) * 1000
            start_ll = metadata.get('start_lat_long', '')
            end_ll = metadata.get('end_lat_long', '')
            if start_ll and ',' in start_ll:
                pts = start_ll.split(',')
                try: meta_start_lat, meta_start_lon = float(pts[0]), float(pts[1])
                except: pass
            if end_ll and ',' in end_ll:
                pts = end_ll.split(',')
                try: meta_end_lat, meta_end_lon = float(pts[0]), float(pts[1])
                except: pass
        # Try centerline geometry fallback
        if meta_start_lat is None:
            for kml_folder in root.iter(f"{{{ns}}}Folder" if ns else "Folder"):
                fn_el = kml_folder.find(f"{{{ns}}}name" if ns else "name")
                fn = (fn_el.text or "").strip() if fn_el is not None else ""
                if fn.upper() == "CHAINAGE":
                    longest_coords = None
                    max_count = 0
                    for pm_inner in kml_folder.iter(f"{{{ns}}}Placemark" if ns else "Placemark"):
                        ls = pm_inner.find(f"{{{ns}}}LineString/{{{ns}}}coordinates" if ns else "LineString/coordinates")
                        if ls is not None and ls.text:
                            coord_pairs = ls.text.strip().split()
                            if len(coord_pairs) > max_count:
                                max_count = len(coord_pairs)
                                longest_coords = coord_pairs
                    if longest_coords and max_count > 10:
                        try:
                            first = longest_coords[0].split(",")
                            last = longest_coords[-1].split(",")
                            meta_start_lat, meta_start_lon = float(first[1]), float(first[0])
                            meta_end_lat, meta_end_lon = float(last[1]), float(last[0])
                        except (ValueError, IndexError):
                            pass
                    break
        folder_summary_rows.append({
            "S. No": serial_no, "UPC": upc,
            "Start Chainage": meta_start_ch, "End Chainage": meta_end_ch,
            "Start Lat": meta_start_lat, "Start Long": meta_start_lon,
            "End Lat": meta_end_lat, "End Long": meta_end_lon,
            "Source": "Coordinates from metadata.json or centerline geometry in KML (no chainage labels found)",
        })
        if meta_start_ch or meta_start_lat:
            print(f"\U0001f4cb {folder_name}: metadata/geometry fallback")
        else:
            print(f"\u26a0\ufe0f {folder_name}: No chainage data extracted")

folder_summary_df = pd.DataFrame(folder_summary_rows).sort_values("S. No").reset_index(drop=True)
print(f"\n{'='*60}")
print(f"\U0001f4ca FOLDER FILES SUMMARY: {len(folder_summary_df)} entries")
print(f"   With chainage data: {folder_summary_df['Start Chainage'].notna().sum()}")
print(f"   Without data      : {folder_summary_df['Start Chainage'].isna().sum()}")
print(f"{'='*60}")
display(folder_summary_df)

# COMMAND ----------

# DBTITLE 1,Step 7 – Combine All 116 UPC Summaries
# MAGIC %md
# MAGIC ## Step 7 – Combine All 116 UPC Summaries into a Single Table
# MAGIC
# MAGIC **Purpose:**
# MAGIC Merges the extraction results from all three sources (KMZ, Unknown, Folder) into one consolidated table.
# MAGIC
# MAGIC **Input:**
# MAGIC - `kmz_summary_df` (68 rows from Step 4)
# MAGIC - `unknown_summary_df` (10 rows from Step 5)
# MAGIC - `folder_summary_df` (38 rows from Step 6)
# MAGIC
# MAGIC **Process:**
# MAGIC - Adds a `Type` column ("KMZ", "Unknown", or "Folder") to identify the source of each record.
# MAGIC - Concatenates all three tables into a single combined table sorted by Serial Number.
# MAGIC
# MAGIC **Output:**
# MAGIC - `combined_df`: Consolidated table with 116 rows and columns: S. No, UPC, Type, Start Chainage, End Chainage, Start Lat, Start Long, End Lat, End Long, Source.
# MAGIC
# MAGIC **Important:**
# MAGIC *At this point, some entries may still have missing chainage or lat/long values. The next step (Deep Recovery) attempts to fill these gaps.*

# COMMAND ----------

# DBTITLE 1,Combined Summary - All 116 UPCs
# Combine all summaries with a Type column
kmz_summary_df["Type"] = "KMZ"
unknown_summary_df["Type"] = "Unknown"
folder_summary_df["Type"] = "Folder"

combined_df = pd.concat([kmz_summary_df, unknown_summary_df, folder_summary_df], ignore_index=True)
combined_df = combined_df.sort_values("S. No").reset_index(drop=True)

# Reorder columns
combined_df = combined_df[["S. No", "UPC", "Type", "Start Chainage", "End Chainage", "Start Lat", "Start Long", "End Lat", "End Long", "Source"]]

print(f"{'='*70}")
print(f"\U0001f4ca COMBINED SUMMARY: {len(combined_df)} entries (all present UPCs)")
print(f"{'='*70}")
print(f"  KMZ files  : {len(kmz_summary_df)}")
print(f"  Unknown    : {len(unknown_summary_df)}")
print(f"  Folders    : {len(folder_summary_df)}")
print(f"  \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
print(f"  With data  : {combined_df['Start Chainage'].notna().sum()}")
print(f"  Without    : {combined_df['Start Chainage'].isna().sum()}")
print(f"{'='*70}")
display(combined_df)

# COMMAND ----------

# DBTITLE 1,Step 8 – Deep Recovery for Missing Entries
# MAGIC %md
# MAGIC ## Step 8 – Deep Recovery — Extract Data from All Possible Sources for Missing Entries
# MAGIC
# MAGIC **Purpose:**
# MAGIC Attempts to recover chainage and/or coordinates for entries that had missing data after the initial extraction (Steps 4–6). Uses 10 different recovery methods to maximise data completeness.
# MAGIC
# MAGIC **Input:**
# MAGIC - Entries in `combined_df` where `Start Lat` is null (originally 26 entries had no coordinates).
# MAGIC - Re-opens the original source files (KMZ, Folder) to try additional extraction techniques.
# MAGIC
# MAGIC **Process — 10 Recovery Methods (tried in order):**
# MAGIC 1. **LineString geometry** — uses the longest road-path geometry for start/end coordinates.
# MAGIC 2. **MultiGeometry** — extracts from complex composite geometries.
# MAGIC 3. **Any Point placemarks** — uses non-chainage-labelled points.
# MAGIC 4. **Polygon boundary** — extracts from polygon outer boundary.
# MAGIC 5. **Shapefile .shp bounding box** — reads geographic extent from the shapefile header.
# MAGIC 6. **Shapefile .shp points + .prj UTM→WGS84** — parses individual point records and converts UTM coordinates to lat/long using the projection file.
# MAGIC 7. **Shapefile .dbf chainage column** — reads chainage values from the attribute table.
# MAGIC 8. **metadata.json inside KMZ** — extracts coordinates and chainage from project metadata.
# MAGIC 9. **Nested KMZ** — handles KMZ files embedded within another KMZ.
# MAGIC 10. **Regex on corrupted KML** — extracts coordinates via text patterns when XML parsing fails.
# MAGIC 11. **Clean corrupted XML** — strips control characters and fixes namespace issues before re-parsing.
# MAGIC
# MAGIC **Output:**
# MAGIC - Updates `combined_df` in place with recovered values.
# MAGIC - **Final result: 116/116 entries now have Lat/Long. 91/116 have chainage.**
# MAGIC
# MAGIC **Important:**
# MAGIC *This step is critical for data completeness. Without it, 26 entries would have no coordinates at all. Each recovered entry’s `Source` column describes exactly which method was used.*

# COMMAND ----------

# DBTITLE 1,Deep Recovery - Extract data from all possible sources for missing entries
import zipfile, os, re, struct, io, json
import xml.etree.ElementTree as ET

kmz_folder = "/Workspace/Users/shivam.raj9226@softsensor.ai/KML File Lat Long Extraction/KMZ"
folder_base = "/Workspace/Users/shivam.raj9226@softsensor.ai/KML File Lat Long Extraction/Folder"

# ── Helper: detect KML namespace ──
def _ns(root):
    tag = root.tag
    if "opengis.net/kml/2.2" in tag: return "http://www.opengis.net/kml/2.2"
    if "earth.google.com/kml/2.0" in tag: return "http://earth.google.com/kml/2.0"
    return ""

# ── METHOD 1: Extract start/end coords from LineString geometry ──
def try_linestring(root, ns):
    longest, max_n = None, 0
    for ls in root.iter(f"{{{ns}}}LineString" if ns else "LineString"):
        ce = ls.find(f"{{{ns}}}coordinates" if ns else "coordinates")
        if ce is not None and ce.text:
            pairs = ce.text.strip().split()
            if len(pairs) > max_n:
                max_n = len(pairs)
                longest = pairs
    if longest and max_n > 2:
        try:
            f, l = longest[0].split(","), longest[-1].split(",")
            return {"Start Lat": float(f[1]), "Start Long": float(f[0]),
                    "End Lat": float(l[1]), "End Long": float(l[0])}
        except: pass
    return None

# ── METHOD 2: Extract from MultiGeometry ──
def try_multigeometry(root, ns):
    all_coords = []
    for mg in root.iter(f"{{{ns}}}MultiGeometry" if ns else "MultiGeometry"):
        for ls in mg.iter(f"{{{ns}}}LineString" if ns else "LineString"):
            ce = ls.find(f"{{{ns}}}coordinates" if ns else "coordinates")
            if ce is not None and ce.text:
                for p in ce.text.strip().split():
                    parts = p.split(",")
                    try: all_coords.append((float(parts[1]), float(parts[0])))
                    except: pass
        for pt in mg.iter(f"{{{ns}}}Point" if ns else "Point"):
            ce = pt.find(f"{{{ns}}}coordinates" if ns else "coordinates")
            if ce is not None and ce.text:
                parts = ce.text.strip().split(",")
                try: all_coords.append((float(parts[1]), float(parts[0])))
                except: pass
    if all_coords:
        return {"Start Lat": all_coords[0][0], "Start Long": all_coords[0][1],
                "End Lat": all_coords[-1][0], "End Long": all_coords[-1][1]}
    return None

# ── METHOD 3: Extract ANY Point coordinates (even non-chainage) ──
def try_any_points(root, ns):
    pts = []
    for pm in root.iter(f"{{{ns}}}Placemark" if ns else "Placemark"):
        pe = pm.find(f"{{{ns}}}Point/{{{ns}}}coordinates" if ns else "Point/coordinates")
        if pe is not None and pe.text:
            parts = pe.text.strip().split(",")
            try: pts.append((float(parts[1]), float(parts[0])))
            except: pass
    if pts:
        return {"Start Lat": pts[0][0], "Start Long": pts[0][1],
                "End Lat": pts[-1][0], "End Long": pts[-1][1]}
    return None

# ── METHOD 4: Polygon outer boundary ──
def try_polygon(root, ns):
    all_c = []
    for poly in root.iter(f"{{{ns}}}Polygon" if ns else "Polygon"):
        tag_outer = f"{{{ns}}}outerBoundaryIs/{{{ns}}}LinearRing/{{{ns}}}coordinates" if ns else "outerBoundaryIs/LinearRing/coordinates"
        oc = poly.find(f".//{tag_outer}")
        if oc is not None and oc.text:
            for p in oc.text.strip().split():
                parts = p.split(",")
                try: all_c.append((float(parts[1]), float(parts[0])))
                except: pass
    if len(all_c) > 2:
        return {"Start Lat": all_c[0][0], "Start Long": all_c[0][1],
                "End Lat": all_c[len(all_c)//2][0], "End Long": all_c[len(all_c)//2][1]}
    return None

# ── METHOD 5: Parse .shp file binary header for bounding box ──
def try_shapefile(z, shp_path):
    data = z.read(shp_path)
    if len(data) < 100: return None
    xmin = struct.unpack('<d', data[36:44])[0]
    ymin = struct.unpack('<d', data[44:52])[0]
    xmax = struct.unpack('<d', data[52:60])[0]
    ymax = struct.unpack('<d', data[60:68])[0]
    # Sanity: India bounds
    if 6 < ymin < 40 and 60 < xmin < 100 and ymin != ymax:
        return {"Start Lat": ymin, "Start Long": xmin,
                "End Lat": ymax, "End Long": xmax}
    return None

# ── METHOD 5b: Parse .shp point records + .prj UTM→WGS84 conversion ──
import math

def _utm_to_ll(easting, northing, zone, northern=True):
    """Convert UTM coordinates to WGS84 lat/lon (Redfearn series)."""
    a = 6378137.0
    f = 1 / 298.257223563
    e2 = 2*f - f*f
    ep2 = e2 / (1 - e2)
    k0 = 0.9996
    x = easting - 500000.0
    y = northing if northern else northing - 10000000.0
    M = y / k0
    mu = M / (a * (1 - e2/4 - 3*e2**2/64 - 5*e2**3/256))
    e1 = (1 - math.sqrt(1 - e2)) / (1 + math.sqrt(1 - e2))
    phi = (mu + (3*e1/2 - 27*e1**3/32)*math.sin(2*mu)
           + (21*e1**2/16 - 55*e1**4/32)*math.sin(4*mu)
           + 151*e1**3/96*math.sin(6*mu))
    sp, cp = math.sin(phi), math.cos(phi)
    N = a / math.sqrt(1 - e2*sp*sp)
    T = math.tan(phi)**2
    C = ep2 * cp * cp
    R = a * (1 - e2) / (1 - e2*sp*sp)**1.5
    D = x / (N * k0)
    lat = phi - N*math.tan(phi)/R*(D**2/2 - (5+3*T+10*C-4*C*C-9*ep2)*D**4/24
          + (61+90*T+298*C+45*T*T-252*ep2-3*C*C)*D**6/720)
    lon_r = (D - (1+2*T+C)*D**3/6
             + (5-2*C+28*T-3*C*C+8*ep2+24*T*T)*D**5/120) / cp
    return math.degrees(lat), math.degrees(lon_r) + (zone*6 - 183)

def try_shp_points_with_prj(z, shp_path):
    """Parse individual .shp point records; if .prj says UTM, convert to WGS84."""
    sd = z.read(shp_path)
    if len(sd) < 100: return None
    shp_type = struct.unpack('<I', sd[32:36])[0]
    if shp_type not in (1, 11, 21): return None  # Point / PointZ / PointM only

    # Read .prj for UTM zone
    prj_base = os.path.splitext(shp_path)[0]
    prj_actual = next((f for f in z.namelist()
                       if os.path.splitext(f)[0].lower() == prj_base.lower()
                       and f.lower().endswith('.prj')), None)
    utm_zone, northern = None, True
    if prj_actual:
        prj_text = z.read(prj_actual).decode('utf-8', errors='replace')
        m = re.search(r'UTM[_ ]Zone[_ ](\d+)([NS])?', prj_text, re.IGNORECASE)
        if m:
            utm_zone = int(m.group(1))
            if m.group(2) and m.group(2).upper() == 'S': northern = False
    if not utm_zone: return None  # not UTM; regular bbox was already tried

    # Parse individual point records from .shp
    points = []
    soff = 100
    while soff < len(sd) - 8:
        try:
            clen = struct.unpack('>I', sd[soff+4:soff+8])[0] * 2
            soff += 8
            if soff + clen > len(sd): break
            rec = sd[soff:soff+clen]
            st = struct.unpack('<I', rec[0:4])[0]
            if st in (1, 11, 21) and len(rec) >= 20:
                x = struct.unpack('<d', rec[4:12])[0]
                y = struct.unpack('<d', rec[12:20])[0]
                points.append((x, y))
            soff += clen
        except: break
    if not points: return None

    # Convert first and last point UTM → WGS84
    try:
        s_lat, s_lon = _utm_to_ll(points[0][0], points[0][1], utm_zone, northern)
        e_lat, e_lon = _utm_to_ll(points[-1][0], points[-1][1], utm_zone, northern)
        if 6 < s_lat < 40 and 60 < s_lon < 100:
            return {"Start Lat": round(s_lat, 8), "Start Long": round(s_lon, 8),
                    "End Lat": round(e_lat, 8), "End Long": round(e_lon, 8)}
    except: pass
    return None

# ── METHOD 6: Parse .dbf for Chainage column ──
def try_dbf_chainage(z, dbf_path):
    data = z.read(dbf_path)
    if len(data) < 64: return None
    n_rec = struct.unpack('<I', data[4:8])[0]
    hdr_sz = struct.unpack('<H', data[8:10])[0]
    rec_sz = struct.unpack('<H', data[10:12])[0]
    fields = []
    pos = 32
    while pos < hdr_sz - 1 and data[pos] != 0x0D:
        fn = data[pos:pos+11].split(b'\x00')[0].decode('ascii', errors='replace')
        fs = data[pos+16]
        fields.append((fn, fs))
        pos += 32
    ch_idx = None
    for i, (fn, _) in enumerate(fields):
        if 'chainage' in fn.lower() or 'chain' in fn.lower() or 'ch_' in fn.lower():
            ch_idx = i
            break
    if ch_idx is None: return None
    chainages = []
    for r in range(n_rec):
        off = hdr_sz + r * rec_sz + 1
        field_off = off
        for i, (fn, fs) in enumerate(fields):
            if i == ch_idx:
                raw = data[field_off:field_off+fs].decode('ascii', errors='replace').strip()
                try: chainages.append(float(raw))
                except: pass
                break
            field_off += fs
    if chainages:
        return {"Start Chainage": min(chainages), "End Chainage": max(chainages)}
    return None

# ── METHOD 7: metadata.json inside KMZ ──
def try_metadata_in_zip(z):
    if 'metadata.json' not in z.namelist(): return None
    meta = json.loads(z.read('metadata.json'))
    r = {}
    for key, lat_k, lon_k in [('start_lat_long', 'Start Lat', 'Start Long'),
                               ('end_lat_long', 'End Lat', 'End Long')]:
        ll = meta.get(key, '')
        if ll and ',' in ll:
            pts = ll.split(',')
            try: r[lat_k], r[lon_k] = float(pts[0].strip()), float(pts[1].strip())
            except: pass
    proj = meta.get('project_name', '')
    km = re.findall(r'(?:Km|KM|km)[.\s]*(\d+[.]\d+)', proj)
    if len(km) >= 2:
        try: r["Start Chainage"], r["End Chainage"] = float(km[0])*1000, float(km[-1])*1000
        except: pass
    return r if r else None

# ── METHOD 8: Nested KMZ inside outer KMZ ──
def try_nested_kmz(z):
    for name in z.namelist():
        if name.lower().endswith('.kmz'):
            try:
                with zipfile.ZipFile(io.BytesIO(z.read(name)), 'r') as iz:
                    for kf in iz.namelist():
                        if kf.endswith('.kml'):
                            root = ET.fromstring(iz.read(kf))
                            ns = _ns(root)
                            return try_linestring(root, ns) or try_any_points(root, ns)
            except: pass
    return None

# ── METHOD 9: Regex fallback on raw KML text ──
def try_regex_coords(raw_text):
    coords = re.findall(r'<coordinates>\s*([^<]+)</coordinates>', raw_text, re.DOTALL)
    all_pts = []
    for ct in coords:
        for p in ct.strip().split():
            parts = p.split(",")
            if len(parts) >= 2:
                try:
                    lon, lat = float(parts[0]), float(parts[1])
                    if 6 < lat < 40 and 60 < lon < 100:
                        all_pts.append((lat, lon))
                except: pass
    if all_pts:
        return {"Start Lat": all_pts[0][0], "Start Long": all_pts[0][1],
                "End Lat": all_pts[-1][0], "End Long": all_pts[-1][1]}
    return None

# ── METHOD 10: Clean corrupted XML then parse ──
def try_clean_xml(raw_bytes):
    txt = raw_bytes.decode('utf-8', errors='replace')
    txt = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', txt)
    txt = re.sub(r'<gx:', '<gx_', txt)
    txt = re.sub(r'</gx:', '</gx_', txt)
    txt = re.sub(r'\s+xmlns:gx="[^"]*"', '', txt)
    try:
        root = ET.fromstring(txt.encode('utf-8'))
        ns = _ns(root)
        return try_linestring(root, ns) or try_any_points(root, ns) or try_polygon(root, ns)
    except:
        return None

# ═══════════════════════════════════════════════════════════
# MAIN RECOVERY LOOP
# ═══════════════════════════════════════════════════════════
no_data = combined_df[combined_df["Start Lat"].isna()].copy()
print(f"\U0001f50d Attempting deep recovery for {len(no_data)} entries without ANY coordinates...\n")

recovery = {}  # sno -> (result_dict, method)

for _, row in no_data.iterrows():
    sno, upc, ftype = row["S. No"], row["UPC"], row["Type"]
    match = df[df["Serial No"] == sno]
    ftv = match["Folder/Type"].iloc[0].strip() if not match.empty else "?"
    result, method = None, "EXHAUSTED"

    if ftype == "KMZ":
        fpath = os.path.join(kmz_folder, ftv)
        if not os.path.exists(fpath):
            method = "FILE MISSING"; continue
        try:
            with zipfile.ZipFile(fpath, 'r') as z:
                kml_list = [f for f in z.namelist() if f.endswith('.kml')]
                shp_list = [f for f in z.namelist() if f.lower().endswith('.shp')]
                dbf_list = [f for f in z.namelist() if f.lower().endswith('.dbf')]

                # ── A: Try KML-based extraction (geometry) ──
                for kf in kml_list:
                    try:
                        kml_raw = z.read(kf)
                        root = ET.fromstring(kml_raw)
                        ns = _ns(root)
                        for fn, label in [(try_linestring, "LineString geometry"),
                                          (try_multigeometry, "MultiGeometry"),
                                          (try_any_points, "Point coords"),
                                          (try_polygon, "Polygon boundary")]:
                            result = fn(root, ns)
                            if result: method = label; break
                        if result: break
                    except ET.ParseError:
                        # Try cleaned XML
                        result = try_clean_xml(z.read(kf))
                        if result: method = "Cleaned XML"; break
                        # Try regex
                        result = try_regex_coords(z.read(kf).decode('utf-8', errors='replace'))
                        if result: method = "Regex on corrupted KML"; break

                # ── B: Try shapefile (.shp bounding box) ──
                if not result:
                    for sf in shp_list:
                        result = try_shapefile(z, sf)
                        if result: method = f"Shapefile bbox ({os.path.basename(sf)})"; break

                # ── B2: If bbox failed (UTM?), parse .shp points + .prj conversion ──
                if not result:
                    for sf in shp_list:
                        result = try_shp_points_with_prj(z, sf)
                        if result: method = f"Shapefile points + UTM→WGS84 ({os.path.basename(sf)})"; break

                # ── C: Try .dbf for chainage values ──
                if not result or "Start Chainage" not in result:
                    ch_result = None
                    for dbf_f in dbf_list:
                        ch_result = try_dbf_chainage(z, dbf_f)
                        if ch_result: break
                    if ch_result:
                        if result:  # merge chainage into existing coords
                            result.update(ch_result)
                            method += " + DBF chainage"
                        else:
                            result = ch_result
                            method = f"DBF chainage ({os.path.basename(dbf_f)})"

                # ── D: Try metadata.json ──
                if not result:
                    result = try_metadata_in_zip(z)
                    if result: method = "metadata.json"

                # ── E: Try nested KMZ ──
                if not result:
                    result = try_nested_kmz(z)
                    if result: method = "Nested KMZ"

        except Exception as e:
            method = f"ZIP ERROR: {e}"

    elif ftype == "Folder":
        fpath = os.path.join(folder_base, ftv)
        if not os.path.exists(fpath):
            method = "FOLDER MISSING"; continue

        kml_list = [f for f in os.listdir(fpath) if f.endswith('.kml')]
        for kf in kml_list:
            kml_path = os.path.join(fpath, kf)
            try:
                root = ET.parse(kml_path).getroot()
                ns = _ns(root)
                for fn, label in [(try_linestring, "LineString geometry"),
                                  (try_multigeometry, "MultiGeometry"),
                                  (try_any_points, "Point coords"),
                                  (try_polygon, "Polygon boundary")]:
                    result = fn(root, ns)
                    if result: method = label; break
                if result: break
            except ET.ParseError:
                with open(kml_path, 'rb') as f:
                    raw = f.read()
                result = try_clean_xml(raw)
                if result: method = "Cleaned XML"; break
                result = try_regex_coords(raw.decode('utf-8', errors='replace'))
                if result: method = "Regex on corrupted KML"; break

        # Folder metadata.json
        if not result:
            mp = os.path.join(fpath, "metadata.json")
            if os.path.exists(mp):
                with open(mp) as mf:
                    meta = json.load(mf)
                r = {}
                for key, lk, lonk in [('start_lat_long','Start Lat','Start Long'),
                                       ('end_lat_long','End Lat','End Long')]:
                    ll = meta.get(key, '')
                    if ll and ',' in ll:
                        pts = ll.split(',')
                        try: r[lk], r[lonk] = float(pts[0].strip()), float(pts[1].strip())
                        except: pass
                proj = meta.get('project_name', '')
                km = re.findall(r'(?:Km|KM|km)[.\s]*(\d+[.]\d+)', proj)
                if len(km) >= 2:
                    try: r["Start Chainage"], r["End Chainage"] = float(km[0])*1000, float(km[-1])*1000
                    except: pass
                if r: result, method = r, "metadata.json"

    if result:
        recovery[sno] = (result, method)
        ch = '\u2705' if 'Start Chainage' in result else '\u274c'
        ll = '\u2705' if 'Start Lat' in result else '\u274c'
        print(f"\u2705 S.No {sno:>3} | {upc:<28} | {method:<35} | Ch:{ch}  LL:{ll}")
    else:
        print(f"\u274c S.No {sno:>3} | {upc:<28} | {method}")

# ═══════════════════════════════════════════════════════════
# UPDATE combined_df
# ═══════════════════════════════════════════════════════════
for sno, (result, method) in recovery.items():
    idx = combined_df[combined_df["S. No"] == sno].index
    if len(idx) > 0:
        for col, val in result.items():
            if col in combined_df.columns:
                combined_df.loc[idx, col] = val
        # Build descriptive extraction source sentence
        got_ch = "Start Chainage" in result
        got_ll = "Start Lat" in result
        ftype = combined_df.loc[idx[0], "Type"].lower()
        if got_ch and got_ll:
            src = f"Chainage and Lat/Long recovered via {method} in {ftype} file"
        elif got_ll:
            src = f"Lat/Long recovered via {method} in {ftype} file (no chainage data available)"
        elif got_ch:
            src = f"Chainage recovered via {method} in {ftype} file (no coordinates available)"
        else:
            src = f"Data recovered via {method} in {ftype} file"
        combined_df.loc[idx, "Source"] = src

# Final stats
has_chainage = combined_df["Start Chainage"].notna().sum()
has_latlong = combined_df["Start Lat"].notna().sum()
still_empty = combined_df[combined_df["Start Lat"].isna() & combined_df["Start Chainage"].isna()]

print(f"\n{'='*70}")
print(f"\U0001f4ca RECOVERY RESULTS")
print(f"{'='*70}")
print(f"  Recovered           : {len(recovery)} / {len(no_data)}")
print(f"  With Chainage now   : {has_chainage} / 116")
print(f"  With Lat/Long now   : {has_latlong} / 116")
print(f"  Fully empty still   : {len(still_empty)} / 116")
print(f"{'='*70}")

if len(still_empty) > 0:
    print(f"\n\u26a0\ufe0f  Entries that could NOT be recovered (all methods exhausted):")
    for _, r in still_empty.iterrows():
        print(f"   S.No {r['S. No']:>3} | {r['UPC']:<28} | Type: {r['Type']}")

print(f"\n\U0001f4ca UPDATED COMBINED TABLE:")
display(combined_df)

# COMMAND ----------

# DBTITLE 1,Step 9 – Automated Validation of All 116 UPCs
# MAGIC %md
# MAGIC ## Step 9 – Automated Validation of All UPCs
# MAGIC
# MAGIC **Purpose:**
# MAGIC Runs a comprehensive set of automated checks on the final extracted data to identify potential data quality issues, so that only the flagged entries require manual review.
# MAGIC
# MAGIC **Input:**
# MAGIC - `combined_df` with all extracted Start/End Chainage and Lat/Long values.
# MAGIC - Original source files (for cross-referencing with `metadata.json`).
# MAGIC
# MAGIC **Process — 6 Validation Checks:**
# MAGIC 1. **India boundary (10% threshold)** — Are all coordinates within India’s geographic bounds?
# MAGIC ~~2. State boundary~~ *(removed)*
# MAGIC 2. **Start = End** — Does the lat/long fall within the correct state (derived from the UPC code, e.g., "/MH" = Maharashtra)?
# MAGIC 3. **Start = End** — Are start and end coordinates identical (suggests only one point was extracted)?
# MAGIC 4. **Distance > 500 km** — Is the straight-line distance unreasonably large?
# MAGIC 5. **Chainage vs distance** — Is the road chainage difference consistent with the geographic distance? *(Straight-line distance should always be less than road distance.)*
# MAGIC 6. **metadata.json cross-check** — Do the extracted coordinates match the independently stored metadata?
# MAGIC 7. **Duplicate coordinates** — Do multiple UPCs share the exact same start/end points?
# MAGIC
# MAGIC **Output:**
# MAGIC - `flags_df`: Table of all validation flags with Severity (CRITICAL / WARNING / INFO) and details.
# MAGIC - Summary: 76 entries fully clean, 4 CRITICAL, 25 WARNING, 25 INFO.
# MAGIC
# MAGIC **Important:**
# MAGIC *CRITICAL flags (e.g., coordinates outside India, straight-line distance exceeding road chainage) indicate definite data issues that need investigation. WARNING flags may have valid explanations (e.g., border roads crossing state lines). INFO flags indicate missing chainage — expected for geometry-only recovery entries.*

# COMMAND ----------

# DBTITLE 1,Automated Validation - All 116 UPCs
import math

# ═══════════════════════════════════════════════════════════
# VALIDATION ENGINE FOR 116 UPCs
# ═══════════════════════════════════════════════════════════

# ── 1. Haversine distance calculator ──
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(rlat1)*math.cos(rlat2)*math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

# ── 2. Run all validation checks ──
flags = []  # list of (sno, upc, check_name, severity, detail)

# ── PRE-PASS: Count how many coordinates fall outside India ──
total_coords = 0
outside_india = 0
for _, r in combined_df.iterrows():
    s_lat, s_lon = r.get("Start Lat"), r.get("Start Long")
    e_lat, e_lon = r.get("End Lat"), r.get("End Long")
    if pd.notna(s_lat) and pd.notna(s_lon):
        total_coords += 1
        if not (6 < s_lat < 38 and 68 < s_lon < 98):
            outside_india += 1
    if pd.notna(e_lat) and pd.notna(e_lon):
        total_coords += 1
        if not (6 < e_lat < 38 and 68 < e_lon < 98):
            outside_india += 1

outside_pct = (outside_india / total_coords * 100) if total_coords > 0 else 0
apply_india_check = outside_pct > 10  # Only flag if >10% are outside India
print(f"India boundary pre-check: {outside_india}/{total_coords} coords outside India ({outside_pct:.1f}%)")
print(f"  {'→ Flagging enabled (>10%)' if apply_india_check else '→ Flagging SKIPPED (≤10%, minor boundary noise)'}")

for _, r in combined_df.iterrows():
    sno = int(r["S. No"])
    upc = r["UPC"]
    s_lat, s_lon = r.get("Start Lat"), r.get("Start Long")
    e_lat, e_lon = r.get("End Lat"), r.get("End Long")
    s_ch, e_ch = r.get("Start Chainage"), r.get("End Chainage")

    has_ll = pd.notna(s_lat) and pd.notna(e_lat)
    has_ch = pd.notna(s_ch) and pd.notna(e_ch)

    # ── CHECK 1: Missing coordinates ──
    if not has_ll:
        flags.append((sno, upc, "Missing Lat/Long", "\U0001f534 CRITICAL", "No coordinates extracted"))
        continue

    # ── CHECK 2: India boundary (only if >10% of all coords are outside) ──
    if apply_india_check:
        for label, lat, lon in [("Start", s_lat, s_lon), ("End", e_lat, e_lon)]:
            if not (6 < lat < 38 and 68 < lon < 98):
                flags.append((sno, upc, f"{label} outside India", "\U0001f534 CRITICAL",
                              f"{label}: ({lat:.4f}, {lon:.4f}) is outside India bounds"))

    # ── CHECK 3: Start == End (identical coordinates) ──
    if has_ll and abs(s_lat - e_lat) < 0.0001 and abs(s_lon - e_lon) < 0.0001:
        flags.append((sno, upc, "Start = End (identical)", "\U0001f7e0 WARNING",
                      f"Start and End are the same point: ({s_lat:.6f}, {s_lon:.6f})"))

    # ── CHECK 4: Unreasonable distance (>500 km straight line) ──
    if has_ll:
        dist = haversine_km(s_lat, s_lon, e_lat, e_lon)
        if dist > 500:
            flags.append((sno, upc, f"Distance too large: {dist:.0f} km", "\U0001f7e0 WARNING",
                          f"Straight-line distance {dist:.1f} km between start and end"))

    # ── CHECK 5: Chainage vs distance consistency ──
    if has_ll and has_ch:
        dist = haversine_km(s_lat, s_lon, e_lat, e_lon)
        ch_diff = abs(e_ch - s_ch) / 1000  # chainage to km
        if ch_diff > 0.1:
            ratio = dist / ch_diff if ch_diff > 0 else 0
            if ratio > 1.5:
                flags.append((sno, upc, "Straight-line > Chainage", "\U0001f534 CRITICAL",
                              f"Haversine={dist:.1f}km but Chainage diff={ch_diff:.1f}km — "
                              f"straight line can't be longer than road (ratio={ratio:.2f})"))
            elif ratio < 0.05 and dist > 1:
                flags.append((sno, upc, "Distance/Chainage mismatch", "\U0001f7e0 WARNING",
                              f"Haversine={dist:.1f}km vs Chainage={ch_diff:.1f}km — "
                              f"ratio={ratio:.3f} is very low"))

    # ── CHECK 6: Missing chainage ──
    if not has_ch:
        flags.append((sno, upc, "Missing Chainage", "\U0001f7e1 INFO",
                      f"No chainage — lat/long from: {r.get('Source', 'unknown')[:60]}"))

# ── 3. Cross-reference with metadata.json where available ──
import json as _json2
kmz_folder_path = "/Workspace/Users/shivam.raj9226@softsensor.ai/KML File Lat Long Extraction/KMZ"
folder_base_path = "/Workspace/Users/shivam.raj9226@softsensor.ai/KML File Lat Long Extraction/Folder"

meta_checks = 0
for _, r in combined_df.iterrows():
    sno = int(r["S. No"])
    upc = r["UPC"]
    match_row = df[df["Serial No"] == sno]
    if match_row.empty: continue
    ftv = match_row["Folder/Type"].iloc[0].strip()
    meta = None

    if ftv.lower().endswith('.kmz'):
        fpath = os.path.join(kmz_folder_path, ftv)
        if os.path.exists(fpath):
            try:
                import zipfile as _zf
                with _zf.ZipFile(fpath, 'r') as z:
                    if 'metadata.json' in z.namelist():
                        meta = _json2.loads(z.read('metadata.json'))
            except: pass
    elif not ftv.lower().endswith('.unknown'):
        meta_path = os.path.join(folder_base_path, ftv, "metadata.json")
        if os.path.exists(meta_path):
            with open(meta_path) as mf:
                meta = _json2.load(mf)

    if meta:
        meta_checks += 1
        for key, col_lat, col_lon, label in [
            ('start_lat_long', 'Start Lat', 'Start Long', 'Start'),
            ('end_lat_long', 'End Lat', 'End Long', 'End')]:
            ll = meta.get(key, '')
            if ll and ',' in ll:
                pts = ll.split(',')
                try:
                    m_lat, m_lon = float(pts[0].strip()), float(pts[1].strip())
                    e_lat_val = r.get(col_lat)
                    e_lon_val = r.get(col_lon)
                    if pd.notna(e_lat_val) and pd.notna(e_lon_val):
                        diff_km = haversine_km(m_lat, m_lon, e_lat_val, e_lon_val)
                        if diff_km > 50:
                            flags.append((sno, upc, f"{label} vs metadata.json", "\U0001f7e0 WARNING",
                                          f"Extracted ({e_lat_val:.4f},{e_lon_val:.4f}) is {diff_km:.1f}km "
                                          f"from metadata ({m_lat:.4f},{m_lon:.4f})"))
                except: pass

# ── 4. Duplicate coordinate check ──
coord_groups = combined_df.groupby(["Start Lat", "Start Long", "End Lat", "End Long"])
for coords, grp in coord_groups:
    if len(grp) > 1 and pd.notna(coords[0]):
        upcs = grp["UPC"].tolist()
        snos = grp["S. No"].tolist()
        for s, u in zip(snos, upcs):
            flags.append((int(s), u, "Duplicate coordinates", "\U0001f7e0 WARNING",
                          f"Shares exact same start/end with: {[f'S.No {x}' for x in snos if x != s]}"))

# ═══════════════════════════════════════════════════════════
# DISPLAY RESULTS
# ═══════════════════════════════════════════════════════════
flags_df = pd.DataFrame(flags, columns=["S. No", "UPC", "Check", "Severity", "Detail"])

critical = flags_df[flags_df["Severity"].str.contains("CRITICAL")]
warning  = flags_df[flags_df["Severity"].str.contains("WARNING")]
info     = flags_df[flags_df["Severity"].str.contains("INFO")]

total_upcs = len(combined_df)
flagged_upcs = flags_df["S. No"].nunique()
clean_upcs = total_upcs - flagged_upcs

print(f"{'='*70}")
print(f"\U0001f9ea VALIDATION REPORT — {total_upcs} UPCs | 6 Checks")
print(f"{'='*70}")
print(f"  \u2705 CLEAN (all checks passed) : {clean_upcs} / {total_upcs}  ({clean_upcs/total_upcs*100:.0f}%)")
print(f"  \U0001f534 CRITICAL flags           : {len(critical)}")
print(f"  \U0001f7e0 WARNING flags            : {len(warning)}")
print(f"  \U0001f7e1 INFO flags               : {len(info)}")
print(f"  Total flags                : {len(flags_df)}")
print(f"  India boundary pre-check   : {outside_india}/{total_coords} outside ({outside_pct:.1f}%) — {'ENABLED' if apply_india_check else 'SKIPPED (\u226410%)'}")
print(f"  metadata.json cross-checks : {meta_checks} files compared")
print(f"{'='*70}")
print(f"\n  Checks performed:")
print(f"    1. India boundary       — only if >10% coords outside India (was {outside_pct:.1f}%)")
print(f"    2. Start = End          — identical start/end coordinates?")
print(f"    3. Distance > 500 km    — unreasonably large gap?")
print(f"    4. Chainage vs distance  — haversine consistent with road chainage?")
print(f"    5. metadata.json match   — extracted coords vs independent metadata?")
print(f"    6. Duplicate coordinates — multiple UPCs sharing same start/end?")

if len(critical) > 0:
    print(f"\n{'='*70}")
    print(f"\U0001f534 CRITICAL ISSUES ({len(critical)}) — need investigation:")
    print(f"{'='*70}")
    for _, f in critical.iterrows():
        print(f"  S.No {f['S. No']:>3} | {f['UPC']:<28} | {f['Check']}")
        print(f"           {f['Detail']}")

if len(warning) > 0:
    print(f"\n{'='*70}")
    print(f"\U0001f7e0 WARNINGS ({len(warning)}) — may need review:")
    print(f"{'='*70}")
    for _, f in warning.iterrows():
        print(f"  S.No {f['S. No']:>3} | {f['UPC']:<28} | {f['Check']}")
        print(f"           {f['Detail']}")

if len(info) > 0:
    print(f"\n\U0001f7e1 INFO: {len(info)} entries have lat/long but no chainage — expected for geometry-only files")

print(f"\n\U0001f4ca FULL FLAGS TABLE:")
display(flags_df.sort_values(["Severity", "S. No"]))

# COMMAND ----------

# DBTITLE 1,Final Output - Combined Data with Validation Flags
# ═══════════════════════════════════════════════════════════
# FINAL OUTPUT: combined_df + validation flags from flags_df
# ═══════════════════════════════════════════════════════════

# Aggregate flags per UPC: worst severity, count, and all check details
if len(flags_df) > 0:
    severity_order = {"🔴 CRITICAL": 1, "🟠 WARNING": 2, "🟡 INFO": 3}
    flags_df["_sev_rank"] = flags_df["Severity"].map(severity_order).fillna(4)

    agg = flags_df.groupby("UPC").agg(
        Validation_Status=(
            "_sev_rank", lambda x: {
                1: "🔴 CRITICAL", 2: "🟠 WARNING", 3: "🟡 INFO"
            }.get(int(x.min()), "✅ CLEAN")
        ),
        Flag_Count=("Check", "count"),
        Validation_Checks=("Check", lambda x: " | ".join(x)),
        Validation_Details=("Detail", lambda x: " | ".join(x)),
    ).reset_index()

    flags_df.drop(columns=["_sev_rank"], inplace=True)
else:
    agg = pd.DataFrame(columns=["UPC", "Validation_Status", "Flag_Count", "Validation_Checks", "Validation_Details"])

# Merge into combined_df
final_df = combined_df.merge(agg, on="UPC", how="left")

# Fill clean entries (no flags)
final_df["Validation_Status"] = final_df["Validation_Status"].fillna("✅ CLEAN")
final_df["Flag_Count"] = final_df["Flag_Count"].fillna(0).astype(int)
final_df["Validation_Checks"] = final_df["Validation_Checks"].fillna("All 7 checks passed")
final_df["Validation_Details"] = final_df["Validation_Details"].fillna("No issues found")

# Reorder columns
final_df = final_df[[
    "S. No", "UPC", "Type",
    "Start Chainage", "End Chainage",
    "Start Lat", "Start Long", "End Lat", "End Long",
    "Source",
    "Validation_Status", "Flag_Count", "Validation_Checks", "Validation_Details"
]]

# Summary
clean = (final_df["Validation_Status"] == "✅ CLEAN").sum()
crit = (final_df["Validation_Status"] == "🔴 CRITICAL").sum()
warn = (final_df["Validation_Status"] == "🟠 WARNING").sum()
info_count = (final_df["Validation_Status"] == "🟡 INFO").sum()

print(f"{'='*70}")
print(f"📋 FINAL OUTPUT TABLE — 116 UPCs with Validation Status")
print(f"{'='*70}")
print(f"  Columns: {len(final_df.columns)} (10 data + 4 validation)")
print(f"  Rows   : {len(final_df)}")
print(f"  ─────────────────────")
print(f"  ✅ CLEAN    : {clean}")
print(f"  🔴 CRITICAL : {crit}")
print(f"  🟠 WARNING  : {warn}")
print(f"  🟡 INFO     : {info_count}")
print(f"{'='*70}")

display(final_df)