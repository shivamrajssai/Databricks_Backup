# Databricks notebook source
# DBTITLE 1,KML Lat Long Extraction
# MAGIC %md
# MAGIC # KML File Lat/Long Extraction — NHAI Highway UPC Data
# MAGIC
# MAGIC This notebook extracts geographic coordinates (latitude and longitude) from KML/KMZ files linked to NHAI highway UPC (Unique Project Code) records. The source data is an Excel file containing **370 UPCs across five zones (A–E)**, each with a link to a KML or KMZ file hosted on SharePoint, OneDrive, or the NHAI data lake.
# MAGIC
# MAGIC The notebook reads these links, classifies them by type, and processes the downloadable files to extract **start/end coordinates and chainage data** for each UPC.

# COMMAND ----------

# DBTITLE 1,Step 1 – Install Required Libraries
# MAGIC %md
# MAGIC # Step 1 – Install Required Libraries
# MAGIC
# MAGIC **Purpose:**
# MAGIC Installs the necessary software library required to read the Excel input file containing KML links.
# MAGIC
# MAGIC **Important:**
# MAGIC *This step must run before any other step. Without this library, the notebook cannot read Excel files.*

# COMMAND ----------

# DBTITLE 1,Install dependencies
# MAGIC %pip install openpyxl

# COMMAND ----------

# DBTITLE 1,Step 2 – UPC Link Availability Report
# MAGIC %md
# MAGIC # Step 2 – UPC Link Availability Report
# MAGIC
# MAGIC **Purpose:**
# MAGIC Checks how many UPCs across all five zones have a valid KML file link versus those marked as "No Link Available."
# MAGIC
# MAGIC **Input:**
# MAGIC Excel file `370_KML _Links.xlsx` containing UPC records across five zone sheets (Zone A through Zone E).
# MAGIC
# MAGIC **Process:**
# MAGIC * All zone sheets are combined into a single dataset.
# MAGIC * Each UPC's link column is checked — if it says "No Link Available," the UPC is flagged accordingly.
# MAGIC * A zone-wise and overall availability summary is generated.
# MAGIC
# MAGIC **Output:**
# MAGIC * Overall and zone-wise availability counts and percentages.
# MAGIC * List of specific UPCs where no link is available.
# MAGIC
# MAGIC **Important:**
# MAGIC *UPCs marked "No Link Available" cannot be processed further and are excluded from downstream extraction steps.*

# COMMAND ----------

# DBTITLE 1,UPC Link Availability Report
import pandas as pd
from urllib.parse import unquote

file_path = "/Workspace/Users/shivam.raj9226@softsensor.ai/KML File Lat Long Extraction/Input/370_KML _Links.xlsx"
zone_sheets = ['Zone A', 'Zone B', 'Zone C', 'Zone D', 'Zone E']

# ── Read & combine all zone sheets ─────────────────────────────────────────
all_zones = []
for sheet in zone_sheets:
    df_z = pd.read_excel(file_path, sheet_name=sheet, engine="openpyxl")
    df_z['Zone'] = sheet.replace('Zone ', '')
    all_zones.append(df_z)

df_report = pd.concat(all_zones, ignore_index=True)
df_report.columns = ['SNo', 'UPC', 'KML_Files', 'KML_File_Link', 'Zone']

# ── Check column D (KML_File_Link): "No Link Available" = not available ───
df_report['Is_Available'] = df_report['KML_File_Link'].astype(str).str.strip().str.lower() != 'no link available'

# ── OVERALL REPORT ─────────────────────────────────────────────────────────
total = len(df_report)
available = int(df_report['Is_Available'].sum())
not_available = total - available

print("╔══════════════════════════════════════════════════════════════════════╗")
print("║              UPC LINK AVAILABILITY REPORT                          ║")
print("╠══════════════════════════════════════════════════════════════════════╣")
print(f"║  Total UPCs                       :  {total:<28} ║")
print(f"║  Links Available                  :  {available:<28} ║")
print(f"║  Links NOT Available              :  {not_available:<28} ║")
print(f"║  Availability %                   :  {available/total*100:.1f}%{'':<24} ║")
print("╠══════════════════════════════════════════════════════════════════════╣")
print("║  ZONE-WISE BREAKDOWN                                             ║")
print("╠══════════════════════════════════════════════════════════════════════╣")

zone_summary = df_report.groupby('Zone').agg(
    Total=('UPC', 'count'),
    Available=('Is_Available', 'sum'),
    Not_Available=('Is_Available', lambda x: (~x).sum()),
).reset_index()
zone_summary['Available'] = zone_summary['Available'].astype(int)
zone_summary['Not_Available'] = zone_summary['Not_Available'].astype(int)
zone_summary['Availability_pct'] = (zone_summary['Available'] / zone_summary['Total'] * 100).round(1)

for _, r in zone_summary.iterrows():
    print(f"║  Zone {r['Zone']}  | Total: {r['Total']:>3}  | Available: {r['Available']:>3}  | Not Available: {r['Not_Available']:>2}  | {r['Availability_pct']:>5.1f}%  ║")

print("╚══════════════════════════════════════════════════════════════════════╝")

print("\nZone-wise summary table:")
display(zone_summary)

# ── List UPCs where link is "No Link Available" ───────────────────────────
no_link_df = df_report[~df_report['Is_Available']][['Zone', 'SNo', 'UPC', 'KML_File_Link']].reset_index(drop=True)
print(f"\nUPCs with 'No Link Available' ({len(no_link_df)} records):")
display(no_link_df)

# COMMAND ----------

# DBTITLE 1,Step 3 – Classify KML Link Types Across All Zones
# MAGIC %md
# MAGIC # Step 3 – Classify KML Link Types Across All Zones
# MAGIC
# MAGIC **Purpose:**
# MAGIC Categorises each URL into a specific type so that the appropriate download and extraction strategy can be applied in later steps.
# MAGIC
# MAGIC **Input:**
# MAGIC Combined UPC link data from all five zones (produced in Step 2).
# MAGIC
# MAGIC **Process:**
# MAGIC Each link URL is examined and classified into one of the following types:
# MAGIC * **Direct KML / Direct KMZ** — file links pointing directly to a downloadable `.kml` or `.kmz` file
# MAGIC * **RAR Archive** — compressed archives containing KML/KMZ files inside
# MAGIC * **SharePoint Folder / OneDrive Folder** — folder-level links requiring API access or manual navigation
# MAGIC * **SharePoint File Link** — shared file links needing URL conversion before download
# MAGIC * **No Link** — UPCs with no link provided
# MAGIC
# MAGIC **Output:**
# MAGIC A summary showing the count and zone distribution for each link type.

# COMMAND ----------

# DBTITLE 1,Load all Zone sheets and classify link types
import pandas as pd
from urllib.parse import unquote

file_path = "/Workspace/Users/shivam.raj9226@softsensor.ai/KML File Lat Long Extraction/Input/370_KML _Links.xlsx"
zone_sheets = ['Zone A', 'Zone B', 'Zone C', 'Zone D', 'Zone E']

# ── 1. Read & combine all zone sheets ──────────────────────────────────────
all_zones = []
for sheet in zone_sheets:
    df_z = pd.read_excel(file_path, sheet_name=sheet, engine="openpyxl")
    df_z['Zone'] = sheet.replace('Zone ', '')
    all_zones.append(df_z)

df_all = pd.concat(all_zones, ignore_index=True)
df_all.columns = ['SNo', 'UPC', 'KML_Files', 'KML_File_Link', 'Zone']
print(f"Total records across all zones: {len(df_all)}")
print(f"Zone-wise counts:\n{df_all['Zone'].value_counts().sort_index().to_string()}")

# ── 2. Classify each link into a type ──────────────────────────────────────
def classify_link(link):
    """Classify SharePoint/OneDrive KML links into actionable categories."""
    if pd.isna(link):
        return 'No Link'
    decoded = unquote(str(link))
    dl = decoded.lower()
    if dl.endswith('.kml') or '.kml?' in dl or '/kml?' in dl:
        return 'Direct KML'
    elif dl.endswith('.kmz') or '.kmz?' in dl:
        return 'Direct KMZ'
    elif dl.endswith('.rar') or '.rar&' in dl:
        return 'RAR Archive'
    elif '/:u:/' in decoded:  # SharePoint shared single-file link
        if '.kmz' in dl:
            return 'Direct KMZ'
        elif '.kml' in dl:
            return 'Direct KML'
        return 'SharePoint File Link'
    elif '/:f:/' in decoded:  # SharePoint shared folder
        return 'SharePoint Folder'
    elif 'onedrive.aspx' in decoded:
        return 'OneDrive Folder'
    elif '/Forms/AllItems.aspx' in decoded:
        return 'SharePoint Folder Listing'
    else:
        return 'Other'

df_all['Link_Type'] = df_all['KML_File_Link'].apply(classify_link)

# ── 3. Summary table ──────────────────────────────────────────────────────
print("\n" + "="*60)
print("LINK TYPE SUMMARY ACROSS ALL ZONES")
print("="*60)
summary = df_all.groupby(['Zone', 'Link_Type']).size().unstack(fill_value=0)
display(summary)

print("\nOverall link type counts:")
print(df_all['Link_Type'].value_counts().to_string())

# COMMAND ----------

# DBTITLE 1,Step 4 – Link Processing Pipeline Strategy
# MAGIC %md
# MAGIC # Step 4 – Link Processing Pipeline Strategy
# MAGIC
# MAGIC **Purpose:**
# MAGIC Documents the recommended approach for handling each type of KML link, ordered from simplest to most complex. This serves as a reference for the extraction steps that follow.
# MAGIC
# MAGIC **Process:**
# MAGIC 1. **Direct KML** — Download the file and parse it directly for coordinates.
# MAGIC 2. **Direct KMZ** — Download, unzip to extract the KML inside, then parse.
# MAGIC 3. **RAR Archive** — Download, extract KML/KMZ files from the archive, then parse.
# MAGIC 4. **SharePoint File Links** — Convert the sharing URL to a download URL, then download and parse.
# MAGIC 5. **Folder Links** — List folder contents via API, identify KML/KMZ files, then download each.
# MAGIC
# MAGIC **Output:**
# MAGIC A reference document with the processing approach and a sample URL for each link type.
# MAGIC
# MAGIC **Important:**
# MAGIC *This step is informational only — no data is processed. It serves as a guide for the extraction steps that follow.*

# COMMAND ----------

# DBTITLE 1,Pipeline: How to open each link type
# ── PIPELINE: Strategy for each link type ─────────────────────────────────
# This cell documents how to handle each type of link found in the Excel file.
# All links point to SharePoint / OneDrive hosted KML/KMZ files.

pipeline_info = """
╔══════════════════════════════════════════════════════════════════════════════╗
║                    KML LINK PROCESSING PIPELINE                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  LINK TYPE 1: Direct KML  (e.g. ...filename.kml?csf=1&web=1)              ║
║  ─────────────────────────────────────────────────────────────              ║
║  • These are direct SharePoint links to .kml files                         ║
║  • To open: Replace /:u:/ or sharing params with /download endpoint        ║
║  • Download via requests, then parse XML with xml.etree or fastkml         ║
║  • Extract <coordinates> tags for lat/long                                 ║
║                                                                            ║
║  LINK TYPE 2: Direct KMZ  (e.g. ...filename.kmz?csf=1&web=1)              ║
║  ─────────────────────────────────────────────────────────────              ║
║  • KMZ = zipped KML file                                                   ║
║  • Download the .kmz, open with zipfile module                             ║
║  • Extract doc.kml from inside the zip                                     ║
║  • Parse the extracted KML for coordinates                                 ║
║                                                                            ║
║  LINK TYPE 3: RAR Archive  (e.g. ...folder/filename.rar)                   ║
║  ─────────────────────────────────────────────────────────────              ║
║  • RAR files contain KML/KMZ files compressed in RAR format                ║
║  • Download the .rar file                                                  ║
║  • Extract using rarfile or unrar library                                  ║
║  • Find .kml/.kmz files inside, then parse for coordinates                 ║
║                                                                            ║
║  LINK TYPE 4: SharePoint Folder  (/:f:/ links)                             ║
║  ─────────────────────────────────────────────────────────────              ║
║  • These link to a SharePoint shared folder (not a single file)            ║
║  • Requires SharePoint API or manual navigation to list folder files       ║
║  • Use Microsoft Graph API to enumerate folder contents                    ║
║  • Then download individual KML/KMZ files from the listing                 ║
║                                                                            ║
║  LINK TYPE 5: OneDrive Folder  (onedrive.aspx links)                       ║
║  ─────────────────────────────────────────────────────────────              ║
║  • OneDrive web UI folder browsing links                                   ║
║  • Similar to SharePoint folders — points to a directory                   ║
║  • Use OneDrive/Graph API to list children and download files              ║
║  • Or convert to direct download URL using OneDrive URL patterns           ║
║                                                                            ║
║  LINK TYPE 6: SharePoint Folder Listing  (AllItems.aspx)                   ║
║  ─────────────────────────────────────────────────────────────              ║
║  • Classic SharePoint document library view                                ║
║  • Navigate to find KML/KMZ files in the library                           ║
║  • Use SharePoint REST API to enumerate and download                       ║
║                                                                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                            ║
║  RECOMMENDED PROCESSING ORDER (PIPELINE):                                  ║
║                                                                            ║
║  Step 1 → Direct KML files       (simplest — just download & parse)        ║
║  Step 2 → Direct KMZ files       (download, unzip, parse)                  ║
║  Step 3 → RAR archives           (download, extract, parse)                ║
║  Step 4 → SharePoint file links  (convert URL, download, parse)            ║
║  Step 5 → Folder links           (list contents via API, then download)    ║
║                                                                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
print(pipeline_info)

# ── Show sample links for each type ───────────────────────────────────────
print("\nSAMPLE LINKS BY TYPE:")
print("=" * 60)
for link_type in df_all['Link_Type'].unique():
    subset = df_all[df_all['Link_Type'] == link_type]
    sample_link = subset['KML_File_Link'].iloc[0]
    decoded = unquote(str(sample_link))
    print(f"\n▸ {link_type} ({len(subset)} links)")
    print(f"  Zone(s): {', '.join(sorted(subset['Zone'].unique()))}")
    print(f"  Sample:  {decoded[:120]}..." if len(decoded) > 120 else f"  Sample:  {decoded}")

# COMMAND ----------

# DBTITLE 1,Step 5 – Single KMZ File Extraction (Demo)
# MAGIC %md
# MAGIC # Step 5 – Single KMZ File Extraction (Demo)
# MAGIC
# MAGIC **Purpose:**
# MAGIC Demonstrates the complete extraction process on one UPC to verify the approach before running it across all records.
# MAGIC
# MAGIC **Input:**
# MAGIC One KMZ file downloaded from the NHAI data lake for UPC `N/02006/05003/JK`.
# MAGIC
# MAGIC **Process:**
# MAGIC * The KMZ file is downloaded from the NHAI server.
# MAGIC * The KMZ (a compressed archive) is unzipped to extract the KML file inside.
# MAGIC * The KML is parsed to find all location markers (Placemarks).
# MAGIC * Coordinates (latitude, longitude, altitude) are extracted from each Placemark.
# MAGIC * If the file contains **Road Centreline** markers with chainage values, those are displayed with coordinates.
# MAGIC * Otherwise, all extracted coordinate points are shown.
# MAGIC
# MAGIC **Output:**
# MAGIC A table showing extracted coordinates along with chainage information for the selected UPC.
# MAGIC
# MAGIC **Important:**
# MAGIC *This is a demonstration step. Change the UPC and URL variables to test with other records before running the batch process.*

# COMMAND ----------

# DBTITLE 1,Download and extract KMZ for UPC N/02006/05003/JK
import requests
import zipfile
import io
import xml.etree.ElementTree as ET
import re
import pandas as pd

# ── 1. Download the KMZ file ──────────────────────────────────────────────
upc = "N/02006/05003/JK"
url = "https://datalakeg.nhai.gov.in/nhai/CaptureLegacyAlignment/DownloadbyName?FileName=viaduct.kmz&UPC=N/02006/05003/JK"

print(f"Downloading KMZ for UPC: {upc}")
print(f"URL: {url}")

response = requests.get(url, timeout=60, verify=False)
print(f"Status Code: {response.status_code}")
print(f"Content-Type: {response.headers.get('Content-Type', 'N/A')}")
print(f"File size: {len(response.content)} bytes")

# ── 2. Extract KML from KMZ (KMZ is a ZIP archive) ───────────────────────
kmz_bytes = io.BytesIO(response.content)

with zipfile.ZipFile(kmz_bytes, 'r') as kmz:
    file_list = kmz.namelist()
    print(f"\nFiles inside KMZ: {file_list}")
    
    # Find the KML file inside
    kml_file = [f for f in file_list if f.lower().endswith('.kml')][0]
    print(f"Extracting: {kml_file}")
    kml_content = kmz.read(kml_file).decode('utf-8')

print(f"KML content length: {len(kml_content)} chars")

# ── 3. Parse KML and extract Placemarks with coordinates ─────────────────
# Remove default namespace for easier parsing
kml_clean = re.sub(r'\sxmlns="[^"]+"', '', kml_content, count=1)
root = ET.fromstring(kml_clean)

# Find all Placemarks
placemarks = root.findall('.//Placemark')
print(f"\nTotal Placemarks found: {len(placemarks)}")

# ── 4. Extract chainage, lat, long from each Placemark ────────────────────
results = []
for pm in placemarks:
    name_el = pm.find('name')
    desc_el = pm.find('description')
    name = name_el.text.strip() if name_el is not None and name_el.text else ''
    desc = desc_el.text.strip() if desc_el is not None and desc_el.text else ''
    
    # Extract coordinates from Point or LineString
    coords_el = pm.find('.//coordinates')
    if coords_el is not None and coords_el.text:
        coord_text = coords_el.text.strip()
        # Coordinates format: lon,lat,alt (space-separated for multiple points)
        coord_pairs = coord_text.split()
        for pair in coord_pairs:
            parts = pair.split(',')
            if len(parts) >= 2:
                lon = float(parts[0])
                lat = float(parts[1])
                alt = float(parts[2]) if len(parts) > 2 else 0.0
                
                # Placemark name IS the chainage (in meters)
                chainage_m = ''
                if name.replace('.', '').isdigit():
                    chainage_m = name
                
                results.append({
                    'UPC': upc,
                    'Placemark_Name': name,
                    'Description': desc[:100] if desc else '',
                    'Chainage_m': chainage_m,
                    'Chainage_km': round(float(chainage_m) / 1000, 3) if chainage_m else '',
                    'Latitude': lat,
                    'Longitude': lon,
                    'Altitude': alt
                })

df_coords = pd.DataFrame(results)
print(f"\nExtracted {len(df_coords)} coordinate points")
print(f"\nSample Placemark names:")
for n in df_coords['Placemark_Name'].unique()[:10]:
    print(f"  - {n}")

# ── 5. Show extracted data ────────────────────────────────────────────────
# Try road centreline points first; if none, show all extracted data
road_centrelines = df_coords[df_coords['Description'] == 'ROAD_CENTRELINES'].copy()
road_centrelines = road_centrelines[road_centrelines['Chainage_m'] != ''].copy()

if len(road_centrelines) > 0:
    road_centrelines['Chainage_m'] = road_centrelines['Chainage_m'].astype(float)
    display_df = road_centrelines.sort_values('Chainage_m').reset_index(drop=True)
    source = 'Road Centreline (Chainage Points)'
else:
    display_df = df_coords.reset_index(drop=True)
    source = 'All Placemarks'

print(f"\n{'='*60}")
print(f"UPC: {upc}")
print(f"Total Placemarks: {len(placemarks)}")
print(f"Data source: {source}")
print(f"Points extracted: {len(display_df)}")
if len(display_df) > 0:
    print(f"Latitude range: {display_df['Latitude'].min():.6f} to {display_df['Latitude'].max():.6f}")
    print(f"Longitude range: {display_df['Longitude'].min():.6f} to {display_df['Longitude'].max():.6f}")
    ch_vals = display_df[display_df['Chainage_m'] != '']['Chainage_m']
    if len(ch_vals) > 0:
        ch_float = ch_vals.astype(float)
        print(f"Chainage range: {ch_float.min()/1000:.3f} km to {ch_float.max()/1000:.3f} km")
    else:
        print("Chainage: Not available in this KMZ")
print(f"{'='*60}")

print(f"\n--- Extracted Data for UPC: {upc} ---")
display(display_df[['UPC', 'Placemark_Name', 'Description', 'Chainage_m', 'Chainage_km', 'Latitude', 'Longitude', 'Altitude']])

# COMMAND ----------

# DBTITLE 1,Step 6 – Batch Extract All NHAI KMZ Files
# MAGIC %md
# MAGIC # Step 6 – Batch Extract All NHAI KMZ Files
# MAGIC
# MAGIC **Purpose:**
# MAGIC Processes all UPCs that have directly downloadable KMZ links from the NHAI data lake and extracts start/end location data for each.
# MAGIC
# MAGIC **Input:**
# MAGIC All UPCs with valid NHAI data lake KMZ links (approximately 18 records).
# MAGIC
# MAGIC **Process:**
# MAGIC * Each KMZ file is downloaded, unzipped, and the KML inside is parsed.
# MAGIC * A dual extraction strategy is applied:
# MAGIC   1. **Chainage Points** — If the KML contains markers with numeric chainage names and point coordinates, the first and last chainage values with their coordinates are used as start and end.
# MAGIC   2. **LineString Fallback** — If only route path data exists, the first and last coordinates of the path are used as start and end.
# MAGIC
# MAGIC **Output:**
# MAGIC A summary table with columns: Zone, UPC, Start Chainage (m), End Chainage (m), Start Latitude, Start Longitude, End Latitude, End Longitude.
# MAGIC
# MAGIC **Important:**
# MAGIC * *Only NHAI data lake links (directly downloadable without authentication) are processed in this step.*
# MAGIC * *SharePoint/OneDrive KMZ links require separate authentication and are not included here.*
# MAGIC * *Failed downloads are reported separately with the error reason.*

# COMMAND ----------

# DBTITLE 1,Extract Start/End Chainage and Lat/Long for all KMZ UPCs
import requests, zipfile, io, re, warnings
import xml.etree.ElementTree as ET
import pandas as pd
from urllib.parse import unquote

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

# ── 1. Load all zone data and filter NHAI KMZ links ──────────────────────
file_path = "/Workspace/Users/shivam.raj9226@softsensor.ai/KML File Lat Long Extraction/Input/370_KML _Links.xlsx"
zone_sheets = ['Zone A', 'Zone B', 'Zone C', 'Zone D', 'Zone E']

all_zones = []
for sheet in zone_sheets:
    df_z = pd.read_excel(file_path, sheet_name=sheet, engine="openpyxl")
    df_z['Zone'] = sheet.replace('Zone ', '')
    all_zones.append(df_z)

df = pd.concat(all_zones, ignore_index=True)
df.columns = ['SNo', 'UPC', 'KML_Files', 'KML_File_Link', 'Zone']
df['Is_Available'] = df['KML_File_Link'].astype(str).str.strip().str.lower() != 'no link available'

# Filter: available + NHAI datalake KMZ links (directly downloadable)
df_kmz = df[
    df['Is_Available'] &
    df['KML_File_Link'].astype(str).str.lower().str.contains('datalakeg.nhai.gov.in', na=False) &
    df['KML_File_Link'].astype(str).str.lower().str.contains('.kmz', na=False)
].copy()

print(f"Total NHAI KMZ UPCs to process: {len(df_kmz)}")
print(f"Zone breakdown: {df_kmz['Zone'].value_counts().sort_index().to_dict()}")

# ── 2. Helper: download KMZ, extract KML, parse coordinates ──────────────
def extract_kmz_data(url, upc):
    """Download KMZ, extract start/end chainage and lat/long.
    Strategy 1: Placemarks with numeric names (chainage) + Point coords.
    Strategy 2: Fallback to LineString start/end coordinates."""
    try:
        resp = requests.get(url, timeout=120, verify=False)
        if resp.status_code != 200:
            return {'error': f'HTTP {resp.status_code}'}

        kmz_bytes = io.BytesIO(resp.content)
        with zipfile.ZipFile(kmz_bytes, 'r') as kmz:
            kml_files = [f for f in kmz.namelist() if f.lower().endswith('.kml')]
            if not kml_files:
                return {'error': 'No KML file inside KMZ'}
            kml_content = kmz.read(kml_files[0]).decode('utf-8')

        kml_clean = re.sub(r'\sxmlns="[^"]+"', '', kml_content, count=1)
        root = ET.fromstring(kml_clean)

        # Strategy 1: Placemarks with numeric names (chainage) + Point coords
        points = []
        for pm in root.findall('.//Placemark'):
            name_el = pm.find('name')
            if name_el is None or not name_el.text:
                continue
            name = name_el.text.strip()
            if not name.replace('.', '').isdigit():
                continue
            point_coords = pm.find('.//Point/coordinates')
            if point_coords is not None and point_coords.text:
                c = point_coords.text.strip().split(',')
                if len(c) >= 2:
                    points.append({
                        'chainage_m': float(name),
                        'lat': float(c[1].split()[0]),
                        'lon': float(c[0]),
                    })

        if points:
            pts = sorted(points, key=lambda p: p['chainage_m'])
            return {
                'Start_Chainage_m': round(pts[0]['chainage_m'], 3),
                'End_Chainage_m': round(pts[-1]['chainage_m'], 3),
                'Start_Lat': pts[0]['lat'],
                'Start_Long': pts[0]['lon'],
                'End_Lat': pts[-1]['lat'],
                'End_Long': pts[-1]['lon'],
                'Total_Points': len(pts),
                'Source': 'Chainage Points',
                'error': None
            }

        # Strategy 2: Fallback - collect all LineString coordinates
        all_coords = []
        for ls in root.findall('.//LineString/coordinates'):
            if ls.text:
                for pair in ls.text.strip().split():
                    parts = pair.split(',')
                    if len(parts) >= 2:
                        all_coords.append({'lon': float(parts[0]), 'lat': float(parts[1])})

        if all_coords:
            return {
                'Start_Chainage_m': None,
                'End_Chainage_m': None,
                'Start_Lat': all_coords[0]['lat'],
                'Start_Long': all_coords[0]['lon'],
                'End_Lat': all_coords[-1]['lat'],
                'End_Long': all_coords[-1]['lon'],
                'Total_Points': len(all_coords),
                'Source': 'LineString Path',
                'error': None
            }

        return {'error': 'No coordinates found in KML'}
    except Exception as e:
        return {'error': str(e)}

# ── 3. Process all KMZ UPCs ───────────────────────────────────────────────
results = []
for i, (_, row) in enumerate(df_kmz.iterrows()):
    upc = row['UPC']
    zone = row['Zone']
    raw_link = str(row['KML_File_Link'])

    # Handle multiple URLs separated by ';' — take the first one
    first_url = raw_link.split(';')[0].strip().split(' ;')[0].strip().replace('%20;%20', '').strip()

    print(f"[{i+1}/{len(df_kmz)}] Zone {zone} | {upc} ... ", end="")
    data = extract_kmz_data(first_url, upc)

    if data.get('error'):
        print(f"ERROR: {data['error']}")
    else:
        src = data.get('Source', '')
        ch_info = f", {data['Start_Chainage_m']}-{data['End_Chainage_m']} m" if data.get('Start_Chainage_m') else ', no chainage'
        print(f"OK ({data['Total_Points']} pts{ch_info}) [{src}]")

    results.append({
        'Zone': zone,
        'UPC': upc,
        'Start_Chainage_m': data.get('Start_Chainage_m'),
        'End_Chainage_m': data.get('End_Chainage_m'),
        'Start_Lat': data.get('Start_Lat'),
        'Start_Long': data.get('Start_Long'),
        'End_Lat': data.get('End_Lat'),
        'End_Long': data.get('End_Long'),
        'Error': data.get('error')
    })

# ── 4. Final output ───────────────────────────────────────────────────────
df_result = pd.DataFrame(results)
success = df_result[df_result['Error'].isna()]
failed = df_result[df_result['Error'].notna()]

print(f"\n{'='*70}")
print(f"PROCESSING COMPLETE")
print(f"  Total: {len(df_result)} | Success: {len(success)} | Failed: {len(failed)}")
print(f"{'='*70}")

if len(failed) > 0:
    print(f"\nFailed UPCs:")
    display(failed[['Zone', 'UPC', 'Error']])

print(f"\n--- KMZ Extraction Results ---")
display(df_result[['Zone', 'UPC', 'Start_Chainage_m', 'End_Chainage_m', 'Start_Lat', 'Start_Long', 'End_Lat', 'End_Long']])