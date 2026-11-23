# Databricks notebook source
# DBTITLE 1,Install dependencies
# MAGIC %pip install geopandas openpyxl

# COMMAND ----------

# DBTITLE 1,State Extraction Script
"""
===============================================================================
Project Name : State Boundary Detection from NSV Base Layer (Databricks Version)
===============================================================================
"""

# =============================================================================
import os
import glob
import re
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

import geopandas as gpd
import pandas as pd
import numpy as np

# =============================================================================
# CONFIGURATION & DATABRICKS VOLUME PATHS
# =============================================================================

# Base Unity Catalog Volumes paths
CATALOG_BASE_PATH = "/Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output"
NSV_BACKUP_BASE_PATH = os.path.join(CATALOG_BASE_PATH, "Source=NSV_Backup")
RIS_BASE_PATH = os.path.join(CATALOG_BASE_PATH, "Source=NSV")

# Reference State Boundary Shapefile & Output paths
STATE_BOUNDARY_PATH = "/Workspace/Users/shivam.raj9226@softsensor.ai/Survey of india shap files/State Boundary.shp"
STATE_BOUNDARY_DIR = "/Workspace/Users/shivam.raj9226@softsensor.ai/Survey of india shap files"
OUTPUT_DIR_BASE = "/Workspace/Users/shivam.raj9226@softsensor.ai/Drafts"

STATE_NAME_COLUMN = "STATE"
COMMON_CRS = "EPSG:4326"

# =============================================================================
# LOGGER
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
LOGGER = logging.getLogger(__name__)

# =============================================================================
# DATABRICKS UNITY CATALOG VOLUME UTILS
# =============================================================================

def sanitize_param(value, prefix=""):
    """
    Sanitizes inputs like Zone='Zone A' -> 'A', UPC='N/01001/01001/HR' -> 'N_01001_01001_HR'
    """
    val_str = str(value).strip()
    if prefix:
        val_str = val_str.replace(prefix, "").strip()
    val_str = val_str.replace("/", "_")
    return val_str

def find_ris_parquet_file(zone, phase, cycle, upc, base_path=RIS_BASE_PATH):
    """
    Locates RIS parquet file in Databricks Volume structure:
    /Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output/Source=NSV/Zone={Zone}/Phase={Phase}/Cycle={Cycle}/UPC={UPC}/.../Deliverable=Deliverable - 1/*.parquet
    """
    z_clean = sanitize_param(zone, "Zone")
    p_clean = sanitize_param(phase, "Phase")
    c_clean = sanitize_param(cycle, "Cycle")
    u_clean = sanitize_param(upc)

    pattern = os.path.join(
        base_path,
        f"Zone={z_clean}",
        f"Phase={p_clean}",
        f"Cycle={c_clean}",
        f"UPC={u_clean}",
        "**",
        "*.parquet"
    )
    
    LOGGER.info(f"Searching for RIS Parquet with pattern: {pattern}")
    matched_files = glob.glob(pattern, recursive=True)
    
    ris_files = [f for f in matched_files if "RIS" in os.path.basename(f).upper() or "DELIVERABLE" in f.upper()]
    if ris_files:
        return ris_files[0]
    elif matched_files:
        return matched_files[0]
    else:
        LOGGER.warning(f"No RIS file found for UPC '{u_clean}' under pattern: {pattern}")
        return None

def find_nsv_gps_parquet_files(zone, phase, cycle, upc, lane="L1", base_path=NSV_BACKUP_BASE_PATH):
    """
    Locates NSV GPS parquet files in Databricks Volume structure.
    Defaults to looking ONLY for L1 lane files.
    /Volumes/nhai_dhms/bronze_layer/extraction_data/final_bronze_output/Source=NSV_Backup/Zone={Zone}/Phase={Phase}/Cycle={Cycle}/UPC={UPC}/.../Lane=L1/*.parquet
    """
    z_clean = sanitize_param(zone, "Zone")
    p_clean = sanitize_param(phase, "Phase")
    c_clean = sanitize_param(cycle, "Cycle")
    u_clean = sanitize_param(upc)

    lane_target = sanitize_param(lane if lane else "L1", "Lane").upper()

    # Search specifically under Lane=L1 path first
    pattern_specific = os.path.join(
        base_path,
        f"Zone={z_clean}",
        f"Phase={p_clean}",
        f"Cycle={c_clean}",
        f"UPC={u_clean}",
        "**",
        f"Lane={lane_target}",
        "*.parquet"
    )

    LOGGER.info(f"Searching specifically for {lane_target} Lane Parquet with pattern: {pattern_specific}")
    matched_files = glob.glob(pattern_specific, recursive=True)
    
    if not matched_files:
        # Fallback search across directory tree for files containing L1 in path or name
        pattern_fallback = os.path.join(
            base_path,
            f"Zone={z_clean}",
            f"Phase={p_clean}",
            f"Cycle={c_clean}",
            f"UPC={u_clean}",
            "**",
            "*.parquet"
        )
        all_matched = glob.glob(pattern_fallback, recursive=True)
        matched_files = [
            f for f in all_matched
            if f"LANE={lane_target}" in f.upper() or f"_{lane_target}_" in os.path.basename(f).upper() or f"_{lane_target}." in os.path.basename(f).upper()
        ]

    # Filter out lock/temporary files, then keep ONLY files with "GPS" in the name
    filtered_files = [f for f in matched_files if "$" not in os.path.basename(f)]
    gps_files = [f for f in filtered_files if "GPS" in os.path.basename(f).upper()]
    LOGGER.info(f"Found {len(gps_files)} {lane_target} GPS lane files (out of {len(filtered_files)} total) for UPC '{u_clean}'.")

    # ── FALLBACK: if no GPS files for the requested lane, try ANY available lane ──
    if not gps_files:
        pattern_any_lane = os.path.join(
            base_path,
            f"Zone={z_clean}",
            f"Phase={p_clean}",
            f"Cycle={c_clean}",
            f"UPC={u_clean}",
            "**",
            "*.parquet"
        )
        all_parquets = glob.glob(pattern_any_lane, recursive=True)
        all_gps = [f for f in all_parquets if "$" not in os.path.basename(f) and "GPS" in os.path.basename(f).upper()]
        if all_gps:
            # Extract lane from path and pick first available
            chosen = all_gps[0]
            lane_match = re.search(r"Lane=([^/]+)", chosen)
            fallback_lane = lane_match.group(1) if lane_match else "unknown"
            LOGGER.info(f"Lane {lane_target} not found — falling back to Lane={fallback_lane} ({len(all_gps)} GPS files available) for UPC '{u_clean}'.")
            gps_files = all_gps

    return gps_files

# =============================================================================
# DATA PROCESSING & MATCHING
# =============================================================================

def resolve_shapefile_path(path_input):
    if os.path.isfile(path_input):
        return path_input
    if os.path.isdir(path_input):
        shp_files = glob.glob(os.path.join(path_input, "*.shp")) + glob.glob(os.path.join(path_input, "**", "*.shp"), recursive=True)
        if shp_files:
            return shp_files[0]
    # Check parent directory fallback
    parent_dir = os.path.dirname(path_input)
    if os.path.isdir(parent_dir):
        shp_files = glob.glob(os.path.join(parent_dir, "*.shp"))
        if shp_files:
            return shp_files[0]
    return path_input

def load_input_data_parquet(nsv_parquet_file, ris_parquet_file, shapefile_path):
    """
    Reads NSV GPS parquet, RIS parquet, and State Boundary Shapefile.
    """
    try:
        nsv_df = pd.read_parquet(nsv_parquet_file)
        LOGGER.info(f"Loaded NSV Data ({nsv_df.shape[0]} rows) from: {nsv_parquet_file}")
        
        ris_df = pd.read_parquet(ris_parquet_file)
        LOGGER.info(f"Loaded RIS Data ({ris_df.shape[0]} rows) from: {ris_parquet_file}")

        actual_shp_path = resolve_shapefile_path(shapefile_path)
        if not os.path.exists(actual_shp_path):
            raise FileNotFoundError(f"State boundary shapefile not found at: {shapefile_path} or {actual_shp_path}")

        state_boundaries = gpd.read_file(actual_shp_path)
        LOGGER.info(f"Loaded State Boundaries: {len(state_boundaries)} polygons from {actual_shp_path}")

        return nsv_df, state_boundaries, ris_df
    except Exception as error:
        LOGGER.error(f"Error loading input data: {error}")
        raise

def validate_input_data(nsv_dataframe, state_boundaries):
    """
    Validates and identifies Latitude, Longitude, and Chainage columns in NSV Data.
    """
    lat_col = lon_col = chainage_col = None
    for col in nsv_dataframe.columns:
        col_lower = str(col).lower()
        if "lat" in col_lower and not lat_col:
            lat_col = col
        elif ("lon" in col_lower or "lng" in col_lower) and not lon_col:
            lon_col = col
        elif "chainage" in col_lower and not chainage_col:
            chainage_col = col

    missing_columns = []
    if not lat_col: missing_columns.append("Latitude")
    if not lon_col: missing_columns.append("Longitude")

    if missing_columns:
        raise ValueError(f"Missing essential columns in NSV data containing keywords: {missing_columns}")

    if state_boundaries.crs is None:
        raise ValueError("State shapefile CRS is missing.")

    return lat_col, lon_col, chainage_col

def map_nh_number_and_upc(nsv_df, ris_df, lat_col, lon_col, chainage_col, upc_val):
    """
    Maps NHNumber from RIS data to NSV GPS points using:
    1. Start Latitude, Start Longitude, End Latitude, End Longitude bounding box matching
    2. Start/End Chainage range matching
    """
    ris_cols_map = {str(c).strip().lower(): c for c in ris_df.columns}

    start_lat_col = ris_cols_map.get("start latitude") or ris_cols_map.get("startlatitude")
    start_lon_col = ris_cols_map.get("start longitude") or ris_cols_map.get("startlongitude")
    end_lat_col = ris_cols_map.get("end latitude") or ris_cols_map.get("endlatitude")
    end_lon_col = ris_cols_map.get("end longitude") or ris_cols_map.get("endlongitude")

    nh_col = (
        ris_cols_map.get("nhnumber") or 
        ris_cols_map.get("nh_number") or 
        ris_cols_map.get("nhnumber_old")
    )

    start_ch_col = (
        ris_cols_map.get("new nh number startchainage") or 
        ris_cols_map.get("old nh number startchainage") or 
        ris_cols_map.get("startchainage")
    )
    end_ch_col = (
        ris_cols_map.get("new nh number endchainage") or 
        ris_cols_map.get("old nh number endchainage") or 
        ris_cols_map.get("endchainage")
    )

    has_coord_matching = all([start_lat_col, start_lon_col, end_lat_col, end_lon_col, nh_col])
    has_chainage_matching = all([start_ch_col, end_ch_col, nh_col]) and chainage_col is not None

    n_points = len(nsv_df)
    nh_result = pd.Series([None] * n_points, index=nsv_df.index)

    gps_lat = pd.to_numeric(nsv_df[lat_col], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    gps_lon = pd.to_numeric(nsv_df[lon_col], errors="coerce").to_numpy(dtype=float, na_value=np.nan)

    # 1. Vectorized Coordinate Bounding Box Matching (numpy broadcasting)
    if has_coord_matching and len(ris_df) > 0:
        ris_slat = pd.to_numeric(ris_df[start_lat_col], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
        ris_elat = pd.to_numeric(ris_df[end_lat_col], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
        ris_slon = pd.to_numeric(ris_df[start_lon_col], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
        ris_elon = pd.to_numeric(ris_df[end_lon_col], errors="coerce").to_numpy(dtype=float, na_value=np.nan)

        min_lat = np.minimum(ris_slat, ris_elat) - 0.0005
        max_lat = np.maximum(ris_slat, ris_elat) + 0.0005
        min_lon = np.minimum(ris_slon, ris_elon) - 0.0005
        max_lon = np.maximum(ris_slon, ris_elon) + 0.0005
        nh_values = ris_df[nh_col].values

        # Process in chunks to keep memory bounded
        CHUNK = 50_000
        for chunk_start in range(0, n_points, CHUNK):
            chunk_end = min(chunk_start + CHUNK, n_points)
            lat_chunk = gps_lat[chunk_start:chunk_end, np.newaxis]
            lon_chunk = gps_lon[chunk_start:chunk_end, np.newaxis]

            in_box = (
                (min_lat[np.newaxis, :] <= lat_chunk) &
                (lat_chunk <= max_lat[np.newaxis, :]) &
                (min_lon[np.newaxis, :] <= lon_chunk) &
                (lon_chunk <= max_lon[np.newaxis, :])
            )
            has_match = in_box.any(axis=1)
            first_match_idx = np.argmax(in_box, axis=1)

            chunk_idx = nsv_df.index[chunk_start:chunk_end]
            matched_idx = chunk_idx[has_match]
            nh_result.loc[matched_idx] = nh_values[first_match_idx[has_match]]

    # 2. Vectorized Chainage Matching for remaining unmatched
    unmatched_mask = nh_result.isna()
    if has_chainage_matching and unmatched_mask.any():
        um_idx = nsv_df.index[unmatched_mask]
        ch_values = pd.to_numeric(nsv_df.loc[um_idx, chainage_col], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
        start_ch = pd.to_numeric(ris_df[start_ch_col], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
        end_ch = pd.to_numeric(ris_df[end_ch_col], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
        nh_values_ch = ris_df[nh_col].values

        ch_2d = ch_values[:, np.newaxis]
        in_range = (start_ch[np.newaxis, :] <= ch_2d) & (ch_2d <= end_ch[np.newaxis, :])
        has_match_ch = in_range.any(axis=1)
        first_match_ch = np.argmax(in_range, axis=1)

        matched_ch_idx = um_idx[has_match_ch]
        nh_result.loc[matched_ch_idx] = nh_values_ch[first_match_ch[has_match_ch]]

    # 3. Fallback: assign first RIS NHNumber to still-unmatched points
    still_unmatched = nh_result.isna()
    if nh_col and not ris_df.empty and still_unmatched.any():
        nh_result[still_unmatched] = ris_df.iloc[0][nh_col]

    nsv_df["NH Number"] = nh_result.values
    nsv_df["UPC"] = upc_val
    return nsv_df

def convert_points_to_geodataframe(nsv_dataframe, lat_col, lon_col):
    nsv_geodataframe = gpd.GeoDataFrame(
        nsv_dataframe,
        geometry=gpd.points_from_xy(
            nsv_dataframe[lon_col],
            nsv_dataframe[lat_col]
        ),
        crs=COMMON_CRS
    )
    return nsv_geodataframe

def assign_state_to_each_point(nsv_geodataframe, state_boundaries):
    state_boundaries = state_boundaries.to_crs(COMMON_CRS)
    nsv_with_state = gpd.sjoin(
        nsv_geodataframe,
        state_boundaries,
        how="left",
        predicate="within"
    )
    return nsv_with_state

def identify_state_crossings(nsv_with_state):
    nsv_with_state["PREVIOUS_STATE"] = nsv_with_state[STATE_NAME_COLUMN].shift()
    state_crossings = nsv_with_state[
        nsv_with_state[STATE_NAME_COLUMN] != nsv_with_state["PREVIOUS_STATE"]
    ]
    return state_crossings

def save_results(nsv_with_state, state_crossings, file_identifier, output_dir=OUTPUT_DIR_BASE):
    """
    Saves state detection outputs to Databricks Volumes in Parquet (and optional Excel) format.
    """
    cols = nsv_with_state.columns.tolist()
    first_cols = ["NH Number", "UPC"]
    
    for c in first_cols:
        if c in cols:
            cols.remove(c)
    final_cols = first_cols + cols
    
    nsv_with_state = nsv_with_state[final_cols]
    
    nsv_export = nsv_with_state.drop(columns=["geometry"]) if "geometry" in nsv_with_state.columns else nsv_with_state
    crossings_export = state_crossings.drop(columns=["geometry"]) if "geometry" in state_crossings.columns else state_crossings

    os.makedirs(output_dir, exist_ok=True)
    
    # Save as Parquet for native Databricks compatibility
    parquet_base_output = os.path.join(output_dir, f"NSV_Base_With_State_{file_identifier}.parquet")
    parquet_crossings_output = os.path.join(output_dir, f"State_Crossings_{file_identifier}.parquet")
    nsv_export.to_parquet(parquet_base_output, index=False)
    crossings_export.to_parquet(parquet_crossings_output, index=False)
    LOGGER.info(f"Saved Parquet outputs to Databricks Volume:\n - {parquet_base_output}\n - {parquet_crossings_output}")

    # Save as Excel if openpyxl is installed
    try:
        excel_output = os.path.join(output_dir, f"NSV_Base_With_State_{file_identifier}.xlsx")
        with pd.ExcelWriter(excel_output, engine="openpyxl") as writer:
            nsv_export.to_excel(writer, sheet_name="NSV_Base_With_State", index=False)
            crossings_export.to_excel(writer, sheet_name="State_Crossings", index=False)
        LOGGER.info(f"Exported Excel to Databricks Volume: {excel_output}")
    except Exception as e:
        LOGGER.warning(f"Could not export Excel file: {e}")

# =============================================================================
# SUMMARY REPORT GENERATION
# =============================================================================

def extract_state_sequence(nsv_dfs):
    """
    Extracts distinct states passed along the route in order of appearance.
    """
    if isinstance(nsv_dfs, pd.DataFrame):
        nsv_dfs = [nsv_dfs]

    states_sequence = []
    for df in nsv_dfs:
        if STATE_NAME_COLUMN in df.columns:
            for state_val in df[STATE_NAME_COLUMN].dropna():
                s_str = str(state_val).strip()
                if s_str and (not states_sequence or states_sequence[-1] != s_str):
                    states_sequence.append(s_str)

    distinct_states = list(dict.fromkeys(states_sequence))
    return distinct_states

def build_upc_summary_record(zone, phase, cycle, upc, nsv_dfs):
    distinct_states = extract_state_sequence(nsv_dfs)

    state_1 = distinct_states[0] if len(distinct_states) > 0 else ""
    state_2 = distinct_states[1] if len(distinct_states) > 1 else ""
    state_3 = distinct_states[2] if len(distinct_states) > 2 else ""

    # Extract the most common NH Number across all processed dataframes
    nh_number = ""
    dfs_list = nsv_dfs if isinstance(nsv_dfs, list) else [nsv_dfs]
    for df in dfs_list:
        if "NH Number" in df.columns:
            nh_vals = df["NH Number"].dropna()
            if not nh_vals.empty:
                mode_vals = nh_vals.mode()
                nh_number = str(mode_vals.iloc[0]) if not mode_vals.empty else str(nh_vals.iloc[0])
                break

    return {
        "Zone": str(zone),
        "Phase": str(phase),
        "Cycle": str(cycle),
        "UPC": str(upc),
        "NH Number": nh_number,
        "State_1": state_1,
        "State_2": state_2,
        "State_3": state_3
    }

def save_upc_summary_excel(records, output_dir=OUTPUT_DIR_BASE, file_name="UPC_State_Summary.xlsx"):
    """
    Saves the required summary Excel containing:
    ['Zone', 'Phase', 'Cycle', 'UPC', 'State_1', 'State_2', 'State_3']
    """
    os.makedirs(output_dir, exist_ok=True)
    df_summary = pd.DataFrame(records)

    required_cols = ["Zone", "Phase", "Cycle", "UPC", "NH Number", "State_1", "State_2", "State_3", "Remarks"]
    for col in required_cols:
        if col not in df_summary.columns:
            df_summary[col] = ""

    df_summary = df_summary[required_cols]

    excel_path = os.path.join(output_dir, file_name)

    try:
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            df_summary.to_excel(writer, sheet_name="UPC_State_Summary", index=False)
        LOGGER.info(f"Exported Required UPC State Summary Excel to: {excel_path}")
    except Exception as e:
        LOGGER.warning(f"Could not write Excel summary: {e}")

    return df_summary

# =============================================================================
# MAIN WORKFLOW
# =============================================================================

def process_single_lane_parquet(lane_parquet_file, ris_df, state_boundaries, upc):
    """Process a single GPS parquet file using pre-loaded RIS data and state boundaries."""
    LOGGER.info(f"Processing Lane file: {os.path.basename(lane_parquet_file)}")
    try:
        nsv_df = pd.read_parquet(lane_parquet_file)
        LOGGER.info(f"Loaded NSV Data ({nsv_df.shape[0]} rows)")

        # Sample every 10th point for faster state detection
        nsv_df = nsv_df.iloc[::10].reset_index(drop=True)
        LOGGER.info(f"Sampled every 10th point: {nsv_df.shape[0]} rows remaining")

        lat_col, lon_col, chainage_col = validate_input_data(nsv_df, state_boundaries)
        
        nsv_df = map_nh_number_and_upc(nsv_df, ris_df, lat_col, lon_col, chainage_col, upc)
        nsv_gdf = convert_points_to_geodataframe(nsv_df, lat_col, lon_col)
        nsv_with_state = assign_state_to_each_point(nsv_gdf, state_boundaries)
        
        return nsv_with_state
    except Exception as e:
        LOGGER.error(f"Error processing {lane_parquet_file}: {e}")
        raise

def recover_skipped_upc(zone, phase, cycle, upc, state_boundaries):
    """
    Recovery for UPCs where the standard GPS search missed files.
    Recursively searches the UPC folder (across both NSV_Backup and NSV
    sources) for any parquet with GPS lat/lon data, then applies the
    standard spatial join for state detection.
    """
    nh_number = ""
    state_1 = ""
    remarks_parts = []

    # Extract NH Number from RIS if available
    ris_file = find_ris_parquet_file(zone, phase, cycle, upc)
    if ris_file:
        try:
            ris_df = pd.read_parquet(ris_file)
            ris_cols_map = {str(c).strip().lower(): c for c in ris_df.columns}
            nh_col = ris_cols_map.get("nhnumber") or ris_cols_map.get("nh_number") or ris_cols_map.get("nhnumber_old")
            if nh_col and not ris_df.empty:
                nh_number = str(ris_df.iloc[0][nh_col])
                remarks_parts.append("NH from RIS")
        except Exception:
            pass

    # Recursive GPS search across both source paths
    z_clean = sanitize_param(zone, "Zone")
    p_clean = sanitize_param(phase, "Phase")
    c_clean = sanitize_param(cycle, "Cycle")
    u_clean = sanitize_param(upc)

    for base in [NSV_BACKUP_BASE_PATH, RIS_BASE_PATH]:
        if state_1:
            break
        upc_dir = os.path.join(base, f"Zone={z_clean}", f"Phase={p_clean}", f"Cycle={c_clean}", f"UPC={u_clean}")
        if not os.path.isdir(upc_dir):
            continue
        pattern = os.path.join(upc_dir, "**", "*.parquet")
        all_files = [f for f in glob.glob(pattern, recursive=True) if "$" not in os.path.basename(f)]
        # Prioritize files with "GPS" in name, then try the rest
        all_files.sort(key=lambda f: (0 if "GPS" in os.path.basename(f).upper() else 1))

        for pf in all_files[:5]:
            try:
                df = pd.read_parquet(pf)
                lat_col = lon_col = None
                for col in df.columns:
                    cl = str(col).lower()
                    if "lat" in cl and not lat_col:
                        lat_col = col
                    elif ("lon" in cl or "lng" in cl) and not lon_col:
                        lon_col = col
                if not (lat_col and lon_col):
                    continue
                sample = df[[lat_col, lon_col]].dropna().iloc[::10].reset_index(drop=True)
                sample[lat_col] = pd.to_numeric(sample[lat_col], errors="coerce")
                sample[lon_col] = pd.to_numeric(sample[lon_col], errors="coerce")
                sample = sample.dropna()
                if sample.empty:
                    continue
                gdf = gpd.GeoDataFrame(sample, geometry=gpd.points_from_xy(sample[lon_col], sample[lat_col]), crs=COMMON_CRS)
                joined = gpd.sjoin(gdf, state_boundaries, how="left", predicate="within")
                if STATE_NAME_COLUMN in joined.columns:
                    states_found = joined[STATE_NAME_COLUMN].dropna()
                    distinct = list(dict.fromkeys(str(s).strip() for s in states_found if str(s).strip()))
                    if distinct:
                        state_1 = distinct[0]
                        remarks_parts.append(f"State from GPS ({os.path.basename(pf)})")
                        break
            except Exception:
                continue

    if not state_1:
        remarks_parts.append("No GPS data found")

    return {
        "Zone": str(zone), "Phase": str(phase), "Cycle": str(cycle), "UPC": str(upc),
        "NH Number": nh_number, "State_1": state_1, "State_2": "", "State_3": "",
        "Remarks": " | ".join(remarks_parts)
    }

def process_project(zone, phase, cycle, upc, state_boundaries, lane="L1", output_dir=OUTPUT_DIR_BASE):
    LOGGER.info(f"Processing: Zone={zone}, Phase={phase}, Cycle={cycle}, UPC={upc}")
    
    # 1. Find RIS Parquet file
    ris_file = find_ris_parquet_file(zone, phase, cycle, upc)
    if not ris_file:
        LOGGER.error(f"Aborting: RIS file not found for UPC={upc}")
        return None

    # 2. Find L1 GPS files — use only the FIRST one (all cover the same route)
    lane_files = find_nsv_gps_parquet_files(zone, phase, cycle, upc, lane=lane)
    if not lane_files:
        LOGGER.error(f"Aborting: No L1 GPS files found for UPC={upc}")
        return None

    first_gps_file = lane_files[0]
    LOGGER.info(f"Using 1 GPS file (of {len(lane_files)} available): {os.path.basename(first_gps_file)}")

    # 3. Load RIS data once per UPC
    ris_df = pd.read_parquet(ris_file)

    # 4. Process the single GPS file with pre-loaded state boundaries
    try:
        result_df = process_single_lane_parquet(first_gps_file, ris_df, state_boundaries, upc)
        if result_df is not None:
            summary_record = build_upc_summary_record(zone, phase, cycle, upc, [result_df])
            LOGGER.info(f"Done — {summary_record}")
            return summary_record
    except Exception as exc:
        LOGGER.error(f"FAILED UPC={upc}: {exc}")

    return None

def discover_all_upcs(base_path=NSV_BACKUP_BASE_PATH):
    """
    Auto-discovers all Zone/Phase/Cycle/UPC combinations from the
    bronze layer directory structure under Source=NSV_Backup.
    Returns a list of dicts: [{"zone": ..., "phase": ..., "cycle": ..., "upc": ...}, ...]
    """
    discovered = []
    if not os.path.isdir(base_path):
        LOGGER.error(f"NSV Backup base path not found: {base_path}")
        return discovered

    for zone_dir in sorted(os.listdir(base_path)):
        if not zone_dir.startswith("Zone="):
            continue
        zone_val = zone_dir.split("=", 1)[1]
        zone_path = os.path.join(base_path, zone_dir)

        for phase_dir in sorted(os.listdir(zone_path)):
            if not phase_dir.startswith("Phase="):
                continue
            phase_val = phase_dir.split("=", 1)[1]
            phase_path = os.path.join(zone_path, phase_dir)

            for cycle_dir in sorted(os.listdir(phase_path)):
                if not cycle_dir.startswith("Cycle="):
                    continue
                cycle_val = cycle_dir.split("=", 1)[1]
                cycle_path = os.path.join(phase_path, cycle_dir)

                for upc_dir in sorted(os.listdir(cycle_path)):
                    if not upc_dir.startswith("UPC="):
                        continue
                    upc_val = upc_dir.split("=", 1)[1]
                    discovered.append({
                        "zone": zone_val,
                        "phase": phase_val,
                        "cycle": cycle_val,
                        "upc": upc_val
                    })

    LOGGER.info(f"Discovered {len(discovered)} UPCs across all zones.")
    return discovered


def process_all_projects(lane="L1", shapefile_path=STATE_BOUNDARY_PATH, output_dir=OUTPUT_DIR_BASE, zones=None):
    """
    Zone-by-zone processing: discovers every UPC, groups by zone,
    processes each zone sequentially, saves a separate Excel per zone,
    then writes a final consolidated Excel.
    Loads shapefile ONCE and reuses it for all UPCs.
    """
    import time as _time

    LOGGER.info("=" * 60)
    LOGGER.info("BATCH STATE DETECTION — Zone-by-Zone Processing")
    LOGGER.info("=" * 60)

    # ── Load state boundary shapefile ONCE ──
    actual_shp_path = resolve_shapefile_path(shapefile_path)
    state_boundaries = gpd.read_file(actual_shp_path).to_crs(COMMON_CRS)
    LOGGER.info(f"Loaded State Boundaries ONCE: {len(state_boundaries)} polygons")

    # ── Pre-build spatial index once for faster sjoin ──
    _ = state_boundaries.sindex

    all_upcs = discover_all_upcs()
    if not all_upcs:
        LOGGER.error("No UPCs discovered. Exiting.")
        return

    # ── Group UPCs by Zone ──
    zone_groups = defaultdict(list)
    for entry in all_upcs:
        zone_groups[entry["zone"]].append(entry)

    sorted_zones = sorted(zone_groups.keys())
    if zones:
        sorted_zones = [z for z in sorted_zones if z in zones]
    LOGGER.info(f"Processing {len(sorted_zones)} zone(s): {sorted_zones} (out of {len(zone_groups)} total)")

    all_summary_records = []
    batch_start = _time.time()
    global_idx = 0
    total = len(all_upcs)

    for zone_name in sorted_zones:
        zone_upcs = zone_groups[zone_name]
        zone_records = []
        zone_start = _time.time()

        LOGGER.info(f"\n{'=' * 60}")
        LOGGER.info(f"STARTING ZONE: {zone_name} ({len(zone_upcs)} UPCs)")
        LOGGER.info(f"{'=' * 60}")

        for entry in zone_upcs:
            global_idx += 1
            zone, phase, cycle, upc = entry["zone"], entry["phase"], entry["cycle"], entry["upc"]
            LOGGER.info(f"[{global_idx}/{total}] Zone={zone}, Phase={phase}, Cycle={cycle}, UPC={upc}")

            try:
                record = process_project(
                    zone=zone, phase=phase, cycle=cycle, upc=upc,
                    state_boundaries=state_boundaries, lane=lane, output_dir=output_dir
                )
                if record:
                    record["Remarks"] = "Processed"
                    zone_records.append(record)
                    LOGGER.info(f"[{global_idx}/{total}] Done — {record}")
                else:
                    skip_rec = recover_skipped_upc(zone, phase, cycle, upc, state_boundaries)
                    zone_records.append(skip_rec)
                    LOGGER.warning(f"[{global_idx}/{total}] Recovered: {skip_rec['Remarks']} for UPC={upc}")
            except Exception as exc:
                fail_rec = recover_skipped_upc(zone, phase, cycle, upc, state_boundaries)
                fail_rec["Remarks"] = f"Failed — {exc}" + (f" | {fail_rec['Remarks']}" if fail_rec.get('State_1') else "")
                zone_records.append(fail_rec)
                LOGGER.error(f"[{global_idx}/{total}] FAILED Zone={zone} Phase={phase} Cycle={cycle} UPC={upc}: {exc}")

        # ── Save per-zone Excel immediately after zone completes ──
        zone_file = f"UPC_State_Summary_Zone_{zone_name}.xlsx"
        df_zone = save_upc_summary_excel(zone_records, output_dir=output_dir, file_name=zone_file)
        zone_elapsed = _time.time() - zone_start
        n_ok = sum(1 for r in zone_records if r.get("Remarks") == "Processed")
        n_skip = len(zone_records) - n_ok
        LOGGER.info(f"ZONE {zone_name} COMPLETE — {n_ok} processed, {n_skip} skipped/failed, {zone_elapsed/60:.1f} min")
        LOGGER.info(f"Saved: {os.path.join(output_dir, zone_file)}")
        print(f"\n{'='*60}")
        print(f"ZONE {zone_name} — {n_ok} processed, {n_skip} skipped/failed")
        print(f"{'='*60}")
        display(df_zone)
        all_summary_records.extend(zone_records)

    # ── Save final consolidated Excel for ALL zones ──
    if all_summary_records:
        df_final = save_upc_summary_excel(
            all_summary_records,
            output_dir=output_dir,
            file_name="UPC_State_Summary_ALL.xlsx"
        )
        elapsed = _time.time() - batch_start
        LOGGER.info(f"\n{'=' * 60}")
        LOGGER.info(f"ALL ZONES COMPLETE — {len(all_summary_records)}/{total} UPCs in {elapsed/60:.1f} min")
        LOGGER.info(f"Per-zone files: UPC_State_Summary_Zone_<X>.xlsx")
        LOGGER.info(f"Consolidated: {os.path.join(output_dir, 'UPC_State_Summary_ALL.xlsx')}")
        LOGGER.info(f"{'=' * 60}")
        display(df_final)
    else:
        LOGGER.error("No UPCs were processed successfully across any zone.")


# =============================================================================
# RUN: Zone A → save Excel → Zone B → save Excel → … → consolidated Excel
# =============================================================================
process_all_projects(zones=["B"])
