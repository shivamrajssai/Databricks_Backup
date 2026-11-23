# Databricks notebook source
# DBTITLE 1,Install openpyxl
# MAGIC %pip install openpyxl -q

# COMMAND ----------

# DBTITLE 1,Open Old silver population (As per contract) CSV
import pandas as pd

CSV_PATH = "/Workspace/Users/shivam.raj9226@softsensor.ai/SIlver layer defect data extraction/Input/Old silver population(As per contract).csv"

old_apc = pd.read_csv(CSV_PATH)
print('Shape:', old_apc.shape)
print('Columns:', list(old_apc.columns))
display(old_apc)

# COMMAND ----------

# DBTITLE 1,Surveys with Fail Issue greater than zero
# Keep only rows where Fail Issue is filled in (not blank) and greater than 0
fail_issue = pd.to_numeric(old_apc['Fail Issue'], errors='coerce')
failed = old_apc[fail_issue.notna() & (fail_issue > 0)]

print('Rows with Fail Issue > 0:', len(failed), 'of', len(old_apc))
display(failed)

# COMMAND ----------

# DBTITLE 1,Issue tracker for the 53 surveys with Fail Issue > 0
# Issue tracker (ASD-STE100 wording) for every survey in `failed` (Fail Issue > 0).
# Gandhinagar + Delhi rows: from the existing issue trackers.
# Bhopal + Jaipur rows: found from dlm_kpi.dlm_validation_results (latest run), the DB 10m tables and the bronze D1 / backup inputs.
# Code reference: (Clone) DLM Extraction Notebook_(5_Nov_2026) - section_code / nh_number come from LRPMaster first, then PavementType
# (cell 7 get_road_context); V5 expects ONE value, the first non-null value of the D1 PavementType file (cell 15).
import re

COLS = ['RO', 'Zone', 'Phase', 'Cycle', 'UPC', 'Validation fail', 'Issue Description', 'Root Cause', 'Status', 'Required Action']
SPLIT_ACTION = 'Remove the rounding and the second split. Use the start and end chainages from the backup files. Run the survey again. Do a check of the 10m, 100m and 1km tables.'
NEG_IRI_ACTION = 'Remove IRI values < 0 before you calculate the 100m and 1km values. Run the survey again.'
V5_DIR_ACTION = 'Change V5 so that it compares each direction with its own D1 code. Run V5 again.'
V5_RANGE_ACTION = 'Change V5 so that it checks the NH number and the section code for each chainage range. Run V5 again.'

R = [
# ── Gandhinagar (Zone B) ─────────────────────────────────────────────────────────────
('Gandhinagar','B',1,1,'N_04006_03001_GJ','[9] - V4 Hierarchy [6] | V6 Backup [3]',
 'The DB has 5,890 10m rows that are not in the backup. The 100m and 1km values do not agree with the values calculated again from the 10m rows (about 25% of rows). 23,560 of 53,013 10m rows are off the 10m grid, with a length that is not 10 m.',
 'Code bug in the 10m grid split: int(round(s/10))*10 uses banker\'s rounding on off-grid backup chainages.','Open', SPLIT_ACTION),
('Gandhinagar','B',1,2,'N_02001_12001_GJ','[1] - V4 Hierarchy [1]',
 'master_roughness_child_100m: 2 of 6,217 rows do not agree with the recompute. 76 10m rows have a negative IRI.',
 'Negative / -1 IRI in the backup (no reading at near-zero speed) is included in the 100m average.','Open', NEG_IRI_ACTION),
('Gandhinagar','B',2,1,'N_04026_04001_GJ','[9] - V4 Hierarchy [6] | V6 Backup [3]',
 'The DB has 3,213 10m rows that are not in the backup. About 50% of the V4 rows do not agree. 6,424 of 16,103 rows are off the 10m grid.',
 'Same 10m split bug (banker\'s rounding on off-grid backup chainages).','Open', SPLIT_ACTION),
('Gandhinagar','B',1,2,'N_02001_13003_GJ','[1] - V4 Hierarchy [1]',
 'master_roughness_child_100m: 9 of 5,001 rows do not agree with the recompute. Lane R2 has LWP IRI = -1 on 8,330 of 8,331 10m rows, so the left wheel path was not recorded for the full lane. 62 more -1 rows are in L1, L2, L3 and R3.',
 'Data: the R2 LWP sensor reading is missing for the full lane, and there are -1 readings at low speed. Code: the -1 values are averaged as real IRI, so the R2 roughness is too low.','Open',
 'Ask the data team to process R2 LWP again. Remove IRI values < 0 before you calculate averages. Run the survey again.'),
('Gandhinagar','B',1,1,'N_04003_13003_GJ','[9] - V4 Hierarchy [6] | V6 Backup [3]',
 'In lane R1, the 10m Roughness, Rutting and Ravelling tables have 1,602 extra rows. Backup rows that start at a chainage that ends in 5 are rounded and split again, so the code makes duplicate rows. Because of this, the 100m and 1km values also do not agree.',
 'The code uses banker\'s rounding when it splits the 10m backup data. The same rounding also gives the wrong 10m length to the R2 and R3 rows.','Open', SPLIT_ACTION),
('Gandhinagar','B',1,2,'N_02014_01002_GJ','[9] - V4 Hierarchy [6] | V6 Backup [3]',
 'The DB has 59 10m rows (roughness / rutting / ravelling) that are not in the backup. Lane L3 has only 117 backup rows (107,785 to 108,955 m), and all start at a chainage that ends in 5. V4: 12 to 13 of 4,771 100m rows and 2 of 483 1km rows do not agree.',
 'Same 10m split bug.','Open', SPLIT_ACTION),
('Gandhinagar','B',1,2,'N_04009_01001_GJ','[1] - V4 Hierarchy [1]',
 'master_roughness_child_100m: 3 of 5,262 rows do not agree (L1 47,100-47,200 and R2 14,800-14,900 / 15,000-15,100). 125 10m rows have a negative IRI (91 with both wheel paths = -1). For example, 7 of 10 rows in L1 47,100-47,200 are -1, so the stored 100m value is -0.3876.',
 'Negative / -1 IRI in the backup (no reading at near-zero speed) is included in the 100m average.','Open', NEG_IRI_ACTION),
('Gandhinagar','B',1,2,'N_02001_16007_GJ','[1] - V4 Hierarchy [1]',
 'master_roughness_child_100m: 8 of 7,081 rows do not agree with the recompute. 272 10m rows have a negative IRI (218 with both wheel paths = -1), mostly in L1 and L2.',
 'Negative / -1 IRI in the backup (no reading at near-zero speed) is included in the 100m average.','Open', NEG_IRI_ACTION),
# ── Delhi (Zone A / E) ───────────────────────────────────────────────────────────────
('Delhi','A',1,2,'N_03009_01002_HR','[1] - V5 D1 [1] Section Code',
 'The section code in the DB is not the same as the code in D1. All 13,859 DB rows have ISM-DHA. D1 PavementType has GAN-CLD.',
 'D1 LRPMaster has ISM-DHA. All other D1 files have GAN-CLD. The code uses LRPMaster first. V5 uses PavementType.','Open',
 'Get the correct section code from the data team. Make the code and V5 use the same D1 file. Run the survey again.'),
('Delhi','A',1,2,'N_03009_01003_HR','[1] - V5 D1 [1] Section Code',
 'The section code in the DB is not the same as the code in D1. All 19,816 DB rows have DHA-RAJ. D1 PavementType has ISM-NUL.',
 'D1 LRPMaster has DHA-RAJ. All other D1 files have ISM-NUL. The code uses LRPMaster first. V5 uses PavementType.','Open',
 'Get the correct section code from the data team. Make the code and V5 use the same D1 file. Run the survey again.'),
('Delhi','A',1,2,'N_03009_01004_HR','[1] - V5 D1 [1] Section Code',
 'The section code in the DB is not the same as the code in D1. All 14,443 DB rows have RAJ-KHE. D1 PavementType has ISM-NAL.',
 'D1 LRPMaster has RAJ-KHE. All other D1 files have ISM-NAL. The code uses LRPMaster first. V5 uses PavementType.','Open',
 'Get the correct section code from the data team. Make the code and V5 use the same D1 file. Run the survey again.'),
('Delhi','A',1,1,'N_03009_01005_HR','[9] - V4 Hierarchy [6] | V6 Backup [3]',
 'The 10m Roughness, Rutting and Ravelling tables have 1,401 rows that are not in the backup. All these rows are in lane R1. 280-281 of 1,686 100m rows and 29 of 174 1km rows do not agree with the values calculated again from the 10m rows. 5,607 of 16,830 backup rows are not on the 10m grid.',
 'All 2,802 R1 backup rows start at a chainage that ends in 5. The code rounds each chainage and splits it again into 10m rows. The banker\'s rounding puts every second row in two 10m rows. The same rounding gives the wrong length to the R3 rows, which start at a chainage that ends in 1.','Open', SPLIT_ACTION),
('Delhi','A',1,2,'N_01005_01009_HR','[1] - V5 D1 [1] Section Code',
 'The section code in the DB is not the same as the code in D1. All 81,180 DB rows have KUN-PAL. D1 PavementType has EPE-PRW.',
 'D1 LRPMaster has KUN-PAL. All other D1 files (more than 20 files) have EPE-PRW. The code uses LRPMaster first. V5 uses PavementType.','Open',
 'Get the correct section code from the data team. Make the code and V5 use the same D1 file. Run the survey again.'),
('Delhi','A',1,1,'N_01007_01001_DL','[1] - V5 D1 [1] Section Code',
 'The section code is not correct in 2,616 of 4,358 rows. These are the Decreasing (R-lane) rows, which have MEE_DEL. V5 expects one code, DEL-MEE, for all rows.',
 'Data: D1 gives one code for each direction: DEL-MEE (Increasing) and MEE-DEL (Decreasing). D1 also has two LRPMaster files (v1.0 and v1.01). The v1.01 file has a typing error, MEE_DEL (underscore), and the code uses this value. Code: V5 compares all rows with one code only.','Open',
 'Ask the data team to remove or correct the LRPMaster v1.01 file. Change V5 to compare each direction with its own D1 code. Run the survey again.'),
('Delhi','A',1,1,'N_01007_02001_DL','[1] - V5 D1 [1] Section Code',
 'The section code is not correct in 5,788 of 11,575 rows. These are the Decreasing (R-lane) rows, which have MEE_DEL. V5 expects DEL-MEE.',
 'D1 has the same two LRPMaster files (v1.0 and v1.01, typo MEE_DEL) and one code for each direction. V5 compares all rows with one code only.','Open',
 'Ask the data team to remove or correct the LRPMaster v1.01 file. Change V5 to compare each direction with its own D1 code. Run the survey again.'),
('Delhi','A',1,2,'N_03002_01004_HR','[1] - V5 D1 [1] Section Code',
 'The section code in the DB is not the same as the code in D1. All 15,640 DB rows have DND-KMP. D1 PavementType has BAL-KMP.',
 'D1 LRPMaster has DND-KMP. All other D1 files have BAL-KMP. The code uses LRPMaster first. V5 uses PavementType.','Open',
 'Get the correct section code from the data team. Make the code and V5 use the same D1 file. Run the survey again.'),
('Delhi','A',1,1,'N_05006_01001_HR','[10] - V4 Hierarchy [6] | V5 NH [1] | V6 Backup [3]',
 'The DB has 570 10m rows that are not in the backup. Six 100m and 1km tables do not agree with their 10m rows. The NH number fails on 5,613 of 5,723 rows.',
 'All R1 starts end in 5, which causes the 10m split bug. The UPC has 3 roads (NH0011, NH148B, Nizampur Link Road). V5 expects one NH (NIZAMPUR LINK ROAD).','Open',
 'Fix the 10m split. Change V5 so that it checks the NH number for each chainage range. Run the survey again.'),
('Delhi','A',1,2,'N_05006_01001_HR','[2] - V5 NH [1] | V5 Section Code [1]',
 'The NH number fails on 17,592 of 17,721 rows. The section code fails on 3,039 rows.',
 'The UPC has 3 roads and 2 codes (PAN-NAR, NAR-PAC). V5 expects one NH and one code.','Open', V5_RANGE_ACTION),
('Delhi','A',3,1,'N_05006_01001_HR','[2] - V5 NH [1] | V5 Section Code [1]',
 'The NH number fails on 920 of 1,106 rows. The section code fails on 374 rows. The PavementType NH column is \'Old NH number EndChainage\'.',
 'The UPC has 3 roads and 2 codes. V5 expects one NH and one code.','Open', V5_RANGE_ACTION),
('Delhi','A',1,1,'N_05006_01002_HR','[1] - V5 NH [1]',
 'All 1,362 rows fail. The DB has \'ILHM\'. D1 NHMaster and PavementType have \'ILMH\'.',
 'LRPMaster has the typing error "ILHM". The code uses LRPMaster first.','Open',
 'Correct LRPMaster to \'ILMH\'. Run the survey again.'),
('Delhi','A',1,2,'N_01001_03003_RJ','[2] - V5 NH [1] | V5 Section Code [1]',
 'All 28,934 rows fail both checks. The DB has NH-148N and DEL-VAD / VAD-DEL. V5 expects NE0004 and DEL-VRA. The D2 \'100 m\' file covers 151,840-182,900 m. The backup covers 115,594-151,840 m.',
 'LRPMaster does not agree with NHMaster (NE0004) and PavementType (DEL-VRA). The D2 file is from a different package, so the code does not correct the crack area.','Open',
 'Get the correct NH number and code from the data team. Upload the correct D2 file. Run the survey again.'),
('Delhi','E',1,1,'N_04040_02001_UP','[1] - V5 D1 [1] Section Code',
 'All 24,560 rows fail. The DB has SHA-BYP. V5 expects \'SHA -BYP\'.',
 'PavementType has an extra space in the code.','Open',
 'Correct PavementType to "SHA-BYP". Also, remove spaces from codes before V5 compares them.'),
('Delhi','E',1,1,'N_05048_02002_UP','[1] - V5 NH [1]',
 'All 14,232 rows fail. V5 expects \'+1000.0\'. The DB has NH-709AD, which agrees with LRPMaster and NHMaster.',
 'V5 reads a chainage value as the NH number. It reads the wrong D1 column, so the data is correct and the check is wrong.','Open',
 'Change V5 so that it reads the NH column by its exact name. Run V5 again.'),
# ── Bhopal (Zone C) - new diagnosis ──────────────────────────────────────────────────
('Bhopal','C',1,1,'N_05029_02001_MP','[1] - V5 D1 [1] Section Code',
 'The section code is not correct in 9,265 of 18,528 rows. These are the Decreasing (R-lane) rows, which have ITA-OBA. V5 expects one code, OBA-ITA, for all rows.',
 'D1 gives one code for each direction: OBA-ITA (Increasing) and ITA-OBA (Decreasing). LRPMaster and PavementType agree. The data is correct. V5 compares all rows with one code only.','Open', V5_DIR_ACTION),
('Bhopal','C',2,1,'N_04004_05001_MP','[1] - V5 D1 [1] Section Code',
 'All 36,767 rows fail. The DB has \'GUN - BIA\' (with spaces). V5 expects \'GUN-BIA\'.',
 'LRPMaster has \'GUN - BIA\'. PavementType has \'GUN-BIA\'. The code uses LRPMaster first. V5 uses PavementType.','Open',
 'Correct LRPMaster to \'GUN-BIA\'. Also, remove spaces from codes before V5 compares them. Run the survey again.'),
('Bhopal','C',2,1,'N_03010_02002_MP','[1] - V5 D1 [1] Section Code',
 'The section code is not correct in 11,254 of 22,509 rows. The DB has CHH-NAR on the L lanes and NAR-CHH on the R lanes. V5 expects one code, CHH-NAR.',
 'The code gives one code for each direction. V5 compares all rows with one code only. Also, the latest NSV dates (24 Aug, 19 Sep and 20 Sep 2026) have no D1 folder. The D1 files in the 30 Jul and 10 Aug 2026 folders have SAO-CHH, not CHH-NAR.','Open',
 'Ask the data team for the correct section code and the current D1 files. Upload D1 to the latest NSV date. Change V5 to compare each direction with its own code. Run the survey again.'),
('Bhopal','C',1,1,'N_02008_04001_MP','[1] - V4 Hierarchy [1]',
 'master_roughness_child_100m: 2 of 3,600 rows do not agree with the recompute. 96 10m rows have a negative wheel-path IRI (L1 30, L2 23, R1 16, R2 27).',
 'Negative / -1 IRI in the backup (no reading at low speed) is included in the 100m average.','Open', NEG_IRI_ACTION),
('Bhopal','C',1,1,'N_04004_01003_MP','[1] - V4 Hierarchy [1]',
 'master_roughness_child_100m: 1 of 1,685 rows does not agree with the recompute. 34 10m rows have a negative wheel-path IRI (L1 31, L2 1, R1 1, R2 1). Also, all 4,212 R1 rows are off the 10m grid, but V6 passed.',
 'Negative / -1 IRI in the backup (no reading at low speed) is included in the 100m average.','Open', NEG_IRI_ACTION),
('Bhopal','C',2,1,'N_02008_08002_MP','[1] - V5 D1 [1] Section Code',
 'The section code is not correct in 16,283 of 32,614 rows. The DB has GWA-JHA on the L lanes and JHA-GWA on the R lanes. V5 expects GWA-JHA.',
 'LRPMaster has one code for each direction (GWA-JHA Increasing, JHA-GWA Decreasing). PavementType has GWA-JHA for both directions. The code uses LRPMaster first. V5 uses PavementType.','Open',
 'Ask the data team which code is correct for the Decreasing direction. Make LRPMaster and PavementType agree, or change V5 to compare each direction with its own code. Run the survey again.'),
('Bhopal','C',1,1,'N_04004_09001_MP','[1] - V4 Hierarchy [1]',
 'master_roughness_child_100m: 2 of 3,104 rows do not agree with the recompute. 83 10m rows have a negative wheel-path IRI (L1 19, L2 14, R1 28, R2 22).',
 'Negative / -1 IRI in the backup (no reading at low speed) is included in the 100m average.','Open', NEG_IRI_ACTION),
('Bhopal','C',2,1,'N_04006_05002_MP','[1] - V5 D1 [1] Section Code',
 'All 5,430 rows fail. The DB has \'MAC -IND\' (with a space). V5 expects \'MAC-IND\'.',
 'LRPMaster has \'MAC -IND\'. PavementType has \'MAC-IND\'. The code uses LRPMaster first. V5 uses PavementType.','Open',
 'Correct LRPMaster to \'MAC-IND\'. Also, remove spaces from codes before V5 compares them. Run the survey again.'),
('Bhopal','C',1,1,'N_04019_01003_MP','[1] - V5 NH [1]',
 'The NH number fails on 28,614 of 32,671 rows. The DB has NH-34 on most rows and NH-30 on the other rows. V5 expects NH-30.',
 'The UPC has 2 NHs (NH0034 and NH0030) in LRPMaster, NHMaster and PavementType. V5 expects one NH. D1 (31 Jul 2026 folder) has two PavementType files. V5 takes the first file, v1.01, which has only NH0030 and section SLE-JAB. The latest NSV date (19 Sep 2026) has no D1 folder.','Open',
 'Change V5 so that it checks the NH number for each chainage range. Ask the data team to remove the v1.01 PavementType file if it is not for this section. Run V5 again.'),
('Bhopal','C',1,3,'N_04019_01003_MP','[1] - V5 NH [1]',
 'The NH number fails on 28,218 of 32,686 rows. The DB has NH-34 on most rows and NH-30 on the other rows. V5 expects NH-30.',
 'The UPC has 2 NHs (NH0034 and NH0030) in LRPMaster and PavementType. V5 expects one NH. It takes the NH of the first PavementType row (NH0030). The latest NSV dates (24 Aug, 19 Sep 2026) have no D1 folder.','Open', V5_RANGE_ACTION),
('Bhopal','C',1,3,'N_04028_02001_MP','[9] - V4 Hierarchy [6] | V6 Backup [3]',
 'The 10m Roughness, Rutting and Ravelling tables have 6,440 rows that are not in the backup. L1 and L2 have 9,661 rows each. R1 and R2 have about 6,440 rows each. In L1 and L2, 6,441 rows each are off the 10m grid, with a length that is not 10 m. V4: 795-1,289 of 2,579 100m rows and 129-130 of 260 1km rows do not agree.',
 'Same 10m split bug: the code rounds off-grid backup chainages and splits them again into 10m rows.','Open', SPLIT_ACTION),
('Bhopal','C',2,1,'N_04005_02001_MP','[2] - V5 NH [1] | V5 Section Code [1]',
 '14,858 of 22,806 rows fail both checks. The DB has NH-753L / DHN-BOR on most rows and NH-347BG / DHA-BOR on the other rows. V5 expects NH-347BG and DHA-BOR.',
 'The UPC has 2 roads in D1: NH753L (DHN-BOR) and NH347BG (DHA-BOR). LRPMaster, NHMaster and PavementType agree. The data is correct. V5 expects one NH and one code.','Open', V5_RANGE_ACTION),
('Bhopal','C',1,1,'N_01001_04003_MP','[1] - V5 NH [1]',
 'All 26,429 rows fail. The DB has NH-148N. V5 expects NE0004.',
 'LRPMaster and NHMaster have NH148N. PavementType has NE0004. The code uses LRPMaster first. V5 uses PavementType.','Open',
 'Get the correct NH number from the data team. Make LRPMaster, NHMaster and PavementType agree. Run the survey again.'),
('Bhopal','C',1,1,'N_01001_04005_MP','[1] - V5 NH [1]',
 'All 20,250 rows fail. The DB has NH-148N. V5 expects NE0004.',
 'LRPMaster and NHMaster have NH-148N. PavementType has NE0004. The code uses LRPMaster first. V5 uses PavementType.','Open',
 'Get the correct NH number from the data team. Make LRPMaster, NHMaster and PavementType agree. Run the survey again.'),
('Bhopal','C',1,3,'N_01001_04007_MP','[1] - V5 D1 [1] Section Code',
 'All 19,992 rows fail. The DB has DLL-VDR. V5 expects DLL-VRD.',
 'LRPMaster has DLL-VDR. PavementType has DLL-VRD (two letters are in a different order). The code uses LRPMaster first. V5 uses PavementType.','Open',
 'Get the correct section code from the data team. Correct the D1 file that is wrong. Run the survey again.'),
('Bhopal','C',1,1,'N_01001_04008_MP','[1] - V5 NH [1]',
 'All 20,240 rows fail. The DB has NH-148N. V5 expects NE0004.',
 'LRPMaster and NHMaster have NH148N. PavementType has NE0004. The code uses LRPMaster first. V5 uses PavementType.','Open',
 'Get the correct NH number from the data team. Make LRPMaster, NHMaster and PavementType agree. Run the survey again.'),
('Bhopal','C',1,1,'N_01001_04009_MP','[1] - V5 NH [1]',
 'All 16,840 rows fail. The DB has NH-148N. V5 expects NE0004.',
 'LRPMaster and NHMaster have NH148N. PavementType has NE0004. The code uses LRPMaster first. V5 uses PavementType.','Open',
 'Get the correct NH number from the data team. Make LRPMaster, NHMaster and PavementType agree. Run the survey again.'),
('Bhopal','C',1,1,'N_01001_04010_MP','[2] - V4 Hierarchy [1] | V5 NH [1]',
 'All 18,525 rows fail the NH check. The DB has NH-148N. V5 expects NE0004. master_roughness_child_100m: 1 of 1,864 rows does not agree with the recompute. 8 10m rows have a negative wheel-path IRI (L1 3, L2 2, L4 1, R4 2).',
 'NH: LRPMaster and NHMaster have NH148N. PavementType has NE0004. V4: negative / -1 IRI in the backup is included in the 100m average.','Open',
 'Get the correct NH number from the data team and make the D1 files agree. Remove IRI values < 0 before you calculate averages. Run the survey again.'),
('Bhopal','C',1,1,'N_02008_16002_MP','[9] - V4 Hierarchy [6] | V6 Backup [3]',
 'The 10m Roughness, Rutting and Ravelling tables have 1,489 rows that are not in the backup. 31,214 of 32,703 R1 rows and 30,637 of 31,093 R2 rows are off the 10m grid, with a length that is not 10 m. V4: 298-299 of 12,541 100m rows and 30 of 1,267 1km rows do not agree. 104 10m rows also have a negative wheel-path IRI.',
 'Same 10m split bug: the code rounds off-grid R-lane backup chainages and splits them again into 10m rows.','Open', SPLIT_ACTION),
('Bhopal','C',1,2,'N_02008_16002_MP','[5] - V4 Hierarchy [2] | V6 Backup [3]',
 'The 10m Roughness, Rutting and Ravelling tables have 12,688 rows that are not in the backup. 12,679 L1 rows and 12,696 L2 rows are off the 10m grid. V4 fails for Ravelling only: 628 of 12,507 100m rows and 64 of 1,258 1km rows. Roughness and Rutting 100m / 1km pass.',
 'Same 10m split bug on the L lanes. Probable cause for Ravelling only: the extra 10m rows change the covered length that the Ravelling % formula uses. Roughness and Rutting use an average, so they are not changed.','Open', SPLIT_ACTION),
('Bhopal','C',1,1,'N_04019_03001_MP','[9] - V4 Hierarchy [6] | V6 Backup [3]',
 'The 10m Roughness, Rutting and Ravelling tables have 2,425 rows that are not in the backup. All 6,973 R1 rows and 6,971 of 9,396 R2 rows are off the 10m grid. R2 has 2,398 more rows than L1 and L2 (6,998 each). V4: 484-486 of 2,798 100m rows and 49 of 286 1km rows do not agree.',
 'Same 10m split bug: the code rounds off-grid R-lane backup chainages and splits them again into 10m rows.','Open', SPLIT_ACTION),
('Bhopal','C',2,1,'N_05003_04001_MP','[9] - V4 Hierarchy [6] | V6 Backup [3]',
 'The 10m Roughness, Rutting and Ravelling tables have 6,679 rows that are not in the backup. R1 has 20,036 rows and L1 has 13,354 rows. 13,357 R1 rows are off the 10m grid. 20,036 - 13,357 = 6,679, the same as the extra DB rows. V4: 491-1,336 of 2,674 100m rows and 123-134 of 268 1km rows do not agree.',
 'Same 10m split bug: the code rounds off-grid R1 backup chainages and splits them again into 10m rows.','Open', SPLIT_ACTION),
('Bhopal','C',2,1,'N_05042_01001_MP','[1] - V5 D1 [1] Section Code',
 'The section code is not correct in 2,480 of 10,756 rows. These rows have DEW-BYP (bypass). The other rows have DEW-UJJ. V5 expects one code, DEW-UJJ.',
 'The UPC has 2 sections in D1: DEW-UJJ and DEW-BYP. LRPMaster has \'DEW-BYP\'. PavementType has \'DEW- BYP\' (with a space). V5 expects one code.','Open',
 'Change V5 so that it checks the section code for each chainage range. Correct PavementType to \'DEW-BYP\'. Run V5 again.'),
# ── Jaipur (Zone B) - new diagnosis ──────────────────────────────────────────────────
('Jaipur','B',1,1,'N_01001_03004_RJ','[1] - V5 D1 [1] Section Code',
 'The section code is not correct in 12,493 of 24,985 rows. These are the R-lane rows, which have VAD-DEL. V5 expects one code, DEL-VAD.',
 'LRPMaster has one code for each direction (DEL-VAD Increasing, VAD-DEL Decreasing). PavementType has DEL-VAD with Direction \'Both\'. The code uses LRPMaster first. Also, D1 is only in the 25 Jul and 29 Jul 2026 folders. The 14 Aug and 27 Aug 2026 folders have no D1 folder.','Open',
 'Ask the data team which code is correct for the Decreasing direction. Make LRPMaster and PavementType agree, or change V5 to compare each direction with its own code. Run V5 again.'),
('Jaipur','B',1,2,'N_04003_10005_RJ','[1] - V5 D1 [1] Section Code',
 'All 16,209 rows fail. The DB has AMR-JAM. V5 expects ATR-JNR.',
 'LRPMaster has AMR-JAM. PavementType has ATR-JNR. The code uses LRPMaster first. V5 uses PavementType.','Open',
 'Get the correct section code from the data team. Correct the D1 file that is wrong. Run the survey again.'),
('Jaipur','B',1,2,'N_02001_02008_RJ','[1] - V5 D1 [1] Section Code',
 'The section code is not correct in 129 of 48,540 rows. These L-lane rows have GUR-JAY. V5 expects one code, GUR-JAI.',
 'LRPMaster and PavementType have 2 codes: GUR-JAI and GUR-JAY. V5 expects one code.','Open',
 'Ask the data team if GUR-JAY is a second section or a typing error. Correct D1, or change V5 to check the code for each chainage range. Run V5 again.'),
('Jaipur','B',1,1,'N_02001_03005_RJ','[10] - V4 Hierarchy [6] | V5 Section Code [1] | V6 Backup [3]',
 'The 10m Roughness, Rutting and Ravelling tables have 4,654 rows that are not in the backup. 4,652 of 6,978 R1 rows and 4,655 of 6,983 R3 rows are off the 10m grid (the other lanes have about 4,650 rows). V4: 901-932 of 2,794 100m rows and 94 of 282 1km rows do not agree. All 32,563 rows fail the section code check: the DB has JRR, V5 expects AGR-AJM. 83 10m rows have a negative wheel-path IRI.',
 'V4 / V6: same 10m split bug on R1 and R3. V5: LRPMaster has JRR. PavementType has AGR-AJM (Increasing) and AJM-AGR (Decreasing). The code uses LRPMaster first. V5 uses PavementType.','Open',
 'Fix the 10m split (use the start and end chainages from the backup files). Get the correct section code from the data team and correct LRPMaster. Remove IRI values < 0 before you calculate averages. Run the survey again.'),
('Jaipur','B',1,2,'N_02001_06001_RJ','[4] - V5 D1 [4] Pavement Type',
 '3 rows in each of 4 tables (master_cracking and the roughness, rutting and ravelling 10m tables) have a pavement type that is not the same as the D1 overlap. These tables have 105-114 Hybrid rows.',
 'Not confirmed. The validation results table does not keep the failing rows. Probable cause: a D1 PavementType interval boundary is inside a 10m row, so the code and V5 give a different type for that row.','Open - needs check',
 'Run V5 for this survey again and read VAL_DETAILS to get the 3 rows. Compare these rows with D1 PavementType. Then correct the code or D1.'),
('Jaipur','B',1,1,'N_02017_03001_RJ','[2] - V5 NH [1] | V5 Section Code [1]',
 'All 47,719 rows fail both checks. V5 expects None for the NH number and the section code. The DB has NH-27 with PIN-UDA and PIN-SWA.',
 'D1 has no PavementType file (25 Jul and 27 Aug 2026 folders have only LRPMaster). V5 reads the expected NH and code from PavementType only, so it expects None. The DB values come from LRPMaster.','Open',
 'Upload the D1 PavementType file. Or change V5 to use LRPMaster when there is no PavementType. Also, V5 must check the code for each chainage range (2 sections). Run V5 again.'),
('Jaipur','B',1,1,'N_02001_09001_RJ','[1] - V5 NH [1]',
 'All 67,342 rows fail. V5 expects \'NH-334000\'. The DB has NH-48, which agrees with LRPMaster and NHMaster (NH0048).',
 'The NH column in PavementType has chainage values (for example 333550, 334000), not the NH number. V5 reads this value as the NH number, so the data in the DB is correct and the check is wrong.','Open',
 'Ask the data team to correct the NH column in PavementType. Change V5 to use NHMaster / LRPMaster when the PavementType NH is not valid. Run V5 again.'),
]

issues = pd.DataFrame(R, columns=COLS)
issues.insert(0, 'Sr. No.', range(1, len(issues) + 1))

# Check: every survey in `failed` has one row, and the [n] count = Fail Issue in the CSV
chk = issues.assign(n=issues['Validation fail'].str.extract(r'^\[(\d+)\]')[0].astype(int)).merge(
    failed[['RO', 'Zone', 'Phase', 'Cycle', 'UPC', 'Fail Issue']], on=['RO', 'Zone', 'Phase', 'Cycle', 'UPC'], how='outer', indicator=True)
print('Rows:', len(issues), '| surveys in failed:', len(failed), '| not matched:', (chk['_merge'] != 'both').sum(),
      '| count mismatch:', (chk['n'] != chk['Fail Issue']).sum())
print(issues['RO'].value_counts(sort=False).to_dict())
display(issues)

# COMMAND ----------

# DBTITLE 1,Issue buckets - consolidated view
# Classify every survey in `issues` into one or more issue buckets (a survey can have more than one issue).
# Rules use the Root Cause + Issue Description text written in cell 4.
BUCKETS = [
    # (bucket, owner, fix type, keywords - any match)
    ('10m split bug (off-grid chainage rounding)', 'Code - pipeline', 'Fix code',
     ['split bug', "banker's rounding", 'rounds each chainage']),
    ('Negative / -1 IRI included in 100m / 1km average', 'Code - pipeline + Data', 'Fix code',
     ['negative / -1 iri', 'negative iri', '-1 values are averaged']),
    ('V5 expects one section code, D1 has one code per direction', 'Code - validation (V5)', 'Fix validation',
     ['one code for each direction', "direction 'both'"]),
    ('V5 expects one NH / code, UPC has several roads or sections', 'Code - validation (V5)', 'Fix validation',
     ['2 nhs', '2 roads', '3 roads', '2 sections', '2 codes', 'expects one nh']),
    ('D1 typo / extra space in code', 'Data - D1', 'Correct data',
     ['extra space', 'with a space', 'with spaces', 'typing error', 'different order']),
    ('D1 LRPMaster does not agree with PavementType / NHMaster', 'Data - D1', 'Correct data',
     ['all other d1 files', 'lrpmaster does not agree', 'nhmaster have nh148n', 'nhmaster have nh-148n',
      'lrpmaster has amr-jam', 'lrpmaster has jrr']),
    ('D1 file missing / V5 reads wrong D1 column', 'Data - D1 + V5', 'Correct data / validation',
     ['no pavementtype file', 'chainage values', 'reads a chainage value', 'no d1 folder']),
    ('Wrong D2 file (different package)', 'Data - D2', 'Correct data',
     ['d2 file is from a different package']),
    ('Not confirmed - needs check', 'Open', 'Investigate',
     ['not confirmed']),
]

def classify(row):
    text = f"{row['Root Cause']} {row['Issue Description']}".lower()
    return [b for b, _, _, kws in BUCKETS if any(k in text for k in kws)] or ['Unclassified']

issues['Buckets'] = issues.apply(classify, axis=1)
issues['Fail checks'] = issues['Validation fail'].str.extract(r'^\[(\d+)\]')[0].astype(int)
issues['Survey'] = issues['UPC'] + ' P' + issues['Phase'].astype(str) + 'C' + issues['Cycle'].astype(str)
long = issues.explode('Buckets').rename(columns={'Buckets': 'Issue Bucket'}).reset_index(drop=True)
meta = pd.DataFrame([(b, o, f) for b, o, f, _ in BUCKETS], columns=['Issue Bucket', 'Owner', 'Fix Type'])

# 1) Consolidated view: one row per bucket
bucket_summary = (long.groupby('Issue Bucket')
                  .agg(Surveys=('UPC', 'size'),
                       Unique_UPCs=('UPC', 'nunique'),
                       Fail_checks=('Fail checks', 'sum'),
                       ROs=('RO', lambda s: ', '.join(sorted(s.unique()))),
                       Surveys_list=('Survey', lambda s: ', '.join(s)))
                  .reset_index()
                  .merge(meta, on='Issue Bucket', how='left')
                  .sort_values('Surveys', ascending=False))
bucket_summary.insert(0, 'Rank', range(1, len(bucket_summary) + 1))
print(f"Surveys: {len(issues)} | surveys with more than 1 bucket: {(issues['Buckets'].str.len() > 1).sum()} | "
      f"unclassified: {(long['Issue Bucket'] == 'Unclassified').sum()}")
display(bucket_summary[['Rank', 'Issue Bucket', 'Owner', 'Fix Type', 'Surveys', 'Unique_UPCs', 'Fail_checks', 'ROs', 'Surveys_list']])

# 2) Bucket x RO matrix (number of surveys)
bucket_by_ro = long.pivot_table(index='Issue Bucket', columns='RO', values='UPC', aggfunc='count', fill_value=0)
bucket_by_ro = bucket_by_ro.reindex(columns=['Gandhinagar', 'Delhi', 'Bhopal', 'Jaipur'], fill_value=0)
bucket_by_ro['Total'] = bucket_by_ro.sum(axis=1)
display(bucket_by_ro.sort_values('Total', ascending=False).reset_index())

# 3) Owner view: how many surveys each team must act on
owner_view = (long.merge(meta, on='Issue Bucket', how='left')
              .groupby('Owner').agg(Surveys=('Sr. No.', 'nunique')).reset_index().sort_values('Surveys', ascending=False))
display(owner_view)

# 4) Survey-level view with its buckets
display(issues[['Sr. No.', 'RO', 'Zone', 'Phase', 'Cycle', 'UPC', 'Validation fail', 'Buckets']]
        .assign(Buckets=issues['Buckets'].str.join(' | ')))