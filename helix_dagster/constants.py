import os
import re

HELIX_FOLDER_ID = os.environ.get("HELIX_FOLDER_ID")

IGSN_PATTERN = re.compile(r"[A-Za-z]{6}\d{5}(?:-[A-Za-z0-9]+)?")

# /aimdl endpoint data types
# These correspond to meta.data_type values set on Girder items
AIMDL_DATA_TYPES = {
    "pdv_trace": "pdv_trace",
    "pdv_alpss_result": "pdv_alpss_result",
    "pdv_alpss_output": "pdv_alpss_output",
    "xrd_raw": "xrd_raw",
    "xrd_derived": "xrd_derived",
    "xrd_metadata": "xrd_metadata",
    "xrd_calibrant_raw": "xrd_calibrant_raw",
    "xrd_calibrant_derived": "xrd_calibrant_derived",
    "xrf_raw": "xrf_raw",
}

# Default data type for PDV trace matching (used by pdv_trace_inventory asset)
PDV_TRACE_DATA_TYPE = os.environ.get("PDV_TRACE_DATA_TYPE", "pdv_trace")
ALPSS_RESULT_DATA_TYPE = os.environ.get("ALPSS_RESULT_DATA_TYPE", "pdv_alpss_result")

# Hard limit imposed by the /aimdl/datafiles endpoint
AIMDL_PAGE_LIMIT = 100

COLUMN_MAP = {
    "Timestamp": "Timestamp",
    "Exp_ID": "Exp_ID",
    "Flyer_ID": "Flyer_ID",
    "Flyer_material": "Flyer_material",
    "Flyer_Thickness (um)": "Flyer_Thickness_um",
    "Sample_ID": "Sample_IGSN",  # Must match the column name used by process_row()
    "Sample material": "Sample_material",
    "Spacing (um)": "Spacing_um",
    "Waveplate_Angle (Degrees)": "Waveplate_Angle_Degrees",
    "PDV_FileName": "PDV_FileName",
    "PDV_Target_Wavelength (m)": "PDV_Target_Wavelength_m",
    "PDV_Target_Power (dBm)": "PDV_Target_Power_dBm",
    "PDV_Ref_Wavelength (m)": "PDV_Ref_Wavelength_m",
    "PDV_Ref_Power (dBm)": "PDV_Ref_Power_dBm",
    "PDV_Return_Power (dBm)": "PDV_Return_Power_dBm",
    "Flyer_Row": "Flyer_Row",
    "Flyer_Column": "Flyer_Column",
    "Flyer_X_Position_Desired (mm)": "Flyer_X_Position_Desired_mm",
    "Flyer_Y_Position_Desired (mm)": "Flyer_Y_Position_Desired_mm",
    "Flyer_X_Position_Corrected (mm)": "Flyer_X_Position_Final_mm",
    "Flyer_Y_Position_Corrected (mm)": "Flyer_Y_Position_Final_mm",
    "Laser_Ref_Energy (mJ)": "Laser_Ref_Energy_mJ",
    "Laser_Target_Energy (mJ)": "Laser_Target_Energy_mJ",
    "Beam_Profile_FileName": "Beam_Profile_FileName",
    "Shot_Time (seconds)": "Shot_Time_seconds",
    "Notes": "Notes",
}
