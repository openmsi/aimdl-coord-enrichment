import os
import re

HELIX_FOLDER_ID = os.environ.get("HELIX_FOLDER_ID", "6895729dd48b09febbb57fcc")
PDV_FOLDER_ID = os.environ.get("PDV_FOLDER_ID", "684894fb8cee616eb53c9330")
FORM_ID = "68ee6ed7be34766952067408"

IGSN_PATTERN = re.compile(r"[A-Za-z]{6}\d{5}(?:-[A-Za-z0-9]+)?")

COLUMN_MAP = {
    "Timestamp": "Timestamp",
    "Exp_ID": "Exp_ID",
    "Flyer_ID": "Flyer_ID",
    "Flyer_material": "Flyer_material",
    "Flyer_Thickness (um)": "Flyer_Thickness_um",
    "Sample_ID": "sample_IGSN",
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

FORM_SCHEMA_FIELDS = {
    "Timestamp",
    "Exp_ID",
    "Exp_type",
    "Flyer_ID",
    "Flyer_material",
    "Flyer_Thickness_um",
    "sample_IGSN",
    "Sample_material",
    "Spacing_um",
    "Waveplate_Angle_Degrees",
    "PDV_FileName",
    "PDV_Target_Wavelength_m",
    "PDV_Target_Power_dBm",
    "PDV_Ref_Wavelength_m",
    "PDV_Ref_Power_dBm",
    "PDV_Return_Power_dBm",
    "Flyer_Row",
    "Flyer_Column",
    "Flyer_X_Position_Desired_mm",
    "Flyer_Y_Position_Desired_mm",
    "Flyer_X_Position_Final_mm",
    "Flyer_Y_Position_Final_mm",
    "Laser_Ref_Energy_mJ",
    "Laser_Target_Energy_mJ",
    "Beam_Profile_FileName",
    "Shot_Time_seconds",
    "Notes",
    "Performed_by",
    "Grant",
    "Location",
}
