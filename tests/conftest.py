import math

import pandas as pd
import pytest


@pytest.fixture
def sample_dataframe():
    """Build a small DataFrame matching the expected HELIX spreadsheet schema.

    Contains 4 rows exercising valid/invalid IGSNs, present/missing PDV filenames,
    and valid coordinates.
    """
    return pd.DataFrame(
        [
            {
                "Timestamp": "2025-01-01 10:00:00",
                "Exp_ID": "EXP001",
                "Flyer_ID": "F001",
                "Flyer_material": "Copper",
                "Flyer_Thickness (um)": 50.0,
                "Sample_IGSN": "ABCDEF12345",
                "Sample material": "Steel",
                "Spacing (um)": 100.0,
                "PDV_FileName": "shot001",
                "Flyer_Row": 1,
                "Flyer_Column": 1,
                "Flyer_X_Position_Corrected (mm)": 10.5,
                "Flyer_Y_Position_Corrected (mm)": 20.3,
                "Notes": "good row",
            },
            {
                "Timestamp": "2025-01-01 10:05:00",
                "Exp_ID": "EXP002",
                "Flyer_ID": "F002",
                "Flyer_material": "Aluminum",
                "Flyer_Thickness (um)": 75.0,
                "Sample_IGSN": "INVALID",
                "Sample material": "Glass",
                "Spacing (um)": 200.0,
                "PDV_FileName": "shot002",
                "Flyer_Row": 1,
                "Flyer_Column": 2,
                "Flyer_X_Position_Corrected (mm)": 15.0,
                "Flyer_Y_Position_Corrected (mm)": 25.0,
                "Notes": "invalid IGSN",
            },
            {
                "Timestamp": "2025-01-01 10:10:00",
                "Exp_ID": "EXP003",
                "Flyer_ID": "F003",
                "Flyer_material": "Copper",
                "Flyer_Thickness (um)": 50.0,
                "Sample_IGSN": float("nan"),
                "Sample material": "Steel",
                "Spacing (um)": 100.0,
                "PDV_FileName": "shot003",
                "Flyer_Row": 2,
                "Flyer_Column": 1,
                "Flyer_X_Position_Corrected (mm)": float("nan"),
                "Flyer_Y_Position_Corrected (mm)": float("nan"),
                "Notes": "missing IGSN",
            },
            {
                "Timestamp": "2025-01-01 10:15:00",
                "Exp_ID": "EXP004",
                "Flyer_ID": "F004",
                "Flyer_material": "Aluminum",
                "Flyer_Thickness (um)": 75.0,
                "Sample_IGSN": "XYZABC67890-sub1",
                "Sample material": "Glass",
                "Spacing (um)": 200.0,
                "PDV_FileName": float("nan"),
                "Flyer_Row": 2,
                "Flyer_Column": 2,
                "Flyer_X_Position_Corrected (mm)": 12.0,
                "Flyer_Y_Position_Corrected (mm)": 18.0,
                "Notes": "missing PDV filename, valid IGSN with suffix",
            },
        ]
    )
