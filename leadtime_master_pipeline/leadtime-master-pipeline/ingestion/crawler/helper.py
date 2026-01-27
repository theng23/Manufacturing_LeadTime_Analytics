from pathlib import Path
import pandas as pd
import os
try:
    import grp
except ImportError:
    grp = None

import re
from functools import reduce
from pandas import to_datetime
import json  


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR   = os.path.join(SCRIPT_DIR, "ERP_RAW")

GROUP_DIRS = {
    "FABRIC_TRIM": {
        "json_root": os.path.join(BASE_DIR, "FABRIC_TRIM", "JSON"),
    },
    "TREATMENT": {
        "json_root": os.path.join(BASE_DIR, "TREATMENT", "JSON"),
    },
    "TECHNICAL": {
        "json_root": os.path.join(BASE_DIR, "TECHNICAL", "JSON"),
    },
    "COSTING": {
        "json_root": os.path.join(BASE_DIR, "COSTING", "JSON"),
    },
    "CUTTINGDOCKET": {
        "json_root": os.path.join(BASE_DIR, "CUTTINGDOCKET", "JSON"),
    },
    "RAW_DATA": {
        "json_root": os.path.join(BASE_DIR, "RAW_DATA", "JSON"),
    },
    "PO_LIST": {
        "json_root": os.path.join(BASE_DIR, "PO_LIST", "JSON"),
    },
}

def get_json_path(group_name: str, file_name: str) -> str:
    """
    Fallback full path json file by group + file name.
    """
    json_root = GROUP_DIRS[group_name]["json_root"]
    return os.path.join(json_root, file_name)

# helper.py
import os
from pathlib import Path
from datetime import datetime

def get_parquet_output_path(group_name: str,
                            table_name: str,
                            file_name: str | None = None) -> str:
    script_dir = Path(__file__).resolve().parent
    base_dir   = script_dir / "ERP_PARQUET" / group_name / table_name
    base_dir.mkdir(parents=True, exist_ok=True)

    # If file_name is null -> default = TABLE_NAME.parquet 
    if file_name is None:
        file_name = f"{table_name}.parquet"  

    return str(base_dir / file_name)

#---------- Clean column ----------
def clean_column_name(col_name: str) -> str:
    # 1. Uppercase
    name = col_name.upper()

    # 2. Replace by underscore
    name = re.sub(r"[#()]", "", name)
    name = re.sub(r"[^A-Z0-9]+", "_", name)

    # 3. Strip
    name = re.sub(r"_+", "_", name).strip("_")

    return name

#---------- Load clean excel ----------
def load_clean_excel(path, skiprows=11):
    ext = os.path.splitext(path)[1].lower()

    if ext == ".xls":
        # Old Excel format
        df = pd.read_excel(path, engine="xlrd", skiprows=skiprows)
    elif ext in [".xlsx", ".xlsm", ".xlsb"]:
        # New Excel formats
        df = pd.read_excel(path, skiprows=skiprows)
    else:
        # Fallback: treat as CSV
        df = pd.read_csv(path)

    # Clean column names
    df.columns = [clean_column_name(c) for c in df.columns]

    return df

#-------- Clean drop & color ----------
def clean_drop(value):
    if pd.isna(value):
        return value

    s = str(value).strip()

    # 1) Replace space by underscore
    # 2) Replace / ( ) by underscore
    s = re.sub(r"[\/()]", "_", s)      # / ( ) -> _
    s = re.sub(r"\s+", "_", s)         # " " -> _

    # 3)
    s = re.sub(r"_+", "_", s).strip("_")

    return s

#---------- Split and clean ----------
# split by "," for DROP
def split_and_clean_drop(value):
    if pd.isna(value):
        return [pd.NA]

    # Split by ","
    parts = str(value).split(",")

    # clean
    parts_clean = [clean_drop(p) for p in parts]

    return parts_clean

# ---------- Mapping Brand ----------
brand_map = {
    "REISS": "REISS",
    "REISS LTD": "REISS",

    "J. BARBOUR & SONS": "BARBOUR",
    "J BARBOUR & SONS": "BARBOUR",
    "BARBOUR": "BARBOUR",

    "DENIM TEARS": "DENIM TEARS",
    "DENIM TEARS LLC": "DENIM TEARS",

    "CAMILLA AND MARC": "CAMILLA AND MARC",
    "CAMILLA AND MARC LTD": "CAMILLA AND MARC",

    "QUADRANT": "QUADRANT",
    "QUADRANT LTD": "QUADRANT",

    "GOLF WANG": "GOLFWANG",
    "GOLF WANG LLC": "GOLFWANG",
    "GOLF WANG STUDIO": "GOLFWANG",
    "GOLF": "GOLFWANG",

    "MAKER SIXTY FOUR": "M64",
    "MAKER SIXTY FOUR COMPANY LIMITED": "M64",

    "NIKE VIRTUAL STUDIO": "NVS",

    "REISS LTD": "REISS",

    "UN-AVAILABLE": "UA",
    "UNAVAILABLE": "UA",

    "ROLLING STONES": "ROLLING STONES",

    "DREWHOUSE": "DREWHOUSE",

    "STUSSY": "STUSSY",

    "CORTEIZ": "CORTEIZ",

    "THE LOYALIST": "THE LOYALIST",

    "KSUBI": "KSUBI",

    "LSKD": "LSKD",

    "TTT": "TTT",

    "HERSCHEL": "HERSCHEL",

    "REPRESENT": "REPRESENT",

    "AJE": "AJE",

    "BOODY": "BOODY",

    "MSCHF": "MSCHF",

    "PALACE": "PALACE",

    "AIME LEON DORE": "ALD",

    "PATTA": "PATTA",

    "HUNZA": "HUNZA",

    "CEREMONY OF ROSES": "CEREMONY OF ROSES",

    "ASSEMBLY LABEL": "ASSEMBLY LABEL",

    "LINKSOUL": "LINKSOUL",
    "RODD & GUNN": "RODD GUNN",
    "RODD & GUNN": "RODD & GUNN",
    "RTFKT": "RTFKT",

    "NEW YORK OR NOWHERE": "NYON",
    "New York or Nowhere": "NYON",

    "MOOSE KNUCKLES": "MOOSE KNUCKLES",

    "UNIVERSAL": "UNIVERSAL",

    "MADHAPPY": "MADHAPPY",

    "HAPPIER": "HAPPIER",

    "KITH": "KITH",

    "TEAM LIQUID": "TEAM LIQUID",

    "UA": "UA",
    "UA - DEV": "UA - DEV",

    "FRED AGAIN": "FRED AGAIN",

    "EDWARDSX": "EDWARDSX",
    "DREW": "DREWHOUSE",
    "Un-Available": "UN_AVAILABLE"
}

#-------- Normalize ----------
def normalize_raw(name: str) -> str:
    if not isinstance(name, str):
        return name

    name = name.upper().strip()

    # Remove
    remove_words = r"\b(LTD|LIMITED|LLC|COMPANY|CO\.?|CORP\.?|INC\.?|AND SONS|& SONS)\b"
    name = re.sub(remove_words, "", name)

    name = re.sub(r"[^A-Z0-9\s\-&]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()

    return name

#--------- Normalize Brand -------------
def normalize_brand(name: str) -> str:
    if not isinstance(name, str):
        return name

    raw = normalize_raw(name)

    # If dup key → mapping
    if raw in brand_map:
        return brand_map[raw]

    # Not Mapping -> Return Raw
    return raw


# =========================================================

def load_raw_json(path: str):
    """

    Support format ERP:
    1) [ {...}, {...}, ... ]
    2) { "data": [ {...}, ... ] }
    3) { "Data": { "objects": [ {...}, ... ] } }
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Case 1: JSON is list
    if isinstance(data, list):
        return data

    # Case 2: JSON is dict have key "data"
    if isinstance(data, dict) and "data" in data:
        return data["data"]

    # Case 3: JSON dict have key "Data" -> { "objects": [...] }
    if isinstance(data, dict) and "Data" in data:
        inner = data["Data"]
        if isinstance(inner, dict) and "objects" in inner:
            return inner["objects"]

    # Fallback: list null
    return []


def load_json_to_df(path: str, clean_columns: bool = True) -> pd.DataFrame:
    """
    Read JSON RAW from ERP and load DataFrame.
    """
    rows = load_raw_json(path)
    df = pd.DataFrame(rows)

    if clean_columns and not df.empty:
        df.columns = [clean_column_name(c) for c in df.columns]

    return df
