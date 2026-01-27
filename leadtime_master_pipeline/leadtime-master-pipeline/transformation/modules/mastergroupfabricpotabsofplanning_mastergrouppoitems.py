import pandas as pd
import os
import sys
import importlib

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))

sys.path.insert(0, PROJECT_ROOT)

print("TRY IMPORT HELPER FROM:", PROJECT_ROOT)

helper = importlib.import_module("helper")

print("HELPER ACTUAL FILE:", helper.__file__)

from helper import (
    load_json_to_df,
    normalize_brand,
    clean_drop,
    split_and_clean_drop,
    get_parquet_output_path
)

# ================= CONFIG =================
GROUP_NAME = "RAW_DATA"        # GROUP_NAME in ERP_RAW
TABLE_NAME = "FABRIC_TRIM_PLANNING"  # Subfolder in ERP_PARQUET


BASE_DIR   = os.path.join(PROJECT_ROOT, "ERP_RAW")

folder_path = os.path.join(BASE_DIR, GROUP_NAME, "JSON")   # GROUP_NAME
file_name   = "mastergroupfabricpotabsofplanning_mastergrouppoitems.json"
file_path   = os.path.join(folder_path, file_name)

print("FABRIC_PLANNING JSON path:", file_path)

# ================= LOAD RAW JSON =================
df_fabric_planning = load_json_to_df(file_path, clean_columns=True)
print("=== RAW AFTER LOAD + CLEAN ===")
print(df_fabric_planning)

# ================= WORK_TYPE MAP ====================
WORK_TYPE_MAP = {
    1: "Sample",
    2: "Bulk",
    3: "Develop",
}

def map_work_type(x):
    return WORK_TYPE_MAP.get(x, None)

# ================= SELECT COLUMNS =================
select = [
        "CUSTOMERNAME", #"BRAND_NAME"
        "ORDERINTERNALCODES", #"ORDER_INTERNAL",
        "IPOFABRICMASTERCODE",#"MASTER_PO",
        "IPOFABRICMASTERGROUPCODE", #"FABRIC_FAST_CODE",
        "SEASONNAMES", #"SEASON",
        "IPOFABRICITEMCODECOMBINED", #"PO_COMBINED",
        "ORDEREXTERNALCODES", #"ORDER_EXTERNAL",
        "COLOREXTS", #FABRIC_COLOR_EXT",
        "FIRSTSTOCKINDATE", #"FIRST_STOCK_IN_DATE",
        "STOCKINDATE", #"LAST_STOCK_IN_DATE",
        "SKUNOINTERNALS", # :"SKU_INTERNAL"
        "WORKTYPE", # : "WORK_TYPE"
        "REVISEDSTOCKINDATEREASON", # : "REVISED_STOCK_IN_DATE_REASON"
        "GROUPFABRICTYPES"        # : "FABRIC_TRYM_TYPE"
    ]
df_fabric_planning_select = df_fabric_planning[select].copy()

# ================= RENAME TO STANDARD =================
df_fabric_planning_rename = df_fabric_planning_select.rename(
        columns=
            {
            "CUSTOMERNAME" : "BRAND_NAME",
            "ORDERINTERNALCODES" :"ORDER_INTERNAL",
            "IPOFABRICMASTERCODE" :"MASTER_PO",
            "IPOFABRICMASTERGROUPCODE" :"FABRIC_TRIM_FAST_CODE",
            "SEASONNAMES" : "SEASON",
            "IPOFABRICITEMCODECOMBINED" : "PO_COMBINED",
            "ORDEREXTERNALCODES" : "ORDER_EXTERNAL",
            "COLOREXTS" : "FABRIC_COLOR_EXT",
            "FIRSTSTOCKINDATE" : "FIRST_STOCK_IN_DATE",
            "STOCKINDATE" : "LAST_STOCK_IN_DATE",
            "SKUNOINTERNALS"  :"SKU_INTERNAL",
            "WORKTYPE"  : "WORK_TYPE",
            "REVISEDSTOCKINDATEREASON" : "REVISED_STOCK_IN_DATE_REASON",
            "GROUPFABRICTYPES"          : "FABRIC_TRIM_TYPE"
            }
)

# Create list from PO_COMBINED
df_fabric_planning_rename["PO_COMBINED_LIST"] = df_fabric_planning_rename["PO_COMBINED"].apply(split_and_clean_drop)

# Explode
df_exploded = df_fabric_planning_rename.explode("PO_COMBINED_LIST").copy()

# Alias PO_COMBINED & delete extra column
df_exploded["PO_COMBINED"] = df_exploded["PO_COMBINED_LIST"]
df_exploded_planning = df_exploded.drop(columns=["PO_COMBINED_LIST"]).reset_index(drop=True)
df_exploded_planning = df_exploded_planning.rename(columns={"PO_COMBINED" : "PO"})

# SKU_INTERNAL SPLIT
df_exploded_planning["SKU_INTERNAL_LIST"] = df_exploded_planning["SKU_INTERNAL"].apply(split_and_clean_drop)
# explode
df_exploded_sku = df_exploded_planning.explode("SKU_INTERNAL_LIST").copy()
# Alias PO_COMBINED & delete extra column
df_exploded_sku["SKU_INTERNAL"] = df_exploded_sku["SKU_INTERNAL_LIST"]
df_exploded_final = df_exploded_sku.drop(columns=["SKU_INTERNAL_LIST"]).reset_index(drop=True)

df_exploded_final["WORK_TYPE"] = df_exploded_final["WORK_TYPE"].apply(map_work_type)
df_exploded_final["CATEGORY"] = "FABRIC"


print("=== TRANSFORMED ===")
print(df_exploded_final)

# ================= WRITE PARQUET (USING HELPER) =================
import pyarrow as pa
import pyarrow.parquet as pq

schema = pa.schema([(col, pa.string()) for col in df_exploded_final.columns])
df_string = df_exploded_final.fillna("").astype(str)
table = pa.Table.from_pandas(df_string, schema=schema, preserve_index=False)

PARQUET_FILE_NAME = "raw_mastergroupfabricpotabsofplanningmastergrouppoitems"

parquet_path = get_parquet_output_path(GROUP_NAME, TABLE_NAME,PARQUET_FILE_NAME)
pq.write_table(table, parquet_path, compression="snappy")


print(f"[OK] Parquet saved: {parquet_path}")


# ================= UPLOAD TO ONELAKE =================
from azure.core.credentials import AccessToken
import time

scope = "https://example-erp/"
token_provider = get_bearer_token_provider(AzureCliCredential(), scope)

class FabricTokenAdapter:
    def __init__(self, provider):
        self.provider = provider

    def get_token(self, *scopes, **kwargs):
        token = self.provider()
        expiry = int(time.time()) + 3600
        return AccessToken(token, expiry)

credential = FabricTokenAdapter(token_provider)

service_client = DataLakeServiceClient(
    credential=credential
)

fs = service_client.get_file_system_client(WORKSPACE_ID)

# === UPLOAD ALL PARQUET IN GROUP/TABLE ===
PARQUET_ROOT      = os.path.join(PROJECT_ROOT, "ERP_PARQUET")
PARQUET_GROUP_DIR = os.path.join(PARQUET_ROOT, GROUP_NAME)
PARQUET_TABLE_DIR = os.path.join(PARQUET_GROUP_DIR, TABLE_NAME)

parquet_files = [
    f for f in os.listdir(PARQUET_TABLE_DIR)
    if f.endswith(".parquet")
]

for fname in parquet_files:
    local_path = os.path.join(PARQUET_TABLE_DIR, fname)
    destination_path = (
        f"{LAKEHOUSE_ID}/Files/RAW_LEADTIME/{GROUP_NAME}/{TABLE_NAME}/{fname}"
    )

    print("[INFO] uploading:", fname, "→", destination_path)

    file_client = fs.get_file_client(destination_path)
    with open(local_path, "rb") as f:
        file_client.upload_data(f.read(), overwrite=True)

print("[DONE] Upload completed for:", GROUP_NAME, "/", TABLE_NAME)

# =========================================================
# UPLOAD TO LAKE (optional)
# =========================================================
# Tenant-specific identifiers are injected via environment variables.
try:
    from storage.lake_uploader import build_destination_path, upload_file

    dest_path = build_destination_path(GROUP_NAME, TABLE_NAME, PARQUET_FILE_NAME)
    print("[INFO] uploading:", PARQUET_FILE_NAME, "->", dest_path)
    upload_file(parquet_path, dest_path)
    print("[OK] uploaded:", PARQUET_FILE_NAME)
except Exception as e:
    print("[SKIP] upload step (not configured):", e)
