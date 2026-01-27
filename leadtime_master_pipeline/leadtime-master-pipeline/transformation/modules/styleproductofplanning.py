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
TABLE_NAME = "PRODUCTION_PLANNING"  # Subfolder in ERP_PARQUET


BASE_DIR   = os.path.join(PROJECT_ROOT, "ERP_RAW")

folder_path = os.path.join(BASE_DIR, GROUP_NAME, "JSON")   # GROUP_NAME
file_name   = "styleproductofplanning.json"
file_path   = os.path.join(folder_path, file_name)

print("PRODUCTION_PLANNING JSON path:", file_path)

# ================= LOAD RAW JSON =================
df_style_planning = load_json_to_df(file_path, clean_columns=True)
print("=== RAW AFTER LOAD + CLEAN ===")
print(df_style_planning.head(5))

# ================= SELECT COLUMNS =================
selected = [
    "STYLENOINTERNAL",  #STYLE_INTERNAL
    "POSEASONCODE",     #SEASON
    "COLORNAME",        #COLOR
    "SHIPDATE",         #CUSTOMER_REQUEST_DATE
    "POPARTNERNAME",    #CUSTOMER (BRAND_NAME)
    "SHIPMENTDATEMAX", #SHIPMENT_DATE (External)
    "DROPCODE",         #DROP
    "POCODE",           #ORDER_INTERNAL
    "LASTCUTTINGDOCKET",    #CD
    "CUTTINGDOCKETRELEASEDATE", #CD_RELEASED_DATE
    "CUTTINGDOCKETSTATUSNAME", #CD_STATUS
    "SKUNOINTERNAL",    #SKU_INTERNAL
    "WORKTYPENAME"     #"WORK_TYPE",
]
df_style_planning_selected = df_style_planning[selected].copy()

# ================= RENAME TO STANDARD =================
df_style_planning_rename = df_style_planning_selected.rename(
        columns={
        "STYLENOINTERNAL":          "STYLE_INTERNAL",
        "POSEASONCODE":             "SEASON",
        "SHIPDATE":                 "CUSTOMER_EXPECTED_DATE",
        "POPARTNERNAME":            "BRAND_NAME",
        "SHIPMENTDATEMAX" :         "SHIPMENT_DATE",
        "DROPCODE":                 "DROP",
        "COLORNAME":                "COLOR",
        "POCODE":                   "ORDER_INTERNAL",
        "WORKTYPENAME":             "WORK_TYPE",
        "SKUNOINTERNAL" :           "SKU_INTERNAL",
        "LASTCUTTINGDOCKET":        "CD",
        "CUTTINGDOCKETRELEASEDATE": "CD_RELEASED_DATE",
        "CUTTINGDOCKETSTATUSNAME" : "CD_STATUS"
        }
)


df_style_planning_rename["BRAND_NAME"] = df_style_planning_rename["BRAND_NAME"].apply(normalize_brand)
df_style_planning_rename["DROP"] = df_style_planning_rename["DROP"].apply(clean_drop)
df_style_planning_rename["COLOR"] = df_style_planning_rename["COLOR"].apply(clean_drop)


print("=== TRANSFORMED ===")
print(df_style_planning_rename)

# ================= WRITE PARQUET (USING HELPER) =================
import pyarrow as pa
import pyarrow.parquet as pq

schema = pa.schema([(col, pa.string()) for col in df_style_planning_rename.columns])
df_string = df_style_planning_rename.fillna("").astype(str)
table = pa.Table.from_pandas(df_string, schema=schema, preserve_index=False)

PARQUET_FILE_NAME = "raw_styleproductofplanning"

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
