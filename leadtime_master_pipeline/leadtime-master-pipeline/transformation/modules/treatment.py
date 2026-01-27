import pandas as pd
import os
import sys
import importlib

# ================= PATH SETUP =================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))

sys.path.insert(0, PROJECT_ROOT)

print("TRY IMPORT HELPER FROM:", PROJECT_ROOT)

# Import module helper_phase (để xem __file__)
import helper_phase1
print("HELPER_PHASE ACTUAL FILE:", helper_phase1.__file__)

# Import hàm cần dùng
from helper_phase1 import load_base_and_date_columns
from helper import (
    load_json_to_df,
    normalize_brand,
    clean_drop,
    split_and_clean_drop,
    get_parquet_output_path
)


# ================= CONFIG =================
GROUP_NAME = "TREATMENT"   # GROUP_NAME in ERP_RAW 
TABLE_NAME = "TREATMENT_REQUEST"     # Subfolder in ERP_PARQUET 

BASE_DIR = os.path.join(PROJECT_ROOT, "ERP_RAW")

# Folder root all JSON 
FOLDER = os.path.join(BASE_DIR, "TREATMENT", "JSON")
print("JSON root folder:", FOLDER)

# ================= LOAD MULTI JSON (PHASE HELPER) =================
df_all = load_base_and_date_columns(FOLDER)
print("=== DF_ALL (MULTI JSON) ===")
print(df_all.head())
print("DF_ALL shape:", df_all.shape)




# ================= RENAME TO STANDARD =================
df_all_rename = df_all.rename(
        columns=
        {
            "REQUESTCODE": "REQUEST",
            "PURCHASEORDERCODE": "ORDER_INTERNAL",
            "POPARTNERNAME": "BRAND_NAME",
            "STYLENOINTERNAL" : "STYLE_INTERNAL",
            "SKUNO" : "SKU",
            "SKUNOINTERNAL" : "SKU_INTERNAL",
            "WORKTYPENAME" : "WORK_TYPE",
            "PURREJECTEDREASON" : "REJECT_REASON",
            "SEASONNAME" : "SEASON",
            "CFC" : "ORDER_EXTERNAL",
            "DROPCODE" : "DROP",
            "STYLECOLORCODE": "COLOR"
    }
)

df_all_rename["BRAND_NAME"] = df_all_rename["BRAND_NAME"].apply(normalize_brand)
df_all_rename["COLOR"] = df_all_rename["COLOR"].apply(clean_drop)
df_all_rename["DROP"] = df_all_rename["DROP"].apply(clean_drop)


print("=== TRANSFORMED ===")
print(df_all_rename)

# ================= WRITE PARQUET (USING HELPER) =================
import pyarrow as pa
import pyarrow.parquet as pq

schema = pa.schema([(col, pa.string()) for col in df_all_rename.columns])
df_string = df_all_rename.fillna("").astype(str)
table = pa.Table.from_pandas(df_string, schema=schema, preserve_index=False)

PARQUET_FILE_NAME = "raw_treatment"

parquet_path = get_parquet_output_path(GROUP_NAME, TABLE_NAME,PARQUET_FILE_NAME)
pq.write_table(table, parquet_path, compression="snappy")


print(f"[OK] Parquet saved: {parquet_path}")


# # ================= UPLOAD TO ONELAKE =================
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
