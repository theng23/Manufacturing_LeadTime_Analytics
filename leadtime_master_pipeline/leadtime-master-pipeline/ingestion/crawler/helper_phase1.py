import pandas as pd
import re
import os
import sys
import importlib
import json
from pathlib import Path

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


# =========================================================
# CATEGORY + SUBCATEGORY DETECTION
# =========================================================
def classify_category_subcategory(source: str) -> tuple[str, str]:
    s = source.lower()

    if any(keyword in s for keyword in ["fabrictechnicals", "trimtechnicals", "cmp", "pom"]):
        category = "TECHNICAL"
    elif "trim" in s:
        category = "TRIM"
    elif "fabric" in s:
        category = "FABRIC"
    elif any(keyword in s for keyword in ["printing", "printoutsource", "dyewash", "emb"]):
        category = "TREATMENT"
    elif "costing" in s:
        category = "COSTING"
    elif "cuttingdocket" in s:
        category = "CUTTINGDOCKET"
    else:
        category = "OTHER"

    if "develop" in s:
        sub_category = "DEVELOP"
    elif "labdip" in s:
        sub_category = "LABDIP"
    elif "swatch" in s:
        sub_category = "SWATCH"
    elif "printoutsource" in s:
        sub_category = "OUTSOURCE"
    elif "dyewash" in s:
        sub_category = "DYEWASH"
    elif "printing" in s:
        sub_category = "PRINTING"
    elif "emb" in s:
        sub_category = "EMB"
    elif "pom" in s:
        sub_category = "POM"
    elif "cmp" in s:
        sub_category = "CMP"
    elif "trimtechnical" in s:
        sub_category = "TRIM"
    elif "fabrictechnical" in s:
        sub_category = "FABRIC"
    else:
        sub_category = ""

    return category, sub_category


# =========================================================
# CLEAN COLUMN NAME
# =========================================================
def clean_column_name(col_name: str) -> str:
    name = col_name.upper()
    name = re.sub(r"[#()]", "", name)
    name = re.sub(r"[^A-Z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("_")
    return name


# =========================================================
# ALIAS GROUPS
# =========================================================
DATE_ALIAS_GROUPS = {
    "SENT_DATE": ["SENTPURDATE","SENT_PRT_DATE","SENTTECHNICALDATE"],
    "EXPECT_RECEIVE": ["EXPECTRECEIVEDATE","EXPECTEDRECEIVEDATE", "EXPECT RECEIVE"],
    "CUSTOMER_APPROVED_DATE": ["CUSTOMERAPPROVEDDATE"],
    "RESPONSE_DATE": ["UPDATEDDATE"],
    "RECEIVE_DATE" : ["RECEIVEDATEOFPUR", "RECEIVEDATEOF_PRT","TECHNICALRECEIVEDDATE", "RECEIVEDATEOFTECHNICAL"],
    "SENT_MER_DATE": ["PRT_SENTMERDATE","SENTMERDATE"],
    "CREATED_DATE": ["CREATEDDATE", "CREATEDDATETOJSON"],
    "CLIENT_SENT_DATE" : ["CLIENTSENTDATE"],
    "FINANCE_APPROVED_DATE": ["FINANCEAPPROVEDDATE"],
    "COSTING_READY": ["MERSENTDATE"],
    "COSTING_CUSTOMER_STATUS": ["CUSTOMERSTATUSNAME"],
}

BASE_COLUMNS = [
    "REQUESTCODE", "PURCHASEORDERCODE", "POPARTNERNAME", "SEASONNAME", "STYLECOLORCODE", "CFC", "DROPCODE",
    "STYLENOINTERNAL", "SKUNO","SKUNOINTERNAL" ,"WORKTYPENAME", "PURREJECTEDREASON","CUTTING_DOCKET","STATUSNAME"
]

COLUMN_ALIAS_GROUPS = {
    "POPARTNERNAME": ["PARTNERNAME", "PARNETNAME", "PO_PARTNER_NAME","CUSTOMERNAME"],
    "REQUESTCODE": ["REQUEST", "REQUEST_CODE"],
    "PURCHASEORDERCODE": ["POCODE", "ORDER_CODE", "PO_NO","ORDERINTERNALCODE"],
    "STYLENOINTERNAL": ["STYLE_INTERNAL", "STYLE_NO", "STYLENO"],
    "SKUNO": ["SKU", "SKU_NO", "SKU_EXTERNAL"],
    "WORKTYPENAME": ["WORKTYPENAME"],
    "PURREJECTEDREASON": ["REJECTREASON", "REASON_REJECTED", "REJECT_REASON","REJECTEDREASON","REASONOFREJECT"],
    "CUTTING_DOCKET": ["CODE"],
    "STATUS": ["STATUSNAME"],
}


# =========================================================
# MAIN JSON LOADER
# =========================================================
def load_base_and_date_columns(
    folder_path: str,
) -> pd.DataFrame:

    root = Path(folder_path)

    json_files = list(root.rglob("*.json"))

    frames = []
    skipped = []
    row_stats = []
    debug_printed = 0

    date_canonical_cols = list(DATE_ALIAS_GROUPS.keys())

    for p in json_files:
        rel = str(p.relative_to(root))

        try:
            df = pd.read_json(p, dtype=str, lines=False)
            raw_count = len(df)
            df.columns = [clean_column_name(c) for c in df.columns]

            if debug_printed < 3:
                print(f"\n=== COLUMNS in {rel} ===")
                print(df.columns.tolist())
                debug_printed += 1

            rename_map = {}

            for canon, aliases in DATE_ALIAS_GROUPS.items():
                possible = {canon} | {clean_column_name(a) for a in aliases}
                matches = [c for c in df.columns if c in possible]
                if matches:
                    rename_map[matches[0]] = canon

            for canon, aliases in COLUMN_ALIAS_GROUPS.items():
                possible = {canon} | {clean_column_name(a) for a in aliases}
                matches = [c for c in df.columns if c in possible]
                if matches:
                    rename_map[matches[0]] = canon

            df = df.rename(columns=rename_map)

            if df.columns.duplicated().any():
                print(f"Duplicate columns detected in {rel}, fixing…")
                df = df.loc[:, ~df.columns.duplicated()]

            for base in BASE_COLUMNS:
                if base not in df.columns:
                    df[base] = pd.NA

            real_date_cols = [c for c in date_canonical_cols if c in df.columns]
            keep_cols = BASE_COLUMNS + real_date_cols

            sub = df[keep_cols].copy()
            sub["SOURCE_FILE"] = rel

            category, sub_category = classify_category_subcategory(rel)
            sub["CATEGORY"] = category
            sub["SUB_CATEGORY"] = sub_category

            frames.append(sub)
            row_stats.append((rel, len(sub)))

        except Exception as e:
            skipped.append((rel, f"JSON read error: {e}"))

    print("\n===== ROW COUNT PER FILE =====")
    for src, rows in row_stats:
        print(f"{src}: {rows} rows")

    if frames:
        union_df = pd.concat(frames, ignore_index=True)
    else:
        union_df = pd.DataFrame(
            columns=BASE_COLUMNS + date_canonical_cols + ["SOURCE_FILE", "CATEGORY", "SUB_CATEGORY"]
        )

    print("\n===== FINAL UNION =====")
    print("Rows:", len(union_df))
    print("Columns:", list(union_df.columns))

    if skipped:
        print("\n===== SKIPPED FILES =====")
        for s, e in skipped[:20]:
            print(f"{s} -> {e}")

    return union_df
