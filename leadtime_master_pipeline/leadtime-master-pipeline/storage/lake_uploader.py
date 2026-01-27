"""
Lake uploader (public-safe)

Keeps upload capability without embedding tenant-specific identifiers.
All tenant settings are injected via environment variables.

Required env vars:
- ONELAKE_DFS_URL
- ONELAKE_FILE_SYSTEM
- ONELAKE_LAKEHOUSE_ID
Optional:
- ONELAKE_BASE_FOLDER (default: RAW_LEADTIME)

Auth:
- DefaultAzureCredential (Azure CLI / Managed Identity / etc.)
"""

from __future__ import annotations
import os

def build_destination_path(group_name: str, table_name: str, file_name: str) -> str:
    lakehouse_id = os.getenv("ONELAKE_LAKEHOUSE_ID")
    base_folder  = os.getenv("ONELAKE_BASE_FOLDER", "RAW_LEADTIME")
    if not lakehouse_id:
        raise RuntimeError("Missing env var: ONELAKE_LAKEHOUSE_ID")
    return f"{lakehouse_id}/Files/{base_folder}/{group_name}/{table_name}/{file_name}"

def upload_file(local_path: str, dest_path: str) -> None:
    dfs_url = os.getenv("ONELAKE_DFS_URL")
    file_system = os.getenv("ONELAKE_FILE_SYSTEM")
    if not dfs_url:
        raise RuntimeError("Missing env var: ONELAKE_DFS_URL")
    if not file_system:
        raise RuntimeError("Missing env var: ONELAKE_FILE_SYSTEM")

    try:
        from azure.identity import DefaultAzureCredential
        from azure.storage.filedatalake import DataLakeFileClient
    except Exception as e:
        raise RuntimeError(
            "Azure SDK not installed. Install azure-identity and azure-storage-file-datalake."
        ) from e

    credential = DefaultAzureCredential()
    file_client = DataLakeFileClient(
        account_url=dfs_url,
        file_system_name=file_system,
        file_path=dest_path,
        credential=credential,
    )

    with open(local_path, "rb") as f:
        file_client.upload_data(f, overwrite=True)
