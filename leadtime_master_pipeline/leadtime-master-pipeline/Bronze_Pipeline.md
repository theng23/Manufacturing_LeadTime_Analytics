# Bronze Master File Pipeline 

A production-style, **one-click** data pipeline demonstrating end-to-end orchestration:

1) Crawl ERP data (browser + API)  
2) Store raw JSON snapshots  
3) Transform JSON into Parquet datasets (module-based)  
4) Optionally upload Parquet to a lakehouse (tenant configuration via env vars)

This repository is a **public-safe** version: credentials, internal URLs, and tenant IDs are removed.

## Run (Windows)
1. Copy `config/.env.example` to `.env` and fill values (do not commit `.env`).
2. Run:
```bat
master_run\run.bat
```

## Orchestration
`transformation/supervisor.py` runs each transformation module as a separate process and stops on first failure.

## Optional lake upload
Implemented in `storage/lake_uploader.py` using `DefaultAzureCredential` and env vars.
If not configured, modules print `[SKIP] upload step`.
