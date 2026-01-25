# Manufacturing_LeadTime_Pipeline_Analytics

## Introduction
An end-to-end data analytics project designed to analyze manufacturing lead time.
The solution transforms raw operational data into a Power BI dashboard that supports monitoring delays and process performance.


## Architecture
<img width="4151" height="3154" alt="LEADTIME_WORKFLOW_PROCESS" src="https://github.com/user-attachments/assets/4a63ea05-1519-47f6-b12f-53c9ad544008" />

## Data flow
1. Data Sources: ERP + XLSX
2. Ingest (12:15PM daily):
   - Crawl ERP -> Raw JSON
   - Read XLSX convert to CSV
   - Combine -> Bronze Masterfile
   - Store parquet-> Local + Upload OneLake -> Lakehouse Bronze
3. Transform (Silver):
    - Spark read Delta/Bronze Data -> Standardize + Join
    - Create Dim tables
    - Transform Fact
    - Generate unique keys
    - Create temp views
    - Upsert Fact table to Silver
4. Gold
    - Create MasterFile -> Star Schema
5. Serving
    - Semantic Moldel
    - Power BI dasboard
 
## 
  
