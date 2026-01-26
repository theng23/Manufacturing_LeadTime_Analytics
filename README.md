# Manufacturing_LeadTime_Pipeline_Analytics

## Introduction
An end-to-end data analytics project designed to analyze manufacturing lead time.
The solution transforms raw operational data into a Power BI dashboard that supports monitoring delays and process performance.


## Architecture
This project follows a Lakehouse architecture pattern:

- Raw data ingestion (Bronze)
- Standardized transformations (Silver)
- Analytics-ready data model (Gold)

Below is the end-to-end workflow:
<img width="5900" height="3296" alt="LEADTIME_WORKFLOW" src="https://github.com/user-attachments/assets/7e4bbcf7-c2d3-46f4-b130-277e5d388563" />

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
  
