# Manufacturing_LeadTime_Pipeline_Analytics

## Introduction
Manufacturing Lead Time Pipeline & Analytics is an end-to-end data platform designed to collect, standardize, and analyze manufacturing lead time across multiple operational stages.

The solution transforms raw operational data from an ERP system and manually maintained Excel files into an analytics-ready data model, serving a Power BI dashboard for monitoring delays, bottlenecks, and process performance.

## Problem Context
Manufacturing lead time data is fragmented across multiple systems:
- Not all lead time milestones are available in the ERP system
- Several critical dates are manually maintained in Excel files
- Data formats are inconsistent and difficult to scale for analytics
- Manual reporting introduces delays and quality risks

The objective of this project is to build a centralized and automated data pipeline that transforms raw operational data into a reliable analytics foundation.

## Architecture
This project follows a Lakehouse architecture pattern:

- Raw data ingestion (Bronze)
- Standardized transformations (Silver)
- Analytics-ready data model (Gold)

Below is the end-to-end workflow:
<img width="5900" height="3296" alt="LEADTIME_WORKFLOW" src="https://github.com/user-attachments/assets/7e4bbcf7-c2d3-46f4-b130-277e5d388563" />

## Data Pipeline Flow
Data Flow
1. Data Sources
- ERP System
   - Operational manufacturing data
- Excel (XLSX) Files
   - Planning data
   - Warehouse and shipment-related inputs
     
Some critical lead time milestones are not fully available in ERP and are maintained manually via Excel, requiring additional normalization and validation.

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
  
