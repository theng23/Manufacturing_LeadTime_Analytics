# Manufacturing LeadTime Analytics

## Introduction
- End-to-end Lakehouse data pipeline for manufacturing lead time
- Ingests ERP and Excel data, standardizes and models analytics-ready datasets
- Built with Python, Spark, Delta Lake, and Power BI
- Designed for scalable, production-style analytics

## Problem Context
Manufacturing lead time data is fragmented across multiple systems:
- Not all lead time milestones are available in the ERP system
- Several critical dates are manually maintained in Excel files
- Data formats are inconsistent and difficult to scale for analytics
- Manual reporting introduces delays and quality risks

The objective of this project is to build a centralized and automated data pipeline that transforms raw operational data into a reliable analytics foundation.

## Primary Goal
Provide end-to-end visibility of manufacturing lead time from order initiation to final shipment.

The platform aims to align ERP data with actual production workflows, improve data accuracy and accountability across departments, and establish a reliable foundation for operational planning, auditing, and future automation.

## Technology Stack

</details>
  
<br>

| Category | Technology | Purpose |
|--------|------------|---------|
| Data Processing | Python | Ingest and preprocess ERP and Excel data, perform initial cleansing, and write data in Parquet format |
| Distributed Processing | PySpark / SQL | Transform and standardize data in the Silver layer, including joins, deduplication, and business logic |
| Storage | Delta Lake | Store Silver and Gold datasets with ACID guarantees and support incremental upserts |
| Data Platform | Microsoft Fabric / OneLake | Central Lakehouse platform for data storage, notebook execution, and pipeline orchestration |
| Architecture | Lakehouse Architecture | Enable end-to-end data flow from raw ingestion to analytics-ready datasets |
| Analytics & Visualization | Power BI | Build semantic models and dashboards for lead time analysis and reporting |

## Architecture
This project follows a Lakehouse architecture pattern:
- Raw data ingestion (Bronze)
- Standardized transformations (Silver)
- Analytics-ready data model (Gold)

Below is the end-to-end automation workflow:

<img width="6149" height="3323" alt="LEADTIME_WORKFLOW" src="https://github.com/user-attachments/assets/386764e8-cf7a-4605-95f4-f6e36acb3742" />

## Data Pipeline Flow

### 1. Data Sources

</details>

<br>

| Dataset | Source |
|-------|-------|
|Shipment Date|Planning files|
|Docket Received Date & Fabric Stock-in Date|Warehouse and shipment-related inputs|
|Production Leadtime |ERP files|
|Phase 1 & Phase 2 on ERP|ERP files|
|Other Information|ERP System|

Some critical lead time milestones are not fully available in ERP and are maintained manually via Excel, requiring additional normalization and validation.

### 2. Ingestion Layer (Bronze)
- Scheduled daily ingestion
- Crawl ERP system and store raw JSON snapshots
- Read Excel files and convert to CSV
- Persist raw data as Parquet
- Upload data to OneLake (Lakehouse Bronze layer)

**Purpose**
- Preserve raw data
- Enable replay and reprocessing
- Avoid business logic at ingestion stage

### 3. Transformation Layer (Silver)
- Read Bronze data using Spark
- Standardize column names and data types
- Normalize date formats
- Join ERP and Excel datasets
- Generate business keys
- Deduplicate records
- Create validated fact and dimension datasets
- Upsert transformed data into Silver tables

### 4. Analytics Layer (Gold)
- Consolidate Silver datasets
- Apply Star Schema modeling
- Separate fact and dimension tables
- Define consistent grains and metrics

The Gold layer acts as the single source of truth for analytics.

### 5. Serving Layer
- Semantic model built on top of Gold data
- Business metrics and KPIs defined
- Power BI dashboard for monitoring lead time performance


## Data Modeling

<img width="722" height="501" alt="Screenshot 2026-01-26 164823" src="https://github.com/user-attachments/assets/8546d6f9-9374-40cd-add2-2c9b22c1d417" />

The Silver layer uses a denormalized lead time fact table to simplify analytics
and minimize Power BI relationship complexity for operational reporting.

### Example: Silver Lead Time Fact Table (Simplified)

```sql
CREATE TABLE IF NOT EXISTS LEADTIME.SILVER_FACT_LEADTIME (
    leadtime_key STRING,
    brand STRING,
    style STRING,
    stage STRING,
    event_date DATE,
    leadtime_days INT,
    category STRING,
    sub_category STRING
)
USING DELTA;
```

## Pipeline Execution
<img width="858" height="308" alt="image (8)" src="https://github.com/user-attachments/assets/4268579d-a1e7-45cd-b4ba-1bd4972cb50a" />

This image illustrates a successful end-to-end execution of the pipeline, from Silver layer transformation to semantic model refresh.

## Project Structure
**BRONZE Stage Structure:**
```
Manufacturing_LeadTime_Analytics-main/
└─ leadtime_master_pipeline/
   └─ leadtime-master-pipeline/
      ├─ README.md
      ├─ requirements.txt
      ├─ config/
      │  └─ __init__.py
      ├─ ingestion/
      │  ├─ __init__.py
      │  ├─ crawler/
      │  │  ├─ __init__.py
      │  │  ├─ erp_cralwer.py
      │  │  ├─ helper.py
      │  │  └─ helper_phase1.py
      │  └─ raw_storage/
      │     ├─ __init__.py
      │     └─ json_writer.py
      ├─ transformation/
      │  ├─ __init__.py
      │  ├─ supervisor.py
      │  └─ modules/
      │     ├─ __init__.py
      │     ├─ costing.py
      │     ├─ cuttingdocket.py
      │     ├─ fabric_trim.py
      │     ├─ managecostingsheetclient.py
      │     ├─ manageliststyleoforder.py
      │     ├─ managepurchaseorder.py
      │     ├─ mastergroupfabricpotabsofplanning_mastergrouppoitems.py
      │     ├─ styleproductofplanning.py
      │     ├─ technical.py
      │     └─ treatment.py
      ├─ storage/
      │  ├─ __init__.py
      │  └─ lake_uploader.py
      └─ master_run/
         ├─ run.bat
         └─ setup_venv.py

```



---

**Configuration**

Create `.env` from `config/.env.example`.

| Key | Description | Example |
|---|---|---|
| ERP_BASE_URL | ERP system base URL | https://erp.company.local |
| ERP_USERNAME | ERP login username | admin |
| ERP_PASSWORD | ERP login password | ******** |
| RAW_OUTPUT_PATH | Local raw landing folder for Bronze stage | ./ingestion/raw_storage/erp_json |
| ENABLE_TRANSFORM | Enable transformation step | 1 |
| ENABLE_UPLOAD | Enable lake upload step | 0 |
| LAKE_OUTPUT_PATH | Target path for lake upload | ./storage/output |
| LOG_PATH | Log file path | ./logs/bronze.log |

## Outcomes & Results

The platform establishes a structured and analytics-ready foundation for
analyzing manufacturing lead time across multiple production stages.

It enables:
- Consistent representation of lead time data across heterogeneous source systems
- Stage-level visibility into process duration and sequencing
- Simplified consumption for reporting and exploratory analysis

The dashboard design follows a two-level structure:

**Overview**
- Consolidated view of end-to-end lead time
- Flexible filtering for focused analysis
- Comparative views to support interpretation across stages

**Stage Details**
- Granular breakdown of duration within each production stage
- Clear separation of stage-specific process components
