# Manufacturing LeadTime Analytics

## ⭐Introduction
- End-to-end Lakehouse data pipeline for manufacturing lead time
- Ingests ERP and Excel data, standardizes and models analytics-ready datasets
- Built with Python, Spark, Delta Lake, and Power BI
- Designed for scalable, production-style analytics

## 🧠Problem Context
Manufacturing lead time data is fragmented across multiple systems:
- Not all lead time milestones are available in the ERP system
- Several critical dates are manually maintained in Excel files
- Data formats are inconsistent and difficult to scale for analytics
- Manual reporting introduces delays and quality risks

The objective of this project is to build a centralized and automated data pipeline that transforms raw operational data into a reliable analytics foundation.

## 🎯Primary Goal
Provide end-to-end visibility of manufacturing lead time from order initiation to final shipment.

The platform aims to align ERP data with actual production workflows, improve data accuracy and accountability across departments, and establish a reliable foundation for operational planning, auditing, and future automation.

![Screenshot_2-2-2026_222357_](https://github.com/user-attachments/assets/2d6a6c29-ac49-473d-b582-2369988b4f85)

## ⚒️Technology Stack

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

## 📊Architecture
This project follows a Lakehouse architecture pattern:
- Raw data ingestion (Bronze)
- Standardized transformations (Silver)
- Analytics-ready data model (Gold)

Below is the end-to-end automation workflow:

<img width="6551" height="3769" alt="LEADTIME_WORKFLOW" src="https://github.com/user-attachments/assets/5216a4dc-c610-4e0e-b725-95e3776f0721" />

## 🔗Data Pipeline Flow


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
The Bronze layer is orchestrated locally via a master run script.
It handles environment setup, ERP crawling, and raw data persistence
before pushing data to the Lakehouse.


| Layer | Tools | Transformation & Processing |  Purpose |
|--------|--------|--------|--------|
| Bronze Layer| - Python <br> - VS Code | - Scheduled daily ingestion<br> - Crawl ERP system and store raw JSON snapshots<br> - Read Excel files and convert to CSV<br> - Persist raw data as Parquet<br> - Upload data to OneLake (Lakehouse Bronze layer) | - Preserve raw data <br> - Enable replay and reprocessing <br> - Avoid business logic at ingestion stage |



### 3. Transformation Layer (Silver)

| Layer | Tools | Input | Transformation & Processing | Output |
|------|---------|-------|---------------------|--------|
| Silver | Microsoft Fabric Notebooks (Spark) | Bronze Delta tables | - Schema standardization<br> - Date normalization<br> - Business key generation<br> - ERP–Excel joins<br> - Leadtime calculation<br> - Validation & deduplication | SILVER_FACT_LEADTIME |



### 4. Analytics Layer (Gold)
| Layer | Platform | Input | Key Responsibilities | Output |
|------|----------|-------|----------------------|--------|
| Gold | Microsoft Fabric (Lakehouse) | Silver Delta tables | - Star schema modeling<br> - Fact & dimension separation<br> - Metric standardization | Analytics-ready Gold tables |
| Serving | Power BI (Direct Lake) | Gold Lakehouse tables | - Semantic model<br> - KPI definitions<br> - Auto-refresh orchestration | Leadtime performance dashboards |



## 📖Data Modeling

<img width="722" height="501" alt="Screenshot 2026-01-26 164823 1" src="https://github.com/user-attachments/assets/e9a348e8-5d32-4be7-b05f-c6a863aff75d" />

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

## Project Structure
### 🔥**Bronze Stage Structure:**
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
#### Pipeline Step:
| Step | Bronze Pipeline Stage | Duration |
|-----|---------------|------------------|
| 1 | ERP Crawling | ~3 minutes |
| 2 | Raw JSON Persistence | ~1 minute |
| 3 | Transformation Orchestration | ~1 minute |
| 4 | Domain Transformations (Modules) | ~4 minutes |
| 5 | Lakehouse Upload | ~1 minute |
| **Total** | End-to-End Pipeline | **~10 minutes** |

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

---
### 🔥**Silver Stage Structure:**
**Microsoft Fabric Workspace**

```
│
├─ Lakehouse
│  ├─ bronze/
│  │  └─ raw_erp_data (Delta / Parquet)
│  │
│  └─ silver/
│     └─ SILVER_FACT_LEADTIME (Delta Table)
│
├─ Notebooks
│  └─ SILVER_FACT_LEADTIME.ipynb
│     - Read Bronze Delta tables
│     - Apply business rules and leadtime logic
│     - Generate validated Silver fact table
│
└─ Semantic Model (Power BI)
   - Directly references SILVER_FACT_LEADTIME
   - Auto-refresh after notebook execution
```
#### Pipeline Step:
| Step | Stage | Description | Duration |
|-----|-------|-------------|----------|
| 1 | Bronze | Raw ERP data storage | ~3 min |
| 2 | Silver | Business logic & leadtime calc | ~6 min |
| 3 | BI | Power BI refresh | ~1 min |
| **Total** | Pipeline | Analytics-ready data | **~10 min** |

---

## ⏱️ Final Outcome

**ERP → Lakehouse → Silver → Power BI Dashboard: ~20 minutes**

This project delivers an end-to-end, fully automated analytics pipeline with a predictable and repeatable SLA, transforming raw ERP data into decision-ready dashboards within 20 minutes.

---

**Pipeline Execution**

<img width="858" height="308" alt="image (8)" src="https://github.com/user-attachments/assets/4268579d-a1e7-45cd-b4ba-1bd4972cb50a" />

This image illustrates a successful end-to-end execution of the pipeline, from Silver layer transformation to semantic model refresh.

---

## 🚀Outcomes & Results

This project delivers a scalable and analytics-ready foundation
for manufacturing lead time analysis across multiple production stages.

By consolidating ERP and Excel-based operational data into a unified
Lakehouse architecture, the platform transforms fragmented and manual
reporting processes into a consistent, automated analytics workflow.

### Key Outcomes

- Established a single source of truth for manufacturing lead time data
  across Fabric, Trim, Technical, Treatment, Costing, and Planning domains
- Eliminated manual Excel consolidation by automating ingestion and
  transformation of heterogeneous data sources
- Standardized lead time definitions and calculation logic across stages
- Enabled reliable historical analysis and trend comparison over time

### Analytical Capabilities

The resulting analytics layer enables:

- End-to-end visibility from PO submission to actual ex-factory date
- Stage-level lead time breakdown aligned with real operational workflows
- Identification of delay patterns and bottlenecks across production stages
- Consistent comparison of actual timelines versus expected milestones

### Dashboard Design (Conceptual)

The Power BI dashboard is designed around a two-level analytical structure:

**Overview**
- Consolidated end-to-end lead time performance
- High-level trend analysis across seasons, brands, and stages
- Flexible filtering to support focused operational review

**Stage Details**
- Detailed visibility into individual production stages
- Clear separation of process-specific lead time components
- Support for root-cause analysis and operational follow-up

