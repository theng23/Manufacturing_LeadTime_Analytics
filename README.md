# Manufacturing_LeadTime_Analytics

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
- Python (Pandas, PyArrow)
- Apache Spark / PySpark
- Delta Lake
- Microsoft Fabric / OneLake
- Lakehouse Architecture
- Power BI & DAX

## Architecture
This project follows a Lakehouse architecture pattern:
- Raw data ingestion (Bronze)
- Standardized transformations (Silver)
- Analytics-ready data model (Gold)

Below is the end-to-end workflow:

<img width="6149" height="3323" alt="LEADTIME_WORKFLOW" src="https://github.com/user-attachments/assets/386764e8-cf7a-4605-95f4-f6e36acb3742" />

## Data Pipeline Flow

### 1. Data Sources
- **ERP System**
  - Operational manufacturing data
- **Excel (XLSX) Files**
  - Planning data
  - Warehouse and shipment-related inputs

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

<img width="452" height="295" alt="Leadtime Data Model" src="https://github.com/user-attachments/assets/d282da30-739d-4611-b61f-9cc037eff9fb" />

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

## Outcomes & Capabilities

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


