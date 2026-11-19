# End-to-End Azure Data Engineer Retail Analytics Project (Medallion Lakehouse)

## Overview
This project implements a production-grade **Retail Sales & Inventory Lakehouse** on **Microsoft Azure** using the **Medallion Architecture (Bronze -> Silver -> Gold)** with **Azure Data Lake Storage Gen2 (ADLS Gen2)** (`abfss://`), **Azure Data Factory (ADF)**, **Azure Databricks (PySpark & Spark SQL)**, and **Azure Synapse Analytics / SQL Analytics** for reporting.

### Business Problem
A multi-store retail chain operates brick-and-mortar stores and an online storefront. Raw transactional data (POS sales, product catalog, store master data, and inventory snapshots) arrives in batches with schema drift, duplicate records, late-arriving transactions, and inconsistent currency/status values. The data engineering pipeline standardizes, deduplicates, enriches, and models this data into a dimensional star schema to power executive sales, margin, and stock-out analytics.

---

## Architecture Diagram (Logical Flow)
```
[POS / ERP / Web Orders]
        │
        ▼ (Batch / Incremental Ingestion)
[Azure Data Factory (ADF)]
        │
        ▼
[ADLS Gen2 - Bronze Layer (Raw Parquet / JSON / CSV)]
        │  abfss://bronze@<storage_account>.dfs.core.windows.net/retail/
        ▼
[Azure Databricks - Silver Layer (Cleaned, Deduplicated, SCD Type 2 Delta/Parquet)]
        │  abfss://silver@<storage_account>.dfs.core.windows.net/retail/
        ▼
[Azure Databricks / Spark SQL - Gold Layer (Star Schema: Fact & Dim Tables)]
        │  abfss://gold@<storage_account>.dfs.core.windows.net/retail/
        ▼
[Synapse Serverless SQL / Power BI]
```

---

## Repository Structure
```
azure_retail_data_engineer_project/
├── README.md
├── config/
│   └── pipeline_config.json
├── data/
│   ├── sample_sales_transactions.csv
│   ├── sample_products.json
│   └── sample_stores.csv
├── adf/
│   ├── linked_service_adls_gen2.json
│   ├── pipeline_ingest_retail_bronze.json
│   └── trigger_daily_retail_ingest.json
├── databricks/
│   ├── 01_bronze_to_silver_transformations.py
│   ├── 02_silver_to_gold_star_schema.py
│   └── 03_gold_analytics_queries.sql
└── sql/
    └── synapse_external_tables_and_views.sql
```

---

## Key Data Engineering Concepts Covered
1. **ADLS Gen2 Hierarchical Namespace & `abfss://` Paths**: Partitioned storage paths by ingestion date (`year=YYYY/month=MM/day=DD`).
2. **Bronze to Silver Curation (PySpark)**:
   - Explicit schema enforcement and timestamp normalization.
   - Deduplication using `ROW_NUMBER()` window functions partitioned by business key and ordered by ingestion/event timestamp.
   - Data quality filtering (null checks, non-negative quantities/prices, valid order status).
   - **SCD Type 2** dimension history tracking for `dim_product` and `dim_store`.
3. **Silver to Gold Dimensional Modeling (PySpark & Spark SQL)**:
   - Surrogate key generation, fact table calculation (`gross_amount`, `discount_amount`, `net_amount`, `profit_amount`), and partition pruning.
   - Analytical SQL queries using CTEs, `JOIN`s, aggregations (`SUM`, `COUNT`, `AVG`), and window functions (`RANK`, `DENSE_RANK`, lag/running totals).
4. **ADF Orchestration**:
   - Parameterized pipeline copying source files to Bronze and triggering Databricks notebooks.

---

## How to Run Locally / In Databricks
1. Upload `data/*` to your ADLS Gen2 Bronze container or mount/point the scripts to local paths for testing in Databricks Community Edition / local PySpark.
2. Execute `databricks/01_bronze_to_silver_transformations.py` to generate Silver datasets.
3. Execute `databricks/02_silver_to_gold_star_schema.py` to build Gold dimension and fact tables.
4. Run `databricks/03_gold_analytics_queries.sql` or `sql/synapse_external_tables_and_views.sql` for BI and ad-hoc analytics.
