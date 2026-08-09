# 🏥 FHIR to Delta Lake: An Medallion Architecture Pipeline

This repository contains an end-to-end Data Engineering project that extracts highly nested healthcare data (FHIR standard) from a REST API and transforms it into a business-ready star schema. 

Built entirely on **Databricks**, this project demonstrates how to handle dynamic JSON schemas, perform Change Data Capture (CDC) with SCD Type 2 history tracking, and orchestrate multi-step data workflows.

---

## 🚀 How It Works (The Pipeline Orchestration)

This project is fully automated and orchestrated using **Databricks Workflows**. The workflow is configured to run sequentially in two main tasks:

### Task 1: API Ingestion (Python Notebook)
The workflow first triggers the `FHIR API File/API_Notebook.ipynb` script. 
* Connects to the HAPI FHIR REST API.
* Handles pagination to fetch bulk medical records (`Patient`, `Encounter`, `Condition`, `Observation`).
* Safely lands the raw JSON payloads into Databricks Cloud Storage (Unity Catalog Volumes).

### Task 2: Data Transformation (Lakeflow / DLT Pipeline)
Once the API extraction is successful, the workflow triggers the Spark Declarative Pipeline (Lakeflow/DLT) to process the data through the Medallion Architecture.

*(Attach your Databricks Workflow and DLT DAG screenshots below)*

---

## 🏗️ Medallion Architecture Breakdown

Inside Task 2, the data moves through three distinct layers, applying complex Data Engineering transformations at each step:

### 🥉 Bronze Layer (Raw Ingestion & Schema Evolution)
* Utilizes **Databricks Auto Loader** (`cloudFiles`) to incrementally stream raw JSON files into Delta/Parquet format.
* Implements schema inference, schema evolution, and `_rescued_data` columns to protect the pipeline against upstream API changes.
* Injects row-level data lineage (extraction timestamps and API parameters) into every record.

### 🥈 Silver Layer (Flattening, Cleansing & CDC)
* Flattens deeply nested FHIR JSON arrays using advanced PySpark array extraction (`getItem`, `getField`).
* Implements **Change Data Capture (CDC)** using Lakeflow's `apply_changes`.
* Configures **SCD Type 2** to track the historical state of patient demographics and clinical encounters over time, explicitly excluding audit timestamps from triggering false history rows.

### 🥇 Gold Layer (Business-Ready Analytics)
* Resolves foreign keys mapping clinical observations and diagnoses to specific patient encounters.
* Prevents "fan-out" row explosion by separating `Encounter` fact tables from `Observation` fact tables.
* Filters for current active states (`__END_AT IS NULL`) to provide accurate, materialized dimensional views optimized for BI tools like PowerBI and Tableau.

---

## 📊 Observability & Metadata Logging

To ensure production-level reliability, this project utilizes a two-tier metadata logging strategy:

1. **Custom API Control Table:** The Python API script writes its execution metrics (timestamp, target catalog, objects processed, success/failure status) to a custom `pipeline_audit_log` Delta table. This allows analysts to immediately verify if fresh data was pulled that morning.
2. **Native Pipeline Event Logs:** For the Bronze, Silver, and Gold transformations, the project leverages the native Databricks Lakeflow Event Log table name `event_log`. This captures granular system metrics—such as CDC deduplication counts, Auto Loader file processing times, and data quality checks—without bloating the actual pipeline code.

---

## 🛠️ Tech Stack
* **Compute Engine:** Databricks
* **Languages:** Python, PySpark, SQL
* **Frameworks:** Databricks Lakeflow Pipelines (pyspark.pipelines / DLT), Databricks Workflows
* **Storage:** Unity Catalog, Delta Lake, Cloud Storage (Volumes)
* **Ingestion:** Databricks Auto Loader (`cloudFiles`)

---

## 🧠 Key Data Engineering Challenges Solved
1. **Dynamic Schema Drift:** Built resilience against upstream API changes. When the API endpoint was updated (causing fields to disappear), the pipeline's Bronze layer rescued the data, and the Silver layer was easily refactored without breaking the Gold layer.
2. **The "Explode" Trap:** Avoided standard PySpark `explode()` functions on deeply nested arrays to maintain a strict 1:1 Primary Key ratio, ensuring safe and accurate CDC deduplication.
3. **Idempotent Processing:** Utilizing Auto Loader and streaming tables ensures that if the pipeline fails midway, it can be rerun without duplicating data in the warehouse.

---

## ⚙️ How to Run This Project

1. **Clone the Repo:** Connect your Databricks workspace to this Git repository using Git Folders.
2. **Setup Unity Catalog:** Create a catalog (e.g., `fhir_api_data_catalog`), schemas (`bronze`, `silver`, `gold`), and a Volume for the raw API data.
3. **Configure the Pipeline:** Create a new Delta Live Tables pipeline and point the source code to the Medallion notebook in this repo.
4. **Build the Workflow:** Go to Databricks Workflows. Create Task 1 (Notebook task pointing to the API script) and Task 2 (Pipeline task pointing to the DLT pipeline). Set Task 2 to depend on Task 1.
5. **Run:** Click "Run Now" on the workflow!