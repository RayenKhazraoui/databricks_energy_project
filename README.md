#  Project description
This project leverages Azure Databricks and Azure Data Factory to collect, process, and refine raw electricity and weather data for the Netherlands. Using a medallion architecture (bronze, silver, gold), the system organizes and transforms incoming data into clean, analytics ready datasets suitable for downstream consumption. The pipeline operates in batch mode while taking advantage of Databricks’ incremental processing capabilities to efficiently handle new data as it arrives.

## Data Sources and Ingestion Architecture

## Energy Data (ENTSO‑E Transparency Platform API)

This project uses the ENTSO‑E Transparency Platform API to collect electricity data for the Netherlands. The following datasets are retrieved:

- Cumulative electricity generation in 15‑minute intervals  
- Electricity generation broken down by energy type  
- Total electricity load  

These datasets form the foundation for all downstream energy analytics.

### Ingestion Process (Energy Data)

The energy data ingestion is orchestrated using Azure Data Factory (ADF). The process operates as follows:

1. ADF pipelines issue scheduled API calls to the ENTSO‑E endpoints.  
2. The raw XML responses are stored in Azure Data Lake Storage in the bronze (raw) layer.  
3. The XML is preserved in its original structure for traceability and controlled downstream transformation in Databricks.

---

## Weather Data (KNMI Weather API)

To incorporate environmental context into the energy analysis, the project uses the KNMI Weather API. The retrieved dataset contains hourly Dutch weather observations.

The weather ingestion flow differs from the energy ingestion flow. Instead of ADF, the KNMI API is called directly from a Databricks notebook:

1. A Python notebook calls the KNMI API and retrieves the hourly weather data.  
2. The retrieved data is immediately written into Delta tables in the bronze layer of the Lakehouse.

---

# Data Processing Architecture

## Lakehouse Integration

The Azure Data Lake is mounted as a volume within a Databricks catalog. This allows both the raw energy XML files and the weather tables to be accessed uniformly through the Databricks filesystem.

---

## Bronze to Silver Processing (Energy Data)

### Bronze Layer

- ENTSO‑E XML files are ingested using Databricks Autoloader with the `availableNow` trigger.  
- Each XML file is loaded into the bronze table as a single row containing the full document, preserving raw structure for controlled downstream parsing.

### Silver Layer

Transformation from bronze to silver is performed with Spark Structured Streaming, using the `availableNow` trigger for incremental processing. As new records appear in the bronze table, the streaming job:

1. Parses the XML content into a normalized schema.  
2. Applies data type transformations and standardizes timestamps.  
3. Writes the output into a silver‑layer table using an SCD Type 1 pattern.

An SCD Type 1 approach is used because many XML files contain overlapping time ranges, and the most recent value must replace older entries.

---

## Bronze to Silver Processing (Weather Data)

### Bronze Layer

- Weather data is retrieved inside a Databricks notebook.  
- The API response is loaded into a pandas DataFrame.  
- The DataFrame is written directly into the bronze table as new rows, without storing intermediate JSON files in the data lake.

### Silver Layer

Weather data is processed into the silver layer using an SCD Type 1 strategy:

1. Fields are cleaned and normalized.  
2. Units are standardized into consistent metric formats.  
3. Overlapping or duplicated observations are replaced with the most recent entries.

The resulting silver table provides an up‑to‑date weather dataset ready for joining with transformed energy data.

---

# Gold Layer (Aggregation and Enrichment)

The Gold layer is implemented using Databricks Delta Live Tables (DLT), within the modern Lakehouse Data Platform (LDP). This stage combines curated Silver‑layer datasets and produces aggregated, analysis‑ready tables.

## Transformation Logic

Within the DLT pipeline, joins are performed between the energy and weather datasets based on matching timestamps. These joins create unified, time‑aligned records that consolidate electricity load, generation, and meteorological conditions.

The pipeline outputs several aggregated tables:

- Daily statistics  
- Monthly statistics  
- Yearly statistics  

These tables use LDP materialized views for efficient incremental recomputation, ensuring that only newly added or updated Silver‑layer rows trigger downstream updates.

## Pipeline Structure and Orchestration

- Ingestion and Silver‑layer processing are implemented using Databricks notebooks.  
- Silver‑to‑Gold transformations run entirely within a DLT pipeline, ensuring schema enforcement, lineage tracking, and incremental data guarantees.  
- Orchestration is configured sequentially using Databricks Jobs so that each stage runs only after the previous one completes successfully.  
- Deployment is automated using DABS, enabling consistent, version‑controlled delivery across development, testing, and production environments.
