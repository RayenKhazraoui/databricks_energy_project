Data Sources and Ingestion Architecture

This project uses the ENTSO‑E Transparency Platform API to collect electricity data for the Netherlands. The following datasets are retrieved:

Cumulative electricity generation in 15‑minute intervals
Electricity generation broken down by energy type
Total electricity load

These datasets form the foundation for all downstream energy analytics.
Ingestion Process (Energy Data)
The energy data ingestion is orchestrated using Azure Data Factory (ADF). The process operates as follows:

ADF pipelines issue scheduled API calls to the ENTSO‑E endpoints.
The raw XML responses are stored in Azure Data Lake Storage in the bronze (raw) layer.
Data is preserved in its original structure to maintain traceability and to allow controlled downstream transformation in Databricks.

Weather Data (KNMI Weather API)
To incorporate environmental context into the energy analysis, the project uses the KNMI Weather API. The retrieved dataset contains hourly Dutch weather observations
The weather ingestion flow differs from the energy ingestion flow. Instead of using ADF, the KNMI API is called directly from a Databricks notebook:

A Python notebook calls the KNMI API and retrieves the hourly weather data.
The retrieved data is immediately written into Delta tables in the bronze layer of the Lakehouse.

Data Processing Architecture
Lakehouse Integration
The Azure Data Lake is mounted as a volume within a Databricks catalog. This allows the raw energy XML files and the weather tables to be accessed uniformly through the Databricks filesystem.

Bronze to Silver Processing (Energy Data)
Bronze Layer
The ENTSO‑E XML files stored in Azure Data Lake Storage are ingested into the bronze layer using Databricks Autoloader with the availableNow trigger. This provides incremental ingestion, ensuring that newly arrived XML files are detected and processed without reprocessing previously ingested data.
Each XML file is loaded into the bronze table as a single row containing the full XML document. This preserves the raw structure and allows downstream parsing in a controlled manner.
Silver Layer
Transformation from bronze to silver is performed using Spark Structured Streaming, again with an availableNow trigger for incremental processing. As new records appear in the bronze table, the streaming job:

Parses the XML content into a normalized schema.
Applies data type conversions and standardizes timestamps.
Writes the output into a silver‑layer table using an SCD Type 1 pattern.

An SCD Type 1 approach is required because many XML files contain overlapping time ranges. Only the most recent value for each timestamp is retained, ensuring that the silver layer remains current and free of historical duplicates.

Bronze to Silver Processing (Weather Data)
Bronze Layer
Weather data from the KNMI API is retrieved directly inside a Databricks notebook. The API response is loaded into a pandas DataFrame. The DataFrame is then written directly into the bronze table as new rows, without storing intermediate JSON files in the data lake.
Silver Layer
The weather data is processed into the silver layer using the same SCD Type 1 strategy applied to the energy data. The processing step:

Cleans and normalizes the raw fields.
Converts units into consistent metric standards.
Handles overlapping or duplicated observations by retaining only the most recent entries.

The resulting silver table provides a clean, up‑to‑date weather dataset that can be reliably joined with the transformed energy data for downstream analytics.

Gold Layer (Aggregation and Enrichment)
The Gold layer is implemented using Databricks Delta Live Tables (DLT), also referred to within the current Lakehouse Data Platform (LDP) framework. The purpose of this stage is to combine the curated Silver‑layer datasets and produce aggregated and analysis‑ready tables.
Transformation Logic
Within the DLT pipeline, joins are performed between the energy and weather datasets based on their timestamp fields. These joins allow the creation of unified time‑aligned records that consolidate load, generation, and meteorological conditions.
The pipeline produces multiple aggregated tables, including:

Daily statistics
Monthly statistics
Yearly statistics

Each of these tables is optimized for analytical workloads through the use of materialized views provided by the LDP. Materialized views enable efficient incremental recomputation, ensuring that only newly added or updated rows in the Silver layer trigger downstream updates.
Pipeline Structure and Orchestration
The ingestion and Silver‑layer processing are implemented through standard Databricks notebooks. In contrast, the Silver‑to‑Gold transformations are executed entirely within a DLT pipeline, which ensures schema enforcement, lineage tracking, and incremental data guarantees.
The full orchestration is configured sequentially using Databricks Jobs, ensuring that each stage runs only after the previous one has completed successfully. The entire deployment is automated using DABS for consistent, version‑controlled delivery across development, testing, and production environments.

