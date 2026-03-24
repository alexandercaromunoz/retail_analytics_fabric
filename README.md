# Retail Lakehouse Data Engineering Project

End-to-end **Data Engineering project** implementing a modern **Lakehouse architecture** using open retail data.
The project demonstrates how to design, ingest, process, and model analytical data following industry best practices.

This repository showcases skills in **data modeling, distributed processing, API ingestion, orchestration, and cloud data architecture**.

---

# Project Overview

The system processes retail transaction data and builds an analytical model for business reporting.

The solution includes:

* API-based data ingestion
* Medallion Lakehouse architecture
* Spark-based data processing
* Dimensional modeling (Star Schema)
* Orchestration pipelines
* Infrastructure as Code
* CI/CD pipeline
* Monitoring and data quality checks

The final data model enables business analytics such as:

* Sales analysis
* Customer behavior
* Invoice performance
* Revenue reporting

---

# Architecture

The platform follows the **Medallion Architecture pattern**:

Bronze → Raw data ingestion
Silver → Data cleaning and transformation
Gold → Dimensional model optimized for analytics

Main architectural components:

Source Data
→ Ingestion API
→ Bronze Layer (Raw Data)
→ Spark Processing
→ Silver Layer (Clean Data)
→ Gold Layer (Star Schema)
→ Business Analytics

---

# Technologies Used

Core technologies used in this project include:

Programming

* Python
* SQL

Data Processing

* Apache Spark

API Development

* FastAPI

Containerization

* Docker

Infrastructure as Code

* Terraform

Orchestration

* Apache Airflow / Azure Data Factory

Analytics

* Power BI

Cloud / Platform Concepts

* Lakehouse Architecture
* Distributed Processing
* Data Pipelines

---

# Data Source

This project uses the **Online Retail Dataset** from Kaggle.

Dataset link:
https://www.kaggle.com/datasets/ulrikthygepedersen/online-retail-dataset

The dataset contains real retail transactions including:

* Invoice numbers
* Product codes
* Quantities
* Unit prices
* Customer identifiers
* Countries
* Transaction timestamps

---

# Data Architecture

The project implements a **Lakehouse architecture** with three layers.

Bronze Layer
Stores raw ingested data exactly as received from the source files.

Example table:

online_sales_raw

Silver Layer
Contains cleaned and validated data ready for transformations.

Transformations include:

* Data type normalization
* Removal of invalid records
* Standardization of fields
* Data quality checks

Gold Layer
Implements a **Dimensional Data Warehouse model** optimized for analytics.

---

# Data Model

The analytical model follows a **Star Schema** design.

Fact Tables

FactSalesLine
Grain: one row per product sold in an invoice.

FactInvoice
Grain: one row per invoice.

Dimensions

DimCustomer
DimProduct
DimDate

This design allows flexible analytical queries while maintaining high performance.

---

# Ingestion API

The system exposes a REST API to ingest sales data files.

Main capabilities:

* Upload CSV files
* Validate input structure
* Store raw data in Bronze layer
* Prevent duplicate file ingestion
* Maintain ingestion traceability

Example endpoints:

POST /upload-file
Upload sales file

POST /sales
Insert record

GET /sales
Retrieve records

PUT /sales/{id}
Update record

DELETE /sales/{id}
Delete record

---

# Spark Data Pipelines

Apache Spark jobs process data between layers.

Bronze → Silver

* Data cleaning
* Schema validation
* Data normalization

Silver → Gold

* Dimension generation
* Surrogate key assignment
* SCD Type 2 handling
* Fact table generation

---

# Orchestration

Data pipelines are orchestrated using workflow tools.

Responsibilities include:

* Scheduling batch processing
* Executing Spark jobs
* Managing dependencies
* Monitoring pipeline execution

---

# Infrastructure

Infrastructure is defined using **Infrastructure as Code**.

Terraform scripts provision resources such as:

* Storage
* Data processing environments
* Networking
* Security configurations

---

# Data Governance and Security

Security best practices implemented include:

* Role Based Access Control (RBAC)
* Secure data transport
* Encrypted storage
* Access control for datasets

---

# Monitoring and Data Quality

Monitoring capabilities include:

* Pipeline logging
* Execution metrics
* Data quality validation
* Alert mechanisms

Example validations:

* Duplicate invoice detection
* Null value checks
* Schema validation

---

# Key Data Engineering Concepts Demonstrated

This project demonstrates several important data engineering concepts:

* Lakehouse architecture
* Medallion data modeling
* Dimensional modeling
* Star schema design
* Slowly Changing Dimensions (SCD Type 2)
* Distributed data processing
* API-based ingestion
* Data pipeline orchestration
* Infrastructure as Code
* Data governance and security

---

# Project Structure

Repository structure:

```
api/                → ingestion API
spark_jobs/         → Spark transformations
pipelines/          → orchestration workflows
infrastructure/     → Terraform infrastructure
docs/               → architecture and data model documentation
monitoring/         → logging and data quality checks
```

---

# Future Improvements

Possible enhancements include:

* Streaming ingestion with Kafka
* Event-driven pipelines
* Machine learning integration
* Advanced data quality frameworks

---

# Jhon Alexander Caro

Data Engineer Portfolio Project

Designed to demonstrate modern data engineering architecture and best practices.
