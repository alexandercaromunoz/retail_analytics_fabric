# Retail Lakehouse Data Engineering Project — Context

## Overview

This project implements an end-to-end **Data Engineering solution** using a modern **Lakehouse architecture**.
It is designed as a portfolio project to demonstrate real-world data platform design, development, and operational practices.

The solution processes retail transactional data and transforms it into an analytical model for business intelligence.

---

## Data Source

* Online Retail Dataset (Kaggle)
* Contains transactional sales data:

  * InvoiceNo
  * StockCode
  * Quantity
  * UnitPrice
  * CustomerID
  * Country
  * InvoiceDate

---

## Architecture

The system follows a **Medallion Architecture**:

* **Bronze Layer** → Raw data ingestion
* **Silver Layer** → Cleaned and validated data
* **Gold Layer** → Dimensional model (Star Schema)

### Data Flow

Source → API Ingestion → Bronze → Spark Processing → Silver → Spark Processing → Gold → BI

---

## Technology Stack

* Python
* Apache Spark
* FastAPI
* Docker
* Terraform
* GitHub Actions
* Power BI

---

## Ingestion Layer

* REST API used to upload CSV files
* API stores raw data in Bronze layer
* No business transformations are applied at this stage

### Key Features

* File ingestion
* Basic validation
* Duplicate file detection
* Traceability via metadata

---

## Data Model

### Fact Tables

#### FactSalesLine

* Grain: 1 row per product per invoice
* Contains:

  * sale_id (surrogate key)
  * invoice_id (business key)
  * product_key
  * customer_key
  * date_key
  * quantity
  * unit_price
  * sales_amount
  * transaction_type (SALE / RETURN / CANCELLED)

#### FactInvoice

* Grain: 1 row per invoice
* Contains:

  * invoice_id
  * customer_key
  * date_key
  * total_items
  * total_quantity
  * sales_amount
  * invoice_status

---

### Dimensions

#### DimCustomer (SCD Type 2)

* customer_key (PK)
* customer_id (business key)
* country
* effective_date
* end_date
* is_current

#### DimProduct

* product_key (PK)
* stock_code
* description

#### DimDate

* date_key (YYYYMMDD)
* date
* year
* month
* day
* quarter
* week_of_year
* day_of_week
* is_weekend

---

## Key Modeling Decisions

* Star schema for analytical workloads
* Fact table at the lowest granularity (invoice line)
* Additional aggregated fact table (FactInvoice) for performance
* Revenue stored as a precomputed metric (`sales_amount`)
* Customer included in FactSalesLine for query simplicity
* Returns stored as negative quantities in the same fact table
* Cancelled invoices preserved for full business traceability

---

## Data Processing

Implemented using Apache Spark:

### Bronze → Silver

* Data type normalization
* Data cleansing
* Separation of returns and invalid records
* Deduplication

### Silver → Gold

* Dimension generation
* Surrogate key assignment
* SCD Type 2 handling for customers
* Fact table creation

---

## Incremental Load Strategy

* Sliding window approach (current + previous month)
* Supports late-arriving data
* Prevents full dataset reprocessing

---

## Data Quality

Includes:

* Null checks
* Duplicate detection
* Invalid record handling
* Data categorization:

  * valid sales
  * returns
  * inconsistent data

---

## Monitoring and Observability

* ETL control table (batch tracking)
* Logging of pipeline executions
* Metrics:

  * rows processed
  * execution time
* Alerts for failures

---

## Infrastructure

Managed using Infrastructure as Code:

* Terraform used to provision resources
* Supports reproducible environments

---

## CI/CD

* Implemented using GitHub Actions
* Includes:

  * code validation
  * testing
  * pipeline deployment

---

## Future Enhancements

* Streaming ingestion (Kafka / Event Hub)
* Event-driven pipelines
* Customer segmentation
* Machine learning integration
* Advanced data quality frameworks

---

## Purpose

This project demonstrates:

* Data platform architecture design
* Data modeling (dimensional modeling)
* Distributed data processing
* API-based ingestion
* Pipeline orchestration
* Data governance and observability

---

## Usage

This document can be used to:

* Resume the project in a new conversation
* Provide context to collaborators
* Document the architecture in the repository
