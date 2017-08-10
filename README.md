# Cloud Enterprise Data Warehouse (Azure)

## Overview
Centralized analytical warehouse migrating from on-prem SQL Server to Azure SQL Data Warehouse (now Azure Synapse Analytics).

## Architecture
- **Source**: On-premises SQL Server
- **ETL**: Azure Data Factory
- **Storage**: Azure SQL Data Warehouse
- **Visualization**: Power BI

## Technologies 
- Azure SQL Data Warehouse (Gen2)
- Azure Data Factory V2
- SQL Server 2016/2019
- Power BI Desktop/Service
- Azure Storage Account

## Project Structure
```
├── adf/                    # Azure Data Factory pipelines
├── sql/                    # SQL scripts
├── powerbi/               # Power BI reports
├── config/                # Configuration files
└── docs/                  # Documentation
```

## Setup Instructions

### Prerequisites
- Azure subscription
- SQL Server Management Studio
- Power BI Desktop
- Azure CLI

### Deployment
1. Create Azure resources using ARM template
2. Configure Azure Data Factory pipelines
3. Run initial data migration
4. Deploy Power BI reports

## Features
- Incremental data loading
- Slowly Changing Dimensions (SCD Type 2)
- Star schema design
- Partitioning strategy
- Security with Row-Level Security
- Automated data quality checks
