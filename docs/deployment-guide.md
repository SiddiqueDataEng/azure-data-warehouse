# Deployment Guide

## Prerequisites
- Azure subscription with appropriate permissions
- Azure CLI installed
- SQL Server Management Studio (SSMS)
- Power BI Desktop

## Step 1: Deploy Azure Resources

```bash
# Login to Azure
az login

# Create resource group
az group create --name rg-enterprise-dw --location eastus

# Deploy ARM template
az deployment group create \
  --resource-group rg-enterprise-dw \
  --template-file config/deployment.json \
  --parameters sqlServerName=sql-enterprise-dw \
               administratorLogin=sqladmin \
               administratorLoginPassword=YourPassword123! \
               dataFactoryName=adf-enterprise-dw \
               storageAccountName=stgenterprisedw
```

## Step 2: Configure Firewall Rules

```bash
# Add your IP to SQL Server firewall
az sql server firewall-rule create \
  --resource-group rg-enterprise-dw \
  --server sql-enterprise-dw \
  --name AllowMyIP \
  --start-ip-address YOUR_IP \
  --end-ip-address YOUR_IP
```

## Step 3: Execute SQL Scripts

Connect to Azure SQL DW using SSMS and execute scripts in order:
1. `sql/01_create_database.sql`
2. `sql/02_create_staging_tables.sql`
3. `sql/03_create_dimension_tables.sql`
4. `sql/04_create_fact_tables.sql`
5. `sql/05_load_procedures.sql`

## Step 4: Configure Azure Data Factory

1. Open Azure Data Factory in Azure Portal
2. Create Linked Services:
   - On-premises SQL Server (using Self-hosted Integration Runtime)
   - Azure SQL Data Warehouse
   - Azure Blob Storage
3. Import pipeline from `adf/pipeline_full_load.json`
4. Configure datasets for each table

## Step 5: Initial Data Load

```sql
-- Execute initial date dimension load
EXEC dwh.LoadDimDate;

-- Trigger ADF pipeline for initial load
-- Use Azure Portal or Azure CLI
```

## Step 6: Schedule Pipeline

Configure trigger in Azure Data Factory:
- Schedule: Daily at 2:00 AM
- Parameters: LastLoadDate = previous day

## Monitoring

- Use Azure Data Factory monitoring dashboard
- Query `etl.AuditLog` table for detailed logs
- Set up Azure Monitor alerts for failures

## Performance Tuning

1. Adjust DWU based on workload (DW100c to DW30000c)
2. Review and optimize distribution keys
3. Update statistics regularly
4. Implement result set caching for frequent queries
