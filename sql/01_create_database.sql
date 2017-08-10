-- Create Azure SQL Data Warehouse Database
-- Target: Azure SQL DW Gen2 (2018-2020)

-- Database is created via Azure Portal or ARM template
-- This script creates the schema objects

-- Create schemas for organization
CREATE SCHEMA staging;
GO

CREATE SCHEMA dwh;
GO

CREATE SCHEMA etl;
GO

-- Create master tables for ETL control
CREATE TABLE etl.ETLControl (
    ETLControlID INT IDENTITY(1,1) PRIMARY KEY,
    TableName NVARCHAR(100) NOT NULL,
    LastLoadDate DATETIME2,
    LastLoadStatus NVARCHAR(50),
    RecordsProcessed BIGINT,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    ModifiedDate DATETIME2 DEFAULT GETDATE()
);
GO

-- Create audit table
CREATE TABLE etl.AuditLog (
    AuditID BIGINT IDENTITY(1,1) PRIMARY KEY,
    PipelineName NVARCHAR(200),
    ActivityName NVARCHAR(200),
    StartTime DATETIME2,
    EndTime DATETIME2,
    Status NVARCHAR(50),
    RowsRead BIGINT,
    RowsWritten BIGINT,
    ErrorMessage NVARCHAR(MAX)
) WITH (DISTRIBUTION = ROUND_ROBIN);
GO
