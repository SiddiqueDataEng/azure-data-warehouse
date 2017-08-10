-- Dimension Tables (Star Schema)
-- Using SCD Type 2 for historical tracking

-- Dimension: Date
CREATE TABLE dwh.DimDate (
    DateKey INT PRIMARY KEY NOT NULL,
    FullDate DATE NOT NULL,
    DayOfMonth INT,
    DayName NVARCHAR(10),
    DayOfWeek INT,
    DayOfYear INT,
    WeekOfYear INT,
    MonthName NVARCHAR(10),
    MonthOfYear INT,
    Quarter INT,
    Year INT,
    IsWeekend BIT,
    IsHoliday BIT
) WITH (DISTRIBUTION = REPLICATE);
GO

-- Dimension: Customer (SCD Type 2)
CREATE TABLE dwh.DimCustomer (
    CustomerKey BIGINT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT NOT NULL,
    CustomerName NVARCHAR(200),
    Email NVARCHAR(200),
    Phone NVARCHAR(50),
    Address NVARCHAR(500),
    City NVARCHAR(100),
    State NVARCHAR(50),
    Country NVARCHAR(100),
    PostalCode NVARCHAR(20),
    EffectiveDate DATETIME2 NOT NULL,
    EndDate DATETIME2,
    IsCurrent BIT NOT NULL,
    CreatedDate DATETIME2 DEFAULT GETDATE()
) WITH (DISTRIBUTION = REPLICATE);
GO

CREATE INDEX IX_DimCustomer_CustomerID ON dwh.DimCustomer(CustomerID);
GO

-- Dimension: Product (SCD Type 2)
CREATE TABLE dwh.DimProduct (
    ProductKey BIGINT IDENTITY(1,1) PRIMARY KEY,
    ProductID INT NOT NULL,
    ProductName NVARCHAR(200),
    Category NVARCHAR(100),
    SubCategory NVARCHAR(100),
    UnitPrice DECIMAL(18,2),
    Cost DECIMAL(18,2),
    IsActive BIT,
    EffectiveDate DATETIME2 NOT NULL,
    EndDate DATETIME2,
    IsCurrent BIT NOT NULL,
    CreatedDate DATETIME2 DEFAULT GETDATE()
) WITH (DISTRIBUTION = REPLICATE);
GO

CREATE INDEX IX_DimProduct_ProductID ON dwh.DimProduct(ProductID);
GO
