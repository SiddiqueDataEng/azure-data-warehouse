-- Staging Tables for Data Warehouse
-- These tables receive raw data from source systems

-- Staging: Customers
CREATE TABLE staging.Customers (
    CustomerID INT NOT NULL,
    CustomerName NVARCHAR(200),
    Email NVARCHAR(200),
    Phone NVARCHAR(50),
    Address NVARCHAR(500),
    City NVARCHAR(100),
    State NVARCHAR(50),
    Country NVARCHAR(100),
    PostalCode NVARCHAR(20),
    CreatedDate DATETIME2,
    ModifiedDate DATETIME2,
    LoadDate DATETIME2 DEFAULT GETDATE()
) WITH (DISTRIBUTION = ROUND_ROBIN, HEAP);
GO

-- Staging: Products
CREATE TABLE staging.Products (
    ProductID INT NOT NULL,
    ProductName NVARCHAR(200),
    Category NVARCHAR(100),
    SubCategory NVARCHAR(100),
    UnitPrice DECIMAL(18,2),
    Cost DECIMAL(18,2),
    IsActive BIT,
    CreatedDate DATETIME2,
    ModifiedDate DATETIME2,
    LoadDate DATETIME2 DEFAULT GETDATE()
) WITH (DISTRIBUTION = ROUND_ROBIN, HEAP);
GO

-- Staging: Sales Orders
CREATE TABLE staging.SalesOrders (
    OrderID INT NOT NULL,
    CustomerID INT,
    OrderDate DATETIME2,
    ShipDate DATETIME2,
    TotalAmount DECIMAL(18,2),
    Status NVARCHAR(50),
    CreatedDate DATETIME2,
    LoadDate DATETIME2 DEFAULT GETDATE()
) WITH (DISTRIBUTION = ROUND_ROBIN, HEAP);
GO

-- Staging: Order Details
CREATE TABLE staging.OrderDetails (
    OrderDetailID INT NOT NULL,
    OrderID INT,
    ProductID INT,
    Quantity INT,
    UnitPrice DECIMAL(18,2),
    Discount DECIMAL(5,2),
    LineTotal DECIMAL(18,2),
    LoadDate DATETIME2 DEFAULT GETDATE()
) WITH (DISTRIBUTION = ROUND_ROBIN, HEAP);
GO
