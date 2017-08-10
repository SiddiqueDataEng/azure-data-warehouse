-- Fact Tables (Star Schema)

-- Fact: Sales
CREATE TABLE dwh.FactSales (
    SalesKey BIGINT IDENTITY(1,1),
    OrderID INT NOT NULL,
    OrderDetailID INT NOT NULL,
    CustomerKey BIGINT NOT NULL,
    ProductKey BIGINT NOT NULL,
    OrderDateKey INT NOT NULL,
    ShipDateKey INT,
    Quantity INT,
    UnitPrice DECIMAL(18,2),
    Discount DECIMAL(5,2),
    LineTotal DECIMAL(18,2),
    Cost DECIMAL(18,2),
    Profit DECIMAL(18,2),
    CreatedDate DATETIME2 DEFAULT GETDATE()
) WITH (
    DISTRIBUTION = HASH(OrderDateKey),
    CLUSTERED COLUMNSTORE INDEX
);
GO

-- Fact: Daily Sales Summary
CREATE TABLE dwh.FactDailySales (
    DateKey INT NOT NULL,
    TotalOrders INT,
    TotalQuantity BIGINT,
    TotalRevenue DECIMAL(18,2),
    TotalCost DECIMAL(18,2),
    TotalProfit DECIMAL(18,2),
    UniqueCustomers INT,
    CreatedDate DATETIME2 DEFAULT GETDATE()
) WITH (
    DISTRIBUTION = HASH(DateKey),
    CLUSTERED COLUMNSTORE INDEX
);
GO
