-- Stored Procedures for ETL Processing

-- Load DimDate
CREATE PROCEDURE dwh.LoadDimDate
AS
BEGIN
    DECLARE @StartDate DATE = '2015-01-01';
    DECLARE @EndDate DATE = '2025-12-31';
    
    TRUNCATE TABLE dwh.DimDate;
    
    WITH DateSequence AS (
        SELECT TOP (DATEDIFF(DAY, @StartDate, @EndDate) + 1)
            DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1, @StartDate) AS FullDate
        FROM sys.objects a CROSS JOIN sys.objects b
    )
    INSERT INTO dwh.DimDate (
        DateKey, FullDate, DayOfMonth, DayName, DayOfWeek, DayOfYear,
        WeekOfYear, MonthName, MonthOfYear, Quarter, Year, IsWeekend, IsHoliday
    )
    SELECT 
        CAST(FORMAT(FullDate, 'yyyyMMdd') AS INT) AS DateKey,
        FullDate,
        DAY(FullDate) AS DayOfMonth,
        DATENAME(WEEKDAY, FullDate) AS DayName,
        DATEPART(WEEKDAY, FullDate) AS DayOfWeek,
        DATEPART(DAYOFYEAR, FullDate) AS DayOfYear,
        DATEPART(WEEK, FullDate) AS WeekOfYear,
        DATENAME(MONTH, FullDate) AS MonthName,
        MONTH(FullDate) AS MonthOfYear,
        DATEPART(QUARTER, FullDate) AS Quarter,
        YEAR(FullDate) AS Year,
        CASE WHEN DATEPART(WEEKDAY, FullDate) IN (1, 7) THEN 1 ELSE 0 END AS IsWeekend,
        0 AS IsHoliday
    FROM DateSequence;
END;
GO

-- Load DimCustomer (SCD Type 2)
CREATE PROCEDURE dwh.LoadDimCustomer
AS
BEGIN
    -- Close expired records
    UPDATE dwh.DimCustomer
    SET EndDate = GETDATE(),
        IsCurrent = 0
    WHERE IsCurrent = 1
    AND CustomerID IN (
        SELECT s.CustomerID
        FROM staging.Customers s
        INNER JOIN dwh.DimCustomer d ON s.CustomerID = d.CustomerID AND d.IsCurrent = 1
        WHERE s.CustomerName <> d.CustomerName
           OR s.Email <> d.Email
           OR s.Address <> d.Address
    );
    
    -- Insert new and changed records
    INSERT INTO dwh.DimCustomer (
        CustomerID, CustomerName, Email, Phone, Address, City, State, Country, PostalCode,
        EffectiveDate, EndDate, IsCurrent
    )
    SELECT 
        s.CustomerID,
        s.CustomerName,
        s.Email,
        s.Phone,
        s.Address,
        s.City,
        s.State,
        s.Country,
        s.PostalCode,
        GETDATE() AS EffectiveDate,
        NULL AS EndDate,
        1 AS IsCurrent
    FROM staging.Customers s
    LEFT JOIN dwh.DimCustomer d ON s.CustomerID = d.CustomerID AND d.IsCurrent = 1
    WHERE d.CustomerKey IS NULL
       OR s.CustomerName <> d.CustomerName
       OR s.Email <> d.Email
       OR s.Address <> d.Address;
END;
GO

-- Load DimProduct (SCD Type 2)
CREATE PROCEDURE dwh.LoadDimProduct
AS
BEGIN
    -- Close expired records
    UPDATE dwh.DimProduct
    SET EndDate = GETDATE(),
        IsCurrent = 0
    WHERE IsCurrent = 1
    AND ProductID IN (
        SELECT s.ProductID
        FROM staging.Products s
        INNER JOIN dwh.DimProduct d ON s.ProductID = d.ProductID AND d.IsCurrent = 1
        WHERE s.ProductName <> d.ProductName
           OR s.UnitPrice <> d.UnitPrice
    );
    
    -- Insert new and changed records
    INSERT INTO dwh.DimProduct (
        ProductID, ProductName, Category, SubCategory, UnitPrice, Cost, IsActive,
        EffectiveDate, EndDate, IsCurrent
    )
    SELECT 
        s.ProductID,
        s.ProductName,
        s.Category,
        s.SubCategory,
        s.UnitPrice,
        s.Cost,
        s.IsActive,
        GETDATE() AS EffectiveDate,
        NULL AS EndDate,
        1 AS IsCurrent
    FROM staging.Products s
    LEFT JOIN dwh.DimProduct d ON s.ProductID = d.ProductID AND d.IsCurrent = 1
    WHERE d.ProductKey IS NULL
       OR s.ProductName <> d.ProductName
       OR s.UnitPrice <> d.UnitPrice;
END;
GO

-- Load FactSales
CREATE PROCEDURE dwh.LoadFactSales
AS
BEGIN
    INSERT INTO dwh.FactSales (
        OrderID, OrderDetailID, CustomerKey, ProductKey, OrderDateKey, ShipDateKey,
        Quantity, UnitPrice, Discount, LineTotal, Cost, Profit
    )
    SELECT 
        so.OrderID,
        od.OrderDetailID,
        dc.CustomerKey,
        dp.ProductKey,
        CAST(FORMAT(so.OrderDate, 'yyyyMMdd') AS INT) AS OrderDateKey,
        CAST(FORMAT(so.ShipDate, 'yyyyMMdd') AS INT) AS ShipDateKey,
        od.Quantity,
        od.UnitPrice,
        od.Discount,
        od.LineTotal,
        dp.Cost * od.Quantity AS Cost,
        od.LineTotal - (dp.Cost * od.Quantity) AS Profit
    FROM staging.OrderDetails od
    INNER JOIN staging.SalesOrders so ON od.OrderID = so.OrderID
    INNER JOIN dwh.DimCustomer dc ON so.CustomerID = dc.CustomerID AND dc.IsCurrent = 1
    INNER JOIN dwh.DimProduct dp ON od.ProductID = dp.ProductID AND dp.IsCurrent = 1
    WHERE NOT EXISTS (
        SELECT 1 FROM dwh.FactSales fs 
        WHERE fs.OrderID = so.OrderID AND fs.OrderDetailID = od.OrderDetailID
    );
END;
GO
