CREATE PROCEDURE dbo.usp_LoadCustomerMetrics
    @RunDate      DATETIME,
    @LookbackDays INT = 30
AS
BEGIN
    SET NOCOUNT ON;

    DELETE FROM dbo.CustomerMetrics
    WHERE SnapshotDate = CAST(@RunDate AS DATE);

    INSERT INTO dbo.CustomerMetrics (
        CustomerID,
        SnapshotDate,
        LastOrderDate,
        TotalOrders,
        TotalSpend,
        DaysSinceLastOrder
    )
    SELECT
        c.CustomerID,
        CAST(@RunDate AS DATE)                          AS SnapshotDate,
        MAX(o.OrderDate)                                AS LastOrderDate,
        COUNT(*)                                        AS TotalOrders,
        ISNULL(SUM(o.TotalDue), 0)                      AS TotalSpend,
        DATEDIFF(DAY, MAX(o.OrderDate), @RunDate)       AS DaysSinceLastOrder
    FROM dbo.Customer c
    LEFT JOIN dbo.SalesOrderHeader o
        ON c.CustomerID = o.CustomerID
    WHERE o.OrderDate >= DATEADD(DAY, -@LookbackDays, @RunDate)
    GROUP BY c.CustomerID;
END;
