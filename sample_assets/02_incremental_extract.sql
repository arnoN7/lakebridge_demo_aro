SELECT TOP 500
    c.CustomerID,
    c.CustomerCode,
    ISNULL(c.MiddleName, '')                              AS MiddleName,
    CONVERT(VARCHAR(10), c.CreatedDate, 120)              AS CreatedDateStr,
    DATEDIFF(DAY, c.CreatedDate, GETDATE())               AS DaysSinceCreate,
    ROW_NUMBER() OVER (ORDER BY c.UpdatedDate DESC, c.CustomerID DESC) AS rn
FROM dbo.Customer c WITH (NOLOCK)
WHERE c.IsActive = 1
  AND c.UpdatedDate >= DATEADD(DAY, -7, GETDATE())
ORDER BY c.UpdatedDate DESC;
