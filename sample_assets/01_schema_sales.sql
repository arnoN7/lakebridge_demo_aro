CREATE TABLE dbo.Customer (
    CustomerID   INT           NOT NULL,
    CustomerCode NVARCHAR(20)  NOT NULL,
    FirstName    NVARCHAR(100),
    MiddleName   NVARCHAR(100),
    LastName     NVARCHAR(100),
    Email        NVARCHAR(255),
    IsActive     BIT           NOT NULL DEFAULT 1,
    CreatedDate  DATETIME      NOT NULL DEFAULT GETDATE(),
    UpdatedDate  DATETIME      NULL,
    CONSTRAINT PK_Customer PRIMARY KEY (CustomerID)
);

CREATE TABLE dbo.SalesOrderHeader (
    SalesOrderID INT            NOT NULL,
    CustomerID   INT            NOT NULL,
    OrderDate    DATETIME       NOT NULL,
    Status       INT            NOT NULL,
    TotalDue     DECIMAL(18,2)  NOT NULL,
    ModifiedDate DATETIME       NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_SalesOrderHeader PRIMARY KEY (SalesOrderID)
);
