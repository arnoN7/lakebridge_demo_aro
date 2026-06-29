CREATE PROCEDURE dbo.usp_UpdateInventory
    @ProductID   INT,
    @Quantity    INT,
    @WarehouseID INT = 1
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE dbo.Inventory
    SET QuantityOnHand = QuantityOnHand - @Quantity,
        LastModified   = GETDATE()
    WHERE ProductID   = @ProductID
      AND WarehouseID = @WarehouseID;

    INSERT INTO dbo.InventoryLog (ProductID, WarehouseID, QuantityChange, LogDate)
    VALUES (@ProductID, @WarehouseID, -@Quantity, GETDATE());
END;
