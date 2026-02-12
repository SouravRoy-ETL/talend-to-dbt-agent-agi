-- Migrated Job: load_dimprodinventory_Copy_0.1
WITH 
tDBInput_1 AS ( 
    SELECT Production.ProductInventory.ProductID,
		Production.ProductInventory.LocationID,
		Production.ProductInventory.Shelf,
		Production.ProductInventory.Bin,
		Production.ProductInventory.Quantity,
		Production.ProductInventory.ModifiedDate
FROM	Production.ProductInventory 
),
tDBInput_2 AS ( 
    SELECT Production.Product.ProductID,
		Production.Product.Name,
		Production.Product.ProductNumber,
		Production.Product.MakeFlag,
		Production.Product.FinishedGoodsFlag,
		Production.Product.Color,
		Production.Product.SafetyStockLevel,
		Production.Product.ReorderPoint,
		Production.Product.StandardCost,
		Production.Product.ListPrice,
		Production.Product.Size,
		Production.Product.SizeUnitMeasureCode,
		Production.Product.WeightUnitMeasureCode,
		Production.Product.Weight,
		Production.Product.DaysToManufacture,
		Production.Product.ProductLine,
		Production.Product.Class,
		Production.Product.Style,
		Production.Product.ProductSubcategoryID,
		Production.Product.ProductModelID,
		Production.Product.SellStartDate,
		Production.Product.SellEndDate,
		Production.Product.DiscontinuedDate,
		Production.Product.ModifiedDate
FROM	Production.Product 
),
tMap_1 AS (
 WITH tDBInput_1 AS (SELECT ProductID, Shelf, Bin, Quantity, ModifiedDate FROM ProductInventory_Input), tDBInput_2 AS (SELECT ProductID FROM Product) SELECT t1.ProductID AS ProductID, row_number() OVER () AS productinventory, t1.Shelf AS Shelf, t1.Bin AS Bin, t1.Quantity AS Quantity, t1.ModifiedDate AS ModifiedDate, context.getProperty('vJobPID') AS DI_JobID, current_date AS DI_CreatedDate, current_date AS DI_ModifiedDate, NULL AS productkey, 4 AS SOR_ID FROM tDBInput_1 LEFT JOIN tDBInput_2 ON t1.ProductID = t2.ProductID 
),
final_cte AS ( SELECT * FROM tMap_1 )

SELECT * FROM final_cte