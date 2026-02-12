-- Migrated Job: dimproducts_Copy_0.1
WITH 
tDBInput_1 AS ( 
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
		Production.Product.DiscontinuedDate
FROM	Production.Product 
),
tDBInput_3 AS ( 
    SELECT  Production.ProductSubcategory.ProductSubcategoryID,
		 Production.ProductSubcategory.ProductCategoryID,
		 Production.ProductSubcategory.Name
FROM	 Production.ProductSubcategory 
),
tDBInput_4 AS ( 
    SELECT  Production.ProductCategory.ProductCategoryID,
		 Production.ProductCategory.Name
FROM	 Production.ProductCategory 
),
tDBInput_2 AS ( 
    SELECT Production.ProductModel.ProductModelID,
		 Production.ProductModel.Name
FROM	 Production.ProductModel 
),
tMap_1 AS (
 WITH tDBInput_1 AS (SELECT * FROM (VALUES (1, 'Product1', 1, 1, 1, 1, 'PN1', 1, 1, 'Color1', 10, 5, 100.0, 200.0, 'Size1', 5.0, 5, 'PLine', 'Class', 'Style', '2020-01-01', '2025-01-01', NULL)) AS t(ProductID, Name, ProductSubcategoryID, ProductModelID, ProductCategoryID, ProductNumber, MakeFlag, FinishedGoodsFlag, Color, SafetyStockLevel, ReorderPoint, StandardCost, ListPrice, Size, Weight, DaysToManufacture, ProductLine, _Class, Style, SellStartDate, SellEndDate, DiscontinuedDate)), tDBInput_2 AS (SELECT * FROM (VALUES (1, 'Model1')) AS t(ProductModelID, Name)), tDBInput_3 AS (SELECT * FROM (VALUES (1, 'Subcat1', 1)) AS t(ProductSubcategoryID, Name, ProductCategoryID)), tDBInput_4 AS (SELECT * FROM (VALUES (1, 'Cat1')) AS t(ProductCategoryID, Name)) SELECT t1.ProductID AS ProductID, row_number() OVER () AS productkey, t1.Name AS Name, t2.Name AS ProductModelName, t4.Name AS ProductCategoryName, t3.Name AS ProductSubCategoryName, t1.ProductNumber AS ProductNumber, t1.MakeFlag AS MakeFlag, t1.FinishedGoodsFlag AS FinishedGoodsFlag, t1.Color AS Color, t1.SafetyStockLevel AS SafetyStockLevel, t1.ReorderPoint AS ReorderPoint, t1.StandardCost AS StandardCost, t1.ListPrice AS ListPrice, t1.Size AS Size, t1.Weight AS Weight, t1.DaysToManufacture AS DaysToManufacture, t1.ProductLine AS ProductLine, t1._Class AS _Class, t1.Style AS Style, t1.SellStartDate AS SellStartDate, t1.SellEndDate AS SellEndDate, t1.DiscontinuedDate AS DiscontinuedDate, context.getProperty('vJobPID') AS DI_JobID, current_date AS DI_CreatedDate, current_date AS DI_ModifiedDate, 1 AS SOR_ID FROM tDBInput_1 AS t1 LEFT JOIN tDBInput_2 AS t2 ON t1.ProductModelID = t2.ProductModelID LEFT JOIN tDBInput_3 AS t3 ON t1.ProductSubcategoryID = t3.ProductSubcategoryID LEFT JOIN tDBInput_4 AS t4 ON t3.ProductCategoryID = t4.ProductCategoryID 
),
final_cte AS ( SELECT * FROM tMap_1 )

SELECT * FROM final_cte