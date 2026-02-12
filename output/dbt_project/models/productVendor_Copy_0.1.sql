-- Migrated Job: productVendor_Copy_0.1
WITH 
tDBInput_1 AS ( 
    SELECT 
  `productvendor`.`ProductID`, 
  `productvendor`.`BusinessEntityID`, 
  `productvendor`.`AverageLeadTime`, 
  `productvendor`.`StandardPrice`, 
  `productvendor`.`LastReceiptCost`, 
  `productvendor`.`LastReceiptDate`, 
  `productvendor`.`MinOrderQty`, 
  `productvendor`.`MaxOrderQty`, 
  `productvendor`.`OnOrderQty`, 
  `productvendor`.`UnitMeasureCode`, 
  `productvendor`.`ModifiedDate`, 
  `productvendor`.`ETLLoadID`, 
  `productvendor`.`ETLLoadDate`
FROM `productvendor` 
),
tDBInput_2 AS ( 
    SELECT 
  `dimvendors`.`VendorSK`, 
  `dimvendors`.`BusinessEntityID`, 
  `dimvendors`.`AccountNumber`, 
  `dimvendors`.`VendorName`, 
  `dimvendors`.`CreditRating`, 
  `dimvendors`.`PreferredVendorStatus`, 
  `dimvendors`.`ActiveFlag`, 
  `dimvendors`.`PurchasingWebServiceURL`, 
  `dimvendors`.`AddressTypeName`, 
  `dimvendors`.`AddressLine1`, 
  `dimvendors`.`AddressLine2`, 
  `dimvendors`.`GeoSK`, 
  `dimvendors`.`SOR_ID`, 
  `dimvendors`.`SOR_LoadDate`, 
  `dimvendors`.`SOR_UpdateDate`, 
  `dimvendors`.`DI_Tool`, 
  `dimvendors`.`DI_Job_ID`, 
  `dimvendors`.`DI_Create_Date`, 
  `dimvendors`.`DI_Modified_Date`
FROM `dimvendors` 
),
tDBInput_3 AS ( 
    SELECT 
  `dimproducts_purchased`.`ProductPurchasedSK`, 
  `dimproducts_purchased`.`ProductID`, 
  `dimproducts_purchased`.`ProductNumber`, 
  `dimproducts_purchased`.`ProductName`, 
  `dimproducts_purchased`.`ProductSubcategoryID`, 
  `dimproducts_purchased`.`ProductSubcategoryName`, 
  `dimproducts_purchased`.`ProductCategoryID`, 
  `dimproducts_purchased`.`ProductCategoryName`, 
  `dimproducts_purchased`.`ProductModelID`, 
  `dimproducts_purchased`.`ModelName`, 
  `dimproducts_purchased`.`FinishedGoodsFlag`, 
  `dimproducts_purchased`.`MakeFlag`, 
  `dimproducts_purchased`.`StandardCost`, 
  `dimproducts_purchased`.`ListPrice`, 
  `dimproducts_purchased`.`ProductLine`, 
  `dimproducts_purchased`.`ProductClass`, 
  `dimproducts_purchased`.`ProductStyle`, 
  `dimproducts_purchased`.`WeightUnitMeasureCode`, 
  `dimproducts_purchased`.`SizeUnitMeasureCode`, 
  `dimproducts_purchased`.`ProductColor`, 
  `dimproducts_purchased`.`SafetyStockLevel`, 
  `dimproducts_purchased`.`ReorderPoint`, 
  `dimproducts_purchased`.`ProductSize`, 
  `dimproducts_purchased`.`ProductWeight`, 
  `dimproducts_purchased`.`DaysToManufacture`, 
  `dimproducts_purchased`.`SellStartDate`, 
  `dimproducts_purchased`.`SellEndDate`, 
  `dimproducts_purchased`.`DiscontinuedDate`, 
  `dimproducts_purchased`.`SOR_ID`, 
  `dimproducts_purchased`.`SOR_LoadDate`, 
  `dimproducts_purchased`.`SOR_UpdateDate`, 
  `dimproducts_purchased`.`DI_Tool`, 
  `dimproducts_purchased`.`DI_Job_ID`, 
  `dimproducts_purchased`.`DI_Create_Date`, 
  `dimproducts_purchased`.`DI_Modified_Date`
FROM `dimproducts_purchased` 
),
tMap_1 AS (
 WITH tDBInput_1 AS (SELECT * FROM productVendor), tDBInput_2 AS (SELECT * FROM dimVendors), tDBInput_3 AS (SELECT * FROM productPurchased) SELECT NULL AS ProductVendorSK, tDBInput_3.ProductPurchasedSK AS ProductPurchasedSK, tDBInput_2.VendorSK AS VendorSK, tDBInput_1.ProductID AS ProductID, tDBInput_2.BusinessEntityID AS VendorID, tDBInput_1.AverageLeadTime AS AverageLeadTime, tDBInput_1.StandardPrice AS StandardPrice, tDBInput_1.LastReceiptCost AS LastReceiptCost, tDBInput_1.LastReceiptDate AS LastReceiptDate, tDBInput_1.MinOrderQty AS MinOrderQty, tDBInput_1.MaxOrderQty AS MaxOrderQty, tDBInput_1.OnOrderQty AS OnOrderQty, tDBInput_1.UnitMeasureCode AS UnitMeasureCode, tDBInput_2.SOR_ID AS SOR_ID, tDBInput_2.SOR_LoadDate AS SOR_LoadDate, tDBInput_2.SOR_UpdateDate AS SOR_UpdateDate, 'Talend' AS DI_Tool, context.getProperty('vJobPID') AS DI_Job_ID, current_date AS DI_Create_Date, current_date AS DI_Modified_Date FROM tDBInput_1 LEFT JOIN tDBInput_2 ON tDBInput_2.BusinessEntityID = tDBInput_1.BusinessEntityID LEFT JOIN tDBInput_3 ON tDBInput_3.ProductID = tDBInput_1.ProductID 
),
final_cte AS ( SELECT * FROM tMap_1 )

SELECT * FROM final_cte