-- Migrated Job: fact_purchasing_0.1
WITH 
tDBInput_1 AS ( 
    SELECT 
  `purchaseorderdetail`.`PurchaseOrderID`, 
  `purchaseorderdetail`.`PurchaseOrderDetailID`, 
  `purchaseorderdetail`.`DueDate`, 
  `purchaseorderdetail`.`OrderQty`, 
  `purchaseorderdetail`.`ProductID`, 
  `purchaseorderdetail`.`UnitPrice`, 
  `purchaseorderdetail`.`LineTotal`, 
  `purchaseorderdetail`.`ReceivedQty`, 
  `purchaseorderdetail`.`RejectedQty`, 
  `purchaseorderdetail`.`StockedQty`
FROM `purchaseorderdetail` 
),
tDBInput_2 AS ( 
    SELECT 
  `purchaseorderheader`.`PurchaseOrderID`, 
  `purchaseorderheader`.`OrderStatus`, 
  `purchaseorderheader`.`EmployeeID`, 
  `purchaseorderheader`.`VendorID`, 
  `purchaseorderheader`.`ShipMethodID`, 
  `purchaseorderheader`.`OrderDate`, 
  `purchaseorderheader`.`ShipDate`, 
  `purchaseorderheader`.`SubTotal`, 
  `purchaseorderheader`.`TaxAmt`, 
  `purchaseorderheader`.`Freight`, 
  `purchaseorderheader`.`TotalDue`
FROM `purchaseorderheader` 
),
tDBInput_3 AS ( 
    SELECT 
  `vendor`.`BusinessEntityID`,  
  `vendor`.`VendorName`
FROM `vendor` 
),
tMap_1 AS (
 WITH tDBInput_1 AS (SELECT * FROM purchase_order_detail), tDBInput_2 AS (SELECT * FROM purchase_order_header), tDBInput_3 AS (SELECT * FROM vendor) SELECT NULL AS Purchasekey, tDBInput_1.PurchaseOrderDetailID AS PurchaseOrderDetailID, tDBInput_1.PurchaseOrderID AS PurchaseOrderID, tDBInput_1.DueDate AS DueDate, tDBInput_1.OrderQty AS OrderQty, tDBInput_1.ProductID AS ProductID, tDBInput_1.UnitPrice AS UnitPrice, tDBInput_1.LineTotal AS LineTotal, tDBInput_1.ReceivedQty AS ReceivedQty, tDBInput_1.RejectedQty AS RejectedQty, tDBInput_1.StockedQty AS StockedQty, tDBInput_2.OrderStatus AS OrderStatus, tDBInput_2.EmployeeID AS EmployeeID, tDBInput_2.VendorID AS VendorID, tDBInput_2.ShipMethodID AS ShipMethodID, tDBInput_2.OrderDate AS OrderDate, tDBInput_2.ShipDate AS ShipDate, tDBInput_2.SubTotal AS SubTotal, tDBInput_2.TaxAmt AS TaxAmt, tDBInput_2.Freight AS Freight, tDBInput_2.TotalDue AS TotalDue, tDBInput_3.BusinessEntityID AS BusinessEntityID, tDBInput_3.VendorName AS VendorName, context.getProperty('vJobPID') AS DI_JOB_ID, current_date AS DI_CREATED_DATE, current_date AS DI_MODIFIED_DATE, 2 AS SOR_ID FROM tDBInput_1 LEFT JOIN tDBInput_2 ON tDBInput_1.PurchaseOrderID = tDBInput_2.PurchaseOrderID LEFT JOIN tDBInput_3 ON tDBInput_2.VendorID = tDBInput_3.VendorID 
),
final_cte AS ( SELECT * FROM tMap_1 )

SELECT * FROM final_cte