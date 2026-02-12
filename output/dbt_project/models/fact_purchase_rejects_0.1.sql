-- Migrated Job: fact_purchase_rejects_0.1
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
tFileInputExcel_1 AS ( SELECT * FROM {{ source('raw', 'src_tFileInputExcel_1') }} ),
tMap_1 AS (
 WITH tDBInput_1 AS (SELECT PurchaseOrderDetailID, PurchaseOrderID, OrderQty, UnitPrice, LineTotal FROM purchase_order_detail), tDBInput_2 AS (SELECT PurchaseOrderID, OrderDate, ShipDate, SubTotal, TaxAmt FROM purchase_order_header), tFileInputExcel_1 AS (SELECT A FROM reject_code) SELECT t1.PurchaseOrderDetailID AS PurchaseOrderDetailID, t1.PurchaseOrderID AS PurchaseOrderID, t1.OrderQty AS OrderQty, t1.UnitPrice AS UnitPrice, t1.LineTotal AS LineTotal, t2.OrderDate AS OrderDate, t2.ShipDate AS ShipDate, t2.SubTotal AS SubTotal, t2.TaxAmt AS TaxAmt, current_date AS SHIPDATEKEY, current_date AS CREATEDDATE, context.getProperty('vJobPID') AS JOBID, current_date AS MODIFIED_DATE, t3.A AS REJECT_CODE, 2 AS SOR_ID, row_number() OVER () AS factpurchase_rejectskey, 2 AS rejectedquantity FROM tDBInput_1 AS t1 CROSS JOIN tDBInput_2 AS t2 CROSS JOIN tFileInputExcel_1 AS t3 WHERE t2.PurchaseOrderID = t1.PurchaseOrderID 
),
final_cte AS ( SELECT * FROM tMap_1 )

SELECT * FROM final_cte