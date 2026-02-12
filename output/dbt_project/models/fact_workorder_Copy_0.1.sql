-- Migrated Job: fact_workorder_Copy_0.1
WITH 
tDBInput_2 AS ( 
    SELECT Production.WorkOrder.WorkOrderID,
		Production.WorkOrder.ProductID,
		Production.WorkOrder.OrderQty,
		Production.WorkOrder.StockedQty,
		Production.WorkOrder.ScrappedQty,
		Production.WorkOrder.StartDate,
		Production.WorkOrder.EndDate,
		Production.WorkOrder.DueDate,
		Production.WorkOrder.ScrapReasonID,
		Production.WorkOrder.ModifiedDate
FROM	Production.WorkOrder 
),
tDBInput_1 AS ( 
    SELECT 
  `dim_scarpreason`.`SOR_ID`, 
  `dim_scarpreason`.`scrapreason_durable_sk`, 
  `dim_scarpreason`.`ScrapReasonID`, 
  `dim_scarpreason`.`Name`, 
  `dim_scarpreason`.`DI_JOB_ID`, 
  `dim_scarpreason`.`DI_CREATED_DATE`, 
  `dim_scarpreason`.`DI_MODIFIED_DATE`
FROM `dim_scarpreason` 
),
tMap_1 AS (
 WITH tDBInput_1 AS (SELECT scrapreason_durable_sk, Name FROM scrapreason), tDBInput_2 AS (SELECT WorkOrderID, ProductID, OrderQty, StockedQty, ScrappedQty, StartDate, EndDate, DueDate, ModifiedDate FROM workorder) SELECT row_number() OVER () AS WORKORDERKEY, tDBInput_2.WorkOrderID AS WorkOrderID1, tDBInput_2.ProductID AS ProductID1, tDBInput_2.OrderQty AS OrderQty1, tDBInput_2.StockedQty AS StockedQty1, tDBInput_2.ScrappedQty AS ScrappedQty1, tDBInput_2.StartDate AS StartDate1, tDBInput_2.EndDate AS EndDate1, tDBInput_2.DueDate AS DueDate1, tDBInput_2.ModifiedDate AS ModifiedDate1, tDBInput_2.ProductID AS ProductID, tDBInput_1.scrapreason_durable_sk AS scrapreasonkey1, tDBInput_1.Name AS Name, current_date AS STARTDATEKEY, current_date AS FullDateAlternateKey, current_date AS ENDDATEKEY, current_date AS DUEDATEKEY, current_date AS CREATEDDATE, context.getProperty('vJobPID') AS JOBID, current_date AS MODIFIED_DATE, 2 AS SOR_ID FROM tDBInput_2 CROSS JOIN tDBInput_1 
),
final_cte AS ( SELECT * FROM tMap_1 )

SELECT * FROM final_cte