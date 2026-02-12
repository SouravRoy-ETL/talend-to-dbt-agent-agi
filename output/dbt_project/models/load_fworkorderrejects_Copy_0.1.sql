-- Migrated Job: load_fworkorderrejects_Copy_0.1
WITH 
tDBInput_1 AS ( 
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
tMap_1 AS (
 WITH tDBInput_1 AS (SELECT * FROM "WorkOrder") SELECT t1."WorkOrderID" AS "WorkOrderID", row_number() OVER () AS "workorderrejects", t1."OrderQty" AS "OrderQty", t1."StockedQty" AS "StockedQty", t1."ScrappedQty" AS "ScrappedQty", NULL AS "WorkOrder_StartDateSK", NULL AS "WorkOrder_EndDateSK", NULL AS "WorkOrder_DueDateSK", t1."StartDate" AS "StartDate", t1."EndDate" AS "EndDate", t1."DueDate" AS "DueDate", t1."ProductID" AS "ProductID", context.getProperty('vJobPID') AS "DI_JOB_ID", current_date AS "DI_CREATED_DATE", current_date AS "DI_MODIFIED_DATE", NULL AS "workorderroutingkey", NULL AS "workorderkey", NULL AS "scrapreasonkey", 3 AS "SOR_ID" FROM tDBInput_1 AS t1 
),
final_cte AS ( SELECT * FROM tMap_1 )

SELECT * FROM final_cte