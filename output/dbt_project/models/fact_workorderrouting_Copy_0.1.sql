-- Migrated Job: fact_workorderrouting_Copy_0.1
WITH 
tDBInput_1 AS ( 
    SELECT Production.WorkOrderRouting.WorkOrderID,
		Production.WorkOrderRouting.ProductID,
		Production.WorkOrderRouting.OperationSequence,
		Production.WorkOrderRouting.LocationID,
		Production.WorkOrderRouting.ScheduledStartDate,
		Production.WorkOrderRouting.ScheduledEndDate,
		Production.WorkOrderRouting.ActualStartDate,
		Production.WorkOrderRouting.ActualEndDate,
		Production.WorkOrderRouting.ActualResourceHrs,
		Production.WorkOrderRouting.PlannedCost,
		Production.WorkOrderRouting.ActualCost,
		Production.WorkOrderRouting.ModifiedDate
FROM	Production.WorkOrderRouting 
),
tDBInput_2 AS ( 
    SELECT 
  `dimlocation`.`LocationSK`, 
  `dimlocation`.`LocationID`, 
  `dimlocation`.`LocationName`, 
  `dimlocation`.`CostRate`, 
  `dimlocation`.`Availability`, 
  `dimlocation`.`ModifiedDate`, 
  `dimlocation`.`SOR_ID`, 
  `dimlocation`.`SOR_LoadDate`, 
  `dimlocation`.`SOR_UpdateDate`, 
  `dimlocation`.`DI_Tool`, 
  `dimlocation`.`DI_Job_ID`, 
  `dimlocation`.`DI_Create_Date`, 
  `dimlocation`.`DI_Modified_Date`
FROM `dimlocation` 
),
tDBInput_3 AS ( 
    SELECT 
  `fact_workorder`.`WorkOrderSK`, 
  `fact_workorder`.`WorkOrderID`, 
  `fact_workorder`.`ProductSK`, 
  `fact_workorder`.`OrderQty`, 
  `fact_workorder`.`StockedQty`, 
  `fact_workorder`.`ScrappedQty`, 
  `fact_workorder`.`WorkOrder_StartDateSK`, 
  `fact_workorder`.`WorkOrder_EndDateSK`, 
  `fact_workorder`.`WorkOrder_DueDateSK`, 
  `fact_workorder`.`StartDate`, 
  `fact_workorder`.`EndDate`, 
  `fact_workorder`.`DueDate`, 
  `fact_workorder`.`ScrapReasonSK`, 
  `fact_workorder`.`SOR_ID`, 
  `fact_workorder`.`SOR_LoadDate`, 
  `fact_workorder`.`SOR_UpdateDate`, 
  `fact_workorder`.`DI_Tool`, 
  `fact_workorder`.`DI_Job_ID`, 
  `fact_workorder`.`DI_Create_Date`, 
  `fact_workorder`.`DI_Modified_Date`
FROM `fact_workorder` 
),
tDBInput_4 AS ( 
    SELECT 
  `dim_rejectcodes`.`DI_Reject_SK`, 
  `dim_rejectcodes`.`DI_RejectCode`, 
  `dim_rejectcodes`.`DI_RejectReason`, 
  `dim_rejectcodes`.`DI_RejectDescription`, 
  `dim_rejectcodes`.`SOR_ID`
FROM `dim_rejectcodes` 
),
tMap_1 AS (
 WITH tDBInput_1 AS (SELECT * FROM workorder_routing), tDBInput_2 AS (SELECT * FROM location), tDBInput_3 AS (SELECT * FROM workorder), tDBInput_4 AS (SELECT * FROM reject_codes) SELECT NULL AS WorkOrderRoutingSK, tDBInput_3.WorkOrderSK, tDBInput_3.ProductSK, tDBInput_1.OperationSequence, tDBInput_2.LocationSK, tDBInput_1.ScheduledStartDate, tDBInput_1.ScheduledEndDate, tDBInput_1.ActualStartDate, tDBInput_1.ActualEndDate, CAST(strftime('%Y%m%d', tDBInput_1.ScheduledStartDate) AS INTEGER) AS ScheduledStartDateSK, CAST(strftime('%Y%m%d', tDBInput_1.ScheduledEndDate) AS INTEGER) AS ScheduledEndDateSK, CAST(strftime('%Y%m%d', tDBInput_1.ActualStartDate) AS INTEGER) AS ActualStartDateSK, CAST(strftime('%Y%m%d', tDBInput_1.ActualEndDate) AS INTEGER) AS ActualEndDateSK, tDBInput_1.ActualResourceHrs, tDBInput_1.PlannedCost, tDBInput_1.ActualCost, SOR._SOR_ID AS SOR_ID, tDBInput_1.ModifiedDate AS SOR_LoadDate, tDBInput_1.ModifiedDate AS SOR_UpdateDate, 'Talend' AS DI_Tool, context.getProperty('vJobPID') AS DI_Job_ID, current_date AS DI_Create_Date, current_date AS DI_Modified_Date FROM tDBInput_1 LEFT JOIN tDBInput_2 ON tDBInput_2.LocationID = tDBInput_1.LocationID LEFT JOIN tDBInput_3 ON tDBInput_3.WorkOrderID = tDBInput_1.WorkOrderID 
),
final_cte AS ( SELECT * FROM tMap_1 )

SELECT * FROM final_cte