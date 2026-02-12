-- Migrated Job: dimproductcosthistory_Copy1_0.1
WITH 
tDBInput_1 AS ( 
    SELECT ProductCostHistory.ProductID,
		ProductCostHistory.StartDate,
		ProductCostHistory.EndDate,
		ProductCostHistory.StandardCost,
		ProductCostHistory.ModifiedDate
FROM	Production.ProductCostHistory 
),
tDBInput_2 AS ( 
    SELECT 
  `dim_product`.`ProductID`, 
  `dim_product`.`productkey`, 
  `dim_product`.`Name`, 
  `dim_product`.`ProductModelName`, 
  `dim_product`.`ProductCategoryName`, 
  `dim_product`.`ProductSubCategoryName`, 
  `dim_product`.`ProductNumber`, 
  `dim_product`.`MakeFlag`, 
  `dim_product`.`FinishedGoodsFlag`, 
  `dim_product`.`Color`, 
  `dim_product`.`SafetyStockLevel`, 
  `dim_product`.`ReorderPoint`, 
  `dim_product`.`StandardCost`, 
  `dim_product`.`ListPrice`, 
  `dim_product`.`Size`, 
  `dim_product`.`Weight`, 
  `dim_product`.`DaysToManufacture`, 
  `dim_product`.`ProductLine`, 
  `dim_product`.`Class`, 
  `dim_product`.`Style`, 
  `dim_product`.`SellStartDate`, 
  `dim_product`.`SellEndDate`, 
  `dim_product`.`DiscontinuedDate`, 
  `dim_product`.`DI_JobID`, 
  `dim_product`.`DI_CreatedDate`, 
  `dim_product`.`DI_ModifiedDate`, 
  `dim_product`.`SOR_ID`
FROM `dim_product` 
),
tMap_1 AS (
 WITH tDBInput_1 AS (SELECT * FROM productcost), tDBInput_2 AS (SELECT * FROM dim_product) SELECT t2.productkey AS productkey, row_number() OVER () AS productcosthistorykey, t1.StandardCost AS StandardCost, NULL AS EffectiveStartDate, NULL AS EffectiveEndDate, NULL AS CurrentIndicator, t1.ModifiedDate AS ModifiedDate, context.getProperty('vJobPID') AS DI_JobID, current_date AS DI_CREATEDDATE, current_date AS DI_ModifiedDate, 3 AS SOR_ID FROM tDBInput_1 AS t1 CROSS JOIN tDBInput_2 AS t2 
),
tDBSCD_1 AS (
 WITH tMap_1 AS (SELECT * FROM input_table), scd_output AS (SELECT row_number() OVER (ORDER BY productkey) AS productcosthistorykey, productkey, EffectiveStartDate, EffectiveEndDate, CurrentIndicator FROM tMap_1) SELECT productcosthistorykey, productkey, EffectiveStartDate, EffectiveEndDate, CurrentIndicator FROM scd_output 
),
final_cte AS ( SELECT * FROM tDBSCD_1 )

SELECT * FROM final_cte