-- Migrated Job: shipmethodmysql_op_0.1
WITH 
tDBInput_1 AS ( 
    SELECT 
  `shipmethod`.`ShipMethodID`, 
  `shipmethod`.`ShipMethodName`, 
  `shipmethod`.`ShipBase`, 
  `shipmethod`.`ShipRate`, 
  `shipmethod`.`ModifiedDate`, 
  `shipmethod`.`ETLLoadID`, 
  `shipmethod`.`ETLLoadDate`
FROM `shipmethod` 
),
tMap_1 AS (
 WITH tDBInput_1 AS (SELECT ShipMethodID, ShipMethodName, ShipBase, ShipRate FROM ShipMethod) SELECT t1.ShipMethodID AS ShipMethodID, row_number() OVER() AS shipmethodkey, t1.ShipMethodName AS ShipMethodName, t1.ShipBase AS ShipBase, t1.ShipRate AS ShipRate, context.getProperty('vJobPID') AS DI_JOB_ID, current_date AS DI_CREATED_DATE, current_date AS DI_MODIFIED_DATE, 3 AS SOR_ID FROM tDBInput_1 AS t1 
),
final_cte AS ( SELECT * FROM tMap_1 )

SELECT * FROM final_cte