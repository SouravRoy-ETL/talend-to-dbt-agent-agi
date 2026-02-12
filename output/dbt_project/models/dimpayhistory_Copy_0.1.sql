-- Migrated Job: dimpayhistory_Copy_0.1
WITH 
tDBInput_1 AS ( 
    SELECT 
  AW2017NEU_HR2020.EMPLOYEEPAYHISTORY.BUSINESSENTITYID, 
  AW2017NEU_HR2020.EMPLOYEEPAYHISTORY.RATECHANGEDATE, 
  AW2017NEU_HR2020.EMPLOYEEPAYHISTORY.RATE, 
  AW2017NEU_HR2020.EMPLOYEEPAYHISTORY.PAYFREQUENCY, 
  AW2017NEU_HR2020.EMPLOYEEPAYHISTORY.MODIFIEDDATE
FROM AW2017NEU_HR2020.EMPLOYEEPAYHISTORY 
),
tDBInput_2 AS ( 
    SELECT 
  `dimemployee`.`BusinessEntityID`, 
  `dimemployee`.`EmployeeKey`, 
  `dimemployee`.`EmployeeNationalID`, 
  `dimemployee`.`AddressLine1`, 
  `dimemployee`.`AddressLine2`, 
  `dimemployee`.`FirstName`, 
  `dimemployee`.`LastName`, 
  `dimemployee`.`MiddleName`, 
  `dimemployee`.`HireDate`, 
  `dimemployee`.`BirthDate`, 
  `dimemployee`.`EmailAddress`, 
  `dimemployee`.`Phone`, 
  `dimemployee`.`SalariedFlag`, 
  `dimemployee`.`Gender`, 
  `dimemployee`.`DI_JOB_ID`, 
  `dimemployee`.`DI_MODIFIED_DATE`, 
  `dimemployee`.`DI_CREATED_DATE`, 
  `dimemployee`.`Geokey`, 
  `dimemployee`.`SOR_ID`
FROM `dimemployee` 
),
tMap_1 AS (
 WITH tDBInput_1 AS (SELECT * FROM employee_pay_hist), tDBInput_2 AS (SELECT * FROM employee) SELECT t2.DI_MODIFIED_DATE AS DI_MODIFIED_DATE, row_number() OVER () AS PAYKEYHISTORYKEY, context.getProperty('vJobPID') AS DI_JOB_ID, current_date AS DI_CREATED_ID, t1.RATE AS Rate, t1.PAYFREQUENCY AS PayFrequency, t1.MODIFIEDDATE AS ModifiedDate, NULL AS scd_start, NULL AS scd_end, NULL AS scd_Active, t2.EmployeeKey AS EmployeeKey, 1 AS SOR_ID, t1.RATECHANGEDATE AS RATECHANGEDATE FROM tDBInput_1 AS t1 LEFT JOIN tDBInput_2 AS t2 ON t2.BusinessEntityID = t1.BUSINESSENTITYID 
),
tDBSCD_1 AS (
 WITH tMap_1 AS (SELECT * FROM some_source_table), dim_payhistory AS (SELECT PAYKEYHISTORYKEY, RATECHANGEDATE, DI_MODIFIED_DATE, scd_start, scd_end, scd_active FROM dim_payhistory) SELECT t1.RATECHANGEDATE AS RATECHANGEDATE, t1.DI_MODIFIED_DATE AS DI_MODIFIED_DATE, t1.PAYKEYHISTORYKEY AS PAYKEYHISTORYKEY, t1.scd_start AS scd_start, t1.scd_end AS scd_end, t1.scd_active AS scd_active FROM tMap_1 AS t1 LEFT JOIN dim_payhistory AS d ON t1.PAYKEYHISTORYKEY = d.PAYKEYHISTORYKEY 
),
final_cte AS ( SELECT * FROM tDBSCD_1 )

SELECT * FROM final_cte