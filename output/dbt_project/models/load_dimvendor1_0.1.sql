-- Migrated Job: load_dimvendor1_0.1
WITH 
tDBInput_1 AS ( 
    SELECT 
  `vendor`.`BusinessEntityID`, 
  `vendor`.`AccountNumber`, 
  `vendor`.`VendorName`, 
  `vendor`.`CreditRating`, 
  `vendor`.`PreferredVendorStatus`, 
  `vendor`.`ActiveFlag`, 
  `vendor`.`ModifiedDate`
FROM `vendor` 
),
tDBInput_2 AS ( 
    SELECT 
  `dim_geography`.`Geographykey`, 
  `dim_geography`.`StateProvinceCode`, 
  `dim_geography`.`IsOnlyStateProvinceFlag`, 
  `dim_geography`.`StateProvinceName`, 
  `dim_geography`.`CountryRegionName`, 
  `dim_geography`.`CountryRegionCode`, 
  `dim_geography`.`PostalCode` 
FROM `dim_geography` 
),
tDBInput_3 AS ( 
    SELECT 
  AW2017NEU_PERSON.ADDRESS.ADDRESSID, 
  AW2017NEU_PERSON.ADDRESS.ADDRESSLINE1, 
  AW2017NEU_PERSON.ADDRESS.ADDRESSLINE2, 
  AW2017NEU_PERSON.ADDRESS.CITY, 
  AW2017NEU_PERSON.ADDRESS.STATEPROVINCEID, 
  AW2017NEU_PERSON.ADDRESS.POSTALCODE, 
  AW2017NEU_PERSON.ADDRESS.MODIFIEDDATE
FROM AW2017NEU_PERSON.ADDRESS 
),
tMap_1 AS (
 WITH tDBInput_1 AS (SELECT * FROM vendor), tDBInput_2 AS (SELECT * FROM geography), tDBInput_3 AS (SELECT * FROM Address) SELECT t2.Geographykey AS GeographyKey, row_number() OVER () AS VENDORkEY, t1.PreferredVendorStatus AS PreferredVendorStatus, t1.CreditRating AS CreditRating, 2 AS CreditRate, t1.ActiveFlag AS ActiveFlag, t3.ADDRESSLINE1 AS AddressLine1, t3.ADDRESSLINE2 AS AddressLine2, t1.ModifiedDate AS ModifiedDate, context.getProperty('vJobPID') AS DI_JobID, current_date AS DI_CreatedDate, current_date AS DI_ModifiedDate, 1 AS SOR_ID FROM tDBInput_1 AS t1 CROSS JOIN tDBInput_2 AS t2 CROSS JOIN tDBInput_3 AS t3 
),
final_cte AS ( SELECT * FROM tMap_1 )

SELECT * FROM final_cte