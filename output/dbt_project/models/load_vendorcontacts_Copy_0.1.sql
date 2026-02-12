-- Migrated Job: load_vendorcontacts_Copy_0.1
WITH 
tDBInput_1 AS ( 
    SELECT 
  AW2017NEU_PERSON.PERSON.BUSINESSENTITYID, 
  AW2017NEU_PERSON.PERSON.FIRSTNAME, 
  AW2017NEU_PERSON.PERSON.MIDDLENAME, 
  AW2017NEU_PERSON.PERSON.LASTNAME,
  AW2017NEU_PERSON.PERSON.SUFFIX
FROM AW2017NEU_PERSON.PERSON 
),
tDBInput_2 AS ( 
    SELECT 
  AW2017NEU_PERSON.PERSONPHONE.BUSINESSENTITYID, 
  AW2017NEU_PERSON.PERSONPHONE.PHONENUMBER
FROM AW2017NEU_PERSON.PERSONPHONE 
),
tDBInput_3 AS ( 
    SELECT 
  AW2017NEU_PERSON.EMAILADDRESS.BUSINESSENTITYID,  
  AW2017NEU_PERSON.EMAILADDRESS.EMAILADDRESS
FROM AW2017NEU_PERSON.EMAILADDRESS 
),
tDBInput_4 AS ( 
    SELECT 
  `dim_vendor`.`GeographyKey`, 
  `dim_vendor`.`VENDORkEY`, 
  `dim_vendor`.`PreferredVendorStatus`, 
  `dim_vendor`.`CreditRate`, 
  `dim_vendor`.`ActiveFlag`, 
  `dim_vendor`.`AddressLine1`, 
  `dim_vendor`.`AddressLine2`, 
  `dim_vendor`.`ModifiedDate`, 
  `dim_vendor`.`DI_JobID`, 
  `dim_vendor`.`DI_CreatedDate`, 
  `dim_vendor`.`DI_ModifiedDate`, 
  `dim_vendor`.`SOR_ID`
FROM `dim_vendor` 
),
tMap_1 AS (
 WITH tDBInput_1 AS (SELECT BUSINESSENTITYID, FIRSTNAME, MIDDLENAME, LASTNAME, SUFFIX FROM Person), tDBInput_2 AS (SELECT BUSINESSENTITYID, PHONENUMBER FROM Phone), tDBInput_3 AS (SELECT BUSINESSENTITYID, EMAILADDRESS FROM Emailaddress), tDBInput_4 AS (SELECT VENDORkEY FROM vendor) SELECT tDBInput_1.BUSINESSENTITYID AS BusinessEntity_ID, NULL AS vendorcontactsKey, tDBInput_1.FIRSTNAME AS FirstName, tDBInput_1.MIDDLENAME AS MiddleName, tDBInput_1.LASTNAME AS LastName, tDBInput_1.SUFFIX AS Suffix, tDBInput_2.PHONENUMBER AS phoneNumber, tDBInput_3.EMAILADDRESS AS EmailAddress, context.getProperty('vJobPID') AS DI_JobID, current_date AS DI_CreatedDate, current_date AS DI_ModifiedDate, tDBInput_4.VENDORkEY AS VENDORkEY, 3 AS SOR_ID FROM tDBInput_1 CROSS JOIN tDBInput_2 CROSS JOIN tDBInput_3 CROSS JOIN tDBInput_4 
),
final_cte AS ( SELECT * FROM tMap_1 )

SELECT * FROM final_cte