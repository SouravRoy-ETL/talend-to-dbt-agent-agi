-- Migrated Job: Copy_of_dim_employee_0.1
WITH 
tDBInput_1 AS ( 
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
tDBInput_3 AS ( 
    SELECT 
  AW2017NEU_PERSON.PERSON.BUSINESSENTITYID, 
  AW2017NEU_PERSON.PERSON.PERSONTYPE, 
  AW2017NEU_PERSON.PERSON.TITLE, 
  AW2017NEU_PERSON.PERSON.FIRSTNAME, 
  AW2017NEU_PERSON.PERSON.MIDDLENAME, 
  AW2017NEU_PERSON.PERSON.LASTNAME, 
  AW2017NEU_PERSON.PERSON.SUFFIX, 
  AW2017NEU_PERSON.PERSON.EMAILPROMOTION, 
  AW2017NEU_PERSON.PERSON.MODIFIEDDATE
FROM AW2017NEU_PERSON.PERSON 
),
tDBInput_4 AS ( 
    SELECT 
  AW2017NEU_PERSON.PERSONPHONE.BUSINESSENTITYID, 
  AW2017NEU_PERSON.PERSONPHONE.PHONENUMBER, 
  AW2017NEU_PERSON.PERSONPHONE.PHONENUMBERTYPEID, 
  AW2017NEU_PERSON.PERSONPHONE.MODIFIEDDATE
FROM AW2017NEU_PERSON.PERSONPHONE 
),
tDBInput_5 AS ( 
    SELECT 
  AW2017NEU_PERSON.EMAILADDRESS.BUSINESSENTITYID, 
  AW2017NEU_PERSON.EMAILADDRESS.EMAILADDRESSID, 
  AW2017NEU_PERSON.EMAILADDRESS.EMAILADDRESS, 
  AW2017NEU_PERSON.EMAILADDRESS.MODIFIEDDATE 
FROM AW2017NEU_PERSON.EMAILADDRESS 
),
tDBInput_2 AS ( 
    SELECT 
  AW2017NEU_HR2020.EMPLOYEE.BUSINESSENTITYID, 
  AW2017NEU_HR2020.EMPLOYEE.NATIONALIDNUMBER, 
  AW2017NEU_HR2020.EMPLOYEE.LOGINID, 
  AW2017NEU_HR2020.EMPLOYEE.JOBTITLE, 
  AW2017NEU_HR2020.EMPLOYEE.BIRTHDATE, 
  AW2017NEU_HR2020.EMPLOYEE.MARITALSTATUS, 
  AW2017NEU_HR2020.EMPLOYEE.GENDER, 
  AW2017NEU_HR2020.EMPLOYEE.HIREDATE, 
  AW2017NEU_HR2020.EMPLOYEE.SALARIEDFLAG, 
  AW2017NEU_HR2020.EMPLOYEE.VACATIONHOURS, 
  AW2017NEU_HR2020.EMPLOYEE.SICKLEAVEHOURS, 
  AW2017NEU_HR2020.EMPLOYEE.CURRENTFLAG, 
  AW2017NEU_HR2020.EMPLOYEE.MODIFIEDDATE
FROM AW2017NEU_HR2020.EMPLOYEE 
),
tDBInput_6 AS ( 
    SELECT 
  `dim_geography`.`City`, 
  `dim_geography`.`Geographykey`, 
  `dim_geography`.`StateProvinceCode`, 
  `dim_geography`.`IsOnlyStateProvinceFlag`, 
  `dim_geography`.`StateProvinceName`, 
  `dim_geography`.`CountryRegionName`, 
  `dim_geography`.`CountryRegionCode`, 
  `dim_geography`.`PostalCode`, 
  `dim_geography`.`DI_JobID`, 
  `dim_geography`.`DI_CreatedDate`, 
  `dim_geography`.`DI_ModifiedDate`, 
  `dim_geography`.`salesTerritoryKey`, 
  `dim_geography`.`SOR_ID`
FROM `dim_geography` 
),
tMap_1 AS (
 WITH tDBInput_1 AS (SELECT * FROM (VALUES (1, '123 Main St', NULL)) AS t(id, addressline1, addressline2)), tDBInput_2 AS (SELECT * FROM (VALUES (1, 'N123456789', '2020-01-01', '1990-01-01', 'Y', 'M')) AS t(id, nationalidnumber, hiredate, birthdate, salariedflag, gender)), tDBInput_3 AS (SELECT * FROM (VALUES (1, 'John', 'Doe', 'M')) AS t(id, firstname, lastname, middlename)), tDBInput_4 AS (SELECT * FROM (VALUES (1, '555-1234')) AS t(id, phonenumber)), tDBInput_5 AS (SELECT * FROM (VALUES (1, 'john.doe@example.com')) AS t(id, emailaddress)), tDBInput_6 AS (SELECT * FROM (VALUES (1, 101)) AS t(id, geographykey)) SELECT tDBInput_3.businesentityid AS BusinessEntityID, row_number() OVER () AS EmployeeKey, tDBInput_2.nationalidnumber AS EmployeeNationalID, tDBInput_1.addressline1 AS AddressLine1, tDBInput_1.addressline2 AS AddressLine2, tDBInput_3.firstname AS FirstName, tDBInput_3.lastname AS LastName, tDBInput_3.middlename AS MiddleName, tDBInput_2.hiredate AS HireDate, tDBInput_2.birthdate AS BirthDate, tDBInput_5.emailaddress AS EmailAddress, tDBInput_4.phonenumber AS Phone, tDBInput_2.salariedflag AS SalariedFlag, tDBInput_2.gender AS Gender, context.getProperty('vJobPID') AS DI_JOB_ID, current_date AS DI_MODIFIED_DATE, current_date AS DI_CREATED_DATE, tDBInput_6.geographykey AS Geokey, 2 AS SOR_ID FROM tDBInput_3 LEFT JOIN tDBInput_4 ON tDBInput_4.businesentityid = tDBInput_3.businesentityid LEFT JOIN tDBInput_5 ON tDBInput_5.businesentityid = tDBInput_3.businesentityid LEFT JOIN tDBInput_2 ON 1=1 LEFT JOIN tDBInput_1 ON 1=1 LEFT JOIN tDBInput_6 ON 1=1 
),
final_cte AS ( SELECT * FROM tMap_1 )

SELECT * FROM final_cte