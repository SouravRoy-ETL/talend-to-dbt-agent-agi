-- Migrated Job: dimlocationmysql_0.1
WITH 
tDBInput_1 AS ( 
    SELECT Location.LocationID,
		Location.Name,
		Location.CostRate,
		Location.Availability,
		Location.ModifiedDate
FROM	production.Location 
),
tMap_1 AS (
 WITH tDBInput_1 AS (SELECT LocationID, Name, CostRate, Availability, ModifiedDate FROM Location) SELECT current_date AS DI_Modified_Date, NULL AS locationkey, current_date AS DI_Created_Date, context.getProperty('vJobPID') AS DI_Job_id, tDBInput_1.LocationID, tDBInput_1.Name AS LocationName, tDBInput_1.CostRate, tDBInput_1.Availability, tDBInput_1.ModifiedDate, NULL AS productkey, NULL AS productinventorykey, 2 AS SOR_ID FROM tDBInput_1 
),
final_cte AS ( SELECT * FROM tMap_1 )

SELECT * FROM final_cte