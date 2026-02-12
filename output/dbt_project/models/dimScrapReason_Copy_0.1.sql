-- Migrated Job: dimScrapReason_Copy_0.1
WITH 
tDBInput_1 AS ( 
    SELECT Production.ScrapReason.ScrapReasonID,
		Production.ScrapReason.Name,
		Production.ScrapReason.ModifiedDate
FROM	Production.ScrapReason 
),
tMap_1 AS (
 WITH tDBInput_1 AS (SELECT ScrapReasonID, Name FROM some_table) SELECT 3 AS SOR_ID, NULL AS scrapreason_durable_sk, tDBInput_1.ScrapReasonID, tDBInput_1.Name, Pid AS DI_JOB_ID, current_date AS DI_CREATED_DATE, current_date AS DI_MODIFIED_DATE FROM tDBInput_1 CROSS JOIN (SELECT 1 AS Pid) AS dummy 
),
final_cte AS ( SELECT * FROM tMap_1 )

SELECT * FROM final_cte