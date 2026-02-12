-- Migrated Job: dimdatemysql_0.1
WITH 
tFileInputDelimited_1 AS ( SELECT * FROM {{ source('raw', 'src_tFileInputDelimited_1') }} ),
tMap_1 AS (
 WITH tFileInputDelimited_1 AS (SELECT * FROM (VALUES (1, '2023-01-01', '2023-01-01', 1, 'January', 1, 2023, 1, 1, 2023, 1)) AS t(sor_ID, FullDateAlternateKey, DayNumberOfMonth, MonthName, MonthNumberOfYear, CalenderYear, FiscalQuarter, CalenderQuarter, FiscalYear)) SELECT NULL AS sor_ID, NULL AS DateKey, NULL AS FullDateAlternateKey, NULL AS DayNumberOfMonth, NULL AS MonthName, NULL AS MonthNumberOfYear, NULL AS CalenderYear, NULL AS FiscalQuarter, NULL AS CalenderQuarter, NULL AS FiscalYear FROM tFileInputDelimited_1 
),
final_cte AS ( SELECT * FROM tMap_1 )

SELECT * FROM final_cte