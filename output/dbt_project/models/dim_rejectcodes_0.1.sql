-- Migrated Job: dim_rejectcodes_0.1
WITH 
tFileInputExcel_1 AS ( SELECT * FROM {{ source('raw', 'src_tFileInputExcel_1') }} ),
tMap_1 AS (
 WITH tFileInputExcel_1 AS (SELECT * FROM (VALUES (1, 'test')) AS t(id, name)) SELECT NULL AS DI_Reject_SK, NULL AS DI_RejectCode, NULL AS DI_RejectReason, NULL AS DI_RejectDescription, NULL AS SOR_ID FROM tFileInputExcel_1 
),
final_cte AS ( SELECT * FROM tMap_1 )

SELECT * FROM final_cte