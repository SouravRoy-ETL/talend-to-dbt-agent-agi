-- Migrated Job: DI_CNTL_LogCatcher_5.0
WITH 
tMap_1 AS (
 WITH logc AS (SELECT * FROM dual) SELECT logc.moment AS moment, logc.pid AS pid, logc.root_pid AS root_pid, logc.father_pid AS father_pid, logc.project AS project, logc.job AS job, logc.context AS context, logc.priority AS priority, logc.type AS type, logc.origin AS origin, logc.message AS message, logc.code AS code FROM logc 
),
tSetGlobalVar_1 AS (
 WITH tSetGlobalVar_1 AS (SELECT * FROM tMap_1) SELECT * FROM tSetGlobalVar_1 
),
tSendMail_2 AS (
 SELECT 'No SQL required for tSendMail component' AS note 
),
final_cte AS ( SELECT * FROM tSendMail_2 )

SELECT * FROM final_cte