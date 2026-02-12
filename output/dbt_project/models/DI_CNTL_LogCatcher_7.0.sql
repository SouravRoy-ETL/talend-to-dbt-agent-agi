-- Migrated Job: DI_CNTL_LogCatcher_7.0
WITH 
tMap_1 AS (
 WITH logc AS (SELECT * FROM (VALUES (1, '2023-01-01', 123, 456, 'proj', 'job', 'ctx', 1, 'type', 'orig', 'msg', 'code')) AS t(moment, pid, root_pid, father_pid, project, job, context, priority, type, origin, message, code)) SELECT logc.moment AS moment, logc.pid AS pid, logc.root_pid AS root_pid, logc.father_pid AS father_pid, logc.project AS project, logc.job AS job, logc.context AS context, logc.priority AS priority, logc.type AS type, logc.origin AS origin, logc.message AS message, logc.code AS code FROM logc 
),
tSetGlobalVar_1 AS (
 WITH tMap_1 AS (SELECT * FROM some_input_table) SELECT * FROM tMap_1 
),
tSendMail_2 AS (
 SELECT 'No SQL required for tSendMail component' AS note 
),
final_cte AS ( SELECT * FROM tSendMail_2 )

SELECT * FROM final_cte