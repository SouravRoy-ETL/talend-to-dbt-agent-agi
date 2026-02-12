-- Migrated Job: DI_CNTL_AssertCatcher_7.0
WITH 
tMap_2 AS (
 WITH dual AS (SELECT 1 AS dummy), tDBInput_1 AS (SELECT moment, pid, project, job, language, origin, status, substatus, description FROM some_source_table) SELECT t1.moment AS moment, t1.pid AS pid, t1.project AS project, t1.job AS job, t1.language AS language, t1.origin AS origin, t1.status AS status, t1.substatus AS substatus, t1.description AS description FROM tDBInput_1 AS t1 CROSS JOIN dual 
),
tSetGlobalVar_2 AS (
 WITH tSetGlobalVar_2 AS (SELECT * FROM tMap_2) SELECT * FROM tSetGlobalVar_2 
),
tSendMail_2 AS (
 WITH tSetGlobalVar_2 AS (SELECT * FROM (VALUES (1)) AS t(id)) SELECT * FROM tSetGlobalVar_2 
),
final_cte AS ( SELECT * FROM tSendMail_2 )

SELECT * FROM final_cte