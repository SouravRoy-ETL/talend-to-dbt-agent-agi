-- Migrated Job: DI_CNTL_FlowMeter_7.0
WITH 
tMap_2 AS (
 WITH dual AS (SELECT * FROM (VALUES (1)) AS t(id)), flow_in AS (SELECT * FROM dual) SELECT flow_in.moment, flow_in.pid, flow_in.father_pid, flow_in.root_pid, flow_in.system_pid, flow_in.project, flow_in.job, flow_in.job_repository_id, flow_in.job_version, flow_in.context, flow_in.origin, flow_in.label, flow_in.count, flow_in.reference, flow_in.thresholds FROM flow_in 
),
final_cte AS ( SELECT * FROM tMap_2 )

SELECT * FROM final_cte