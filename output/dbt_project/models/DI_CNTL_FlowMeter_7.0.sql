-- Migrated Job: DI_CNTL_FlowMeter_7.0
WITH 
tMap_2 AS (
 WITH flow_in AS (SELECT * FROM dual), dual AS (SELECT 1 as dummy) SELECT flow_in.moment AS moment, flow_in.pid AS pid, flow_in.father_pid AS father_pid, flow_in.root_pid AS root_pid, flow_in.system_pid AS system_pid, flow_in.project AS project, flow_in.job AS job, flow_in.job_repository_id AS job_repository_id, flow_in.job_version AS job_version, flow_in.context AS context, flow_in.origin AS origin, flow_in.label AS label, flow_in.count AS count, flow_in.reference AS reference, flow_in.thresholds AS thresholds FROM flow_in 
),
final_cte AS ( SELECT * FROM tMap_2 )

SELECT * FROM final_cte