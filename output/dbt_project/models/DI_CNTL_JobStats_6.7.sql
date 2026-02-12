-- Migrated Job: DI_CNTL_JobStats_6.7
WITH 
tMSSqlInput_1 AS ( 
    SELECT Job_pid,
		             moment,
                     job_sk
 -- FROM	dbo.cntl_JobStats with (updlock)  where moment > getdate() - 1
FROM	dbo.cntl_JobStats with (nolock) 
),
tDBInput_1 AS ( 
    SELECT Job_pid,
                     root_name
 -- FROM	dbo.cntl_JobStats with (updlock)  where moment > getdate() - 1
FROM	dbo.cntl_JobStats with (nolock) 
),
tMap_1 AS (
 WITH stats01 AS (SELECT * FROM tDBInput_1), lookup_jobPID AS (SELECT * FROM tMSSqlInput_1), lookup_rootpid AS (SELECT * FROM tDBInput_1) SELECT stats01.moment AS moment, stats01.pid AS pid, stats01.father_pid AS father_pid, stats01.root_pid AS root_pid, stats01.system_pid AS system_pid, stats01.project AS project, stats01.job AS job, stats01.job_repository_id AS job_repository_id, stats01.job_version AS job_version, stats01.context AS context, stats01.origin AS origin, stats01.message_type AS message_type, stats01.message AS message, CASE WHEN stats01.duration IS NOT NULL THEN stats01.duration / 1000 ELSE NULL END AS duration, stats01.pid AS pid, stats01.father_pid AS father_pid, stats01.root_pid AS root_pid, stats01.system_pid AS system_pid, stats01.job AS job, stats01.project AS project, stats01.context AS context, stats01.message_type AS message_type, stats01.message AS message, CASE WHEN stats01.pid = stats01.root_pid THEN stats01.job ELSE CASE WHEN lookup_rootpid.root_name IS NULL THEN 'Unknown' ELSE lookup_rootpid.root_name END END AS root_name, stats01.duration AS duration, lookup_jobPID.moment AS job_start_time, CASE WHEN stats01.duration IS NOT NULL THEN stats01.moment ELSE NULL END AS job_end_time, CASE WHEN stats01.pid = stats01.root_pid THEN 'Y' ELSE 'N' END AS is_root_job, stats01.pid AS Job_pid, stats01.moment AS moment, stats01.pid AS pid, stats01.father_pid AS father_pid, stats01.root_pid AS root_pid, stats01.system_pid AS system_pid, stats01.project AS project, stats01.job AS job, stats01.job_repository_id AS job_repository_id, stats01.job_version AS job_version, stats01.context AS context, stats01.origin AS origin, stats01.message_type AS message_type, stats01.message AS message, lookup_jobPID.moment AS job_start_time, stats01.job AS job_name, stats01.message AS job_status, CASE WHEN stats01.pid = stats01.root_pid THEN stats01.job ELSE CASE WHEN lookup_rootpid.root_name IS NULL THEN 'Unknown' ELSE lookup_rootpid.root_name END END AS root_name, CASE WHEN stats01.pid = stats01.root_pid THEN 'Y' ELSE 'N' END AS is_root_job, stats01.moment AS moment, stats01.context AS context, stats01.origin AS origin, stats01.message_type AS message_type, stats01.message AS message, stats01.duration AS duration, CASE WHEN stats01.duration IS NOT NULL THEN stats01.moment ELSE NULL END AS job_finish_time, stats01.message AS job_status, stats01.duration AS job_duration, lookup_jobPID.job_sk AS job_sk FROM stats01 LEFT JOIN lookup_jobPID ON lookup_jobPID.Job_pid = stats01.pid LEFT JOIN lookup_rootpid ON lookup_rootpid.Job_pid = stats01.root_pid 
),
tJavaRow_1 AS (
 WITH tMap_1 AS (SELECT pid, root_name, job, message_type, duration, job_start_time, job_end_time, is_root_job FROM some_source_table) SELECT pid AS vRootPID, pid AS vJobPID, root_name AS vRootName, job AS vJobName, message_type AS vJobMessage, duration AS duration, job_start_time AS job_start_time, job_end_time AS job_end_time, is_root_job AS is_root_job FROM tMap_1 
),
tSendMail_1 AS (
 WITH tJavaRow_1 AS (SELECT 1 AS dummy_col) SELECT 'dummy' AS dummy_output FROM tJavaRow_1 
),
final_cte AS ( SELECT * FROM tSendMail_1 )

SELECT * FROM final_cte