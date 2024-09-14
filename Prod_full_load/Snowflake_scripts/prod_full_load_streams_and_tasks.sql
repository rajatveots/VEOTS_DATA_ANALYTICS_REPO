create or replace table FL_GLUE_JOB_LOGS(
    JOB_ID VARCHAR,
    START_TIME TIMESTAMP_NTZ,
    END_TIME TIMESTAMP_NTZ,
    STATUS VARCHAR,
    ERROR_MESSAGE varchar
);


GRANT SELECT ON TABLE FL_GLUE_JOB_LOGS TO ROLE veots_analyst;

GRANT INSERT, UPDATE ON TABLE FL_GLUE_JOB_LOGS TO ROLE veots_analyst;


CREATE OR REPLACE STREAM FL_GLUE_JOB_LOGS_STREAM
ON TABLE FL_GLUE_JOB_LOGS 
APPEND_ONLY = TRUE;


CREATE OR REPLACE TASK FL_TASK
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('FL_GLUE_JOB_LOGS_STREAM')
AS
  CALL FL_DATA_REFRESH('prod-veots');



ALTER TASK FL_TASK RESUME;
ALTER TASK FL_TASK SUSPEND;


select * from FL_GLUE_JOB_LOGS;
select * from FL_GLUE_JOB_LOGS_STREAM;



SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE TASK_NAME = 'S3_to_SF_TASK'
ORDER BY START_TIME DESC;



SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
  where STATE != 'SKIPPED'
  ORDER BY SCHEDULED_TIME;



SELECT th.NAME, qh2.* FROM
TABLE(information_schema.task_history(TASK_NAME=>'S3_TO_SF_TASK' )) th,
TABLE(information_schema.query_history()) qh,
TABLE(information_schema.query_history()) qh2
WHERE qh.QUERY_ID = th.QUERY_ID
AND qh.start_time between th.query_start_time and th.completed_time
AND qh2.start_time between th.query_start_time and th.completed_time
AND qh.SESSION_ID = qh2.SESSION_ID
ORDER BY qh2.start_time;


