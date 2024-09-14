CREATE OR REPLACE STREAM GLUE_JOB_LOGS_STREAM
ON TABLE GLUE_JOB_LOGS 
APPEND_ONLY = TRUE;


CREATE OR REPLACE TASK S3_to_SF_TASK
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('GLUE_JOB_LOGS_STREAM')
AS
  CALL INCREMENTAL_INSERT_OVERWRITE_LOOP('staging-veots');


ALTER TASK S3_to_SF_TASK RESUME;


------------------------------------queries to see task logs------------------------------------------------------


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

select * from TABLE(information_schema.query_history())
where QUERY_TYPE = 'CALL';


SELECT 
    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', START_TIME) AS START_TIME_IST,
    CONVERT_TIMEZONE('America/Los_Angeles', 'Asia/Kolkata', END_TIME) AS END_TIME_IST,
    TOTAL_ELAPSED_TIME,
    *
FROM 
    TABLE(information_schema.query_history())
    where user_name = 'SYSTEM';


