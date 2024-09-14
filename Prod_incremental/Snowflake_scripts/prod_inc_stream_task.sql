create or replace table INC_GLUE_JOB_LOGS(
    JOB_ID VARCHAR,
    START_TIME TIMESTAMP_NTZ,
    END_TIME TIMESTAMP_NTZ,
    STATUS VARCHAR,
    ERROR_MESSAGE varchar
);

select * from INC_GLUE_JOB_LOGS;


GRANT SELECT ON TABLE INC_GLUE_JOB_LOGS TO ROLE veots_analyst;

GRANT INSERT, UPDATE ON TABLE INC_GLUE_JOB_LOGS TO ROLE veots_analyst;


CREATE OR REPLACE STREAM INC_GLUE_JOB_LOGS_STREAM
ON TABLE INC_GLUE_JOB_LOGS 
APPEND_ONLY = TRUE;


CREATE OR REPLACE TASK INC_TASK
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('INC_GLUE_JOB_LOGS_STREAM')
AS
  CALL INC_DATA_INGEST('prod-veots');



ALTER TASK INC_TASK RESUME;
ALTER TASK INC_TASK SUSPEND;


select * from INC_GLUE_JOB_LOGS;
select * from INC_GLUE_JOB_LOGS_STREAM;



SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
WHERE TASK_NAME = 'INC_TASK'
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


SELECT * FROM
TABLE(information_schema.task_history(TASK_NAME=>'INC_TASK' ));


select * from TABLE(information_schema.query_history())
where QUERY_TYPE = 'CALL';
