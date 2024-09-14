CREATE OR REPLACE PROCEDURE incremental_insert_overwrite_loop(database_name VARCHAR)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    sql_statement VARCHAR;
    c1 CURSOR FOR select query from incremental_lookup where active = 'Y';
BEGIN
    FOR record IN c1
    DO
        sql_statement := REPLACE(record.query, 'dbname', database_name);
        EXECUTE IMMEDIATE sql_statement;
    END FOR;

    CREATE OR REPLACE TABLE PRODUCT_QR 
    AS
    SELECT * FROM PRODUCT_QR_V;

    update GLUE_JOB_LOGS set STATUS = 'empty_stream' where JOB_ID in (select JOB_ID from GLUE_JOB_LOGS_STREAM) and 1 = 2;
    
    RETURN 'Procedure executed successfully';
END;
$$;