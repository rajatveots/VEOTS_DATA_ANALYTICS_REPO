CREATE OR REPLACE PROCEDURE VEOTS_DB.ANALYTICS.INSERT_OVERWRITE_LOOP("DATABASE_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    sql_statement VARCHAR;
    c1 CURSOR FOR select query from lookup where active = ''Y'';
BEGIN
    FOR record IN c1
    DO
        sql_statement := REPLACE(record.query, ''dbname'', database_name);
        EXECUTE IMMEDIATE sql_statement;
    END FOR;
    RETURN ''Procedure executed successfully'';
END;
';