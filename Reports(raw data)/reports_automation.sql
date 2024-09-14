-- We are providing raw data to clients in form of single table. ADMIN_BUSINESS_INSIGHTS_V and ADMIN_TRACKING_INSIGHTS_V are the views 
-- from which we export the raw data to the client. We use copy into command to store the data into S3 bucket. From S3 bucket the data 
-- will be available on the web app.

-- We have created a lookup table to specify what client and which month data should be loaded. Using that lookup table, we use cursor 
-- to loop over the lookup table and load the copy into statement for each record in lookup table.

CREATE OR REPLACE PROCEDURE VEOTS_DB.ANALYTICS.REPORTS_POPULATION_PER_MONTH()
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    c1 CURSOR FOR select CLIENT,INSIGHTS,VIEW,YEAR,MONTH from reports_lookup where ISACTIVE = ''Y'';
    stmt STRING;
BEGIN
    FOR record IN c1
    DO
        stmt := ''COPY INTO @veots_stage_reports/'' || record.INSIGHTS || ''/'' || record.CLIENT || ''/'' || record.YEAR || ''/'' || record.MONTH || ''/'' || record.CLIENT || ''_'' || record.YEAR || ''_'' || record.MONTH || ''
        FROM (SELECT * FROM '' || record.VIEW ||'' 
              WHERE "Client Id" = '''''' || record.CLIENT || ''''''
              and date_part(year, QR_GENERATION_DATE) = '' || record.YEAR || ''
             and TO_CHAR(QR_GENERATION_DATE, ''''MON'''') = '''''' || record.MONTH || ''''''
               )
            HEADER = TRUE
            single = FALSE
            overwrite = TRUE'';
        
        EXECUTE IMMEDIATE stmt;
    END FOR;
    RETURN ''Procedure executed successfully'';
END;
';



call Reports_population_per_month();
