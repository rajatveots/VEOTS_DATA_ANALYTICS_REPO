-------------------DYNAMIC TABLES 

---------------DORS-----------------

--Tables are created and populated using the Insert into statement.
-- Each table has its corresponding view. Additionally we have made views To store the data from the array type of the table

-- The row_number() in the view is used to select the latest record according to UPDATEDAT date when we have records with duplicate objectIds

-- records with duplicate objectId come when the record is updated in the source(MongoDB) and we have a new record for the updated record


create or replace TABLE DORS (
	_ID OBJECT,
	DOR VARCHAR(16777216),
	CREATEDAT TIMESTAMP_NTZ(9),
	INDEX NUMBER(38,0),
	UPDATEDAT TIMESTAMP_NTZ(9),
	VECTOR3PTR NUMBER(38,0),
	CLIENT_ID VARCHAR(16777216)
);



INSERT INTO DORS 
select 
  data : _id :: VARIANT _id,
  data : DOR :: VARCHAR DOR,
  data : createdAt :: TIMESTAMP_NTZ createdAt, 
  data : index :: NUMBER index,
  data : updatedAt :: TIMESTAMP_NTZ updatedAt,
  data : vector3ptr :: NUMBER vector3ptr,
  CLIENT_ID 
from 
  (
    SELECT 
      parse_json($1) data,SPLIT(METADATA$FILENAME,'/')[2]::VARCHAR  CLIENT_ID
    FROM 
      @veots_stage/prod-veots/dors 
  );



CREATE OR REPLACE VIEW DORS_V AS
SELECT 
_ID:oid::VARCHAR OID ,
DOR ,
CREATEDAT , 
INDEX ,
UPDATEDAT , 
VECTOR3PTR ,
SPLIT(CLIENT_ID,'_')[0]::VARCHAR CLIENT_ID 
FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM DORS)
WHERE ISACTIVE = 1;   ---39

----------------------TORS---------------------------


create or replace TABLE TORS (
    _id OBJECT,
	TOG VARCHAR(16777216),
	createdAt TIMESTAMP_NTZ(9),
	index NUMBER,
	postSalePtr ARRAY,
	preSaleptr ARRAY,
	updatedAt TIMESTAMP_NTZ(9),
	client_id VARCHAR
);



INSERT INTO TORS 
select 
  
  
  data : _id :: VARIANT _id, 
  data : TOG :: VARCHAR TOG,
  data : createdAt :: TIMESTAMP_NTZ createdAt, 
  data : index :: NUMBER index,
  data : preSaleptr :: VARIANT preSaleptr,
  data : postSalePtr :: VARIANT postSalePtr,
  data : updatedAt :: TIMESTAMP_NTZ updatedAt,
  CLIENT_ID 
from 
  (
    SELECT 
      parse_json($1) data,SPLIT(METADATA$FILENAME,'/')[2]::VARCHAR  CLIENT_ID
    FROM 
      @veots_stage/prod-veots/tors 
  );
  

      
  
CREATE OR REPLACE VIEW TORS_V AS
SELECT 
    _id:oid::varchar OID ,
	TOG ,
	createdAt ,
	index ,
	updatedAt ,
	SPLIT(CLIENT_ID,'_')[0]::VARCHAR CLIENT_ID 
FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY _id:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM TORS)
WHERE ISACTIVE = 1;


CREATE OR REPLACE VIEW TORS_POSTSALEPTR_V AS
SELECT T.oid tors_oid,
P.INDEX,
P.VALUE:_id:oid::VARCHAR OID,
P.value:start::NUMBER ps_start,
P.VALUE:end::NUMBER ps_end 
  
FROM
(SELECT _ID:oid :: varchar oid , POSTSALEPTR, 
ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE  FROM TORS )T,
LATERAL FLATTEN (T.POSTSALEPTR) P 
WHERE T.ISACTIVE = 1;


CREATE OR REPLACE VIEW TORS_PRESALEPTR_V AS
SELECT T.oid tors_oid,
P.INDEX,
P.VALUE:_id:oid::VARCHAR OID,
P.value:start::NUMBER ps_start,
P.VALUE:end::NUMBER ps_end 
  
FROM
(SELECT _ID:oid :: varchar oid , preSaleptr, 
ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE  FROM TORS )T,
LATERAL FLATTEN (T.preSaleptr) P 
WHERE T.ISACTIVE = 1;



-----------------------------Vectortable4_1 ----------------------------

create or replace TABLE VECTORTABLE4_1 (
	_ID OBJECT,
	COORDINATES ARRAY,
	DIRTYBITPRESALE VARCHAR(16777216),
	INDEX NUMBER(38,0),
	RANDOMNUMPRESALE VARCHAR(16777216),
	CREATEDAT TIMESTAMP_NTZ(9),
	UDIDPRESALE VARCHAR(16777216),
	UPDATEDAT TIMESTAMP_NTZ(9),
	CLIENT_ID VARCHAR(16777216)
);

INSERT  INTO VECTORTABLE4_1
select 
  data : _id :: VARIANT _id,
  data : coordinates COORDINATES,
  data : dirtyBitPreSale :: VARCHAR dirtyBitPostSale,
  data : index :: NUMBER index,
  data :randomNumPreSale :: VARCHAR randomNumPostSale, 
  data :createdAt   createdAt,
  data : udidPreSale :: VARCHAR UDIDPostSALE, 
  data : updatedAt :: TIMESTAMP_NTZ  updatedAt,
  CLIENT_ID 
from 
  (
    SELECT 
      parse_json($1) data,SPLIT(METADATA$FILENAME,'/')[2]::VARCHAR  CLIENT_ID
    FROM 
      @veots_stage/prod-veots/vectortable4_1/ 
  );  


  

CREATE OR REPLACE VIEW VECTORTABLE4_1_V AS
SELECT _ID:oid::VARCHAR oid , 
DIRTYBITPRESALE,
INDEX,
RANDOMNUMPRESALE,
CREATEDAT ,
UDIDPRESALE,
UPDATEDAT,
SPLIT(CLIENT_ID,'_')[0]::VARCHAR CLIENT_ID 
FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM VECTORTABLE4_1)
WHERE ISACTIVE = 1;


CREATE OR REPLACE VIEW 	VECTORTABLE4_1_COORDINATES_V AS
SELECT V.VECTORTABLE4_1_OID, C.INDEX,C.VALUE:_id:oid::VARCHAR OID , 
    C.VALUE:scanType::NUMBER scanType ,
    C.VALUE:date::TIMESTAMP_NTZ date,
    C.VALUE:reason::VARCHAR reason ,
    C.VALUE:establishmentName::VARCHAR establishmentName,
    C.VALUE:loc[0]::FLOAT LAT ,
    C.value:loc[1]::FLOAT LONG 
    ,
    C.value:address:county::VARCHAR city,
    C.value:address:state::VARCHAR state,
    C.value:address:country::VARCHAR country FROM
(SELECT _ID:oid::VARCHAR VECTORTABLE4_1_OID , COORDINATES,
ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM VECTORTABLE4_1) V,
LATERAL FLATTEN (COORDINATES) C 
WHERE V.ISACTIVE = 1;

----------------------VECTORTABLE4_2-----------------------------------------



create or replace TABLE VECTORTABLE4_2 (
	
	_id OBJECT,
	coordinates ARRAY,
	dirtyBitPostSale VARCHAR(16777216),
	index NUMBER,
    randomNumPostSale VARCHAR,
	createdAt TIMESTAMP_NTZ,
	udidPostSale VARCHAR(16777216),
	updatedAt TIMESTAMP_NTZ(9),
	client_id VARCHAR
);



INSERT  INTO VECTORTABLE4_2
select 
    
  data : _id :: VARIANT _id,
  data : coordinates COORDINATES,
  data : dirtyBitPostSale :: VARCHAR dirtyBitPostSale,
  data : index :: NUMBER index,
  data :randomNumPostSale :: VARCHAR randomNumPostSale, 
  data : createdAt :: TIMESTAMP_NTZ  createdAt,
  data : udidPostSale :: VARCHAR UDIDPostSALE, 
  data : updatedAt :: TIMESTAMP_NTZ  updatedAt,
  CLIENT_ID 
from 
  (
    SELECT 
      parse_json($1) data,SPLIT(METADATA$FILENAME,'/')[2]::VARCHAR  CLIENT_ID
    FROM 
      @veots_stage/prod-veots/vectortable4_2/ 
  );

  

CREATE OR REPLACE VIEW VECTORTABLE4_2_V AS
SELECT _ID:oid:: VARCHAR OID,
        DIRTYBITPOSTSALE,
        INDEX,
        RANDOMNUMPOSTSALE,
        CREATEDAT,
        UDIDPOSTSALE,
        UPDATEDAT,
        SPLIT(CLIENT_ID,'_')[0]::VARCHAR CLIENT_ID
FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM VECTORTABLE4_2)
WHERE ISACTIVE = 1;


CREATE OR REPLACE VIEW VECTORTABLE4_2_COORDINATES_V	 AS
SELECT V.VECTORTABLE4_2_OID, C.INDEX,C.VALUE:_id:oid::VARCHAR OID , 
    C.VALUE:scanType::NUMBER scanType ,
    C.VALUE:date::TIMESTAMP_NTZ date,
    C.VALUE:reason::VARCHAR reason ,
    C.VALUE:establishmentName::VARCHAR establishmentName,
    C.VALUE:loc[0]::FLOAT LAT ,
    C.value:loc[1]::FLOAT LONG,
    
    C.value:address:county::VARCHAR city,
    C.value:address:state::VARCHAR state,
    C.value:address:country::VARCHAR country
    FROM
(SELECT _ID:oid::VARCHAR VECTORTABLE4_2_OID , COORDINATES, 
ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM VECTORTABLE4_2) V,
LATERAL FLATTEN (COORDINATES) C 
WHERE V.ISACTIVE = 1;


-----------------vectorTable6 ----------------


create or replace TABLE VECTORTABLE6 (
	
	_id OBJECT,
	coordinates ARRAY,
	qrId VARCHAR(16777216),
	createdAt TIMESTAMP_NTZ(9),
	updatedAt TIMESTAMP_NTZ(9),
	client_id VARCHAR
);

INSERT  INTO VECTORTABLE6
select 
  data : _id :: VARIANT _id,
  data : coordinates :: VARIANT COORDINATES,
  data : qrId :: VARCHAR qrId,
  data : createdAt :: TIMESTAMP_NTZ  createdAt,
 
  data : updatedAt :: TIMESTAMP_NTZ  updatedAt,
  CLIENT_ID 
from 
  (
    SELECT 
      parse_json($1) data,SPLIT(METADATA$FILENAME,'/')[2]::VARCHAR  CLIENT_ID
    FROM 
      @veots_stage/prod-veots/vectortable6/
  );



CREATE OR REPLACE VIEW VECTORTABLE6_V AS
SELECT _id:oid::VARCHAR OID,
       QRID,
       CREATEDAT,
       UPDATEDAT,
       SPLIT(CLIENT_ID,'_')[0]::VARCHAR CLIENT_ID
       FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY _id:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM VECTORTABLE6)
       WHERE ISACTIVE = 1;
       
	   
	   
	   

CREATE OR REPLACE VIEW VECTORTABLE6_COORDINATES_V	 AS
SELECT V.VECTORTABLE6_OID, C.INDEX,C.VALUE:_id:oid::VARCHAR OID , 
    C.VALUE:scanType::NUMBER scanType ,
    C.VALUE:date::TIMESTAMP_NTZ date,
    C.VALUE:establishmentName::VARCHAR establishmentName,
    C.VALUE:loc[0]::FLOAT LAT ,
    C.value:loc[1]::FLOAT LONG,
    C.value:address:county::VARCHAR city,
    C.value:address:state::VARCHAR state,
    C.value:address:country::VARCHAR country
    FROM
(SELECT _ID:oid::VARCHAR VECTORTABLE6_OID , COORDINATES,
ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM VECTORTABLE6) V,
LATERAL FLATTEN (COORDINATES) C 
WHERE V.ISACTIVE = 1;



------------VECTORTABLE7----------------------------


create or replace TABLE VECTORTABLE7 (
	_id OBJECT,
	qrId VARCHAR(16777216),
    dirtyBit BOOLEAN,
    udid VARCHAR,
    coordinates ARRAY,
    createdAt TIMESTAMP_NTZ,
    updatedAt TIMESTAMP_NTZ,
	client_id VARCHAR
);


INSERT  INTO VECTORTABLE7
select 
    
  data : _id :: VARIANT _id,
    data :qrId:: VARCHAR(16777216),
    data :dirtyBit:: BOOLEAN,
    data :udid ::VARCHAR,
    data :coordinates:: ARRAY,
    data :createdAt ::TIMESTAMP_NTZ,
    data :updatedAt ::TIMESTAMP_NTZ,
	
  CLIENT_ID 
from 
  (
    SELECT 
      parse_json($1) data,SPLIT(METADATA$FILENAME,'/')[2]::VARCHAR  CLIENT_ID
    FROM 
      @veots_stage/prod-veots/vectortable7/
  );


CREATE OR REPLACE VIEW VECTORTABLE7_V AS
SELECT _ID:oid::VARCHAR OID ,
        QRID,
        DIRTYBIT,
        UDID,
        CREATEDAT,
        UPDATEDAT,
        SPLIT(CLIENT_ID,'_')[0]::VARCHAR CLIENT_ID
        FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM VECTORTABLE7)
        WHERE ISACTIVE = 1;


CREATE OR REPLACE VIEW VECTORTABLE7_COORDINATES_V	 AS
SELECT V.VECTORTABLE7_OID, C.INDEX,C.VALUE:_id:oid::VARCHAR OID , 
    C.VALUE:scanType::NUMBER scanType ,
    C.VALUE:date::TIMESTAMP_NTZ date,
    C.VALUE:establishmentName::VARCHAR establishmentName,
    C.VALUE:reason::VARCHAR reason,
    C.VALUE:loc[0]::FLOAT LAT ,
    C.value:loc[1]::FLOAT LONG,
    C.value:address:county::VARCHAR city,
    C.value:address:state::VARCHAR state,
    C.value:address:country::VARCHAR country
    
    FROM
(SELECT _ID:oid::VARCHAR VECTORTABLE7_OID , COORDINATES, 
ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM VECTORTABLE7) V,
LATERAL FLATTEN (COORDINATES) C 
WHERE V.ISACTIVE = 1; 



-----------------------VECTORTABLE8-----------------------



create or replace TABLE VECTORTABLE8 (
	_id OBJECT,
	createdAt TIMESTAMP_NTZ(9),
	qrId VARCHAR(16777216),
	scannerDetails ARRAY,
	updatedAt TIMESTAMP_NTZ(9),
	client_id VARCHAR
);




INSERT  INTO VECTORTABLE8
select 
  data : _id :: VARIANT _id,
  data : createdAt :: TIMESTAMP_NTZ  createdAt,
  data : qrId :: VARCHAR qrId,
  data : coordinates :: VARIANT scannerDetails,
  data : updatedAt :: TIMESTAMP_NTZ  updatedAt,
  CLIENT_ID 
from 
  (
    SELECT 
      parse_json($1) data,SPLIT(METADATA$FILENAME,'/')[2]::VARCHAR  CLIENT_ID
    FROM 
      @veots_stage/prod-veots/vectortable8/
  );

CREATE OR REPLACE VIEW vectortable8_V AS
SELECT _id:oid::VARCHAR OID,
       QRID,
       CREATEDAT,
       UPDATEDAT,
       SPLIT(CLIENT_ID,'_')[0]::VARCHAR CLIENT_ID
FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY _id:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM vectortable8)
WHERE ISACTIVE = 1;


CREATE OR REPLACE VIEW VECTORTABLE8_SCANNERDETAILS_V AS
SELECT V.VECTORTABLE8_OID , S.index,
S.VALUE:_id:oid::VARCHAR OID,
S.VALUE:contact::string contact,
S.value:date::TIMESTAMP_NTZ date,
S.VALUE:establishmentName::VARCHAR establishmentName,
S.value:loc[0]::float lat,
S.value:loc[1]::float long,
S.value:address:county::VARCHAR city,
S.value:address:state::VARCHAR state,
S.value:address:country::VARCHAR country
    
FROM
(SELECT _ID:oid::VARCHAR VECTORTABLE8_OID , SCANNERDETAILS,
ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM VECTORTABLE8) V,
LATERAL FLATTEN(SCANNERDETAILS) S
WHERE V.ISACTIVE = 1;


----------------VECTORTABLE9-------------------




create or replace TABLE VECTORTABLE9 (
	_id OBJECT,
	createdAt TIMESTAMP_NTZ(9),
	qrId VARCHAR(16777216),
	scannerDetails ARRAY,
	updatedAt TIMESTAMP_NTZ(9),
	client_id VARCHAR
);




INSERT  INTO VECTORTABLE9
select 
  data : _id :: VARIANT _id,
  data : createdAt :: TIMESTAMP_NTZ  createdAt,
  data : qrId :: VARCHAR qrId,
  data : coordinates :: VARIANT scannerDetails,
  data : updatedAt :: TIMESTAMP_NTZ  updatedAt,
  CLIENT_ID 
from 
  (
    SELECT 
      parse_json($1) data,SPLIT(METADATA$FILENAME,'/')[2]::VARCHAR  CLIENT_ID
    FROM 
      @veots_stage/prod-veots/vectortable9/
  );

CREATE OR REPLACE VIEW vectortable9_V AS
SELECT _id:oid::VARCHAR OID,
       QRID,
       CREATEDAT,
       UPDATEDAT,
       SPLIT(CLIENT_ID,'_')[0]::VARCHAR CLIENT_ID
FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY _id:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM vectortable9) 
WHERE ISACTIVE = 1;


CREATE OR REPLACE VIEW VECTORTABLE9_SCANNERDETAILS_V AS
SELECT V.VECTORTABLE9_OID , S.index,
S.VALUE:_id:oid::VARCHAR OID,
S.VALUE:contact::string contact,
S.value:date::TIMESTAMP_NTZ date,
S.VALUE:establishmentName::VARCHAR establishmentName,
S.value:loc[0]::float lat,
S.value:loc[1]::float long,
S.value:address:county::VARCHAR city,
S.value:address:state::VARCHAR state,
S.value:address:country::VARCHAR country
FROM
(SELECT _ID:oid::VARCHAR VECTORTABLE9_OID , SCANNERDETAILS,
ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM VECTORTABLE9) V,
LATERAL FLATTEN(SCANNERDETAILS) S
WHERE V.ISACTIVE = 1;