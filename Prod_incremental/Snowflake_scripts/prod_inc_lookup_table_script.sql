create or replace table INC_LOOKUP
(sr_no int,
table_name varchar,
query varchar,
active varchar);


insert into INC_LOOKUP values(1,'ADMINS','INSERT  INTO ADMINS 
select 
 
  data : _id :: VARIANT, 
  data : blockingHistory:: ARRAY,
  --data : contactNum :: VARCHAR, 
  data : creatorId :: VARCHAR,
  data : deletionDetails :: ARRAY,
  data : createdAt :: TIMESTAMP_NTZ, 
  data : emailId :: VARCHAR, 
  data : isBlocked :: BOOLEAN, 
  data : isDeleted :: BOOLEAN, 
  data : roles :: ARRAY, 
  data : userName :: VARCHAR,
  data :userId ::VARCHAR,
  data : usersCreated :: ARRAY ,
  data : updatedAt :: TIMESTAMP_NTZ 
 
from 
  (
    SELECT 
      parse_json($1) data 
    FROM 
      @veots_stage/dbname/static_coll/admins
  );','Y');



insert into INC_LOOKUP values(2,'MASTERCLIENTS','INSERT  INTO MASTERCLIENTS
select 
data:_id::VARIANT,
data:masterClientId::VARCHAR,
data:srId::VARCHAR,
data:useVeotsSubdomain::BOOLEAN,
data:domain::TEXT,
data:batchesGenerated::ARRAY,
data:billingDate::NUMBER,
data:isBlocked::BOOLEAN,
data:isDeleted::BOOLEAN,
data:serviceStatus::TEXT,
data:subClients::ARRAY,
data:createdBy::VARIANT,
data:organizationDetails::VARIANT,
data:authPersonInfo::VARIANT,
data:paymentContactInfo::VARIANT,
data:adminAccessSubUsers::VARIANT,
data:regularAccessSubUsers::VARIANT,
data:solutionCostsDetails::VARIANT,
data:servicesCostDetails::VARIANT,
data:uiDesignCriteria::VARIANT,
data:blockingHistory::ARRAY,
data:blackListingHistory::ARRAY,
data:whitelistedHistory::ARRAY,
data:serviceStatusHistory::ARRAY,
data:createdAt::TIMESTAMP_NTZ,
data:updatedAt::TIMESTAMP_NTZ
FROM
(SELECT parse_json($1) data FROM @veots_stage/dbname/static_coll/masterclients);','Y');



insert into INC_LOOKUP values(3,'BATCHES','INSERT  INTO BATCHES

select 
data:_id::VARIANT,
data:batchType::VARCHAR,
data:createdByType::VARCHAR,
data:createdBy::VARIANT,
data:Pimage::VARCHAR ,
data:QRDetails::VARIANT,
data:QROnprod::VARCHAR,
data:expiryImage::VARCHAR,
data:addDetails::VARCHAR,
data:additionalImageDetails::VARCHAR,
data:batchNo::VARCHAR,
data:brand::VARCHAR,
data:blockingHistory::ARRAY, 
data:deletionDetails::VARIANT,
data:clientId::VARCHAR,
data:costStructure::VARIANT,
data:createdAt::TIMESTAMP_NTZ,
--data:couponApplicable::BOOLEAN,
data:dor::VARCHAR,
data:extraImages::ARRAY,
data:expiryDate::VARCHAR,

data:manuAdd::VARCHAR, 
data:manuLicenseNo::VARCHAR,
data:manuWebsiteLink::VARCHAR,
data:mfgDate::VARCHAR, 
data:mrp::NUMBER,
data:prodName::VARCHAR,
data:prodVedioLink::VARCHAR,
data:status::VARCHAR,
data:serialNo::VARCHAR,
data:tor::VARCHAR,

data:trackingQrDetails_quat::ARRAY,
--data:reasonStatusChange::VARCHAR,
data:shelfLife::NUMBER,
data:trackingQrDetails_tert::ARRAY,
--data:statusChangeDate::TIMESTAMP_NTZ, 
--data:serviceStatus ::VARCHAR,
data:type::NUMBER,
data:updatedAt::TIMESTAMP_NTZ,
data:warrantyApp::BOOLEAN,
data:warrantyPeriod::NUMBER,
data:warrantyTC::VARCHAR
from
(SELECT parse_json($1) data FROM @veots_stage/dbname/static_coll/batches);','Y');



insert into INC_LOOKUP values(4,'PRODUCTS','INSERT  INTO PRODUCTS
SELECT 
	DATA:_id::VARIANT ,
    DATA:masterClient::VARIANT,
	DATA:batchDetails::VARIANT,
	DATA:billCopy::VARCHAR,
	DATA:customerId::VARIANT,
	DATA:fakeDetails::ARRAY,
	DATA:postSaleCode::VARCHAR,
	DATA:preSaleCode :: VARCHAR,
	data:prodReachedCus::BOOLEAN,
	DATA:purChaseDate::TIMESTAMP_NTZ,
	DATA:purchaserContact::VARCHAR,
	DATA:createdAt::TIMESTAMP_NTZ,
	DATA:updatedAt::TIMESTAMP_NTZ,
	DATA:rating::VARCHAR,
	DATA:isWarrantyConsentGiven ::BOOLEAN,
	DATA:mfgDate ::TIMESTAMP_NTZ,
	DATA:expiryDate ::TIMESTAMP_NTZ,
	DATA:feedback ::VARCHAR
from
(SELECT parse_json($1) data FROM @veots_stage/dbname/static_coll/products);','Y');

insert into INC_LOOKUP values(5,'SUBCLIENTS','INSERT  INTO SUBCLIENTS
SELECT 
DATA:_id ::VARIANT,
DATA:blockingHistory::ARRAY,
DATA:createdByType::VARCHAR,
DATA:createdBy::VARIANT,
--DATA:creatorId::VARCHAR,
DATA:deletionDetails::ARRAY,
DATA:emailId::VARCHAR,
DATA:isBlocked::BOOLEAN,
DATA:isDeleted::BOOLEAN,
DATA:masterClient::VARIANT,
DATA:roles::ARRAY,
DATA:userId::VARCHAR,
DATA:userName::VARCHAR,
DATA:createdAt::TIMESTAMP_NTZ,
DATA:updatedAt::TIMESTAMP_NTZ

from
(SELECT parse_json($1) data FROM @veots_stage/dbname/static_coll/subclients);','Y');

insert into INC_LOOKUP values(6,'ONETIMECLIENTS','INSERT  INTO OneTimeClients


SELECT 
DATA:_id::VARIANT ,
DATA:address::VARCHAR,
DATA:OTClientId::NUMBER ,
DATA:clientName::VARCHAR,
DATA:contactNum:: VARCHAR,
DATA:emailAddress::VARCHAR,
--DATA:invoiceDate::VARCHAR,
DATA:placeOfService::VARCHAR,
DATA:taxId::VARCHAR,
DATA:createdAt::TIMESTAMP_NTZ,
DATA:updatedAt::TIMESTAMP_NTZ

from
(SELECT parse_json($1) data FROM @veots_stage/dbname/static_coll/onetimeclients);','Y');

insert into INC_LOOKUP values(7,'CUSTOMERS','INSERT  INTO customers


SELECT 
DATA:_id::VARIANT ,
DATA:customerUdid::VARCHAR,
--DATA:country::VARCHAR ,
DATA:companyName::VARCHAR,
DATA:dob:: TIMESTAMP_NTZ,
DATA:gender::VARCHAR,
DATA:email::VARCHAR,
DATA:status::VARCHAR,
DATA:userName::VARCHAR,
DATA:createdAt::TIMESTAMP_NTZ,
DATA:updatedAt::TIMESTAMP_NTZ,
DATA:mobileNoDetails:intelMobileNo :: VARCHAR,
CASE   WHEN DATEDIFF(YEAR,DATA:dob:: TIMESTAMP_NTZ,CURRENT_DATE()) >=60 then ''60+'' 
       WHEN DATEDIFF(YEAR,DATA:dob:: TIMESTAMP_NTZ,CURRENT_DATE()) >=40 then ''40-60''
       WHEN DATEDIFF(YEAR,DATA:dob:: TIMESTAMP_NTZ,CURRENT_DATE()) >=25 then ''25-40''
       when DATEDIFF(YEAR,DATA:dob:: TIMESTAMP_NTZ,CURRENT_DATE()) >=18 then ''18-25''
     ELSE NULL END :: VARCHAR AGE_GROUP
	 

from
(SELECT parse_json($1) data FROM @veots_stage/dbname/static_coll/customers);','Y');


insert into INC_LOOKUP values(8,'TYPEONECOUPONSETS','insert  into typeonecouponsets

select 
data:_id::object,
data:masterClient::object,
data:couponLogoLink::VARCHAR,
data:couponHeader::VARCHAR,
data:couponText::VARCHAR,
data:distributionType::VARCHAR,
data:distributionRatio::VARCHAR,
data:expiryDate::TIMESTAMP_NTZ,
data:offerDetails::VARCHAR,
data:remainingCoupons::NUMBER,
data:termsAndConditions::VARCHAR,
data:totalCouponCount::NUMBER,
data:isDeleted::BOOLEAN,
data:createdAt::TIMESTAMP_NTZ,
data:updatedAt::TIMESTAMP_NTZ
from (
select parse_json($1) data from @veots_stage/dbname/static_coll/typeonecouponsets/
);','Y');


insert into INC_LOOKUP values(9,'TYPEONECOUPONCODES','insert  into typeonecouponcodes
select 
    data:_id::VARIANT,
    data:assignedTo::VARIANT,
    data:couponCode::VARCHAR,
    data:deliveredAt::TIMESTAMP_NTZ,
    data:isAssigned::BOOLEAN,
    data:isScratched::BOOLEAN,
    data:productId::VARIANT,
    data:setId::VARIANT,
    data:createdAt::TIMESTAMP_NTZ,
    data:updatedAt::TIMESTAMP_NTZ   
    from (
select parse_json($1) data from @veots_stage/dbname/static_coll/typeonecouponcodes/);','Y');


insert into INC_LOOKUP values(10,'DORS','INSERT  INTO DORS 
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
      parse_json($1) data,SPLIT(METADATA$FILENAME,''/'')[2]::VARCHAR  CLIENT_ID
    FROM 
      @veots_stage/dbname/dors 
  );','Y');

insert into INC_LOOKUP values(11,'TORS','INSERT  INTO TORS 
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
      parse_json($1) data,SPLIT(METADATA$FILENAME,''/'')[2]::VARCHAR  CLIENT_ID
    FROM 
      @veots_stage/dbname/tors 
  );
','Y');

  
  insert into INC_LOOKUP values(12,'VECTORTABLE4_1','INSERT  INTO VECTORTABLE4_1
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
      parse_json($1) data,SPLIT(METADATA$FILENAME,''/'')[2]::VARCHAR  CLIENT_ID
    FROM 
      @veots_stage/dbname/vectortable4_1/ 
  );','Y');

  
insert into INC_LOOKUP values(13,'VECTORTABLE4_2','INSERT  INTO VECTORTABLE4_2
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
      parse_json($1) data,SPLIT(METADATA$FILENAME,''/'')[2]::VARCHAR  CLIENT_ID
    FROM 
      @veots_stage/dbname/vectortable4_2/ 
  );','Y');

insert into INC_LOOKUP values(14,'VECTORTABLE6','INSERT  INTO VECTORTABLE6
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
      parse_json($1) data,SPLIT(METADATA$FILENAME,''/'')[2]::VARCHAR  CLIENT_ID
    FROM 
      @veots_stage/dbname/vectortable6/
  );','Y');

insert into INC_LOOKUP values(15,'VECTORTABLE7','INSERT  INTO VECTORTABLE7
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
      parse_json($1) data,SPLIT(METADATA$FILENAME,''/'')[2]::VARCHAR  CLIENT_ID
    FROM 
      @veots_stage/dbname/vectortable7/
  );','Y');


insert into INC_LOOKUP values(16,'VECTORTABLE8','INSERT  INTO VECTORTABLE8
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
      parse_json($1) data,SPLIT(METADATA$FILENAME,''/'')[2]::VARCHAR  CLIENT_ID
    FROM 
      @veots_stage/dbname/vectortable8/
  );','Y');

  insert into INC_LOOKUP values(17,'VECTORTABLE9','INSERT  INTO VECTORTABLE9
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
      parse_json($1) data,SPLIT(METADATA$FILENAME,''/'')[2]::VARCHAR  CLIENT_ID
    FROM 
      @veots_stage/dbname/vectortable9/
  );','Y');
