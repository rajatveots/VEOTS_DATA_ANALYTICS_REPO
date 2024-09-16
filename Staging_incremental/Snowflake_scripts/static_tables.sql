-----------------STATIC TABLES SCRIPT----------------------

-------------ADMINS-------------

--Tables are created and populated using the Insert into statement.
-- Each table has its corresponding view. Additionally we have made views To store the data from the array type of the table

-- The row_number() in the view is used to select the latest record according to UPDATEDAT date when we have records with duplicate objectIds

-- records with duplicate objectId come when the record is updated in the source(MongoDB) and we have a new record for the updated record




create or replace TABLE ADMINS (	
	_id OBJECT,
    blockingHistory ARRAY,
	creatorId VARCHAR(16777216),
    deletionDetails ARRAY,
    createdAt TIMESTAMP_NTZ(9),
	emailId VARCHAR(16777216),
    isBlocked BOOLEAN,
	isDeleted BOOLEAN,
	roles ARRAY,
    userName VARCHAR,
    userId VARCHAR,
    usersCreated ARRAY,
	updatedAt TIMESTAMP_NTZ(9));


INSERT  INTO ADMINS 
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
      @veots_stage/staging-veots/static_coll/admins
  );



CREATE OR REPLACE VIEW ADMINS_V AS
SELECT _ID:oid::VARCHAR OID,
    --   CONTACTNUM,
       CREATORID,
       EMAILID,
       CREATEDAT,
       ISBLOCKED,
       ISDELETED,
       UPDATEDAT
       FROM (SELECT * , ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE
       FROM ADMINS)
       WHERE ISACTIVE = 1;

       
CREATE OR REPLACE  VIEW ADMINS_ROLES_V AS
SELECT A.ADMIN_OID ,
	   R.INDEX , 
	   R.VALUE::VARCHAR ROLE 
	 FROM
(SELECT _ID:oid::VARCHAR ADMIN_OID ,
ROLES, ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM ADMINS ) A,
LATERAL FLATTEN (A.ROLES) R 
WHERE A.ISACTIVE = 1;	   


CREATE OR REPLACE  VIEW ADMINS_USERSCREATED_V AS
SELECT A.ADMIN_OID ,
	   R.INDEX , 
	   R.VALUE::VARCHAR USERSCREATED 
	 FROM
(SELECT _ID:oid::VARCHAR ADMIN_OID ,
USERSCREATED ,ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE  FROM ADMINS ) A,
LATERAL FLATTEN (A.USERSCREATED) R 
WHERE A.ISACTIVE = 1;

CREATE OR REPLACE  VIEW ADMINS_BLOCKINGHISTORY_V AS
SELECT A.ADMIN_OID ,
	   R.INDEX , 
	   R.VALUE:actionType::VARCHAR actionType,
       R.VALUE:date:: TIMESTAMP_NTZ DATE,
       R.VALUE:performedBy:oid:: VARCHAR performedby_oid,
       R.VALUE:reason::VARCHAR reason
	 FROM
(SELECT _ID:oid::VARCHAR ADMIN_OID ,
BLOCKINGHISTORY, ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE  FROM ADMINS ) A,
LATERAL FLATTEN (A.BLOCKINGHISTORY) R 
WHERE A.ISACTIVE = 1;	

CREATE OR REPLACE  VIEW ADMINS_deletionDetails_V AS
SELECT A.ADMIN_OID ,
	   R.INDEX , 
      R.VALUE:date:: TIMESTAMP_NTZ DATE,
      R.VALUE:deletedBy:oid:: VARCHAR performedby_oid,
      R.VALUE:reason::VARCHAR reason
	 FROM
(SELECT _ID:oid::VARCHAR ADMIN_OID ,
deletionDetails,ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM ADMINS ) A,
LATERAL FLATTEN (A.deletionDetails) R 
WHERE A.ISACTIVE = 1;

 
	

--------------------------masterClient----------------------------------


--------------------------------------------------------------------------------------------------------
------------------------------------------Masterclients New schema---------------------------------------
---------------------------------------------------------------------------------------------------------

create or replace table MASTERCLIENTS(
_id OBJECT,
masterClientId VARCHAR,
srId VARCHAR,
useVeotsSubdomain BOOLEAN,
domain TEXT,
batchesGenerated ARRAY,
billingDate NUMBER,
isBlocked BOOLEAN,
isDeleted BOOLEAN,
serviceStatus TEXT,
subClients ARRAY,
createdBy OBJECT,
organizationDetails OBJECT,
authPersonInfo OBJECT,
paymentContactInfo OBJECT,
adminAccessSubUsers OBJECT,
regularAccessSubUsers OBJECT,
solutionCostsDetails OBJECT,
servicesCostDetails OBJECT,
uiDesignCriteria OBJECT,
blockingHistory ARRAY,
blackListingHistory ARRAY,
whitelistedHistory ARRAY,
serviceStatusHistory ARRAY,
createdAt TIMESTAMP_NTZ(9),
updatedAt TIMESTAMP_NTZ(9)
)

----------------
INSERT  INTO MASTERCLIENTS
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
(SELECT parse_json($1) data FROM @veots_stage/staging-veots/static_coll/masterclients);


CREATE OR REPLACE VIEW MASTERCLIENTS_BATCHESGENERATED_V AS
SELECT
MASTERCLIENT_OID,
B.INDEX INDEX,
B.VALUE:oid ::VARCHAR BATCHESGENERATED_OID
FROM
(
SELECT _ID:oid::VARCHAR MASTERCLIENT_OID, BATCHESGENERATED,
ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM MASTERCLIENTS)M,
LATERAL FLATTEN (BATCHESGENERATED) B
WHERE M.ISACTIVE = 1;


CREATE OR REPLACE VIEW MASTERCLIENTS_SUBCLIENTS_V AS
SELECT
MASTERCLIENT_OID,
B.INDEX INDEX,
B.VALUE:oid ::VARCHAR SUBCLIENTS_OID
FROM
(
SELECT _ID:oid::VARCHAR MASTERCLIENT_OID, SUBCLIENTS,
ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM MASTERCLIENTS)M,
LATERAL FLATTEN (SUBCLIENTS) B
WHERE M.ISACTIVE = 1;


CREATE OR REPLACE VIEW MASTERCLIENTS_BLOCKINGHISTORY_V AS
SELECT
MASTERCLIENT_OID,
R.INDEX , 
	   R.VALUE:actionType::VARCHAR actionType,
       R.VALUE:date:: TIMESTAMP_NTZ DATE,
       R.VALUE:performedBy:oid:: VARCHAR performedby_oid,
       R.VALUE:reason::VARCHAR reason
	 
FROM
(
SELECT _ID:oid::VARCHAR MASTERCLIENT_OID, BLOCKINGHISTORY,
ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM MASTERCLIENTS)M,
LATERAL FLATTEN (BLOCKINGHISTORY) R
WHERE M.ISACTIVE = 1;


create or replace view MASTERCLIENTS_organizationDetails_V
AS
Select 
_ID:oid::VARCHAR MASTERCLIENT_OID,
organizationDetails:organizationName::VARCHAR organizationName,
organizationDetails:enterpriseType::VARCHAR enterpriseType,
organizationDetails:addressLine1::VARCHAR addressLine1,
organizationDetails:addressLine2::VARCHAR addressLine2,
organizationDetails:country::VARCHAR country,
organizationDetails:state::VARCHAR state,
organizationDetails:city::VARCHAR city,
organizationDetails:postalCode::NUMBER postalCode,
organizationDetails:entityType::VARCHAR entityType,
organizationDetails:categoryOfOrganization::VARCHAR categoryOfOrganization,
organizationDetails:taxId::VARCHAR taxId,
organizationDetails:businessId::VARCHAR businessId,
organizationDetails:monthlyProductionQuantity::NUMBER monthlyProductionQuantity,
organizationDetails:numberOfSKUs::NUMBER numberOfSKUs,
organizationDetails:serviceType::ARRAY serviceType,
organizationDetails:dunsNumber::VARCHAR dunsNumber,
organizationDetails:companyWebsite::VARCHAR companyWebsite,
organizationDetails:companyDocOptions::VARCHAR companyDocOptions,
organizationDetails:companyDoc::VARCHAR companyDoc
from (SELECT * , ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM masterclients)
WHERE ISACTIVE = 1;




create or replace view masterclients_authPersonInfo_V
as
select 
_ID:oid::VARCHAR MASTERCLIENT_OID,
authPersonInfo:firstName::VARCHAR firstName,
authPersonInfo:lastName::VARCHAR lastName,
authPersonInfo:designation::VARCHAR designation,
authPersonInfo:emailId::VARCHAR emailId,
authPersonInfo:mobileNoDetails:intelMobileNo::VARCHAR MobileNo,
authPersonInfo:auth_Authorization_Doc::VARCHAR auth_Authorization_Doc,
authPersonInfo:auth_Proof_Of_Identity_Doc_Options::VARCHAR auth_Proof_Of_Identity_Doc_Options,
authPersonInfo:auth_Proof_Of_Identity_Doc::VARCHAR auth_Proof_Of_Identity_Doc
from
(SELECT * , ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM masterclients)
WHERE ISACTIVE = 1;


create or replace view masterclients_paymentContactInfo_V
as
select 
_ID:oid::VARCHAR MASTERCLIENT_OID,
paymentContactInfo:firstName::VARCHAR firstName,
paymentContactInfo:lastName::VARCHAR lastName,
paymentContactInfo:designation::VARCHAR designation,
paymentContactInfo:emailId::VARCHAR emailId,
paymentContactInfo:mobileNoDetails:intelMobileNo::VARCHAR MobileNo,
paymentContactInfo:payment_Authorization_Doc::VARCHAR payment_Authorization_Doc,
paymentContactInfo:payment_Proof_Of_Identity_Doc_Options::VARCHAR payment_Proof_Of_Identity_Doc_Options,
paymentContactInfo:payment_Proof_Of_Identity_Doc::VARCHAR payment_Proof_Of_Identity_Doc
from
(SELECT * , ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM masterclients)
WHERE ISACTIVE = 1;



create or replace view masterclients_uiDesignCriteria_V
as
select 
_ID:oid::VARCHAR MASTERCLIENT_OID,
uiDesignCriteria:clientLogo::VARCHAR clientLogo,
uiDesignCriteria:primaryColor::VARCHAR primaryColor,
uiDesignCriteria:secondaryColor::VARCHAR secondaryColor,
uiDesignCriteria:gradientAngle::VARCHAR gradientAngle,
uiDesignCriteria:backgroundColor::VARCHAR backgroundColor,
uiDesignCriteria:modeOfService::VARCHAR modeOfService,
uiDesignCriteria:isfooterIncluded::VARCHAR isfooterIncluded,
uiDesignCriteria:qrScanAccessLevel::ARRAY qrScanAccessLevel,
uiDesignCriteria:comments::VARCHAR comments
from
(SELECT * , ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM masterclients)
WHERE ISACTIVE = 1;



create or replace view masterclients_adminAccessSubUsers_V
as
select 
_ID:oid::VARCHAR MASTERCLIENT_OID,
adminAccessSubUsers:userName1::VARCHAR userName1,
adminAccessSubUsers:emailId1::VARCHAR emailId1,
adminAccessSubUsers:userName2::VARCHAR userName2,
adminAccessSubUsers:emailId2::VARCHAR emailId2
from
(SELECT * , ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM masterclients)
WHERE ISACTIVE = 1;


create or replace view masterclients_regularAccessSubUsers_V
as
select 
_ID:oid::VARCHAR MASTERCLIENT_OID,
regularAccessSubUsers:userName1::VARCHAR userName1,
regularAccessSubUsers:emailId1::VARCHAR emailId1,
regularAccessSubUsers:userName2::VARCHAR userName2,
regularAccessSubUsers:emailId2::VARCHAR emailId2
from
(SELECT * , ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM masterclients)
WHERE ISACTIVE = 1;



Create or replace view masterclients_v
AS
select 
_id:oid::VARCHAR OID,
masterClientId,
organizationDetails:organizationName::VARCHAR CLIENTNAME,
organizationDetails:addressLine1::VARCHAR || ', ' || organizationDetails:addressLine2::VARCHAR 
|| ', ' || organizationDetails:city::VARCHAR || ', ' || organizationDetails:state::VARCHAR 
|| ' ' || organizationDetails:postalCode::VARCHAR MASTERCLIENTADDRESS,
authPersonInfo:firstName::VARCHAR || ' ' || authPersonInfo:lastName::VARCHAR CONTACTPERSONNAME,
authPersonInfo:mobileNoDetails:intelMobileNo::VARCHAR CLIENTCONTACTNUM,
authPersonInfo:emailId::VARCHAR MASTERCLIENTEMAIL,
createdAt CONTRACTDATE,
authPersonInfo:designation::VARCHAR DESIGNATION,
organizationDetails:taxId::VARCHAR TAXID,
paymentContactInfo:firstName::VARCHAR || ' ' || paymentContactInfo:lastName::VARCHAR INVOICEPAYMENTCONTACTNAME,
paymentContactInfo:mobileNoDetails:intelMobileNo::VARCHAR INVOICEPAYMENTCONTACTNUM,
paymentContactInfo:emailId::VARCHAR INVOICEPAYMENTCONTACTEMAIL,
srId,
useVeotsSubdomain,
domain,
billingDate,
isBlocked,
isDeleted,
serviceStatus,
createdBy,
serviceStatusHistory,
createdAt,
updatedAt,
servicesCostDetails:costPerCouponCode::FLOAT costPerCouponCode,
  servicesCostDetails:dataCostPerQr::FLOAT dataCostPerQr,
  servicesCostDetails:cashbackTransactionCost::FLOAT cashbackTransactionCost,
  servicesCostDetails:maintainencePerMonth::FLOAT maintainencePerMonth,
  servicesCostDetails:maintainenceComments::VARCHAR maintainenceComments,
  solutionCostsDetails:covertBaseCost::FLOAT covertBaseCost,
  solutionCostsDetails:covertWarrantyCost::FLOAT covertWarrantyCost,
  solutionCostsDetails:covertLoyaltyCost::FLOAT covertLoyaltyCost,
  solutionCostsDetails:overtBaseCost::FLOAT overtBaseCost,
  solutionCostsDetails:overtWarrantyCost::FLOAT overtWarrantyCost,
  solutionCostsDetails:overtLoyaltyCost::FLOAT overtLoyaltyCost,
  solutionCostsDetails:trackingBaseCost::FLOAT trackingBaseCost
from (SELECT * , ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM masterclients)
WHERE ISACTIVE = 1;
 
 
 ------BATCHES --------------
 
 
 
 

create or replace TABLE BATCHES (
	_id OBJECT,
    batchType VARCHAR,
    createdByType VARCHAR,
    createdBy OBJECT,
	pimage VARCHAR(16777216),
	QRDetails ARRAY,
	QROnprod VARCHAR(16777216),
    expiryImage VARCHAR,
	addDetails VARCHAR(16777216),
	additionalImageDetails VARCHAR(16777216),
	batchNo VARCHAR(16777216),
	brand VARCHAR(16777216),
	blockingHistory ARRAY,
	deletionDetails VARIANT,  ---- not present in DB but mentioned in the excel provided
	clientId VARCHAR(16777216),
	costStructure OBJECT,
	createdAt TIMESTAMP_NTZ(9),
	--couponApplicable BOOLEAN,
	dor VARCHAR(16777216),
	extraImages ARRAY,
	expiryDate VARCHAR,
	manuAdd VARCHAR(16777216),
	manuLicenseNo VARCHAR(16777216),
	manuWebsiteLink VARCHAR(16777216),
	mfgDate VARCHAR,
	mrp NUMBER,
	prodName VARCHAR(16777216),
    prodVedioLink VARCHAR,
	status VARCHAR(16777216),
	serialNo VARCHAR(16777216),
	tor VARCHAR(16777216),
	trackingQrDetails_quat ARRAY,
	--reasonStatusChange VARCHAR(16777216),
	shelfLife NUMBER,
	trackingQrDetails_tert ARRAY,
	--statusChangeDate TIMESTAMP_NTZ(9),
	--serviceStatus VARCHAR,
	type NUMBER,
	updatedAt TIMESTAMP_NTZ(9),
	warrantyApp BOOLEAN,
	warrantyPeriod NUMBER,
    warrantyTC VARCHAR
);
  
 
 
 
 INSERT  INTO BATCHES

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
(SELECT parse_json($1) data FROM @veots_stage/staging-veots/static_coll/batches);



CREATE OR REPLACE VIEW BATCHES_V AS
SELECT _ID:oid::VARCHAR OID,
PIMAGE,
QRONPROD,
ADDDETAILS,
ADDITIONALIMAGEDETAILS,
BATCHNO,
BRAND,
BLOCKINGHISTORY,
DELETIONDETAILS,
CLIENTID,
CREATEDAT,
--COUPONAPPLICABLE,
EXPIRYDATE,
MANUADD,
MANULICENSENO,
MANUWEBSITELINK,
MFGDATE,
MRP,
PRODNAME,
STATUS,
SERIALNO,
TOR,
DOR,
--REASONSTATUSCHANGE,
SHELFLIFE,
--STATUSCHANGEDATE,
--------------------------serviceStatus mentioned in the sheet but not in the DB
--SERVICESTATUS,
TYPE,
UPDATEDAT,
WARRANTYAPP,
WARRANTYPERIOD,
COSTSTRUCTURE:servicesCostDetails:costPerCouponCode::FLOAT costPerCouponCode,
COSTSTRUCTURE:servicesCostDetails:dataCostPerQr::FLOAT dataCostPerQr,
COSTSTRUCTURE:servicesCostDetails:maintainencePerMonth::FLOAT maintainencePerMonth,
COSTSTRUCTURE:servicesCostDetails:cashbackTransactionCost::FLOAT cashbackTransactionCost,
COSTSTRUCTURE:servicesCostDetails:maintainenceComments::VARCHAR maintainenceComments,
COSTSTRUCTURE:solutionCostsDetails:covertBaseCost::FLOAT covertBaseCost,
COSTSTRUCTURE:solutionCostsDetails:covertLoyaltyCost::FLOAT covertLoyaltyCost,
COSTSTRUCTURE:solutionCostsDetails:covertWarrantyCost::FLOAT covertWarrantyCost,
COSTSTRUCTURE:solutionCostsDetails:infoBaseCost::FLOAT infoBaseCost,
COSTSTRUCTURE:solutionCostsDetails:overtBaseCost::FLOAT overtBaseCost,
COSTSTRUCTURE:solutionCostsDetails:overtLoyaltyCost::FLOAT overtLoyaltyCost,
COSTSTRUCTURE:solutionCostsDetails:overtWarrantyCost::FLOAT overtWarrantyCost,
COSTSTRUCTURE:solutionCostsDetails:trackingBaseCost::FLOAT trackingBaseCost
FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM BATCHES)
WHERE ISACTIVE = 1; 



CREATE OR REPLACE VIEW BATCHES_QRDETAILS_V AS
SELECT
BATCHES_OID,
Q.INDEX,
Q.VALUE:_id:oid::VARCHAR OID,
Q.VALUE:createdAt::TIMESTAMP_NTZ createdAt,
Q.VALUE:generatedBy::VARCHAR generatedBy,
Q.VALUE:generatedByType::VARCHAR generatedByType,
Q.VALUE:quantity::NUMBER quantity
FROM
(SELECT _ID:oid::VARCHAR BATCHES_OID ,QRDETAILS, 
ROW_NUMBER() OVER(PARTITION BY _id:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM BATCHES )B,
LATERAL FLATTEN (QRDETAILS) Q
WHERE B.ISACTIVE = 1;


CREATE OR REPLACE VIEW BATCHES_trackingQrDetails_quat_V AS
SELECT
BATCHES_OID,
Q.INDEX,
Q.VALUE:_id:oid::VARCHAR OID,
Q.VALUE:createdAt::TIMESTAMP_NTZ createdAt,
Q.VALUE:generatedBy::VARCHAR generatedBy,
Q.VALUE:generatedByType::VARCHAR generatedByType,
Q.VALUE:quantity::NUMBER quantity
FROM

(SELECT _ID:oid::VARCHAR BATCHES_OID ,trackingQrDetails_quat,
ROW_NUMBER() OVER(PARTITION BY _id:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM BATCHES )B,
LATERAL FLATTEN (trackingQrDetails_quat) Q
WHERE B.ISACTIVE = 1;


CREATE OR REPLACE VIEW BATCHES_trackingQrDetails_tert_V AS
SELECT
BATCHES_OID,
Q.INDEX,
Q.VALUE:_id:oid::VARCHAR OID,
Q.VALUE:createdAt::TIMESTAMP_NTZ createdAt,
Q.VALUE:generatedBy::VARCHAR generatedBy,
Q.VALUE:generatedByType::VARCHAR generatedByType,
Q.VALUE:quantity::NUMBER quantity
FROM
(SELECT _ID:oid::VARCHAR BATCHES_OID ,trackingQrDetails_tert,
ROW_NUMBER() OVER(PARTITION BY _id:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM BATCHES )B,
LATERAL FLATTEN (trackingQrDetails_tert) Q
WHERE B.ISACTIVE = 1;

-------------------PRODUCTS------------------------
----------------Missing fields in DB -- mfgDate ,expiryDate,feedback,purcaserContact



 

create or replace TABLE PRODUCTS (
	_id OBJECT,
    masterClient OBJECT,
	batchDetails OBJECT,
	billCopy VARCHAR(16777216),
	customerId OBJECT,
	fakeDetails ARRAY,
	postSaleCode VARCHAR(16777216),
	preSaleCode VARCHAR(16777216),
	prodReachedCus BOOLEAN,
	purChaseDate TIMESTAMP_NTZ(9),
	purchaserContact VARCHAR(16777216),
	createdAt TIMESTAMP_NTZ(9),
	updatedAt TIMESTAMP_NTZ(9),
    rating VARCHAR,
	isWarrantyConsentGiven BOOLEAN,
	mfgDate TIMESTAMP_NTZ,
	expiryDate TIMESTAMP_NTZ,
	feedback VARCHAR
);


INSERT  INTO PRODUCTS
SELECT 
	DATA:_id::VARIANT ,
    DATA:masterClient VARIANT,
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
(SELECT parse_json($1) data FROM @veots_stage/staging-veots/static_coll/products);


CREATE OR REPLACE VIEW PRODUCTS_V AS
SELECT 
 _ID:oid:: VARCHAR oid,
 BATCHDETAILS:oid::VARCHAR BATCHDETAILS,
 masterClient:oid::VARCHAR masterClient_oid,
 BILLCOPY,
 CREATEDAT,
 CUSTOMERID:oid ::VARCHAR CUSTOMERID,
 POSTSALECODE,
 PRESALECODE,
 PRODREACHEDCUS,
 PURCHASERCONTACT,
 UPDATEDAT,
 RATING ,
 isWarrantyConsentGiven,
 MFGDATE,
 EXPIRYDATE,
 feedback,
 PURCHASEDATE
FROM (SELECT * , ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM PRODUCTS)
WHERE ISACTIVE = 1;



CREATE OR REPLACE VIEW PRODUCTS_FAKEDETAILS_V AS
SELECT 
PRODUCTS_oid,
F.INDEX ,
F.VALUE:_id:oid::VARCHAR OID,
F.VALUE:fakeRepDate::TIMESTAMP_NTZ fakeRepDate,
--F.VALUE:fakeReportUdid ::VARCHAR fakeReportUdid,
F.VALUE:fakeReportedCoordinates[0]::FLOAT LAT,
F.VALUE:fakeReportedCoordinates[1]::FLOAT LONG,
F.VALUE:reasonRep::VARCHAR reasonRep,
--F.VALUE:fakeComment ::VARCHAR fakeComment ,
--F.VALUE:fakeBill ::VARCHAR fakeBill ,
F.VALUE:fakePostSaleCode ::VARCHAR fakePostSaleCode ,  --no records in db
F.VALUE:isReportConsentGiven :: BOOLEAN isReportConsentGiven
--F.VALUE:fakeReportedCoordinates:address:county::VARCHAR city,
--F.value:fakeReportedCoordinates:address:country::VARCHAR country, 
--F.value:fakeReportedCoordinates:address:state::VARCHAR state 
FROM(
SELECT 
 _ID:oid:: VARCHAR PRODUCTS_oid,
 FAKEDETAILS, ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE
 FROM PRODUCTS) P , LATERAL FLATTEN(FAKEDETAILS)F
 WHERE P.ISACTIVE = 1;





---------------------------SUBCLIENT-------------------

-- with cte as (
--  SELECT parse_json(a.$1) data, b.* FROM @veots_stage/staging-veots/static_coll/subclients a,
--  lateral flatten (data) b)
--  select distinct key from cte

create or replace TABLE SUBCLIENTS (
	_id OBJECT,
	blockingHistory ARRAY,
	createdByType VARCHAR(16777216),
	createdBy OBJECT,
	--creatorId VARCHAR(16777216),
	deletionDetails ARRAY,
	emailId VARCHAR,
	isBlocked BOOLEAN,
    isDeleted BOOLEAN,
	masterClient VARIANT,
	roles ARRAY,
	userId VARCHAR,
    userName VARCHAR,
	createdAt TIMESTAMP_NTZ(9),
	updatedAt TIMESTAMP_NTZ(9)
);

INSERT  INTO SUBCLIENTS
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
(SELECT parse_json($1) data FROM @veots_stage/staging-veots/static_coll/subclients);




CREATE OR REPLACE VIEW SUBCLIENTS_V AS
SELECT 
_ID:oid :: VARCHAR OID,
createdByType,
CREATEDBY,
--CREATORID,
EMAILID,
ISBLOCKED,
ISDELETED,
MASTERCLIENT:oid::VARCHAR MASTERCLIENT,
USERID,
USERNAME
CREATEDAT,
UPDATEDAT
FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM SUBCLIENTS)
WHERE ISACTIVE = 1;



CREATE OR REPLACE  VIEW SUBCLIENTS_ROLES_V AS
SELECT A.SUBCLIENT_OID ,
	   R.INDEX , 
	   R.VALUE::VARCHAR ROLE 
	 FROM
(SELECT _ID:oid::VARCHAR SUBCLIENT_OID ,
ROLES, ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE  FROM SUBCLIENTS ) A,
LATERAL FLATTEN (A.ROLES) R
WHERE A.ISACTIVE = 1;	   


CREATE OR REPLACE  VIEW SUBCLIENTS_BLOCKINGHISTORY_V AS
SELECT A.SUBCLIENT_OID ,
	   R.INDEX , 
	   R.VALUE:actionType::VARCHAR actionType,
       R.VALUE:date:: TIMESTAMP_NTZ DATE,
       R.VALUE:performedBy:: VARCHAR performedby_oid,
       R.VALUE:reason::VARCHAR reason,
       R.VALUE:userType ::VARCHAR userType
	 FROM
(SELECT _ID:oid::VARCHAR SUBCLIENT_OID ,
BLOCKINGHISTORY , ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE  FROM SUBCLIENTS ) A,
LATERAL FLATTEN (A.BLOCKINGHISTORY) R 
WHERE A.ISACTIVE = 1;	

--------------------OneTimeClient-----------------------------
--------------No data in DB

create or replace TABLE OneTimeClients (
	_id OBJECT,
	address VARCHAR,
    OTClientId NUMBER, 
    clientName VARCHAR,	
	contactNum VARCHAR(16777216),
	emailAddress VARCHAR(16777216),
	--invoiceDate TIMESTAMP,
	placeOfService VARCHAR,
	taxId VARCHAR,
	createdAt TIMESTAMP_NTZ(9),
	updatedAt TIMESTAMP_NTZ(9)
);


INSERT  INTO OneTimeClients


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
(SELECT parse_json($1) data FROM @veots_stage/staging-veots/static_coll/onetimeclients);





CREATE OR REPLACE VIEW ONETIMECLIENTS_V AS
SELECT _ID:oid::VARCHAR oid,
ADDRESS,
OTCLIENTID,
CLIENTNAME,
CONTACTNUM,
EMAILADDRESS,
--INVOICEDATE,
PLACEOFSERVICE,
TAXID,
CREATEDAT,
UPDATEDAT
FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM ONETIMECLIENTS)
WHERE ISACTIVE = 1;














-----------------CUSTOMERS----------


create or replace TABLE customers (
	_id OBJECT,
	customerUdid VARCHAR,
    --country VARCHAR, 
    companyName VARCHAR,	
	dob TIMESTAMP_NTZ(9),
	gender VARCHAR(16777216),
	email VARCHAR,
	status VARCHAR,
	userName VARCHAR,
	createdAt TIMESTAMP_NTZ(9),
	updatedAt TIMESTAMP_NTZ(9),
	phoneNum VARCHAR,
	AGE_GROUP VARCHAR
	
	----phoneNo Mentioned in the doc but not in the Database
);


INSERT  INTO customers


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
CASE   WHEN DATEDIFF(YEAR,DATA:dob:: TIMESTAMP_NTZ,CURRENT_DATE()) >=60 then '60+' 
       WHEN DATEDIFF(YEAR,DATA:dob:: TIMESTAMP_NTZ,CURRENT_DATE()) >=40 then '40-60'
       WHEN DATEDIFF(YEAR,DATA:dob:: TIMESTAMP_NTZ,CURRENT_DATE()) >=25 then '25-40'
       when DATEDIFF(YEAR,DATA:dob:: TIMESTAMP_NTZ,CURRENT_DATE()) >=18 then '18-25'
     ELSE NULL END :: VARCHAR AGE_GROUP,
	 

from
(SELECT parse_json($1) data FROM @veots_stage/staging-veots/static_coll/customers);



CREATE OR REPLACE VIEW CUSTOMERS_V 
AS
SELECT 
_ID:oid::VARCHAR oid,
customerUdid,
--country,
companyName,
dob,
gender,
email,
status,
userName,
createdAt,
updatedAt,
phoneNum, AGE_GROUP
from (SELECT *, ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM CUSTOMERS)
WHERE ISACTIVE = 1;


 
 
 ---------------------SKUS
 
create or replace TABLE SKUS (
	_id VARIANT,
	clientId VARCHAR,
    skuId VARCHAR, 
    Pimage VARCHAR,	
	QROnprod VARCHAR,
	prodName VARCHAR,
	brand VARCHAR,
	batchType VARCHAR,
	mrp FLOAT,
	shelfLife VARCHAR,
	warrantyApp VARCHAR,
	warrantyPeriod INT,
	serialNo VARCHAR,
	manuLicenseNo VARCHAR,
	manuAdd VARCHAR,
	addDetails VARCHAR,
	expiryImage VARCHAR,
	extraImage1 VARCHAR,
	extraImage2 VARCHAR,
	prodVedioLink VARCHAR,
	additionalImageDetails VARCHAR,
	manuWebsiteLink VARCHAR,
    warrantyTC VARCHAR,
	createdAt TIMESTAMP_NTZ(9),
	updatedAt TIMESTAMP_NTZ(9)
);

INSERT  INTO SKUS
 SELECT
	DATA:_id:: VARIANT,
	DATA:clientId:: VARCHAR,
    DATA:skuIdL:: VARCHAR, 
    DATA:Pimage:: VARCHAR,	
	DATA:QROnprod:: VARCHAR,
	DATA:prodName:: VARCHAR,
	DATA:brand:: VARCHAR,
	DATA:batchType:: VARCHAR,
	DATA:mrp ::FLOAT,
	DATA:shelfLife:: VARCHAR,
	DATA:warrantyApp:: VARCHAR,
	DATA:warrantyPeriod:: INT,
	DATA:serialNo ::VARCHAR,
	DATA:manuLicenseNo:: VARCHAR,
	DATA:manuAdd ::VARCHAR,
	DATA:addDetails:: VARCHAR,
	DATA:expiryImage ::VARCHAR,
	DATA:extraImage1 ::VARCHAR,
	DATA:extraImage2 ::VARCHAR,
	DATA:prodVedioLink ::VARCHAR,
	DATA:additionalImageDetails ::VARCHAR,
	DATA:manuWebsiteLink ::VARCHAR,
    DATA:warrantyTC ::VARCHAR,
	DATA:createdAt ::TIMESTAMP_NTZ,
	DATA:updatedAt ::TIMESTAMP_NTZ
from
(SELECT parse_json($1) data FROM @veots_stage/staging-veots/static_coll/skus);

CREATE OR REPLACE VIEW SKUS_V
AS
SELECT 

    _ID:oid::VARCHAR oid,
	clientId ,
    skuId , 
    Pimage ,	
	QROnprod ,
	prodName ,
	brand ,
	batchType ,
	mrp FLOAT,
	shelfLife ,
	warrantyApp ,
	warrantyPeriod INT,
	serialNo ,
	manuLicenseNo ,
	manuAdd ,
	addDetails ,
	expiryImage ,
	extraImage1 ,
	extraImage2 ,
	prodVedioLink ,
	additionalImageDetails ,
	manuWebsiteLink ,
    warrantytc,
	createdAt ,
	updatedAt FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY _ID:oid ORDER BY UPDATEDAT DESC) ISACTIVE FROM SKUS)
    WHERE ISACTIVE = 1;


