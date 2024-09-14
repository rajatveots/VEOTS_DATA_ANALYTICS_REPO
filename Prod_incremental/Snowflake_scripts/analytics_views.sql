create or replace view VEOTS_PROD_DB.ANALYTICS.ANALYTICS_V(
	MASTERCLIENTS_OID,
	MASTERCLIENTS_BATCHESGENERATED_OID,
	BATCHES_OID,
	PRODUCTS_OID,
	"Client Name",
	"ClientId",
	"Client address",
	"Client contact name",
	"client contact number",
	"Client contact email address",
	"Contract date",
	"Billing date",
	"Client contact designation",
	"Client GSTIN/UIN",
	"Cient Business period",
	"invoice payment contact name",
	"invoice payment contact number",
	"Invoice payment email address",
	"Client status",
	"Product name",
	"Brand name",
	MRP,
	"Mfg date",
	"shelf life",
	"Expiry date",
	"Batch number",
	"Warrranty Applicable",
	"Product Image",
	TYPE,
	"Product Reached Customer",
	"Purchase Date",
	QUANTITY_PRODUCT_QR,
	QUANTITY_QUAT_QR,
	QUANTITY_TERT_QR,
	"QR Generation Date",
	"Entity",
	CUSTOMER_AGE_GROUP,
	CUSTOMERUDID,
	CUSTOMER_GENDER
) as 
SELECT 
MC.OID MASTERCLIENTS_OID,
MCB.BATCHESGENERATED_OID MASTERCLIENTS_BATCHESGENERATED_OID,
B.OID BATCHES_OID,
P.OID PRODUCTS_OID,
MC.CLIENTNAME "Client Name" ,
MC.MASTERCLIENTID "ClientId",
MC.MASTERCLIENTADDRESS "Client address",
MC.CONTACTPERSONNAME "Client contact name",
MC.CLIENTCONTACTNUM "client contact number",
MC.MASTERCLIENTEMAIL "Client contact email address",
MC.CONTRACTDATE "Contract date",
MC.BILLINGDATE "Billing date",
MC.DESIGNATION "Client contact designation",
MC.TAXID "Client GSTIN/UIN",
DATEDIFF(day,MC.CREATEDAT,CURRENT_DATE()) "Cient Business period",
MC.INVOICEPAYMENTCONTACTNAME "invoice payment contact name",
MC.INVOICEPAYMENTCONTACTNUM "invoice payment contact number",
MC.INVOICEPAYMENTCONTACTEMAIL "Invoice payment email address",
MC.SERVICESTATUS "Client status",
--"Client status change date" ,
--"Client status change reason",
--"Client status changed by" ,
B.PRODNAME "Product name" ,
B.BRAND "Brand name",
B.MRP "MRP",
B.MFGDATE "Mfg date",
B.SHELFLIFE "shelf life",
B.expirydate "Expiry date",
B.BatchNo "Batch number",
B.warrantyApp "Warrranty Applicable",
B.Pimage "Product Image",
b.type,
P.prodReachedCus "Product Reached Customer", --Mentioned in req Doc but not in DB
P.PURCHASEDATE "Purchase Date",
X.QUANTITY_PRODUCT_QR,
X.QUANTITY_QUAT_QR,
X.QUANTITY_TERT_QR,
B.CreatedAt "QR Generation Date",
C.COMPANYNAME "Entity",
C.AGE_GROUP CUSTOMER_AGE_GROUP,
--C.COUNTRY CUSTOMER_COUNTRY,
C.CUSTOMERUDID CUSTOMERUDID,
C.GENDER CUSTOMER_GENDER


FROM MASTERCLIENTS_V MC LEFT JOIN MASTERCLIENTS_BATCHESGENERATED_V MCB
ON MC.OID =MCB.MASTERCLIENT_OID
LEFT JOIN BATCHES_V B ON B.CLIENTID = MC.MASTERCLIENTID
LEFT JOIN PRODUCTS_V P ON MCB.BATCHESGENERATED_OID = P.BATCHDETAILS
LEFT JOIN (
SELECT B.OID , SUM(BQR.QUANTITY) QUANTITY_PRODUCT_QR,
SUM(BQR_TERT.QUANTITY) QUANTITY_TERT_QR,
SUM(BQR_QUAT.QUANTITY) QUANTITY_QUAT_QR

FROM BATCHES_V  B ,  BATCHES_QRDETAILS_V BQR ,BATCHES_TRACKINGQRDETAILS_QUAT_V BQR_QUAT ,BATCHES_TRACKINGQRDETAILS_TERT_V BQR_TERT WHERE 
 B.OID=BQR.BATCHES_OID AND  B.OID=BQR_QUAT.BATCHES_OID  AND B.OID =BQR_TERT.BATCHES_OID
 GROUP BY B.OID) X ON X.OID =B.OID 
LEFT JOIN CUSTOMERS_V C ON P.CUSTOMERID =C.OID;


---------------------------------------------------------------------------------------------------------


create or replace view VEOTS_PROD_DB.ANALYTICS.COUPONS_V(
	TYPEONECOUPONSETS_OID,
	MASTERCLIENT_ID,
	EXPIRYDATE,
	REMAININGCOUPONS,
	TOTALCOUPONCOUNT,
	ISDELETED,
	TYPEONECOUPONCODES_OID,
	ASSIGNEDTO,
	COUPONCODE,
	DELIVEREDAT,
	ISASSIGNED,
	ISSCRATCHED,
	PRODUCTID
) as 
select 
a.oid typeonecouponsets_oid,
a.MASTERCLIENT_ID,
a.EXPIRYDATE,
a.REMAININGCOUPONS,
a.TOTALCOUPONCOUNT,
a.ISDELETED,
b.oid typeonecouponcodes_oid,
b.ASSIGNEDTO,
b.COUPONCODE,
b.DELIVEREDAT,
b.ISASSIGNED,
b.ISSCRATCHED,
b.PRODUCTID
from typeonecouponsets_v a LEFT JOIN typeonecouponcodes_v b on b.setid = a.oid;


  ----------------------------------------------------------------------------------------------------------

  create or replace view TABLE_DATES_V as
select 'ADMINS' table_name , max(createdat) max_createdat, max(updatedat) max_updatedat from ADMINS
union all  
select 'MASTERCLIENTS' table_name, max(createdat) max_createdat, max(updatedat) max_updatedat from MASTERCLIENTS
union all  
select 'BATCHES' table_name, max(createdat) max_createdat, max(updatedat) max_updatedat from BATCHES
union all  
select 'PRODUCTS' table_name, max(createdat) max_createdat, max(updatedat) max_updatedat from PRODUCTS
union all  
select 'SUBCLIENTS' table_name, max(createdat) max_createdat, max(updatedat) max_updatedat from SUBCLIENTS
union all  
select 'ONETIMECLIENTS' table_name, max(createdat) max_createdat, max(updatedat) max_updatedat from ONETIMECLIENTS
union all  
select 'CUSTOMERS' table_name, max(createdat) max_createdat, max(updatedat) max_updatedat from CUSTOMERS
union all  
select 'TYPEONECOUPONSETS' table_name, max(createdat) max_createdat, max(updatedat) max_updatedat from TYPEONECOUPONSETS
union all  
select 'TYPEONECOUPONCODES' table_name, max(createdat) max_createdat, max(updatedat) max_updatedat from TYPEONECOUPONCODES
union all  
select 'DORS' table_name, max(createdat) max_createdat, max(updatedat) max_updatedat from DORS
union all  
select 'TORS' table_name, max(createdat) max_createdat, max(updatedat) max_updatedat from TORS
union all  
select 'VECTORTABLE4_1' table_name, max(createdat) max_createdat, max(updatedat) max_updatedat from VECTORTABLE4_1
union all  
select 'VECTORTABLE4_2' table_name, max(createdat) max_createdat, max(updatedat) max_updatedat from VECTORTABLE4_2
union all  
select 'VECTORTABLE6' table_name, max(createdat) max_createdat, max(updatedat) max_updatedat from VECTORTABLE6
union all  
select 'VECTORTABLE7' table_name, max(createdat) max_createdat, max(updatedat) max_updatedat from VECTORTABLE7
union all  
select 'VECTORTABLE8' table_name, max(createdat) max_createdat, max(updatedat) max_updatedat from VECTORTABLE8
union all  
select 'VECTORTABLE9' table_name, max(createdat) max_createdat, max(updatedat) max_updatedat from VECTORTABLE9;



------------------------------------------------------------------------------------------------------------------



create or replace view VEOTS_PROD_DB.ANALYTICS.TYPE_1_QR_V(
	BATCH_OID,
	CLIENTID,
	TOR,
	DOR,
	TYPE,
	BRAND,
	PRODNAME,
	WARRANTYPERIOD,
	WARRANTYAPP,
	BATCH_CREATION_DATE,
	EXPIRYDATE,
	SHELFLIFE,
	QRID,
	SCAN_OID,
	SCANTYPE,
	DATE,
	ESTABLISHMENTNAME,
	LAT,
	LONG,
	CITY,
	STATE,
	COUNTRY
) as
SELECT 
  X.OID BATCH_OID, 
  X.CLIENTID, 
  X.TOR, 
  X.DOR, 
  X.TYPE, 
  X.BRAND,
  X.PRODNAME,
  X.WARRANTYPERIOD, 
  X.WARRANTYAPP, 
  X.CREATEDAT BATCH_CREATION_DATE, 
  X.EXPIRYDATE,
  X.SHELFLIFE,
  V.QRID, 
  VC.OID SCAN_OID,
  VC.SCANTYPE, 
  VC.DATE, 
  VC.ESTABLISHMENTNAME, 
  VC.LAT, 
  VC.LONG,
  VC.CITY,
  VC.STATE,
  VC.COUNTRY
FROM 
  (
    SELECT 
      B.*, 
      B.TOR || B.DOR || B.CLIENTID || LPAD(
        trim(
          to_char(BQ.index, 'xxxxxx')
        ), 
        3, 
        0
      ) || CASE WHEN B.TYPE = 1 THEN 0 ELSE 1 END AS QR_ID_SUFFIX 
    FROM 
      BATCHES_V B 
      LEFT JOIN BATCHES_QRDETAILS_V BQ ON B.OID = BQ.BATCHES_OID 
    WHERE 
      B.TYPE IN (1)
  ) X 
  LEFT JOIN VECTORTABLE6_V V ON SUBSTRING(V.QRID, 11)= X.QR_ID_SUFFIX 
  LEFT JOIN VECTORTABLE6_COORDINATES_V VC ON VC.VECTORTABLE6_OID = V.OID;


  --------------------------------------------------------------------------------------------------


create or replace view VEOTS_PROD_DB.ANALYTICS.TYPE_2_QR_V(
	BATCH_OID,
	CLIENTID,
	TOR,
	DOR,
	TYPE,
	BRAND,
	PRODNAME,
	WARRANTYPERIOD,
	WARRANTYAPP,
	BATCH_CREATION_DATE,
	EXPIRYDATE,
	SHELFLIFE,
	DIRTYBIT,
	QRID,
	SCAN_OID,
	SCANTYPE,
	DATE,
	ESTABLISHMENTNAME,
	REASON,
	LAT,
	LONG,
	CITY,
	STATE,
	COUNTRY
) as
SELECT 
  X.OID BATCH_OID, 
  X.CLIENTID, 
  X.TOR, 
  X.DOR, 
  X.TYPE,
  X.BRAND,
  X.PRODNAME,
  X.WARRANTYPERIOD, 
  X.WARRANTYAPP, 
  X.CREATEDAT BATCH_CREATION_DATE, 
  X.EXPIRYDATE,
  X.SHELFLIFE,
  V.DIRTYBIT, 
  V.QRID, 
  VC.OID SCAN_OID,
  VC.SCANTYPE, 
  VC.DATE, 
  VC.ESTABLISHMENTNAME, 
  VC.REASON, 
  VC.LAT, 
  VC.LONG,
  VC.CITY,
  VC.STATE,
  VC.COUNTRY
FROM 
  (
    SELECT 
      B.*, 
      B.TOR || B.DOR || B.CLIENTID || LPAD(
        trim(
          to_char(BQ.index, 'xxxxxx')
        ), 
        3, 
        0
      ) || CASE WHEN B.TYPE = 1 THEN 0 ELSE 1 END AS QR_ID_SUFFIX 
    FROM 
      BATCHES_V B 
      LEFT JOIN BATCHES_QRDETAILS_V BQ ON B.OID = BQ.BATCHES_OID 
    WHERE 
      B.TYPE IN (2)
  ) X 
  LEFT JOIN VECTORTABLE7_V V ON SUBSTRING(V.QRID, 11)= X.QR_ID_SUFFIX 
  LEFT JOIN VECTORTABLE7_COORDINATES_V VC ON VC.VECTORTABLE7_OID = V.OID;

  --------------------------------------------------------------------------------------------------


create or replace view VEOTS_PROD_DB.ANALYTICS.TYPE_3_QR_PRESALE_V(
	BATCH_OID,
	CLIENTID,
	BRAND,
	PRODNAME,
	WARRANTYPERIOD,
	WARRANTYAPP,
	BATCH_CREATION_DATE,
	EXPIRYDATE,
	SHELFLIFE,
	DOR,
	TOR,
	VECTOR3PTR,
	TOR_INDEX,
	PS_START,
	PS_END,
	DIRTYBITPRESALE,
	VECTORTABLE4_1_INDEX,
	QR_ID,
	SCAN_OID,
	SCANTYPE,
	DATE,
	ESTABLISHMENTNAME,
	LAT,
	LONG,
	CITY,
	STATE,
	COUNTRY
) as
SELECT 
  B.OID BATCH_OID, 
  B.CLIENTID, 
  B.BRAND,
  B.PRODNAME,
  B.WARRANTYPERIOD, 
  B.WARRANTYAPP, 
  B.CREATEDAT BATCH_CREATION_DATE, 
  B.EXPIRYDATE,
  B.SHELFLIFE,
  D.DOR, 
  T.TOG TOR, 
  D.VECTOR3PTR, 
  T.INDEX TOR_INDEX, 
  TPRE.PS_START, 
  TPRE.PS_END, 
  V.DIRTYBITPRESALE, 
  V.INDEX VECTORTABLE4_1_INDEX, 
  V.randomNumPreSale || T.TOG || D.DOR || B.CLIENTID || LPAD(
    trim(
      to_char(BQ.index, 'xxxxxx')
    ), 
    4, 
    0
  ) || 0 AS QR_ID, 
  V.SCAN_OID,
  V.SCANTYPE, 
  V.DATE, 
  V.ESTABLISHMENTNAME, 
  V.LAT, 
  V.LONG,
  V.CITY,
  V.STATE,
  V.COUNTRY
FROM 
  BATCHES_V B, 
  TORS_V T, 
  DORS_V D, 
  TORS_PRESALEPTR_V TPRE, 
  (
    SELECT 
      V.*, 
      VC.OID SCAN_OID,
      VC.SCANTYPE, 
      DATE, 
      REASON, 
      ESTABLISHMENTNAME, 
      LAT, 
      LONG,
      CITY,
      STATE,
      COUNTRY
    FROM 
      VECTORTABLE4_1_V V 
      LEFT JOIN VECTORTABLE4_1_COORDINATES_V VC ON V.OID = VC.VECTORTABLE4_1_OID
  ) V, 
  BATCHES_QRDETAILS_V BQ 
WHERE 
  B.TYPE IN (3) 
  and B.TOR = T.TOG 
  AND B.DOR = D.DOR 
  AND D.VECTOR3PTR = T.INDEX 
  AND T.OID = TPRE.TORS_OID 
  AND B.CLIENTID = V.CLIENT_ID 
  AND V.INDEX BETWEEN TPRE.PS_START 
  AND TPRE.PS_END 
  AND B.OID = BQ.BATCHES_OID;



  ---------------------------------------------------------------------------------------------


create or replace view VEOTS_PROD_DB.ANALYTICS.TYPE_3_QR_POSTSALE_V(
	BATCH_OID,
	CLIENTID,
	BRAND,
	PRODNAME,
	WARRANTYPERIOD,
	WARRANTYAPP,
	BATCH_CREATION_DATE,
	EXPIRYDATE,
	SHELFLIFE,
	DOR,
	TOR,
	VECTOR3PTR,
	TOR_INDEX,
	PS_START,
	PS_END,
	DIRTYBITPOSTSALE,
	VECTORTABLE4_2_INDEX,
	QR_ID,
	SCAN_OID,
	SCANTYPE,
	DATE,
	ESTABLISHMENTNAME,
	LAT,
	LONG,
	CITY,
	STATE,
	COUNTRY
) as
SELECT 
  B.OID BATCH_OID, 
  B.CLIENTID, 
  B.BRAND,
  B.PRODNAME,
  B.WARRANTYPERIOD, 
  B.WARRANTYAPP, 
  B.CREATEDAT BATCH_CREATION_DATE, 
  B.EXPIRYDATE,
  B.SHELFLIFE,
  D.DOR, 
  T.TOG TOR, 
  D.VECTOR3PTR, 
  T.INDEX TOR_INDEX, 
  TPRE.PS_START, 
  TPRE.PS_END, 
  V.DIRTYBITPOSTSALE, 
  V.INDEX VECTORTABLE4_2_INDEX, 
  V.randomNumPostSale || T.TOG || D.DOR || B.CLIENTID || LPAD(
    trim(
      to_char(BQ.index, 'xxxxxx')
    ), 
    4, 
    0
  ) || 1 AS QR_ID,
  V.SCAN_OID,
  V.SCANTYPE, 
  V.DATE, 
  V.ESTABLISHMENTNAME, 
  V.LAT, 
  V.LONG,
  V.CITY,
  V.STATE,
  V.COUNTRY
FROM 
  BATCHES_V B, 
  TORS_V T, 
  DORS_V D, 
  TORS_POSTSALEPTR_V TPRE,
  
  (
    SELECT 
      V.*, 
      VC.OID SCAN_OID,
      VC.SCANTYPE, 
      DATE, 
      REASON, 
      ESTABLISHMENTNAME, 
      LAT, 
      LONG, CITY,STATE,COUNTRY 
    FROM 
      VECTORTABLE4_2_V V 
      LEFT JOIN VECTORTABLE4_2_COORDINATES_V VC ON V.OID = VC.VECTORTABLE4_2_OID
  ) V, 
  BATCHES_QRDETAILS_V BQ 
WHERE 
  B.TYPE IN (3) 
  and B.TOR = T.TOG 
  AND B.DOR = D.DOR 
  AND D.VECTOR3PTR = T.INDEX 
  AND T.OID = TPRE.TORS_OID 
  AND B.CLIENTID = V.CLIENT_ID 
  AND V.INDEX BETWEEN TPRE.PS_START 
  AND TPRE.PS_END 
  AND B.OID = BQ.BATCHES_OID;

  -----------------------------------------------------------------------------------------------------


  
create or replace view VEOTS_PROD_DB.ANALYTICS.PRODUCT_QR_V(
	BATCH_OID,
	CLIENTID,
	TOR,
	DOR,
	TYPE,
	BRAND,
	"Product Name",
	SUBTYPE,
	DIRTYBIT,
	QRID,
	SCANTYPE,
	DATE,
	ESTABLISHMENTNAME,
	REASON,
	SCAN_OID,
	LAT,
	LONG,
	CITY,
	STATE,
	COUNTRY,
	WARRANTYAPP,
	WARRANTYPERIOD,
	BATCH_CREATION_DATE,
	"Batch Expiry date",
	SHELFLIFE,
	PRODUCT_ID,
	PRODREACHEDCUS,
	PURCHASEDATE,
	"Product Expiry date",
	ENTITY,
	CUSTOMER_AGE_GROUP,
	CUSTOMER_GENDER,
	FR_OID
) as 
SELECT 
  BATCH_OID, 
  CLIENTID, 
  TOR, 
  DOR, 
  TYPE, 
  QR.BRAND,
  QR.PRODNAME "Product Name",
  NULL SUBTYPE, 
  NULL DIRTYBIT, 
  QRID, 
  SCANTYPE, 
  DATE, 
  ESTABLISHMENTNAME, 
  NULL REASON,
  QR.SCAN_OID,
  QR.LAT, 
  QR.LONG, 
  CITY,STATE,QR.COUNTRY,
  WARRANTYAPP, 
  WARRANTYPERIOD, 
  BATCH_CREATION_DATE,
  QR.EXPIRYDATE "Batch Expiry date",
  QR.SHELFLIFE,
  PV.OID PRODUCT_ID, 
  PV.PRODREACHEDCUS, 
  PV.PURCHASEDATE, 
  PV.EXPIRYDATE "Product Expiry date",
  C.COMPANYNAME ENTITY, 
  C.AGE_GROUP CUSTOMER_AGE_GROUP, 
  --C.COUNTRY CUSTOMER_COUNTRY, 
  C.GENDER CUSTOMER_GENDER ,
  F.OID FR_OID
FROM 
  TYPE_2_QR_V QR 
  LEFT JOIN PRODUCTS_V PV ON QR.BATCH_OID = PV.BATCHDETAILS 
  AND PV.PRESALECODE = QR.QRID 
  LEFT JOIN CUSTOMERS_V C ON PV.CUSTOMERID = C.OID 
  LEFT JOIN PRODUCTS_FAKEDETAILS_V F ON F.PRODUCTS_OID = PV.OID
UNION ALL 
SELECT 
  BATCH_OID, 
  CLIENTID, 
  TOR, 
  DOR, 
  TYPE, 
  QR.BRAND,
  QR.PRODNAME "Product Name",
  NULL SUBTYPE, 
  NULL DIRTYBIT, 
  QRID, 
  SCANTYPE, 
  DATE, 
  ESTABLISHMENTNAME, 
  NULL REASON, 
  QR.SCAN_OID,
  QR.LAT, 
  QR.LONG, 
  CITY,STATE,QR.COUNTRY,
  WARRANTYAPP, 
  WARRANTYPERIOD, 
  BATCH_CREATION_DATE, 
  QR.EXPIRYDATE "Batch Expiry date",
  QR.SHELFLIFE,
  PV.OID PRODUCT_ID, 
  PV.PRODREACHEDCUS, 
  PV.PURCHASEDATE,
  PV.EXPIRYDATE "Product Expiry date",
  C.COMPANYNAME ENTITY, 
  C.AGE_GROUP CUSTOMER_AGE_GROUP,
  --C.COUNTRY CUSTOMER_COUNTRY, 
  C.GENDER CUSTOMER_GENDER ,
  F.OID FR_OID
FROM 
  TYPE_1_QR_V QR 
  LEFT JOIN PRODUCTS_V PV ON QR.BATCH_OID = PV.BATCHDETAILS 
  AND PV.PRESALECODE = QR.QRID 
  LEFT JOIN CUSTOMERS_V C ON PV.CUSTOMERID = C.OID 
  LEFT JOIN PRODUCTS_FAKEDETAILS_V F ON F.PRODUCTS_OID = PV.OID
UNION ALL 
SELECT 
  BATCH_OID, 
  CLIENTID, 
  TOR, 
  DOR, 
  3 TYPE, 
  QR.BRAND,
  QR.PRODNAME "Product Name",
  'PRESALE' SUBTYPE, 
  DIRTYBITPRESALE DIRTYBIT, 
  QR_ID QRID, 
  SCANTYPE, 
  DATE, 
  ESTABLISHMENTNAME, 
  NULL REASON, 
  QR.SCAN_OID,
  QR.LAT, 
  QR.LONG,
  CITY,STATE,QR.COUNTRY,
  WARRANTYAPP, 
  WARRANTYPERIOD, 
  BATCH_CREATION_DATE,
  QR.EXPIRYDATE "Batch Expiry date",
  QR.SHELFLIFE,
  PV.OID PRODUCT_ID, 
  PV.PRODREACHEDCUS, 
  PV.PURCHASEDATE,
  PV.EXPIRYDATE "Product Expiry date",
  C.COMPANYNAME ENTITY, 
  C.AGE_GROUP CUSTOMER_AGE_GROUP, 
  --C.COUNTRY CUSTOMER_COUNTRY, 
  C.GENDER CUSTOMER_GENDER ,
  PV.FR_OID
FROM 
  TYPE_3_QR_PRESALE_V QR 
  LEFT JOIN (
    SELECT 
      P.OID, 
      P.CUSTOMERID, 
      BATCHDETAILS, 
      P.PRODREACHEDCUS, 
      P.PURCHASEDATE, 
      P.POSTSALECODE, 
      P.PRESALECODE, 
      P.EXPIRYDATE,
      PF.OID FR_OID,
      PF.REASONREP 
    FROM 
      PRODUCTS_V P LEFT JOIN
      PRODUCTS_FAKEDETAILS_V PF 
    ON 
      P.OID = PF.PRODUCTS_OID
  ) PV ON QR.BATCH_OID = PV.BATCHDETAILS 
  AND PV.PRESALECODE = QR.QR_ID 
  AND COALESCE(PV.POSTSALECODE, 'XXXXX')<> PV.PRESALECODE 
  LEFT JOIN CUSTOMERS_V C ON PV.CUSTOMERID = C.OID 
UNION ALL 
SELECT 
  BATCH_OID, 
  CLIENTID, 
  TOR, 
  DOR, 
  3 TYPE,
  QR.BRAND,
  QR.PRODNAME "Product Name",
  'POSTSALE' SUBTYPE, 
  DIRTYBITPOSTSALE DIRTYBIT, 
  QR_ID QRID, 
  SCANTYPE, 
  DATE, 
  ESTABLISHMENTNAME, 
  NULL REASON, 
  QR.SCAN_OID,
  QR.LAT, 
  QR.LONG, 
  CITY,STATE,QR.COUNTRY,
  WARRANTYAPP, 
  WARRANTYPERIOD, 
  BATCH_CREATION_DATE,
  QR.EXPIRYDATE "Batch Expiry date",
  QR.SHELFLIFE,
  NULL PRODUCT_ID, 
  NULL PRODREACHEDCUS, 
  NULL PURCHASEDATE, 
  NULL "Product Expiry date",
  NULL ENTITY, 
  NULL CUSTOMER_AGE_GROUP, 
  --NULL CUSTOMER_COUNTRY, 
  NULL CUSTOMER_GENDER ,
  NULL FR_OID
FROM 
  TYPE_3_QR_POSTSALE_V QR;

  -----------------------------------------------------------------------------------------------


create or replace view VEOTS_PROD_DB.ANALYTICS.FAKE_REPORTS_V(
	BATCH_OID,
	CLIENTID,
	TOR,
	DOR,
	TYPE,
	SUBTYPE,
	QRID,
	PRODUCTID,
	PRODUCTS_OID,
	INDEX,
	OID,
	FAKEREPDATE,
	FAKEREPORTED_CUSTOMER,
	LAT,
	LONG,
	REASONREP,
	FAKEPOSTSALECODE,
	ISREPORTCONSENTGIVEN
) as 
SELECT 
  PV.BATCH_OID, 
  PV.CLIENTID, 
  PV.TOR, 
  PV.DOR, 
  PV.TYPE, 
  PV.SUBTYPE, 
  PV.QRID, 
  P.OID PRODUCTID, 
  F.* 
FROM 
  PRODUCTS_V P 
  LEFT JOIN PRODUCT_QR PV ON PV.BATCH_OID = P.BATCHDETAILS 
  AND QRID = POSTSALECODE 
  AND QRID = PRESALECODE 
  LEFT JOIN PRODUCTS_FAKEDETAILS_V F ON P.OID = F.PRODUCTS_OID 
WHERE 
  TYPE IN (1, 2) 
  AND POSTSALECODE = PRESALECODE 
  AND F.PRODUCTS_OID IS NOT NULL 
UNION ALL 
SELECT 
  PV.BATCH_OID, 
  PV.CLIENTID, 
  PV.TOR, 
  PV.DOR, 
  PV.TYPE, 
  PV.SUBTYPE, 
  PV.QRID, 
  P.OID PRODUCTID, 
  F.* 
FROM 
  PRODUCTS_V P 
  LEFT JOIN PRODUCT_QR PV ON PV.BATCH_OID = P.BATCHDETAILS 
  AND QRID = PRESALECODE 
  AND TYPE IN (3) 
  LEFT JOIN PRODUCTS_FAKEDETAILS_V F ON P.OID = F.PRODUCTS_OID --WHERE UPPER(REASONREP) LIKE '%POSTSALE%'
WHERE 
  F.OID IS NOT NULL 
  AND COALESCE(POSTSALECODE, 'XXXX') <> PRESALECODE --AND UPPER(REASONREP) LIKE '%PRESALE%' 
  AND SUBTYPE = 'PRESALE';


---------------------------------------------------------------------------------------------------------


create or replace view VEOTS_PROD_DB.ANALYTICS.QR_QUAT_V(
	BATCH_OID,
	CLIENTID,
	TOR,
	DOR,
	TYPE,
	QR_ID_SUFFIX,
	CONTACT,
	DATE,
	LAT,
	LONG,
	CITY,
	STATE,
	COUNTRY,
	ESTABLISHMENTNAME,
	QRID
) as 
SELECT 
  X.OID BATCH_OID, 
  X.CLIENTID, 
  X.TOR, 
  X.DOR, 
  X.TYPE, 
  X.QR_ID_SUFFIX, 
  V.CONTACT, 
  V.DATE, 
  V.LAT, 
  V.LONG,
  V.CITY,
  V.STATE,
  V.COUNTRY,
  
  V.ESTABLISHMENTNAME, 
  V.QRID 
FROM 
  (
    SELECT 
      B.*, 
      B.TOR || B.DOR || B.CLIENTID || LPAD(
        trim(
          to_char(BQ.index, 'xxxxxx')
        ), 
        3, 
        0
      ) QR_ID_SUFFIX 
    FROM 
      BATCHES_V B, 
      BATCHES_TRACKINGQRDETAILS_QUAT_V BQ 
    WHERE 
      B.OID = BQ.BATCHES_OID
  ) X 
  LEFT JOIN (
    SELECT 
      V.OID, 
      V.QRID, 
      VS.CONTACT, 
      VS.DATE, 
      VS.ESTABLISHMENTNAME, 
      VS.LAT, 
      VS.LONG,
      VS.CITY,VS.STATE,VS.COUNTRY
    FROM 
      VECTORTABLE9_V V 
      LEFT JOIN VECTORTABLE9_SCANNERDETAILS_V VS ON V.OID = VS.VECTORTABLE9_OID
  ) V ON SUBSTRING(V.QRID, 10)= X.QR_ID_SUFFIX;
  

---------------------------------------------------------------------------------------------------------

create or replace view VEOTS_PROD_DB.ANALYTICS.QR_TERT_V(
	BATCH_OID,
	CLIENTID,
	TOR,
	DOR,
	TYPE,
	QR_ID_SUFFIX,
	QRID,
	DATE,
	ESTABLISHMENTNAME,
	LAT,
	LONG,
	CITY,
	STATE,
	COUNTRY,
	CONTACT
) as
SELECT 
  X.OID BATCH_OID, 
  X.CLIENTID, 
  X.TOR, 
  X.DOR, 
  X.TYPE, 
  X.QR_ID_SUFFIX, 
  --V.DIRTYBIT,
  V.QRID ,
  VS.DATE, 
      VS.ESTABLISHMENTNAME, 
      VS.LAT, 
      VS.LONG,
      VS.CITY,VS.STATE,VS.COUNTRY,
      VS.CONTACT
FROM 
  (
    SELECT 
      B.*, 
      B.TOR || B.DOR || B.CLIENTID || LPAD(
        trim(
          to_char(BQ.index, 'xxxxxx')
        ), 
        3, 
        0
      ) QR_ID_SUFFIX 
    FROM 
      BATCHES_V B, 
      BATCHES_TRACKINGQRDETAILS_TERT_V BQ 
    WHERE 
      B.OID = BQ.BATCHES_OID
  ) X 
  LEFT JOIN VECTORTABLE8_V V ON SUBSTRING(V.QRID, 11)= X.QR_ID_SUFFIX
  left join VECTORTABLE8_SCANNERDETAILS_V VS ON V.OID = VS.VECTORTABLE8_OID;


  --------------------------------------------------------------------------------------------------------


create or replace view VEOTS_PROD_DB.ANALYTICS.FINAL_PRODUCT_QR_V(
	BATCH_OID,
	BRAND,
	"Product Name",
	CLIENTID,
	TYPE,
	SUBTYPE,
	ENTITY,
	CUSTOMER_AGE_GROUP,
	CUSTOMER_GENDER,
	PRODUCT_ID,
	PRODREACHEDCUS,
	PURCHASEDATE,
	"Product Expiry date",
	WARRANTYPERIOD,
	BATCH_CREATION_DATE,
	"Batch Expiry date",
	SHELFLIFE,
	WARRANTYAPP,
	QRID_COUNT
) as
SELECT BATCH_OID
    ,BRAND
    ,"Product Name"
	,CLIENTID
	,TYPE
	,SUBTYPE
	,ENTITY
	,CUSTOMER_AGE_GROUP
	,CUSTOMER_GENDER
	,PRODUCT_ID
	,PRODREACHEDCUS
	,PURCHASEDATE
	,"Product Expiry date" 
	,WARRANTYPERIOD
	,BATCH_CREATION_DATE
	,"Batch Expiry date" 
	,SHELFLIFE
	,WARRANTYAPP
	,count(DISTINCT QRID) QRID_COUNT
FROM PRODUCT_QR
GROUP BY 
    BATCH_OID
    ,BRAND
    ,"Product Name"
	,CLIENTID
	,TYPE
	,SUBTYPE
	,ENTITY
	,CUSTOMER_AGE_GROUP
	,CUSTOMER_GENDER
	,PRODUCT_ID
	,PRODREACHEDCUS
	,PURCHASEDATE
	,"Product Expiry date" 
	,WARRANTYPERIOD
	,BATCH_CREATION_DATE
	,"Batch Expiry date" 
	,SHELFLIFE
	,WARRANTYAPP;

create or replace table product_qr as
select * from product_qr_v;

    -------------------------------------------------------------------------------------------------

create or replace view QR_SCANS_V as
SELECT 
    BATCH_OID
    ,BRAND
    ,"Product Name"
    ,SCAN_OID
	,ESTABLISHMENTNAME
	,REASON
    ,LAT
    ,LONG
	,CITY
	,STATE
	,COUNTRY
    ,PRODREACHEDCUS
	,QRID
    ,FR_OID
FROM PRODUCT_QR
WHERE LAT is not null;