--This is the view used to get the maximum created and updated date 
--so that in the next glue job only the new data which is not processed will be loaded


create or replace view table_dates_v as
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