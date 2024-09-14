##Created By Mudit For loading MongoDB collections Data into S3 in Parquet Format


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pymongo import MongoClient
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark.sql.functions import col	


import time
from datetime import date
import boto3
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

args = getResolvedOptions(sys.argv,['db','bucket','mongo_uri','excluded_clients'])
db_name =args['db']
bucket_name=args['bucket']
mongo_uri =args['mongo_uri']
excluded_clients=args['excluded_clients']

def get_all_collections(db):
   #Get names of all the collection in MongoDB Database
    uri = mongo_uri
    client = MongoClient(uri, server_api=ServerApi('1'))
    mydatabase = client[db]
    collections = mydatabase.list_collection_names()

    return collections

def collection_lists(collections):
    #Create list of all the static and Dynamic Collections
    tors = []
    dors = []
    vectortable4_1 =[]
    vectortable4_2 =[]
    vectortable6=[]
    vectortable7=[]
    vectortable8=[]
    vectortable9=[]
    static_coll =[]
    
    for i in collections :
   
        if '_tors' in i :
            tors.append(i)
        if '_dors' in i :
            dors.append(i)
        if '_vectortable4_1' in i :
            vectortable4_1.append(i)
        if '_vectortable4_2' in i :
            vectortable4_2.append(i)    
        if '_vectortable6' in i :
            vectortable6.append(i)
        if '_vectortable7' in i :
            vectortable7.append(i) 
        if '_vectortable8' in i :
            vectortable8.append(i) 
        if '_vectortable9' in i :
            vectortable9.append(i)    
            

        
    static_coll = list (set(collections) -set(tors) -set(dors) - set(vectortable4_1) - set(vectortable4_2) -set(vectortable6) -set(vectortable7)-set(vectortable8)-set(vectortable9))        
    return tors,dors,vectortable4_1,vectortable4_2,static_coll , vectortable6,vectortable7,vectortable8,vectortable9

def none_type_to_str(df):
# get dataframe schema
    my_schema = list(df.schema)
    
    null_cols = []
    
    # iterate over schema list to filter for NullType columns
    for st in my_schema:
        if str(st.dataType) == 'NullType':
            null_cols.append(st)
    
    # cast null type columns to string (or whatever you'd like)
    for ncol in null_cols:
        mycolname = str(ncol.name)
        df = df \
            .withColumn(mycolname, df[mycolname].cast('string'))
    return df
    # import the time module

def mongo_to_DF(collection_name,DB_Name,mongo_uri):
    #Returns A MongoDB collection in a DF
  
    try:
       df = spark.read.format("com.mongodb.spark.sql.DefaultSource")\
       .option("uri", mongo_uri)\
       .option("spark.mongodb.input.database",DB_Name)\
       .option("spark.mongodb.input.collection",collection_name)\
       .load
       
       #df = none_type_to_str(df)
       
       return df 

    except Exception as e:        
       return (str(e) + ' in collection -->'  + collection_name )
        

def write_to_s3_storage(df,bucket_name,folder_name,file_name,coll_name,dt):
    #write to S3 Storage
    s3 = boto3.client('s3')
    s3_url = 's3://' + bucket_name + '/' +folder_name + '/' + coll_name + '/' +dt + '/'
    print(s3_url)
    s3.put_object(Bucket=bucket_name , Key=(folder_name + '/'))
    df.write.json(s3_url + file_name)

def delete_from_S3(bucket,folder_location):
    folder = folder_location +'/'
    s3 = boto3.resource('s3')
    try:
        bucket = s3.Bucket(bucket)
        bucket.objects.filter(Prefix=folder).delete()
        return True
    except Exception as e:
        print(f"Failed to delete s3 folder : {e}")
        return False    

def save_collections_to_S3(coll_list,folder_name,db,bucket_name,mongo_uri):
    #save Mongo collection to S3 . Parameters --> list of collection, Folder Name
    
    import time
    from datetime import date

    dt = date.today()

# get the current time in seconds since the epoch
  
    seconds = time.time()
    try:
        for i in coll_list:
            try:
                df = spark.read.format("com.mongodb.spark.sql.DefaultSource")\
                .option("uri", mongo_uri)\
                .option("spark.mongodb.input.database",db)\
                .option("spark.mongodb.input.collection",i).option("sampleSize", 10000).load()
                
                
                #df = df.fillna("")

                
                bucket_name = bucket_name
                #folder_name = db + '/' + folder_name
                coll_name =i
                file_name = i + '_' + str(int( time.time())) +'.json'
                print(file_name)
                #df.show()
                if df.count() > 0 :
                    write_to_s3_storage(df,bucket_name,db +'/'+ folder_name,file_name,coll_name,str(dt))
                else:
                    print("collection has 0 records: " + coll_name)
            except Exception as e:        
                print(str(e) + ' in collection' + i)
                continue
        
    except Exception as e:
        print(str(e) + ' in collection' + i)


def main():
    db = db_name
    testingdb = get_all_collections(db)
    #coupons = get_all_collections("coupons")
    
    exclude_client_list=excluded_clients.split(',')
    
    # Create a new list excluding the unwanted collections
    filtered_coll_list = []
    filtered_coll_list = [j for j in testingdb if not any(i in j for i in exclude_client_list)]

    tors,dors,vectortable4_1,vectortable4_2,static_coll , vectortable6,vectortable7,vectortable8,vectortable9 = collection_lists(filtered_coll_list)
    static_coll
    coll_folders =['tors','dors','vectortable4_1','vectortable4_2','static_coll' , 'vectortable6','vectortable7','vectortable8','vectortable9']
    coll_list=[tors,dors,vectortable4_1,vectortable4_2,static_coll , vectortable6,vectortable7,vectortable8,vectortable9]
    
    i =0 
    bucket = bucket_name
    folder_location = db
    delete_from_S3(bucket,folder_location)
    while i<len(coll_folders):
        save_collections_to_S3(coll_list[i],coll_folders[i],folder_location,bucket,mongo_uri)
        i =i+1
    #db = coupons_db
    #coupons = get_all_collections(db)
    #bucket = bucket_name
    #folder_location = db
    #delete_from_S3(bucket,folder_location)
    #save_collections_to_S3(coupons,db,folder_location,bucket,mongo_uri)

if __name__ == "__main__":
    main()
    
job.commit()