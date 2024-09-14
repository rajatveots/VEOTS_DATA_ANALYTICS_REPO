import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['dbname','sfURL','sfRole','sfUser','sfPassword','sfDatabase','sfSchema','sfWarehouse'])
dbname = args['dbname']
sfURL = args['sfURL']
sfRole = args['sfRole']
sfUser = args['sfUser']
sfPassword = args['sfPassword']
sfDatabase = args['sfDatabase']
sfSchema = args['sfSchema']
sfWarehouse = args['sfWarehouse']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# args = getResolvedOptions(sys.argv,['dbname'])
# job.init(args['JOB_NAME'], args)

from py4j.java_gateway import java_import
java_import(spark._jvm, "net.snowflake.spark.snowflake")

from pyspark.sql.functions import col
sfOptions = {
"sfURL" : sfURL,
"sfRole" :sfRole,
"sfUser" : sfUser,
"sfPassword" :sfPassword,
"sfDatabase" : sfDatabase,
"sfSchema" : sfSchema,
"sfWarehouse" : sfWarehouse
}

query = f"CALL ANALYTICS.insert_overwrite_loop('{dbname}')"
sc._gateway.jvm.net.snowflake.spark.snowflake.Utils.runQuery(sfOptions, query)

job.commit()