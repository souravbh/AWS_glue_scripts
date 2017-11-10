import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.ml.feature import SQLTransformer
from pyspark.sql import SparkSession

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# catalog: database and table names
db_name = "s3geo"

# output s3 and temp directories
output_history_dir = "s3://<give your path>>"

# Create dynamic frames from the source tables 
geolocation = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name="geo")

#use spark data frame objects
geolocation_df = geolocation.toDF()
geolocation_df.createOrReplaceTempView("geoTable")
geolocation_sql_df = spark.sql("SELECT driverid, AVG(velocity) as velavg FROM geoTable GROUP BY driverid")
geolocation_sql_dyf = DynamicFrame.fromDF(geolocation_sql_df, glueContext, "geolocation_sql_dyf")

# Write out the dynamic frame into parquet in "legislator_history" directory parquet
print "Writing to /s3/geolocation/file ..."
glueContext.write_dynamic_frame.from_options(frame = geolocation_sql_dyf, connection_type = "s3", connection_options = {"path": output_history_dir}, format = "json")

#dywriter = DynamicFrameWriter.__init__(glueContext)
#dywriter.from_options(frame = geolocation_sql_dyf, connection_type = "s3", connection_options = {"path": output_history_dir}, format = "json")
