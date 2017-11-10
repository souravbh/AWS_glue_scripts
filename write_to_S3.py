import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())

# catalog: database and table names
db_name = "s3write"

# output s3 and temp directories
output_history_dir = "s3://<<path>>"

# Create dynamic frames from the source tables 
geolocation = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name="geolocation_csv")

# Keep the fields we need and rename some.
geolocation = geolocation.rename_field('idling_ind', 'idling_ind1')

# Write out the dynamic frame into parquet in "legislator_history" directory parquet
print "Writing to /s3/geolocation/file ..."
glueContext.write_dynamic_frame.from_options(frame = geolocation, connection_type = "s3", connection_options = {"path": output_history_dir}, format = "json")


