
from awsglue.context import GlueContext
from pyspark.context import SparkContext
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

# Get arguments passed from Lambda or Glue
#args = getResolvedOptions(sys.argv, ['S3_Source_file', 'S3_Destination_file'])

source_path = "s3://trans123321/abcd/"
destination_path = "s3://automationteraform2310201/cleaned_data/"
# Create Spark and Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Read CSV from S3
df = spark.read.csv(source_path, inferSchema=True, header=True)

# Drop unwanted columns
columns_to_drop = ["community_area", "ward"]
df = df.drop(*columns_to_drop)

# Coalesce to a single output file
df_single = df.coalesce(1)

# Write back to S3 in CSV format
df_single.write.mode("overwrite").option("header", "true").csv(destination_path)
