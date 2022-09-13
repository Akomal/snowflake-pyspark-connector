import pyspark as spark
from pyspark.sql import SparkSession

from pyspark.sql.types import *

spark = SparkSession.builder \
    .master("local") \
    .appName("snowflake-test") \
    .config('spark.jars', 'jar/snowflake-jdbc-3.13.15.jar,jar/spark-snowflake_2.12-2.10.0-spark_3.2.jar') \
    .getOrCreate()



SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
snowflake_database = "FHIR"
snowflake_schema = "PUBLIC"
source_table_name = "claims"
sfOptions = {
    "sfURL" : "" ,
    "sfUser" : "g" ,
    "sfPassword" : "",
    "sfDatabase" : snowflake_database,
    "sfSchema" : snowflake_schema,
    "sfWarehouse" : "COMPUTE_WH"
    }



select_row=""


df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
        .options(**sfOptions) \
        .option("query",select_row) \
        .load()
df.show()


