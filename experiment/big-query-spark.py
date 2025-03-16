import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types

parser = argparse.ArgumentParser()

parser.add_argument('--input-year', required=True)
parser.add_argument('--input-data-type', required=True, choices=['state', 'people-killed'])

args = parser.parse_args()

input_year = args.input_year
input_data_type = args.input_data_type

state_table = "kestra-sandbox-449108.indian_road_accidents.state_wise_road_accidents"
people_killed_table = "kestra-sandbox-449108.indian_road_accidents.people_killed_by_accident_type"

spark = SparkSession.builder \
            .appName("test") \
            .getOrCreate()

bucket = "dataproc-temp-us-central1-432775935527-es23nxzn"
spark.conf.set('temporaryGcsBucket', bucket)

if input_data_type == 'state':
    state_schema = types.StructType([
        types.StructField('year', types.IntegerType(), True), 
        types.StructField('state_ut', types.StringType(), True),  
        types.StructField('accident', types.DoubleType(), True)
    ])

    df_state = spark.read.csv(
        f"gs://indian_road_accidents-sandbox-449108/state_wise_road_accidents/raw/{input_year}/{input_year}.csv", 
        header=True,
        schema=state_schema
    )

    df_state = df_state.withColumn("accident", F.col("accident").cast("int"))
    df_state = df_state.fillna({"accident": 0}) 

    df_state.repartition(4).write \
        .mode("overwrite") \
        .parquet(f"gs://indian_road_accidents-sandbox-449108/state_wise_road_accidents/parquet/{input_year}/")

    df_state.write.format('bigquery') \
        .option('table', state_table) \
        .mode('append') \
        .save()

elif input_data_type == 'people-killed':
    people_killed_schema = types.StructType([
        types.StructField('year', types.IntegerType(), True), 
        types.StructField('type_of_collision', types.StringType(), True),  # Updated field name to match BigQuery table schema
        types.StructField('people_killed', types.IntegerType(), True)
    ])

    df_people_killed = spark.read.csv(
        f"gs://indian_road_accidents-sandbox-449108/people_killed_by_road_accidents/raw/{input_year}/{input_year}.csv", 
        header=True, 
        schema=people_killed_schema
    )

    df_people_killed = df_people_killed.withColumn("people_killed", F.col("people_killed").cast("int"))
    df_people_killed = df_people_killed.fillna({"people_killed": 0}) 

    df_people_killed.repartition(4).write \
        .mode("overwrite") \
        .parquet(f"gs://indian_road_accidents-sandbox-449108/people_killed_by_road_accidents/parquet/{input_year}/")

    df_people_killed.write.format('bigquery') \
        .option('table', people_killed_table) \
        .mode('append') \
        .save()
    
else:
    raise ValueError("Invalid input data type. Choose either 'state' or 'people-killed'.")