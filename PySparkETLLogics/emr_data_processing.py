#########################################################################################################
# Logic to move files of certain format from one S3 location to another.
import boto3

def move_files(source_bucket, source_folder, destination_bucket, destination_folder, file_extension):
    s3 = boto3.client('s3')
    # List objects in the source folder
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_folder)
    # Iterate through each object
    for obj in response['Contents']:
        key = obj['Key']
        # Check if the object has the desired file extension
        if key.endswith(file_extension):
            # Construct the new key for the destination folder
            new_key = key.replace(source_folder, destination_folder)
            # Copy the object to the destination bucket and folder
            s3.copy_object(
                Bucket=destination_bucket,
                Key=new_key,
                CopySource={'Bucket': source_bucket, 'Key': key}
            )
            # Delete the object from the source bucket and folder
            s3.delete_object(Bucket=source_bucket, Key=key)

# Usage example
move_files('amplitude-export-6652', '139599/', 'amplitude-export-6652', 'completed_files/', '_complete')

#########################################################################################################

# Logic to Process GZ JSON files and write to Parquet
from pyspark.sql import SparkSession
import re
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType,LongType
from datetime import datetime, timedelta
import traceback
def process_gz_files(source_bucket, source_folder, destination_bucket, destination_folder,filename):
    spark = SparkSession.builder.appName("GZ to Parquet").getOrCreate()
    # Read gz files from source S3 bucket
    source_bucket = 'amplitude-export-6652'
    source_folder = '139599'
    # Read gz files from source S3 bucket as text
    df = spark.read.text(f"s3a://{source_bucket}/{source_folder}/{filename}")
    # Convert each row of the dataframe into JSON
    df_json = df.toJSON()
    # Iterate through each JSON string and perform desired operations
    json_df = spark.read.json(df_json)
    # Show the dataframe
    #json_df.show()
    # Define the schema for the JSON data
    schema = StructType([
        StructField("insert_id", StringType(), True),
        StructField("insert_key", StringType(), True),
        StructField("schema", StringType(), True),
        StructField("adid", StringType(), True),
        StructField("amplitude_attribution_ids", StringType(), True),
        StructField("amplitude_event_type", StringType(), True),
        StructField("amplitude_id", LongType(), True),
        StructField("app", IntegerType(), True),
        StructField("city", StringType(), True),
        StructField("client_event_time", StringType(), True),
        StructField("client_upload_time", StringType(), True),
        StructField("country", StringType(), True),
        StructField("data", StringType(), True),
        StructField("data_type", StringType(), True),
        StructField("device_brand", StringType(), True),
        StructField("device_carrier", StringType(), True),
        StructField("device_family", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("device_manufacturer", StringType(), True),
        StructField("device_model", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("dma", StringType(), True),
        StructField("event_id", LongType(), True),
        StructField("event_properties", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("global_user_properties", StringType(), True),
        StructField("group_properties", StringType(), True),
        StructField("groups", StringType(), True),
        StructField("idfa", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("is_attribution_event", StringType(), True),
        StructField("language", StringType(), True),
        StructField("library", StringType(), True),
        StructField("location_lat", DoubleType(), True),
        StructField("location_lng", DoubleType(), True),
        StructField("os_name", StringType(), True),
        StructField("os_version", StringType(), True),
        StructField("partner_id", StringType(), True),
        StructField("paying", StringType(), True),
        StructField("plan", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("processed_time", StringType(), True),
        StructField("region", StringType(), True),
        StructField("sample_rate", StringType(), True),
        StructField("server_received_time", StringType(), True),
        StructField("server_upload_time", StringType(), True),
        StructField("session_id", LongType(), True),
        StructField("source_id", StringType(), True),
        StructField("start_version", StringType(), True),
        StructField("user_creation_time", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("user_properties", StringType(), True),
        StructField("uuid", StringType(), True),
        StructField("version_name", StringType(), True)
    ])
    # Parse the filtered lines as JSON
    json_df2 = json_df.select(F.from_json(F.col("value"), schema).alias("data")).select("data.*")
    # Extract date from filename using regular expression
    date_regex = r"(\d{4}-\d{2}-\d{2})"
    json_df3 = json_df2.withColumn("date", F.regexp_extract(F.input_file_name(), date_regex, 1))
    destination_bucket = 'data-eng-prod-silver-layer-amplitude-events'
    destination_folder = 'raw_events/'
    # Write dataframe to parquet files in destination S3 bucket partitioned by date
    json_df3.repartition(5).write.partitionBy("date").parquet(f"s3://{destination_bucket}/{destination_folder}", "append")
    spark.stop()

start_date = datetime(2023, 5, 9)
end_date = datetime(2021, 12, 31)
current_date = start_date
while current_date <= end_date:
    try:
        year = current_date.year
        month = str(current_date.month).zfill(2)
        day = str(current_date.day).zfill(2)
        filename = f"139599_{year}-{month}-{day}*.gz"
        print(filename)
        process_gz_files('amplitude-export-6652', '139599', 'data-eng-prod-silver-layer-amplitude-events', 'raw_events', filename)
    except Exception as e:
        print(f"Error processing file: {filename}")
        print(str(e))
        with open('exception_log.txt', 'a') as file:
            file.write(f"Error processing file: {filename}\n")
            file.write(traceback.format_exc())
    current_date += timedelta(days=1)

# Spark Submit Command
spark-submit --master yarn --deploy-mode client --executor-memory 10g --num-executors 20 load_raw_amplitude_events.py
