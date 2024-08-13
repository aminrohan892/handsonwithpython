########################################################################################################
"""

Reason: This contains EMR PySpark Code to load Kinesis files to Silver Layer in Parquet format
Source: s3://thm-infrastructure-prod-events/
Destination: s3://data-eng-prod-silver-layer-helios-events/backend_events/

"""
########################################################################################################


from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import length
from pyspark.sql.functions import date_format
from pyspark.sql.functions import to_date
from pyspark.sql import functions as F
from pyspark.sql import types as T
import subprocess
import os
import boto3
import base64
import json
from pyspark.sql.functions import sha2, concat_ws
from pyspark.sql.functions import current_date
from pyspark.sql.types import (
    StructType,
    StringType,
    StructField,
    IntegerType,
    TimestampType,
    BooleanType,
    DateType,
    LongType,
)
from pyspark.sql.functions import *
from datetime import date, timedelta

# Initiate Spark Context
sc = SparkContext()

# Initiate Spark's SQL Contexta
sqlctx = SQLContext(sc)

schema = StructType(
    [
        StructField("version", StringType(), True),
        StructField("verb", StringType(), True),
        StructField("app", StringType(), True),
        StructField("course", StringType(), True),
        StructField("course_type", StringType(), True),
        StructField("experimental_group", StringType(), True),
        StructField("has_tophat_email", StringType(), True),
        StructField("server_code", StringType(), True),
        StructField("th_context", StringType(), True),
        StructField("event_time", LongType(), True),
        StructField("timezone_offset", StringType(), True),
        StructField("trial_user", StringType(), True),
        StructField("user", StringType(), True),
        StructField("role", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("web_platform", StringType(), True),
        StructField("online", StringType(), True),
        StructField("indirect_object_child_type", StringType(), True),
        StructField("indirect_object_grandchild_type", StringType(), True),
        StructField("indirect_object_id", StringType(), True),
        StructField("indirect_object_type", StringType(), True),
        StructField("object_child_type", StringType(), True),
        StructField("object_grandchild_type", StringType(), True),
        StructField("object_id", StringType(), True),
        StructField("object_type", StringType(), True),
        StructField("prepositional_object_child_type", StringType(), True),
        StructField("prepositional_object_grandchild_type", StringType(), True),
        StructField("prepositional_object_id", StringType(), True),
        StructField("prepositional_object_type", StringType(), True),
        StructField("subject_child_type", StringType(), True),
        StructField("subject_grandchild_type", StringType(), True),
        StructField("subject_id", StringType(), True),
        StructField("subject_type", StringType(), True),
        StructField("org", StringType(), True),
        StructField("plan", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("is_impersonating", StringType(), True),
        StructField("browser_fingerprint", StringType(), True),
        StructField("extras", StringType(), True),
        StructField("data_namespace", StringType(), True),
        StructField("section_name", StringType(), True),
    ]
)


####################### LOGIC TO PROCESS YESTERDAY's FILES #######################
start_date = date(2024, 3, 25)
end_date = date.today()

delta = timedelta(days=1)
current_date = start_date

while current_date <= end_date:
    # Logic to process files for current_date goes here
    yesterday_date = current_date
    yesterday_year = str(yesterday_date.year)
    if len(str(yesterday_date.month)) == 1:
        yesterday_month = "0" + str(yesterday_date.month)
    else:
        yesterday_month = str(yesterday_date.month)

    if len(str(yesterday_date.day)) == 1:
        yesterday_day = "0" + str(yesterday_date.day)
    else:
        yesterday_day = str(yesterday_date.day)

    s3 = boto3.client("s3")
    bucket_name = "thm-infrastructure-prod-events"

    # Load all Files from the uploaded_files/ sub folder
    file_key_prefix = (
        "uploaded_files"
        + "/"
        + yesterday_year
        + "/"
        + yesterday_month
        + "/"
        + yesterday_day
    )
    paginator = s3.get_paginator("list_objects")
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=file_key_prefix)
    for page in page_iterator:
        for key in page["Contents"]:
            key = key["Key"]
            head = s3.head_object(Bucket=bucket_name, Key=key)
            initial_metadata = head["Metadata"]
            if "loaded-to-silver-layer" in initial_metadata.keys():
                print(str(key) + " is loaded to Silver Layer")
            else:
                print(str(key) + " needs to be processed")

                df = sqlctx.read.schema(schema).json(f"s3://{bucket_name}/{key}")

                # Convert epoch time to Human readable Timestamp
                df = df.withColumn(
                    "event_time",
                    concat_ws(
                        ".",
                        from_unixtime(
                            substring(col("event_time"), 0, 10), "yyyy-MM-dd HH:mm:ss"
                        ),
                        substring(col("event_time"), -3, 3),
                    ),
                )

                # Add Partition Columns
                df = df.withColumn(
                    "_partitionByDate", date_format("event_time", "yyyy/MM/dd")
                )
                df = df.withColumn("_partitionByYear", date_format("event_time", "yyyy"))
                df = df.withColumn("_partitionByMonth", date_format("event_time", "MM"))
                df = df.withColumn("_partitionByDay", date_format("event_time", "dd"))

                # Add Record Hash Column
                df = df.withColumn(
                    "_recordHash",
                    sha2(concat_ws("||", *df.columns), 256),
                )

                # Add Record Ingest Timestamp & Filename
                df = df.withColumn("_ingestDatatime", current_timestamp())
                df = df.withColumn("_sourceFile", lit(key))

                # Write to S3 in Parquet format
                df.write.mode("append").format("parquet").partitionBy(
                    ["_partitionByYear", "_partitionByMonth", "_partitionByDay"]
                ).save(
                    compression="gzip",
                    path="s3://data-eng-prod-silver-layer-helios-events/backend_events/",
                )

                loaded_metadata = {}
                loaded_metadata["loaded-to-silver-layer"] = "true"
                s3.copy_object(
                    Bucket=bucket_name,
                    Key=key,
                    CopySource={"Bucket": bucket_name, "Key": key},
                    Metadata=loaded_metadata,
                    MetadataDirective="REPLACE",
                )
    
    current_date += delta
