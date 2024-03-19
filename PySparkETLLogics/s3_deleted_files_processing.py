######################################################################################################
# Logic to identify the total size of deleted files in an S3 bucket
import boto3

def get_deleted_files_size(bucket_name):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_object_versions')
    pages = paginator.paginate(Bucket=bucket_name, Prefix='')

    total_size = 0

    for page in pages:
        for version in page.get('Versions', []):
            if version['IsLatest'] is False:
                total_size += version['Size']

    return total_size

# Usage example
bucket_name = 'thm-funops-redshift-data'
deleted_files_size = get_deleted_files_size(bucket_name)
print(f"The total size of deleted files in bucket '{bucket_name}' is {deleted_files_size} bytes.")