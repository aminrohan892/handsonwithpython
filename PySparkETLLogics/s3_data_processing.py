##########################################################################################################
import boto3

# Calculate the total number of objects and size of folders in a given S3 bucket using boto3
def calculate_folder_stats(bucket_name, folder_name):
    s3 = boto3.client('s3')
    total_objects = 0
    total_size = 0

    # List objects in the folder
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

    # Iterate through objects and sum up their sizes
    for obj in response['Contents']:
        total_objects += 1
        total_size += obj['Size']

    # Check if there are more pages of results
    while response['IsTruncated']:
        # Get the next page of results
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name, ContinuationToken=response['NextContinuationToken'])
        
        # Iterate through objects and filter out folders
        for obj in response['Contents']:
            total_objects += 1
            total_size += obj['Size']

    return total_objects, total_size

def list_s3_folders(bucket_name):
    s3 = boto3.client('s3')
    folders = []

    # List objects in the bucket
    response = s3.list_objects_v2(Bucket=bucket_name)

    # Iterate through objects and filter out folders
    for obj in response['Contents']:
        if obj['Key'].endswith('/'):
            folders.append(obj['Key'])

    # Check if there are more pages of results
    while response['IsTruncated']:
        # Get the next page of results
        response = s3.list_objects_v2(Bucket=bucket_name, ContinuationToken=response['NextContinuationToken'])
        
        # Iterate through objects and filter out folders
        for obj in response['Contents']:
            if obj['Key'].endswith('/'):
                folders.append(obj['Key'])

    return folders

# Usage example
bucket_name = 'thm-funops-redshift-data'
s3_folders = list_s3_folders(bucket_name)

# Print the list of folders
for folder in s3_folders:
    total_objects, total_size = calculate_folder_stats(bucket_name, folder)
    print(f"The folder {folder} in bucket thm-funops-redshift-data contains {total_objects} with total size of {total_size}")
    #print(f"The folder {folder} in bucket thm-funops-redshift-data")

