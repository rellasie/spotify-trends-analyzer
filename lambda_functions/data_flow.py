import json
import boto3
from botocore.exceptions import ClientError

s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')
glue_client = boto3.client('glue')

SIZE_THRESHOLD_MB = 25
GLUE_JOB_NAME = 'glue_transform_data'
LAMBDA_FUNCTION_NAME = 'lambda_transform_data'

def lambda_handler(event, context):
    try:
        s3_event = event['Records'][0]['s3']
        size_bytes = s3_event['object']['size']
        object_key = s3_event['object']['key']
        bucket_name = s3_event['bucket']['name']
        
        size_mb = size_bytes / (1024 * 1024)
        
        arguments = {
            '--bucket': bucket_name,
            '--key': object_key
        }
        
        if size_mb >= SIZE_THRESHOLD_MB:
            response = process_large_file(arguments)
            status = 'Large file processed'
        else:
            response = process_small_file(bucket_name, object_key)
            status = 'Small file processed'
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': status,
                'response': response
            })
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }

def process_large_file(arguments):
    try:
        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments=arguments
        )
        return f"Glue job started with run ID: {response['JobRunId']}"
    except ClientError as e:
        raise Exception(f"Failed to start Glue job: {str(e)}")

def process_small_file(bucket, key):
    try:
        payload = {
            'bucket': bucket,
            'key': key
        }
        response = lambda_client.invoke(
            FunctionName=LAMBDA_FUNCTION_NAME,
            InvocationType='Event',
            Payload=json.dumps(payload)
        )
        return f"Lambda function invoked with status code: {response['StatusCode']}"
    except ClientError as e:
        raise Exception(f"Failed to invoke Lambda function: {str(e)}")