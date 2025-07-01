import json
import boto3
import os

# Initialize the S3 and SNS clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

# Retrieve the SNS Topic ARN from a configured environment variable
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']

def lambda_handler(event, context):
    """
    This function is triggered by an S3 event. It reads a text file,
    counts the words, and sends the result to an SNS topic.
    """
    try:
        # 1. Get the bucket and file name from the S3 event trigger
        s3_event = event['Records'][0]['s3']
        bucket_name = s3_event['bucket']['name']
        file_name = s3_event['object']['key']

        # 2. Download the file content from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
        file_content = response['Body'].read().decode('utf-8')

        # 3. Count the words and format the notification message
        word_count = len(file_content.split())
        subject = "Word Count Result"
        message = f"The word count in the {file_name} file is {word_count}."

        # 4. Publish the message to the SNS topic
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )

        return {
            'statusCode': 200,
            'body': json.dumps(f"Successfully processed {file_name}.")
        }

    except Exception as e:
        print(f"Error processing file: {e}")
        raise e