# Goal: Detect a paragraph of texts using AWS Comprehend, return the result ('Positive', 'Negative', 'Neutral', 'Mixed')

# Design process:  
S3 -> Lambda -> Comprehend

# Role: Use the role with ComprehendFullAccess and AWSLambdaExecute

# Code:

import boto3

def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.client("s3")
    bucket = "<S3 bucket name>"
    key = "<S3 object name>"
    file = s3.get_object(Bucket = bucket, Key = key)
    paragraph = str(file['Body'].read())
    print(paragraph)
    
    comprehend = boto3.client('comprehend')
    response = comprehend.detect_sentiment(Text = paragraph, LanguageCode = 'en')
    
    print(response)
    return 'Result comes up'
