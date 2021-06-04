# Goal: Detect a paragraph of texts using AWS Comprehend, return the result ('Positive', 'Negative', 'Neutral', 'Mixed')

# Design process:  
S3 -> Lambda -> Comprehend

# Role: Use the role with ComprehendFullAccess and AWSLambdaExecute

# Code:

import boto3
from pprint import pprint # Better to view the result

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
    
    entities = comprehend.detect_entities(Text = paragraph, LanguageCode = 'en')
    pprint(entities)
    
    keyphrase = comprehend.detect_key_phrases(Text = paragraph, LanguageCode = 'en')
    pprint(keyphrase)
    
    return 'Result comes up'

# Result:

# {'Sentiment': 'NEGATIVE', 'SentimentScore': {'Positive': 0.009003359824419022, 'Negative': 0.5344029664993286, 'Neutral': 0.4555799067020416, 'Mixed': 0.0010137574281543493}, 'ResponseMetadata': {'RequestId': '2dee499c-c6ce-4ef1-b1e7-555a55686ef5', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amzn-requestid': '2dee499c-c6ce-4ef1-b1e7-555a55686ef5', 'content-type': 'application/x-amz-json-1.1', 'content-length': '164', 'date': 'Fri, 04 Jun 2021 23:00:48 GMT'}, 'RetryAttempts': 0}}

# Entity returns the word type that comprehend infers
# {'Entities': [{'BeginOffset': 0, 'EndOffset': 6, 'Score': 0.9257438778877258, 'Text': "b'Ford", 'Type': 'PERSON'}

# Key Phrases detects the key noun phrases found in the text. The score reflects the level of confidence that Amazon Comprehend has in the accuracy of the detection.
#{'BeginOffset': 89, 'EndOffset': 103, 'Score': 1.0, 'Text': 'the department'}

# Check link here to get more details about the comprehend method:
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/comprehend.html
