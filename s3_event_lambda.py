# description: This lambda function will send you email once any new objects land in the s3 source bucket, then check specific object name and its timestamp, if meet any condition, then move to another S3 target bucket, then delete those objects from the s3 source bucket.



# create a lambda function and insert code here:

import boto3
import json
from datetime import datetime, timedelta
import urllib.parse


def lambda_handler(event, context):
    # TODO implement
    
    for i in event["Records"]:
        action = i["eventName"]
        bucket_name = i["s3"]["bucket"]["name"]
        object = urllib.parse.unquote_plus(i["s3"]["object"]["key"])
        
    client = boto3.client("ses")
    
    
    subject = str(action) + ' Event from ' + bucket_name
    body = """
          <br>
          This email is to notify you regarding {} event.
          <br>
          The object name is {}.
    """.format(action, object)
    
    message = {"Subject":{"Data": subject}, "Body": { "Html": {"Data": body}}}
    
    response = client.send_email(Source="xxx@domain.com", Destination = {"ToAddresses": ["xx@domain.com", "xxx@domain.com"]}, Message =message )
    
    print('Email has been sent')
    
    # This section is for copy and delete task
    
    SOURCE_BUCKET = '<source bucket name>'
    DESTINATION_BUCKET = '<target bucket name>'

    s3_client = boto3.client('s3')

    # Create a reusable Paginator
    paginator = s3_client.get_paginator('list_objects_v2')

    # Create a PageIterator from the Paginator
    page_iterator = paginator.paginate(Bucket=SOURCE_BUCKET)

    # Loop through each object, looking for ones older than a given time period
    for page in page_iterator:
        if "Contents" in page:
            for object in page['Contents']:
                #if object['Key']=='MyOrder.csv':
                if object['Key'].startswith('<give a key prefix name>') and object['LastModified'] <= datetime.now().astimezone() - timedelta(days=20):
                    key_name = object['Key']
                    timestamp = object['LastModified']
                    print(key_name,'was found')
                    print(key_name, '=>', timestamp)
                    
                    #Copy object
                    print('Start to copy', key_name, 'to', DESTINATION_BUCKET)
                    s3_client.copy_object(
                    Bucket=DESTINATION_BUCKET,
                    Key=key_name,
                    CopySource={'Bucket':SOURCE_BUCKET, 'Key':key_name}
                    )
                    print(key_name, 'has been copied to', DESTINATION_BUCKET)
                    
                    # Delete original object
                    s3_client.delete_object(Bucket=SOURCE_BUCKET, Key=key_name)
                    print(key_name,'has been deleted from', SOURCE_BUCKET)
                
        else:
            print('No contents key for page!!!')
            
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }



    
