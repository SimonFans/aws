# ! pip install smartsheet-python-sdk
# Set up environment variable
# Set up Cron Job: CloudWatch Events also called EventBridge. Navigate to configuration -> Triggers
# Add Layers to support your python modules
# Upload zip file to support additional python modules, follow section "Create the deployment package", https://docs.aws.amazon.com/lambda/latest/dg/python-package-create.html
# Set up your IAM policies

import smartsheet
import gzip
import os
import json
import boto3

def main(event, context):

	# initiate the connection to S3
	s3 = boto3.client("s3")
	
	# Initialize client
	smartsheet_token = os.environ['SmartSheetToken']
	smartsheet_client = smartsheet.Smartsheet(smartsheet_token)
	
	# Make sure we don't miss any errors
	smartsheet_client.errors_as_exceptions(True)

	
	SHEET_ID = os.environ['SheetID']

	# Get all columns
	action = smartsheet_client.Sheets.get_columns(SHEET_ID, include_all=True)
	columns = action.data
	cols = []
	for col in columns:
		cols.append(col.title)

	# Get each card info
	values = []
	sheet_object = smartsheet_client.Sheets.get_sheet(SHEET_ID)
	for row in sheet_object.rows:
		row_values = []
		for cell in row.cells:
			if cell.value:
				row_values.append(cell.value)
			else:
				row_values.append('')
		values.append(row_values)
	
	# Combine card titles with card values
	col_val = []
	for val in values:
		col_val.append(dict(zip(cols, val)))
	print(col_val[0])

	# Convert to JSON
	json_data = json.dumps(col_val, indent=2)
	
	# Convert to bytes
	encoded = bytes(json_data.encode('utf-8'))
	
	# Compress
	compressed = gzip.compress(encoded)
	print('Prepare to upload data into the S3 bucket ....')
	
	# Provide target S3 bucket name and output file name
	bucket = '<your bucket name>'
	#bucket = 'ssdp-s3-csai-dsde-dev'
	filename= 'test_smartsheet' + '.json'
	
	# Write into the S3 bucket
	s3.put_object(Bucket = bucket, Key = filename, Body = encoded)
	print('Your file will land into the bucket', bucket, 'soon!!!')

if __name__ == "__main__":
	main('', '')
  
  
  
 ''' IAM Policies
 Your role should include (1) AWSLambdaBasicExecutionRole and (2) S3 Read/Write access permission (You can create this by yourself in the AWS IAM page)
 
 (1)
 {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "*"
        }
    ]
}

(2) Create your own policy to support lambda to s3 write access

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket",
                "s3:PutObject",
                "s3:PutObjectAcl",
                "s3:GetObjectTagging",
                "s3:PutObjectTagging",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::<Your S3 bucket name>",
                "arn:aws:s3:::<Your S3 bucket name>/*"
            ]
        }
      
    ]
}
 
 
 '''
