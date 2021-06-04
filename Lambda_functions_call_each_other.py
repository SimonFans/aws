# Goal: Implement to invoke lambda function from another lambda function 

# Pre-condition: Create or use the role with 

# Create two functions:
# Function 1:

import boto3, json

def lambda_handler(event, context):
    invoke_instance = boto3.client('lambda', region_name = 'us-west-2')
    payload = {"my_array":[1,2,3]}
    response = invoke_instance.invoke(FunctionName = 'Lambda_function_2', InvocationType = 'RequestResponse', Payload = json.dumps(payload))
    res = response['Payload'].read()
    print(res)

# Function 2:

import json

def lambda_handler(event, context):
    # TODO implement
    received_array = event['my_array']
    sum_res = 0
    for num in received_array:
        sum_res += num
    return 'Return value is {0}'.format(sum_res)
  
# Click Monitor & use Cloudwatch to view logs
