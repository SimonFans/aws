import csv
import boto3
import timeit
import datetime

DEBUG=False
gainsight_objects = {
    'Test' : [3, 3, 'gainsight_2.0/csv/'],
    'GainsightCompany' : [100, 22, 'gainsight_2.0/csv/account/'],
    'GainsightCompanyHistory' : [100, 10, 'gainsight_2.0/csv/account-history/'],
    'GainsightAccountContacts' : [100, 13, 'gainsight_2.0/csv/account-contact/'],
    'GainsightCalltoAction' : [100, 48, 'gainsight_2.0/csv/cta/'],
    'GainsightCSTask' : [100, 40, 'gainsight_2.0/csv/cs-task/'],
    'GainsightRelationship' : [100, 30, 'gainsight_2.0/csv/relationship/'],
    'GainsightRelationshipContact' : [100, 8, 'gainsight_2.0/csv/relationship-contact/'],
    'GainsightPlaybook': [100, 12, 'gainsight_2.0/csv/playbook/'],
    'GainsightSuccessPlan' : [100, 43, 'gainsight_2.0/csv/success-plan/'],
    'GainsightTimelineActivity' : [100, 44, 'gainsight_2.0/csv/timeline-activity/'],
    'GainsightTimeline' : [100, 44, 'gainsight_2.0/csv/timeline-activity/'],
    'GainsightActivityAttendee' : [100, 8, 'gainsight_2.0/csv/timeline-activity-attendee/'] ,
    'GainsightUser' : [100, 20, 'gainsight_2.0/csv/users/']
}

def get_header_info(bucket, key):
    print("In get_header_info")
    bucket_name = bucket
    object_name = key
    expression_type = 'SQL'
    expression = """SELECT * FROM S3Object s limit 1"""
    input_serialization = {"CSV": {"FileHeaderInfo": "NONE"},}
    output_serialization = {"CSV": {},}
    response=""
    col_list=[]
    try:
        s3_client = boto3.client('s3')
        response = s3_client.select_object_content(
            Bucket=bucket_name,
            Key=object_name,
            ExpressionType=expression_type,
            Expression=expression,
            InputSerialization=input_serialization,
            OutputSerialization=output_serialization
            )
        for event in response['Payload']:
            if 'Records' in event:
                col_list=(event['Records']['Payload'].decode('utf-8')).split(",")
    except BaseException as err:
        print("Error getting the header info")
        print(err)
    finally:
        return col_list


def validate(bucket, key, size,file_format,file_type,field_names):
    print("In validate")
    bad_size = []
    bad_cols = []
    bad_format = []
    bad_object = []
    new_cols = []

    # file format check
    if file_format.lower() != "csv" :
        bad_format.append(key)

    # file type check
    if file_type in gainsight_objects.keys():
        val = gainsight_objects[file_type]

        # check if file is empty or less than minimum size expected
        if size < val[0] :
            bad_size.append(key)

        if field_names == None:
            bad_cols.append(key)
        else:
            num_cols = len(field_names)
            #if num_cols=0 , it means there was error while retrieving header info. We want to ingnore this.
            if num_cols > 0 and num_cols < val[1] :
                bad_cols.append(key)
            elif num_cols > val[1] :
                new_cols.append(key)
    else:
        bad_object.append(key)

    return {'bad_format': bad_format, 'bad_size': bad_size, 'bad_cols': bad_cols, 'bad_object': bad_object, 'new_cols': new_cols}

def send_email(bad_cols, bad_size, bad_format, bad_object,new_cols, bucket, action):
    print("In send_email")
    email_addresses = ["<email>"]
    if DEBUG:
        print("bad_cols", len(bad_cols))
        print("bad_size", len(bad_size))
        print("bad_format", len(bad_format))
        print("bad_object", len(bad_object))

    if len(bad_cols) != 0 or len(bad_size) != 0 or len(bad_format) != 0 or len(bad_object) != 0 :
        subject = str(action) + ' Event from ' + bucket
        body = """
              <body>
              This email is to notify you of the following <strong>errors regarding the Gainsight source data files:<br />
              The following file(s) did not meet the <strong>size validation requirements</strong>:<br />
              {} <br />
              The following file(s) did not meet the <strong>column validation requirements</strong>:<br />
              {}
              <br />
              The following file(s) did not meet the <strong>file format validation requirements</strong>:<br />
              {}
              <br />
              The following file(s) did not meet the <strong>file type validation requirements</strong>:<br />
              {}
              </body>
              """.format('\n'.join(bad_size), '\n'.join(bad_cols), '\n'.join(bad_format), '\n'.join(bad_object))
    elif len(new_cols) > 0 :
        body = """
              <body>
              This email is to notify you of the following <strong>warnings regarding the Gainsight source data files:<br />
              The following file(s) contained <strong>new columns</strong>:<br />
              {}
              </body>
              """.format('\n'.join(new_cols))
    else :
        print('no errors detected in uploaded files')
        # Send email/alert
    try :
        ses_client = boto3.client("ses")
        message = {"Subject":{"Data": subject}, "Body": { "Html": {"Data": body}}}
        response = ses_client.send_email(Source="AWS Tableau Airflow Csg <sizhao@tableau.com>", Destination = {"ToAddresses": email_addresses}, Message =message )
    except BaseException as err:
        print("Error sending alert emails")
        print(err)

def copy_object(bucket, key, file_type, destination_bucket, destination_prefix):
    print("In copy_object")
    try:
        s3_client = boto3.client('s3')
        copy_source_object = {'Bucket': bucket, 'Key': key}
        s3_client.copy(CopySource=copy_source_object, Bucket = destination_bucket, Key = destination_prefix)
        print("Copied " + bucket +"/" + key + " to " + destination_bucket + "/" + destination_prefix)
    except BaseException as err:
        print("Error copying file to destination: " + destination_bucket + "/" + destination_prefix)
        print(err)

def lambda_handler(event, context):
    print("In lambda_handler")
    start = timeit.default_timer()

    #Update this variable based on your environment
    runtime_env = 'PROD'

    if runtime_env == 'DEV':
        destination_bucket = 'ssdp-s3-csai-dsde-dev'
    elif runtime_env == 'QA':
        destination_bucket = 'ssdp-s3-csai-dsde-qa'
    elif runtime_env == 'VNEXT':
        destination_bucket = 'ssdp-s3-csai-dsde-vnext'
    elif runtime_env == 'PROD':
        destination_bucket = 'ssdp-s3-csai-dsde-prod'
    else:
        destination_bucket = 'DEV' #set DEV by default

    print('destination_bucket ', destination_bucket)

    bad_size = []
    bad_cols = []
    bad_format = []
    bad_object = []
    new_cols = []

    # Get the object from the event and show its content type
    for record in event['Records'] :
        bucket = record['s3']['bucket']['name']
        key = str(record['s3']['object']['key']).replace("%3A", ":")
        size = record['s3']['object']['size']
        file_format = key[key.rindex(".")+1:]
        if 'type' in record['s3']['object']:
            file_type = record['s3']['object']['type']
        else :
            if "/" in key:
                file_type = key[key.index("/")+1:key.index("-")]
            else:
                file_type = key[0:key.index("-")]
        action = record["eventName"]
        #get header info from S3 object
        fieldnames = get_header_info(bucket, key)

        #LOGGING
        print("Bucket: ", bucket)
        print("Key: ", key)
        print("Size: ", size, " bytes")
        print("Action ", action)
        print("File_format", file_format)
        print("File_type", file_type)
        print("Fieldnames", fieldnames)

        ret = validate(bucket, key, size,file_format,file_type,fieldnames)
        bad_size = ret['bad_size']
        bad_cols = ret['bad_cols']
        bad_format = ret['bad_format']
        bad_object = ret['bad_object']
        new_cols = ret['new_cols']

        if (len(bad_size) == 0 and len(bad_cols) == 0  and len(bad_format) == 0 and len(bad_object)==0) :
            copy_object(bucket, key, file_type, destination_bucket, gainsight_objects[file_type][2] + key[key.rindex("/")+1:])
        else:
            send_email(bad_cols, bad_size, bad_format, bad_object,new_cols, bucket, action)

    stop = timeit.default_timer()
    print('Time in seconds:', stop - start)
