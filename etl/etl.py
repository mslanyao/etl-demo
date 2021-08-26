import os
import json
import boto3
s3_client =boto3.client('s3')
dynamodb = boto3.resource('dynamodb',region_name='ap-southeast-2')

tablename = os.environ['TABLE_NAME']

def handler(event, context):
    print('request: {}'.format(json.dumps(event)))
    table = dynamodb.Table(tablename)

    for record in event["Records"]:
        bucket_name = record["s3"]["bucket"]["name"]
        file_name = record["s3"]["object"]["key"]
        print('bucket is: ' + bucket_name)
        print('file is: ' + file_name)

        s3_clientobj = s3_client.get_object(Bucket=bucket_name, Key=file_name)

        for line in s3_clientobj['Body'].iter_lines():
            fields = line.decode("utf-8").split(",")
            
            ## Transformation
            id = fields[0]
            value =  fields[1].upper()
            response = table.put_item(
            Item={
                    'id': id,
                    'value': value
                }
            )
            print(response)

    return True