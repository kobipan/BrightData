import datetime

import requests;
import boto3;
import json;
import multiprocessing
#from connections import set_redshift_con;

FREQNAMES = ['YEARLY', 'MONTHLY', 'WEEKLY', 'DAILY', 'HOURLY', 'MINUTELY', 'SECONDLY']

(YEARLY,
 MONTHLY,
 WEEKLY,
 DAILY,
 HOURLY,
 MINUTELY,
 SECONDLY) = list(range(7))

region = 'us-west-2'
bucket_name = 'yellowtrip'

def download_file(month_id):
    url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    #print(requests.get(url).content)
    filename = f"yellow_tripdata_{month_id}"
    base_url= f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}.parquet"
    client = set_client()
    print(base_url)
    response = requests.get(base_url)
    response.raise_for_status()
    file_name_list = filename.split('_')
    try:
        client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region})
        print(f"new bucket was created {bucket_name}")
    except:
        print(f"file will uploaded to {bucket_name}")
    url_content = response.content
    #print(url_content)
    client.put_object(Body=url_content, Key=filename,Bucket = bucket_name)

def download_files_parallel(month_to_upload: list):
    pool = multiprocessing.Pool()
    pool.map(download_file, month_to_upload )
    pool.close()
    pool.join()

def set_client(c_type  = 's3'):
    cardencials = {'key_id': 'AKIA2Y242EDD72TSNLLN', 'secret': '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'}
    return set_aws_connection(cardencials, connection_type= c_type, aws_region=region)

def set_aws_connection(cardencials,connection_type,aws_region ):
    aws_access_key_id = cardencials.get('key_id')
    aws_secret_access_key = cardencials.get('secret')
    return boto3.client(connection_type, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_region)


def run_query(query_string):
    athena_client = set_client(c_type='athena')
    query_execution_context = {"Catalog": "awsdatacatalog", "Database": "brightdata"}
    response = athena_client.start_query_execution(
        QueryString=query_string, QueryExecutionContext=query_execution_context, WorkGroup="primary"
    ,ResultConfiguration = {'OutputLocation': 's3://athenabkt/monthly_passengers_count', }
    )
    return response


def get_num_of_pasangers():
    #this function will create csv file on aws containg the aggregated passengers by month
    qury = """SELECT  date_trunc('month',tpep_pickup_datetime) month_id
                ,cast(sum(Passenger_count) as int) Passenger_count
                FROM "AwsDataCatalog"."brightdata"."yellowtrip" 
                group by 1  order by 1 """
    try:
        athena_client = set_client(c_type='athena')
        response =run_query(qury)
        query_execution_id = response['QueryExecutionId']
        athena_client.get_waiter('query_execution_complete').wait(QueryExecutionId=query_execution_id )
        return athena_client.get_query_results(QueryExecutionId=query_execution_id)
    except Exception as e:
        print(e.__str__())

def set_list_of_month():
    s3_client = set_client(c_type='s3')
    response = s3_client.list_objects(bucket_name)
    print(response)


