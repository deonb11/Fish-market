import csv
import boto3
from pprint import pprint as pp
import pandas as pd
import json
import pymongo




s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")
bucket_name = 'data-eng-resources'

s3_object = s3_client.get_object(Bucket = bucket_name, Key = 'python/')

#EXTRACTING DATA FROM S3, LOOPING THROUGH ALL FILES WITH FISH-MARKET
def extract():
    s3_contents = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='python/')
    main_df = []
    for object in s3_contents['Contents']:
        if 'fish-market' in object['Key']:
            file = object['Key']
            # print(file)
            file_csv = s3_client.get_object(Bucket=bucket_name, Key=file)
            df = pd.read_csv(file_csv['Body'])
            main_df.append(df)
    full_info = pd.concat(main_df, axis=0, ignore_index=True )
    return (full_info)
# pp(extract())


#TRANSFORMING DATA, GETTING MEAN AND CONVERTING TO CSV
def trans_to_csv():
    data = extract()
    avg_data= data.groupby('Species').mean()
    fish_csv = 'Deon_Fish.csv'
    avg_data.to_csv(fish_csv, index = True )
    return [fish_csv, avg_data]

# trans_to_csv()
# print(trans_to_csv()[1])
# csv = get_fish_info()
# pp(type(Species_avg(csv)))



# LOADING DATA S3
def load():
    s3_client.upload_file(Filename="Fish.csv", Bucket=bucket_name, Key='Data26/fish/Deon_Fish.csv')

def load_to_local_mongo(csvfile):
    client = pymongo.MongoClient()
    db = client['samples']
    db.drop_collection('proj')
    db.create_collection('proj')
    filename = csvfile

    with open(filename, 'r') as data:
        for line in csv.DictReader(data):
            db.proj.insert_one(line)


def load_to_ec2(csvfile):
    client = pymongo.MongoClient('mongodb://18.198.188.161/Sparta')
    db = client['fishmarkets']

    db.drop_collection('fishmarkets')
    db.create_collection('fishmarkets')
    filename = csvfile

    with open(filename, 'r') as data:
        for line in csv.DictReader(data):
            db.fishmarkets.insert_one(line)


load_to_ec2('Deon_Fish.csv')