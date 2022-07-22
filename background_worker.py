import sys
from os import path
import redis
import boto3
import pandas as pd

class FileHandler:
    def read(self, file_path):
        pass

    def exists(self, file_path):
        pass

class LocalFileHandler(FileHandler):
    def read(self, file_path):
        return pd.read_csv(file_path, sep=',"', engine='python', usecols=['"col10""'])

    def exists(self, file_path):
        return path.exists(file_path)

class RemoteFileHandler(FileHandler):
    s3 = boto3.client(
        service_name='s3',
        aws_access_key_id='access_key',
        aws_secret_access_key='secret_key',
        endpoint_url='https://bucket-name.s3.Region.amazonaws.com',
    )

    def read(self, file_path):
        file = RemoteFileHandler.s3.upload_file('tasks/'+file_path, 'BucketName', file_path)
        return pd.read_csv(file)

    def exists(self, file_path):
        return path.exists(file_path)

#calculate service
def get_file(data):
    s=sys.getsizeof(data)
    print(s)
    df = pd.to_numeric(data['"col10""'].str.strip('"')).sum()
    return df

file_handler=LocalFileHandler() #and/or RemoteFileHandler

with redis.Redis() as client:
    while True:
        file_name = client.brpop('tasks')[1].decode('utf-8')
        if file_handler.exists(file_name):
            data = file_handler.read(file_name)
            result = get_file(data)
            print(result)
            client.rpush('answers', result)
        else:
            print('File '+file_name+' not found')