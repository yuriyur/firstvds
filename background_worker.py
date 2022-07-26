import sys
import time
from os import path

import redis
import boto3
import dask.dataframe as dd
import dask.multiprocessing
from distributed import Client


class FileHandler:
    def read(self, file_path):
        pass

    def exists(self, file_path):
        pass

class LocalFileHandler(FileHandler):
    def read(self, file_path):
        return dd.read_csv(file_path, sep='"",""', engine='python', usecols=['col10'], dtype=float)

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
        return dd.read_csv(file)

    def exists(self, file_path):
        return path.exists(file_path)


def read_file(file_name):
    start_time = time.time()
    data = file_handler.read(file_name)
    print(f"- {(time.time() - start_time)} seconds read -")
    return data

def get_sum(data):
    data_size=sys.getsizeof(data)
    sum_col10 = data['col10'].sum().compute()
    print(f"- {data_size} size -")
    return sum_col10

def future(data):
    start_time = time.time()
    with dask.config.set({"optimization.fuse.active": True}):
        big_future = client_dask.scatter(data)
        results = client_dask.submit(get_sum, big_future)
        print(f"- {results.result()} answer -")
    print(f"- {(time.time() - start_time)} seconds calculate -")
    return results

def main():
    with redis.Redis() as client:
        while True:
            file_name = client.brpop('tasks')[1].decode('utf-8')
            if file_handler.exists(file_name):
                data = read_file(file_name)
                results = future(data)
                client.rpush('answers', results.result())
            else:
                print(f'File {file_name} not found')

file_handler=LocalFileHandler() #and/or RemoteFileHandler
client_dask = Client(processes=False, n_workers=4)
print(client_dask)
print('Dashboard workers: ' + client_dask.dashboard_link)

if __name__ == "__main__":
    main()
