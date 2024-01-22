import boto3
import logging
import os
from botocore.exceptions import ClientError

class S3Helper:
    def __init__(self):
        self.bucket_name = None
        self.client = None

    def init_client(self):
        self.client = boto3.client('s3')

    def list_buckets(self):
        buckets = [bucket['Name'] for bucket in self.client.list_buckets()['Buckets']]
        return buckets

    def create_bucket(self, name):
        if not name:
            return logging.error(f"bucket name could not be empty")

        if name and name in self.list_buckets():
            return logging.warning(f"Bucket - {name}, is already exist")

        self.client.create_bucket(Bucket=name)

    def delete_bucket(self, name):
        try:
            res = self.client.delete_bucket(Bucket=name)
        except self.client.exceptions.NoSuchBucket as e:
            raise Exception(f"There is no such bucket - {name}")
        else:
            return res

    def upload_file(self, file_path, bucket):
        try:
            response = self.client.upload_file(file_path, bucket, file_path)
        except ClientError as e:
            logging.error(e)
            return False
        return response


if __name__ == '__main__':
    s3 = S3Helper()
    # s3.create_bucket('boto3fun2')
    # print(s3.list_buckets())
    # print(s3.delete_bucket('boto3fun'))
    s3.init_client()
    s3.create_bucket('boto3fun')
    print(s3.upload_file('s3.py', 'boto3fun'))
