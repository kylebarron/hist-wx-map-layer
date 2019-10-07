# Helper code for working with S3

from pathlib import Path

import boto3
from botocore.errorfactory import ClientError


class S3():
    def __init__(self, bucket_name):

        # Get S3 credentials
        with Path('~/.credentials/do_spaces.txt').expanduser().open() as f:
            access_key, secret = [x.rstrip('\n') for x in f.readlines()]

        boto_session = boto3.session.Session()
        self.client = boto_session.client(
            's3', region_name='nyc3',
            endpoint_url='https://nyc3.digitaloceanspaces.com',
            aws_access_key_id=access_key, aws_secret_access_key=secret)

        self.bucket_name = bucket_name

    def file_exists(self, s3_key):
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError:
            return False
