# Helper code for working with S3

from pathlib import Path

import boto3
from botocore.errorfactory import ClientError


class S3():
    def __init__(self, bucket_name):

        # Get S3 credentials
        with Path('~/.credentials/do_spaces.txt').expanduser().open() as f:
            access_key, secret = [x.rstrip('\n') for x in f.readlines()]

        self.session = boto3.session.Session()
        self.client = self.session.client(
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

    def get_matching_s3_objects(self, prefix="", suffix=""):
        """
        Generate objects in an S3 bucket.

        :param prefix: Only fetch objects whose key starts with
            this prefix (optional).
        :param suffix: Only fetch objects whose keys end with
            this suffix (optional).
        """
        paginator = self.client.get_paginator("list_objects")

        kwargs = {'Bucket': self.bucket_name}

        # We can pass the prefix directly to the S3 API.  If the user has passed
        # a tuple or list of prefixes, we go through them one by one.
        if isinstance(prefix, str):
            prefixes = (prefix, )
        else:
            prefixes = prefix

        for key_prefix in prefixes:
            kwargs["Prefix"] = key_prefix

            for page in paginator.paginate(**kwargs):
                try:
                    contents = page["Contents"]
                except KeyError:
                    return

                for obj in contents:
                    key = obj["Key"]
                    if key.endswith(suffix):
                        yield obj

    def get_matching_s3_keys(self, prefix="", suffix=""):
        """
        Generate the keys in an S3 bucket.

        :param prefix: Only fetch keys that start with this prefix (optional).
        :param suffix: Only fetch keys that end with this suffix (optional).
        """
        for obj in self.get_matching_s3_objects(prefix, suffix):
            yield obj["Key"]
