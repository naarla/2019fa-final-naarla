from luigi import ExternalTask, Task, LocalTarget, Target, BoolParameter, format
import luigi
from .tasks import Requires, Requirement
from .target import SuffixPreservingLocalTarget
from luigi.contrib.s3 import S3Target
import boto3
import sys, os


class ContentImage(ExternalTask):
    BUCKET = "museumdata"
    IMAGE_ROOT = "images"

    def output(self):
        s3_client = boto3.client('s3')
        objects = s3_client.list_objects(Bucket=self.BUCKET, Prefix=self.IMAGE_ROOT)
        for obj in objects['Contents']:
               print ('Checking for files in S3: %s' % obj['Key'])
        return S3Target(
            "s3://{}/{}".format(self.BUCKET, self.IMAGE_ROOT), format=luigi.format.Nop
        )


class DownloadImage(Task):

    S3_BUCKET = "museumdata"
    LOCAL_ROOT = os.path.abspath("data")
    SHARED_RELATIVE_PATH = "images"


    def requires(self):
        # Depends on Content image External task
        return ContentImage()

    def output(self):
        """:returns file in the local folder set by Share Relative Path"""
        return LocalTarget(
            self.LOCAL_ROOT + "/" + self.SHARED_RELATIVE_PATH,
            format=format.Nop
        )

    def run(self):
         pref =  "/".join(self.input().path.split('/')[3:])
         print("Prefix is {}".format(pref))
         s3_client = boto3.client('s3')
         objects = s3_client.list_objects(Bucket=self.S3_BUCKET, Prefix=pref)

         os.makedirs(self.output().path)
         for obj in objects['Contents']:
            key = obj['Key']
            print ('Now downloading: %s' % key)
            if not key.endswith("/"):
                file_name = key.split('/')[-1]
                s3_client.download_file('museumdata', key, self.output().path+ "/" + file_name)
