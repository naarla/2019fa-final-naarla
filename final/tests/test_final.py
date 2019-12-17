from unittest import TestCase
from luigi import Task, Parameter, ExternalTask, LocalTarget, LuigiStatusCode, build, format
from luigi.contrib.s3 import S3Target
from tempfile import TemporaryDirectory
from luigi.contrib.external_program import ExternalProgramTask
from luigi.task_register import load_task
import os
from final.luigi import DownloadImage
import luigi.target
from mock import Mock
from luigi.mock import MockTarget
from unittest.mock import patch, MagicMock
import os
from final.cli import main

import boto3
from tempfile import NamedTemporaryFile, TemporaryDirectory
from unittest import TestCase
from final.stylize import ProcessImages, OutputStorage, BySegment
from final.luigi import DownloadImage, ContentImage
from final.target import suffix_preserving_atomic_file, SuffixPreservingLocalTarget

from luigi.contrib.s3 import S3Client
from moto import mock_s3

AWS_ACCESS_KEY = "XXXXXXXXXXXXXXXXXXXX"
AWS_SECRET_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"

IMAGE_ROOT = os.path.abspath('data/images')  # Path where the downloaded images are saved
S3_IMAGE_ROOT = 's3://mymuseums/images'
IMAGE = 'fake_image.jpeg'
LOCAL_IMAGE_PATH = os.path.join(IMAGE_ROOT, IMAGE)
S3_IMAGE_PATH = os.path.join(S3_IMAGE_ROOT, IMAGE)

def build_func(func, **kwargs):
    return build([func(**kwargs)],
                 local_scheduler=True,
                 detailed_summary=True
                 ).status

class TasksDataTests(TestCase):
    def test_ContentImage(self):
        """Ensure ContentImage & DownloadImage works as expected"""

        self.assertEqual(build_func(ContentImage), LuigiStatusCode.SUCCESS)

    def test_DownloadImage(self):
        """Ensure DownloadImage  works correctly and as expected"""

        self.assertEqual(build_func(DownloadImage), LuigiStatusCode.SUCCESS)

        with patch('luigi.contrib.s3.S3Target.exists', MagicMock(return_value=True)):
            with patch('luigi.contrib.s3.S3Target.open', MagicMock()):
                self.assertEqual(build_func(DownloadImage), LuigiStatusCode.SUCCESS)


def create_bucket(bucket, dir, temp_file_path, download_path):
    """
    Create mock buckets for testing and put a test file in bucket
    at directory location if provided
    """
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket)
    client = S3Client(AWS_ACCESS_KEY, AWS_SECRET_KEY)

    # if a directory was provided create corresponding path in the bucket
    if dir:
        client.mkdir("s3://{}/{}".format(bucket, dir))
        bucket_path = os.path.join(bucket, dir)
    else:
        bucket_path = bucket

    client.put(temp_file_path, "s3://{}/{}".format(bucket_path, download_path))

def create_temp_files():
    """
    Create a temporary file with contents and return
    tuple with file contents with file path
    :return: (file path, file contents)
    """
    f = NamedTemporaryFile(mode="wb", delete=False)
    tempFileContents = (
        b"I'm a temporary file for testing\nAnd this is the second line for testing\n"
        b"This is the third line for testing."
    )
    tempFilePath = f.name
    f.write(tempFileContents)
    f.close()
    return tempFilePath, tempFileContents


def setupTest(self):
    # Get Temp File Path and Contents
    self.tempFilePath, self.tempFileContents = create_temp_files()
    # Ensure cleanup of file once complete
    self.addCleanup(os.remove, self.tempFilePath)

@mock_s3
class DownloadTest(TestCase):
    def setUp(self):
        setupTest(self)

    def download(self, path, download_file):

        download_path = os.path.join(path, download_file)
        # call creates a bucket with a path for images
        # and adds the temporary file file to bucket
        create_bucket("museumdata", "images", self.tempFilePath, download_path)

        # Using a temporary directory to download load test file from mock
        # s3 and compare content
        with TemporaryDirectory() as tmp:
            fp = os.path.join(tmp, download_file)
            if path == "images":
                dm = DownloadImage()
            dm.run()
            self.assertTrue(os.path.exists(fp))
            with open(fp, "r") as f:
                content = f.read()
            self.assertTrue(self.tempFileContents, content)

@mock_s3
class ImageTest(TestCase):
    def setUp(self):
        setupTest(self)

    def confirm_content_output(self, root, download_file, s3_path):

        download_path = os.path.join(root, download_file)
        # Create bucket and store temp file as test download file
        create_bucket("mybucket", None, self.tempFilePath, download_path)

        if root == "images":
            # Create a ContentImage instance
            ci = ContentImage(root="s3://mybucket/")


        test_s3 = ci.output()

        # Check that file object exists
        self.assertIsNotNone(test_s3)
        # Check that s3 file target path matches expected path
        self.assertEqual(test_s3.path, s3_path)

        # Retrieve contents and compare with temp file contents stored in setup
        with test_s3.open("r") as f:
            content = f.read()
        self.assertTrue(self.tempFileContents, content)



class TaskTest(TestCase):
    '''Testing that the result of Cleaned Reviews, ByStars and ByDecade run has successful status'''

    def test_DownloadImage(self):
        self.assertEqual(
            build(
                [DownloadImage()],
                local_scheduler=True,
                detailed_summary=True,
            ).status,
            LuigiStatusCode.SUCCESS,
        )


    def test_Output(self):
        self.assertEqual(
            build(
                [OutputStorage()],
                local_scheduler=True,
                detailed_summary=True,
            ).status,
            LuigiStatusCode.SUCCESS,
        )

    def test_BySegment(self):
        self.assertEqual(
            build(
                [BySegment()],
                local_scheduler=True,
                detailed_summary=True,
            ).status,
            LuigiStatusCode.SUCCESS,
        )

    def test_ProcessImages(self):
        self.assertEqual(
            build(
                [ProcessImages()],
                local_scheduler=True,
                detailed_summary=True,
            ).status,
            LuigiStatusCode.SUCCESS,
        )
        
class DataTests(TestCase):
    def test_processimages_output(self):
        self.assertEqual(
            ProcessImages().output().path,
            os.path.join(os.getcwd()+"/data/"+"small/"),
        )

    def test_bysegment_has_correct_output(self):
        self.assertEqual(
            BySegment().output().path,
            os.path.join(os.getcwd()+"/data/"+"output"),
        )

    def test_storage_has_correct_output(self):
        self.assertEqual(
            OutputStorage().output().path,
            os.path.join(os.getcwd()+"/data/"+"storage/"),
        )

class MockOutput(OutputStorage):
    """
    Extend the Output class for testing. we can at least confirm
    the file name generated (which should prove workflow executed properly
    """

    def program_args(self):
        # Be sure to use self.temp_output_path
        return ["touch", self.temp_output_path]

    def test_storage_output(self):
        """
        Execute run for output process and determine if output file name (h5py) from process represents expected
        name
        """
        with TemporaryDirectory() as tmp:
            out_fp = os.path.join(tmp, "storage-file.txt")
            dm = MockOutput(
                image=self.image_file, LOCAL_ROOT=tmp
            )
            dm.run()
            self.assertTrue(os.path.exists(out_fp))

class MockProcess(ProcessImages):
    """
    Extend the Output class for testing. we can at least confirm
    the file name generated (which should prove workflow executed properly
    """

    def program_args(self):
        # Be sure to use self.temp_output_path
        return ["touch", self.temp_output_path]

    def test_storage_output(self):
        """
        Execute run for output process and determine if output file name (h5py) from process represents expected
        name
        """
        with TemporaryDirectory() as tmp:
            out_fp = os.path.join(tmp, "resized-image.txt")
            dm = MockProcess(
                image=self.image_file, LOCAL_ROOT=tmp
            )
            dm.run()
            self.assertTrue(os.path.exists(out_fp))

class MockBySegment(BySegment):
    """
    Extend the Output class for testing. we can at least confirm
    the file name generated (which should prove workflow executed properly
    """

    def program_args(self):
        # Be sure to use self.temp_output_path
        return ["touch", self.temp_output_path]

    def test_storage_output(self):
        """
        Execute run for output process and determine if output file name (h5py) from process represents expected
        name
        """
        with TemporaryDirectory() as tmp:
            out_fp = os.path.join(tmp, "adaptive-threshold-processed-images.txt")
            dm = MockProcess(
                image=self.image_file, LOCAL_ROOT=tmp
            )
            dm.run()
            self.assertTrue(os.path.exists(out_fp))
