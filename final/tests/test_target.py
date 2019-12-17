from final.target import suffix_preserving_atomic_file, SuffixPreservingLocalTarget
import os
from tempfile import TemporaryDirectory
from unittest import TestCase
from luigi import Parameter, ExternalTask
from luigi.format import Nop
from tempfile import TemporaryDirectory, NamedTemporaryFile

from pandas import DataFrame
from dask import dataframe as dd
from luigi.contrib.s3 import S3Target, S3Client
import boto3
from moto import mock_s3

class TestPreservingSuffix(TestCase):

    """
    Testing for the temp file preserves the suffix
    and that SuffixPreservingLocalTarget uses suffix atomic
    as the atomic provider

    """
    def test_local_suffix(self):
        with TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'abcd.txt')
            target = SuffixPreservingLocalTarget(path)
            with target.temporary_path() as temp:
                # Suffix is preserved test
                file_base, temp_file_extension = os.path.splitext(temp)
                path_base, path_extension = os.path.splitext(path)
                #asserting
                self.assertEqual(temp_file_extension, path_extension)


    def test_preserving_suffix(self):
        """Testing for the temporary
         file generated preserves
         the suffix"""

        with TemporaryDirectory() as tmp:
            #temp file
            temp_file_path = os.path.join(tmp, 'abcd.txt')
            #passing th temp file path into th suffix_preserving_atomic
            suffix_atomic = suffix_preserving_atomic_file(temp_file_path)
            #validating
            self.assertNotEqual(suffix_atomic.tmp_path, temp_file_path)
            self.assertEqual(suffix_atomic.path, temp_file_path)

            # Suffix is preserved validation
            temp_, temp_file_extension = os.path.splitext(suffix_atomic.tmp_path)
            path_, path_extension = os.path.splitext(temp_file_path)
            self.assertEqual(temp_file_extension, path_extension)



class MockSaveFileTask(ExternalTask):
    downloadFile = Parameter()
    contentFile = Parameter()

    def output(self):
        return S3Target("s3://mybucket/{}".format(self.downloadFile), format=Nop)




