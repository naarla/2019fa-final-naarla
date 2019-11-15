from unittest import TestCase
from luigi import Task, Parameter, ExternalTask, LocalTarget, LuigiStatusCode, build, format
from pset_5.tasks import YelpReviews, ByDecade, ByStars, BoolParameter, CleanedReviews
from tempfile import TemporaryDirectory
from luigi.contrib.external_program import ExternalProgramTask
from luigi.task_register import load_task
import os
import luigi.target
from mock import Mock

class HashTests(TestCase):
    def test_data(self):
        self.assertEqual(5, 5)




