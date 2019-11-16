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

    def test_external_tasks(self):
        task = CleanedReviews()
        task2 = ByStars()
        task3 = ByDecade()
        task4 =YelpReviews()
        assert isinstance(task, Task)
        assert isinstance(task4, ExternalTask)
        assert isinstance(task3, Task)
        assert isinstance(task2, Task)

class TaskTest(TestCase):
    """testing Stylize is a External Program task"""


    def test_CleanedReviews(self):
        self.assertEqual(
            build(
                [CleanedReviews()],
                local_scheduler=True,
                detailed_summary=True,
            ).status,
            LuigiStatusCode.SUCCESS,
        )

    def test_ByStars(self):
        self.assertEqual(
            build(
                [ByStars()],
                local_scheduler=True,
                detailed_summary=True,
            ).status,
            LuigiStatusCode.SUCCESS,
        )

    def test_ByDecade(self):
        self.assertEqual(
            build(
                [ByDecade()],
                local_scheduler=True,
                detailed_summary=True,
            ).status,
            LuigiStatusCode.SUCCESS,
        )

    def test_YelpReviews(self):
        self.assertEqual(
            build(
                [YelpReviews()],
                local_scheduler=True,
                detailed_summary=True,
            ).status,
            LuigiStatusCode.SUCCESS,
        )
