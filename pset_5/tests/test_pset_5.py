from unittest import TestCase
from luigi import Task, Parameter


class HashTests(TestCase):
    def test_data(self):
        self.assertEqual(5, 5)
