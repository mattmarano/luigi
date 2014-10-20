# Copyright (c) 2014 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import luigi
from luigi.tools.range import RangeHourly
from luigi.mock import MockFile, MockFileSystem
import mock
import unittest
import datetime


class CommonDateHourTask(luigi.Task):
    dh = luigi.DateHourParameter()

    def output(self):
        return MockFile(self.dh.strftime('/n2000y01a05n/%Y_%m-_-%daww/21mm%Hdara21/ooo'))


mock_listing_a = [
    'TaskA/2014-03-20/18',
    'TaskA/2014-03-20/21',
    'TaskA/2014-03-20/23',
    'TaskA/2014-03-21/00',
    'TaskA/2014-03-21/00.attempt.1',
    'TaskA/2014-03-21/00.attempt.2',
    'TaskA/2014-03-21/01',
    'TaskA/2014-03-21/02',
    'TaskA/2014-03-21/03.attempt-temp-2014-03-21T13-22-58.165969',
    'TaskA/2014-03-21/03.attempt.1',
    'TaskA/2014-03-21/03.attempt.2',
    'TaskA/2014-03-21/03.attempt.3',
    'TaskA/2014-03-21/03.attempt.latest',
    'TaskA/2014-03-21/04.attempt-temp-2014-03-21T13-23-09.078249',
    'TaskA/2014-03-21/12',
    'TaskA/2014-03-23/12',
]

mock_listing_b = [
    'TaskB/no/worries2014-03-20/23',
    'TaskB/no/worries2014-03-21/01',
    'TaskB/no/worries2014-03-21/03',
    'TaskB/no/worries2014-03-21/04.attempt-yadayada',
    'TaskB/no/worries2014-03-21/05',
]

expected_a = [
    'TaskA(dh=2014-03-20T17)',
    'TaskA(dh=2014-03-20T19)',
    'TaskA(dh=2014-03-20T20)',
]

# expected_reverse = [
# ]

expected_wrapper = [
    'CommonWrapperTask(dh=2014-03-21T00)',
    'CommonWrapperTask(dh=2014-03-21T02)',
    'CommonWrapperTask(dh=2014-03-21T03)',
    'CommonWrapperTask(dh=2014-03-21T04)',
    'CommonWrapperTask(dh=2014-03-21T05)',
]


class TaskA(luigi.Task):
    dh = luigi.DateHourParameter()

    def output(self):
        return MockFile(self.dh.strftime('TaskA/%Y-%m-%d/%H'))


class TaskB(luigi.Task):
    dh = luigi.DateHourParameter()
    complicator = luigi.Parameter()

    def output(self):
        return MockFile(self.dh.strftime('TaskB/%%s%Y-%m-%d/%H') % self.complicator)


class CommonWrapperTask(luigi.WrapperTask):
    dh = luigi.DateHourParameter()

    def requires(self):
        yield TaskA(dh=self.dh)
        yield TaskB(dh=self.dh, complicator='no/worries')  # str(self.dh) would complicate beyond working


# class RangeHourlyBaseTest(unittest.TestCase):

class RangeHourlyTest(unittest.TestCase):
    def _test_filesystems_and_globs(self, task_cls, expected):
        actual = RangeHourly._get_filesystems_and_globs(task_cls)
        self.assertEqual(len(actual), len(expected))
        for (actual_filesystem, actual_glob), (expected_filesystem, expected_glob) in zip(actual, expected):
            self.assertTrue(isinstance(actual_filesystem, expected_filesystem))
            self.assertEqual(actual_glob, expected_glob)

    def test_successfully_inferred(self):
        self._test_filesystems_and_globs(CommonDateHourTask, [
            (MockFileSystem, '/n2000y01a05n/[0-9][0-9][0-9][0-9]_[0-9][0-9]-_-[0-9][0-9]aww/21mm[0-9][0-9]dara21'),
        ])
        self._test_filesystems_and_globs(CommonWrapperTask, [
            (MockFileSystem, 'TaskA/[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
            (MockFileSystem, 'TaskB/no/worries[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
        ])

    @mock.patch('luigi.mock.MockFileSystem.listdir', return_value=mock_listing_a)  # fishy to mock the mock, but MockFileSystem doesn't support globs yet
    def test_missing_tasks_correctly_required(self, mock_listdir):
        task = RangeHourly(of='TaskA',
                           start=datetime.datetime(2014, 3, 20, 17),
                           task_limit=3,
                           range_limit=365 * 24)  #30 * # the test will break sometime around 2044
        actual = [t.task_id for t in task.requires()]
        self.assertEqual(actual, expected_a)

    @mock.patch('luigi.mock.MockFileSystem.listdir', new=lambda _, glob: mock_listing_a if glob.startswith('TaskA') else mock_listing_b)
    def test_missing_wrapper_tasks_correctly_required(self):
        task = RangeHourly(of='CommonWrapperTask',
                           start=datetime.datetime(2014, 3, 20, 23),
                           stop=datetime.datetime(2014, 3, 21, 6),
                           range_limit=365 * 24)  #30 * # the test will break sometime around 2044
        actual = [t.task_id for t in task.requires()]
        self.assertEqual(actual, expected_wrapper)
