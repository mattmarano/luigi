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
from luigi.mock import MockFile
import mock
import unittest
import datetime


class CommonDateHourTask(luigi.Task):
    dh = luigi.DateHourParameter()

    def output(self):
        return luigi.File(self.dh.strftime('/n2000y01a05n/%Y_%m-_-%daww/21mm%Hdara21/ooo'))


mock_listing = [
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

expected = [
    'TaskA(dh=2014-03-20T17)',
    'TaskA(dh=2014-03-20T19)',
    'TaskA(dh=2014-03-20T20)',
]

# expected_reverse = [
# ]


class TaskA(luigi.Task):
    dh = luigi.DateHourParameter()

    def output(self):
        return MockFile(self.dh.strftime('TaskA/%Y-%m-%d/%H'))


# class RangeHourlyBaseTest(unittest.TestCase):

class RangeHourlyTest(unittest.TestCase):
    def test_glob_successfully_inferred(self):
        self.assertEqual(RangeHourly._get_glob(CommonDateHourTask), '/n2000y01a05n/[0-9][0-9][0-9][0-9]_[0-9][0-9]-_-[0-9][0-9]aww/21mm[0-9][0-9]dara21')

    @mock.patch('luigi.mock.MockFileSystem.listdir', return_value=mock_listing)  # fishy to mock the mock, but MockFileSystem doesn't support globs yet
    def test_missing_tasks_correctly_required(self, mock_listdir):
        task = RangeHourly(of='TaskA',
                           start=datetime.datetime(2014, 3, 20, 17),
                           task_limit=3,
                           range_limit=30 * 365 * 24)  # the test will break sometime around 2044
        actual = [t.task_id for t in task.requires()]
        self.assertEqual(actual, expected)
