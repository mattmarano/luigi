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

"""Produces contiguous completed ranges of recurring tasks.
"""

from collections import Counter
from datetime import datetime, timedelta
import logging
import luigi
import luigi.hdfs
from luigi.parameter import ParameterException
from luigi.target import FileSystemTarget
from luigi.task import Register, flatten
import re
import time

logger = logging.getLogger('luigi-interface')
# logging.basicConfig(level=logging.DEBUG)


class RangeHourlyBase(luigi.WrapperTask):
    """Produces a contiguous completed range of a hourly recurring task.

    Made for the common use case where a task is parameterized by datehour and
    assurance is needed that any gaps arising from downtime are eventually
    filled.

    TODO Emits events that one can use to monitor gaps and delays.

    At least one of start and stop needs to be specified.
    """

    of = luigi.Parameter(
        description="task name to be completed. The task must take a single datehour parameter")
        # TODO lift the single parameter constraint by passing unknown parameters through WrapperTask?
    start = luigi.DateHourParameter(
        default=None,
        description="beginning datehour, inclusive. Default: None - work backward forever (requires reverse=True)")
    stop = luigi.DateHourParameter(
        default=None,
        description="ending datehour, exclusive. Default: None - work forward forever")
        # wanted to name them "from" and "to", but "from" is a reserved word :/ So named after https://docs.python.org/2/library/functions.html#range arguments
    reverse = luigi.BooleanParameter(
        default=False,
        description="specifies the preferred order for catching up. False - work from the oldest missing outputs onward; True - from the newest backward")
    task_limit = luigi.IntParameter(
        default=100,  # 50
        description="how many of 'of' tasks to require. Guards against hogging insane amounts of resources scheduling-wise")
        # TODO vary based on cluster load (time of day)? Resources feature suits that better though
    range_limit = luigi.IntParameter(
        default=100 * 24,  # TODO prevent oldest outputs flapping when retention is shorter than this
        description="maximal range over which consistency is assured, in datehours. Guards against infinite loops when start or stop is None")
        # elaborate that it's the latest? FIXME make sure it's latest
        # TODO infinite for reprocessings like anonymize
        # future_limit, past_limit?
        # hours_back, hours_forward? Measured from current time. Usually safe to increase, only worker's memory and time being the limit.
    # TODO overridable exclude_datehours or something...

    def missing_datehours(self, task_cls, finite_datehours):
        """Override in subclasses to do bulk checks.

        This is a conservative base implementation that brutally checks
        completeness, instance by instance. Inadvisable as it may be slow.
        """
        return [d for d in finite_datehours if not task_cls(d).complete()]

    def requires(self):
        # cache because we anticipate lots of tasks
        if hasattr(self, '_cached_requires'):
            return self._cached_requires

        if not self.start and not self.stop:
            raise ParameterException("At least one of start and stop needs to be specified")
        if not self.start and not self.reverse:
            raise ParameterException("Either start needs to be specified or reverse needs to be True")
        # TODO check overridden complete() and exists()

        if self.reverse:
            datehours = [self.stop + timedelta(hours=h) for h in range(-self.range_limit - 1, 0)]
        else:
            datehours = [self.start + timedelta(hours=h) for h in range(self.range_limit)]

        logger.debug('Checking if range [%s, %s) of %s is complete' % (datehours[0], datehours[-1], self.of))
        task_cls = Register.get_task_cls(self.of)
        missing_datehours = self.missing_datehours(task_cls, datehours)
        logger.debug('Range [%s, %s) lacked %d of expected %d %s instances' % (datehours[0], datehours[-1], len(missing_datehours), len(datehours), self.of))
        # obey task_limit
        required_datehours = missing_datehours[:self.task_limit]
        # TODO obey reverse
        if len(required_datehours):
            logger.debug('Requiring %d missing %s instances in range [%s, %s]' % (len(required_datehours), self.of, required_datehours[0], required_datehours[-1]))

        self._cached_requires = [task_cls(d) for d in required_datehours]
        return self._cached_requires


class RangeHourly(RangeHourlyBase):
    """Benefits from bulk completeness information to efficiently cover gaps.

    Current implementation requires the datehour to be included in output
    target's path, and is incompatible with custom complete() or exists(), all
    to efficiently determine missing datehours by filesystem listing.

    Works for the common case of a task writing output to a FileSystemTarget
    with output path built using strftime with format like
    '...%Y...%m...%d...%H...'.

    Intended to be developed to use an explicit API or pick a suitable
    heuristic for other types of tasks too, e.g. Postgres exporters...
    (Eventually Luigi could have ranges of completion as first-class citizens.
    Then this listing business could be factored away/be provided for
    explicitly in target API or some kind of a history server.)

    Usage can be in code or command line, like:

        luigi --module your.module --task RangeHourly --of YourActualTask --start 2014-01-01T00
    """

    def _get_filesystem(self, task_cls, any_datehour):
        return task_cls(any_datehour).output().fs

    @classmethod
    # def _get_ymdh_offsets(self, task_cls):
    #     """Reverse-engineers representation of datehours in output paths."""
    def _get_glob(_, task_cls):
        """Builds a glob listing existing outputs of this task.

        Esoteric reverse engineering, but worth it given that (compared to an
        equivalent contiguousness guarantee by naive complete() checks)
        requests to the filesystem are cut by orders of magnitude, and users
        don't even have to retrofit existing tasks anyhow.

        FIXME constrain, as the resulting listing size would tend to infinity.
        """
        # probe some scattered datehours unlikely to all occur in paths, other than by being sincere datehour parameter's representations
        # TODO limit to [self.start, self.stop) so messages are less confusing
        datehours = [datetime(y, m, d, h) for y in range(2000, 2050, 10) for m in range(1, 4) for d in range(5, 8) for h in range(21, 24)]
        regexes = [re.compile('(%04d).*(%02d).*(%02d).*(%02d)' % (d.year, d.month, d.day, d.hour)) for d in datehours]
        tasks = [task_cls(d) for d in datehours]
        outputs = [flatten(t.output()) for t in tasks]

        for o, t in zip(outputs, tasks):
            if len(o) != 1 or not isinstance(o[0], FileSystemTarget):
                raise Exception("Output must be a single FileSystemTarget; was %r for %r" % (o, t))
        # TODO relax, allow multiple outputs, allow wrapper tasks with multiple requirements

        paths = [o[0].path for o in outputs]
        matches = [r.search(p) for r, p in zip(regexes, paths)]  #  naive, because some matches could be confused by numbers earlier in path, e.g. /foo/fifa2000k/bar/2000-12-31/00

        for m, p, t in zip(matches, paths, tasks):
            if m is None:
                raise Exception("Couldn't deduce datehour representation in output path %r of task %s" % (p, t))

        positions = [Counter((m.start(i), m.end(i)) for m in matches).most_common(1)[0][0] for i in range(1, 5)]  # the most common position of every group is likely to be conclusive hit or miss

        glob = list(paths[0])
        for start, end in positions:
            glob = glob[:start] + ['[0-9]'] * (end - start) + glob[end:]
        # return ''.join(glob)
        return ''.join(glob).rsplit('/', 1)[0]  # chop off the last path item (wouldn't need to if hadoop fs -ls -d equivalent were available)

    # def _get_glob(self, task_cls, datehours):
    #     """Builds a glob that covers all datehours (with potentially some extra)."""
    #     # the glob enumerates dates and uses * for hours, as a tradeoff between unwieldy glob size and unwieldy listing time
    #     dates = set([str(d.date()) for d in datehours])
    #     offsets = self._get_ymdh_offsets(task)

    #     for d
    #     path_static_parts = [re.split(.split()]

    # def _get_tasks(self, task_cls):


    def missing_datehours(self, task_cls, finite_datehours):
        """Infers, by listing the task output target's filesystem.
        """
        fs = self._get_filesystem(task_cls, finite_datehours[0])
        # snakebite globbing is slow and spammy, TODO glob coarser and filter later? to speed up
        # fs = luigi.hdfs.HdfsClient()
        glob = self._get_glob(task_cls)
        logger.debug('Listing %s' % glob)
        time_start = time.time()
        listing = set(fs.listdir(glob))
        logger.debug('Listing took %f s to return %d items' % (time.time() - time_start, len(listing)))

        # quickly learn everything that's missing
        missing_datehours = []
        for d in finite_datehours:
            if not self.stop or d < self.stop:
                if flatten(task_cls(d).output())[0].path not in listing:
                    missing_datehours.append(d)

        return missing_datehours

        # return task()
