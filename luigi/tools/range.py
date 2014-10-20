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

Caveat - if gap causes (missing dependencies) aren't acted upon then this will
eventually schedule the same gaps again and again and make no progress to other
datehours.
TODO foolproof against that kind of misuse?
"""

from collections import Counter
from datetime import datetime, timedelta
import logging
import luigi
import luigi.hdfs
from luigi.parameter import ParameterException
from luigi.target import FileSystemTarget
from luigi.task import Register, flatten_output
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
        default=50,
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

        logger.debug('Checking if range [%s, %s] of %s is complete' % (datehours[0], datehours[-1], self.of))
        task_cls = Register.get_task_cls(self.of)
        missing_datehours = self.missing_datehours(task_cls, datehours)
        logger.debug('Range [%s, %s) lacked %d of expected %d %s instances' % (datehours[0], datehours[-1], len(missing_datehours), len(datehours), self.of))
        # obey task_limit
        if self.reverse:
            required_datehours = missing_datehours[-self.task_limit:]
        else:
            required_datehours = missing_datehours[:self.task_limit]
        if len(required_datehours):
            logger.debug('Requiring %d missing %s instances in range [%s, %s]' % (len(required_datehours), self.of, required_datehours[0], required_datehours[-1]))

        if self.reverse:
            required_datehours.reverse()  # I wish this determined the order tasks were scheduled or executed, but it doesn't. No priorities in Luigi yet
        self._cached_requires = [task_cls(d) for d in required_datehours]
        return self._cached_requires


class RangeHourly(RangeHourlyBase):
    """Benefits from bulk completeness information to efficiently cover gaps.

    The current implementation works for the common case of a task writing
    output to a FileSystemTarget whose path is built using strftime with format
    like '...%Y...%m...%d...%H...', without custom complete() or exists(). All
    to efficiently determine missing datehours by filesystem listing.

    Convenient to use even from command line, like:

        luigi --module your.module --task RangeHourly --of YourActualTask --start 2014-01-01T00

    Intended to be further developed to use an explicit API or pick a suitable
    heuristic for other types of tasks too, e.g. Postgres exporters...
    (Eventually Luigi could have ranges of completion as first-class citizens.
    Then this listing business could be factored away/be provided for
    explicitly in target API or some kind of a history server.)
    """

    @classmethod
    def _get_per_output_glob(_, tasks, outputs, regexes):
        """Builds a glob listing existing output paths.

        Esoteric reverse engineering, but worth it given that (compared to an
        equivalent contiguousness guarantee by naive complete() checks)
        requests to the filesystem are cut by orders of magnitude, and users
        don't even have to retrofit existing tasks anyhow.

        FIXME constrain, as the resulting listing size would tend to infinity.
        """
        paths = [o.path for o in outputs]
        matches = [r.search(p) for r, p in zip(regexes, paths)]  #  naive, because some matches could be confused by numbers earlier in path, e.g. /foo/fifa2000k/bar/2000-12-31/00

        for m, p, t in zip(matches, paths, tasks):
            if m is None:
                raise Exception("Couldn't deduce datehour representation in output path %r of task %s" % (p, t))

        positions = [Counter((m.start(i), m.end(i)) for m in matches).most_common(1)[0][0] for i in range(1, 5)]  # the most common position of every group is likely to be conclusive hit or miss

        glob = list(paths[0])  # TODO sanity check that it's the same for all paths?
        for start, end in positions:
            glob = glob[:start] + ['[0-9]'] * (end - start) + glob[end:]
        # return ''.join(glob)
        return ''.join(glob).rsplit('/', 1)[0]  # chop off the last path item (wouldn't need to if hadoop fs -ls -d equivalent were available)

    @classmethod
    def _get_filesystems_and_globs(cls, task_cls):
        """Returns a (filesystem, glob) tuple per every output location of
        task_cls.

        task_cls can have one or several FileSystemTarget outputs. For
        convenience, task_cls can be a wrapper task, in which case outputs of
        all its dependencies are considered.
        """
        # probe some scattered datehours unlikely to all occur in paths, other than by being sincere datehour parameter's representations
        # TODO limit to [self.start, self.stop) so messages are less confusing
        datehours = [datetime(y, m, d, h) for y in range(2000, 2050, 10) for m in range(1, 4) for d in range(5, 8) for h in range(21, 24)]
        regexes = [re.compile('(%04d).*(%02d).*(%02d).*(%02d)' % (d.year, d.month, d.day, d.hour)) for d in datehours]
        tasks = [task_cls(d) for d in datehours]
        # outputs = [flatten(t.output()) for t in tasks]
        outputs = [flatten_output(t) for t in tasks]

        for o, t in zip(outputs, tasks):
            if len(o) != len(outputs[0]):
                raise Exception("Outputs must be consistent over time, sorry; was %r for %r and %r for %r" % (o, t, outputs[0], tasks[0]))
                # TODO fall back on requiring last couple of days? to avoid astonishing blocking when changes like that are deployed
                # erm, actually it's not hard to test entire hours_back..hours_forward and split into consistent subranges FIXME
            for target in o:
                if not isinstance(target, FileSystemTarget):
                    raise Exception("Output targets must be instances of FileSystemTarget; was %r for %r" % (target, t))

        return [(o[0].fs, cls._get_per_output_glob(tasks, o, regexes)) for o in zip(*outputs)]  # outputs transposed, so here we're iterating over logical outputs, not datehours

    @classmethod
    def _list_existing(_, filesystem, glob):
        logger.debug('Listing %s' % glob)
        time_start = time.time()
        # snakebite globbing is slow and spammy, TODO glob coarser and filter later? to speed up
        #filesystem = luigi.hdfs.HdfsClient()
        listing = set(filesystem.listdir(glob))
        logger.debug('Listing took %f s to return %d items' % (time.time() - time_start, len(listing)))
        return listing

    def missing_datehours(self, task_cls, finite_datehours):
        """Infers them by listing the task output target(s) filesystem.
        """
        listing = set.union(*[self._list_existing(*fg) for fg in self._get_filesystems_and_globs(task_cls)])

        # quickly learn everything that's missing
        missing_datehours = []
        for d in finite_datehours:
            if not self.stop or d < self.stop:
                if any(o.path not in listing for o in flatten_output(task_cls(d))):
                    missing_datehours.append(d)

        return missing_datehours
