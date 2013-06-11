###  A logster parser file that can be used to count the number of different
###  messages in an Apache error_log
###
###  For example:
###  sudo ./logster --dry-run --output=ganglia ErrorLogLogster /var/log/httpd/error_log
###
###

import time
import re

from collections import defaultdict
from logster.logster_helper import MetricObject, LogsterParser
from logster.logster_helper import LogsterParsingException


class ErrorLogLogster(LogsterParser):

    def __init__(self,
                 option_string=None,
                 regexp='^\[[^]]+\] \[(?P<loglevel>\w+)\] .*',
                 levels=('notice', 'warn', 'error', 'crit'),
                 prefix='',
                 num_seconds=10):
        '''Initialize any data structures or variables needed for keeping track
        of the tasty bits we find in the log we are parsing.'''
        self.counters = defaultdict(int)
        self.levels = set(levels)
        self.prefix = prefix
        self.num_seconds = num_seconds
        self.units = 'Logs per {0} sec'.format(num_seconds)
        
        # Regular expression for matching lines we are interested in, and capturing
        # fields from the line
        self.reg = re.compile(regexp)


    def parse_line(self, line):
        '''This function should digest the contents of one line at a time, updating
        object's state variables. Takes a single argument, the line to be parsed.'''

        try:
            # Apply regular expression to each line and extract interesting bits.
            regMatch = self.reg.match(line)

            if regMatch:
                linebits = regMatch.groupdict()
                level = linebits['loglevel']
                level = level.lower()
                
                if level in self.levels:
                    self.counters[level] += 1
                else:
                    self.counters['other'] += 1

            else:
                raise LogsterParsingException, "regmatch failed to match"

        except Exception, e:
            raise LogsterParsingException, "regmatch or contents failed with %s" % e


    def get_state(self, duration):
        '''Run any necessary calculations on the data collected from the logs
        and return a list of metric objects.'''

        return [
            MetricObject(self.prefix + key,
                         value / (duration / self.num_seconds),
                         self.units)
            for key, value in self.counters.items()
            if duration > 0
        ]
