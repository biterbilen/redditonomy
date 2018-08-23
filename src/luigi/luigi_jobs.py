#!/usr/bin/env python
"""
This is the main luigi jobs python script to be run from the command line.

It is composed of a wrapper Task, SubredditTask that will create 1 BuildLDA
task for each week over a given year.
"""

import datetime
import luigi
from tasks import BuildLDA

class SubredditTask(luigi.WrapperTask):
    subreddit = luigi.Parameter(default='politics')

    def requires(self):
        """
        Get the relevant weeks and produce an iterator over those weeks
        """
        weeks = self.get_week_list()
        for week in weeks:
            yield BuildLDA(subreddit=self.subreddit, week=week)

    def get_week_list(self):
        """
        Creates a list of week start dates by adding 7 days successively
        to the first day of a year
        """
        week_length = datetime.timedelta(days=7)
        
        weeks = []
        for year in range(2014, 2015):
            beginning = datetime.date(year, 01, 01)
            for week_number in range(0, 53):
                week_start = beginning + week_length * week_number
                weeks.append(week_start.isoformat())

        return weeks

if __name__ == '__main__':
    subreddits = ['politics', 'worldnews', 'funny', \
                  'askreddit', 'science', 'gaming', \
                  'movies', 'music', 'explainlikeimfive', 'books']

    luigi.build([SubredditTask(subreddit=subreddit) for subreddit in subreddits])
