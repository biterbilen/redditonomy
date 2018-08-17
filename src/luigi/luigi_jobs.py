#!/usr/bin/env python

import datetime
import luigi
from tasks import BuildLDA

class SubredditTask(luigi.WrapperTask):
    subreddit = luigi.Parameter(default='politics')

    def requires(self):
        weeks = self.get_week_list()
        for week in weeks:
            yield BuildLDA(subreddit=self.subreddit, week=week)

    def get_week_list(self):
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
