#!/usr/bin/env python

import datetime
import luigi
from tasks import BuildLDA

def get_week_list():
    week_length = datetime.timedelta(days=7)
    
    week_list = []
    for year in range(2008, 2015):
	beginning = datetime.date(year, 01, 01)
	for week_number in range(0, 53):
	    yield map(
		lambda x: x.isoformat(), \
		[beginning + week_length*week_number, \
		 beginning + week_length*(week_number+1)])

if __name__ == '__main__':
    subreddit = 'politics'
    for week in get_week_list():
        luigi.build([BuildLDA(subreddit=subreddit, week=week[0])])
        break
