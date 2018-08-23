#!/usr/bin/env python
"""
This module contains the main Task definitions for Luigi

There are two Tasks:
- BuildLDA takes in tokenized data from S3, computes LDA model, and writes to postgres db.
- WriteToPG reads in data from S3 and copies it to a postgres db.

Currently, the write-to-pg operation is done in the SparkSubmitTask because
of complications of S3Targets as outputs of Tasks.
"""

import botocore
import configparser
import datetime
import os
import luigi
from luigi import configuration
from luigi.contrib.s3 import S3Target
from luigi.contrib.spark import SparkSubmitTask
from luigi.contrib.postgres import CopyToTable


class BuildLDA(SparkSubmitTask):
    """
    This task takes in a subreddit and a week start date
    and runs a spark-submit job based on the spark app found in the src of this repo.

    The actual spark job is assigned in the file pointed to by the app variable.
    """
    subreddit = luigi.Parameter(default='politics')
    week = luigi.Parameter(default='2008-01-01')

    config = configparser.ConfigParser()
    config.read(os.path.expanduser('~/.aws/credentials'))
    ip = config.get('spark', 'ip')
    port = config.get('spark', 'port')

    spark_submit = '/usr/local/spark/bin/spark-submit'
    master = 'spark://{ip}:{port}'.format(ip=ip, port=port)
    app = u'/home/ubuntu/insight/src/spark/countvectorize-to-postgres.py'
    packages = 'org.postgresql:postgresql:42.2.4'

    def app_options(self):
        """
        These options are appended unpacked into arguments following spark-submit app.py subreddit week
        """
        return [self.subreddit, self.week]

    def output(self):
        return S3Target('s3a://bkrull-insight-bucket/results/{subreddit}/{week}'.format(
            subreddit=self.subreddit, week=self.week))

    @property
    def packages(self):
        return ['org.postgresql:postgresql:42.2.4']

class WriteToPG(CopyToTable):
    subreddit = luigi.Parameter(default='politics')
    week = luigi.Parameter(default='2008-01-01')
    
    config = configparser.ConfigParser()
    config.read(os.path.expanduser('~/.aws/credentials'))
    user = config.get('db', 'user')
    password = config.get('db', 'password')
    host = config.get('db', 'ip')
    port = config.get('db', 'port')
    database = config.get('db', 'database')
    table = 'newresults'

    columns = [
        ('subreddit', 'TEXT'),
        ('week', 'TEXT'),
        ('results', 'TEXT')
    ]

    def input(self):
        return S3Target('s3a://bkrull-insight-bucket/results/{subreddit}/{week}'.format(
            subreddit=self.subreddit, week=self.week))

    def requires(self):
        return BuildLDA(self.subreddit, self.week)
