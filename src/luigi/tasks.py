#!/usr/bin/env python

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
        return [self.subreddit, self.week]

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
        return S3Target('s3a://bkrull-insight-bucket/results/{subreddit}/{week}'.format(subreddit=self.subreddit, week=self.week))

    def requires(self):
        return BuildLDA(self.subreddit, self.week)


    #subreddits = df.select('subreddit') \
    #               .filter(df.subreddit == 'politics') \
    #               .distinct().collect()
