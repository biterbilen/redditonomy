#! /usr/bin/env python

from pyspark.sql.types import *
from pyspark.sql import SparkSession
import os
import configparser

if __name__ == '__main__':
    config = configparser.ConfigParser()

    config.read(os.path.expanduser('~/.aws/credentials'))
    access_id = config.get('aws', 'aws_access_key_id') 
    access_key = config.get('aws', 'aws_secret_access_key')
    dbuser = config.get('db', 'user')
    dbpwd = config.get('db', 'password')
    dbip = config.get('db', 'ip')
    dbport = config.get('db', 'port')
    dbname = config.get('db', 'database')

    os.environ['PYSPARK_SUBMIT_ARGS'] = \
            "--packages org.apache.hadoop:hadoop-aws:2.7.1 pyspark-shell"

    spark = SparkSession.builder \
            .appName("S3 READ TEST") \
            .config("spark.executor.cores", "6") \
            .config("spark.executor.memory", "4gb") \
            .getOrCreate()

    sc=spark.sparkContext

    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.awsAccessKeyId", access_id)
    hadoop_conf.set("fs.s3a.awsSecretAccessKey", access_key)

    schema = StructType([ 
        StructField('archived', BooleanType()),
        StructField('author', IntegerType()),
        StructField('author_flair_css_class', IntegerType()),
        StructField('author_flair_text', StringType()),
        StructField('body', StringType()),
        StructField('controversiality', IntType()),
        StructField('created_utc', DateType()),
        StructField('distinguished', StringType()),
        StructField('downs', IntegerType()),
        StructField('edited', BooleanType()),
        StructField('gilded', IntegerType()),
        StructField('id', StringType()),
        StructField('link_id', StringType()),
        StructField('name', StringType()),
        StructField('parent_id', StringType()),
        StructField('retrieved_on', DateType()),
        StructField('score', IntegerType()),
        StructField('score_hidden', BooleanType()),
        StructField('subreddit', StringType()),
        StructField('subreddit_id', StringType()),
        StructField('ups', IntegerType())
    ])
    base = 's3a://bkrull-insight-bucket/RC/RC_'
    year = 2007
#    df = spark.read.json([base+'{}-{:02d}'.format(year, month) for month in range(11, 11)], schema=schema)


    columns = ['id', 'subreddit', 'date', 'results']
    vals = [(934, '/r/politics', '2200-12-25', "{ 'subreddit': '/r/politics' }")]
    df = spark.createDataFrame(vals, columns)
    df.date.cast(DateType())
    df.drop('archived', 'author_flair_css_class', 'author_flair_text', 'distinguished', 'downs', \ 	'edited', 'gilded', 'id', 'link_id', 'parent_id', 'retrieved_on', 'subreddit_id', 'ups')

    dburl = 'jdbc:postgresql://{ip}:{port}/{database}'.format(ip=dbip,
                                                              port=dbport,
                                                              database=dbname)
    df.write.jdbc(url, 'results',
                  mode='append',
                  properties={'user': dbuser, 'password': dbpwd}
    )
