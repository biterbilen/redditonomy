#! /usr/bin/env python
"""
This module contains the workings of a pre-processing engine for
reddit comment data sitting on S3. It is comprised of the following steps

1. Setup SparkSession
2. Loop over S3 directory structure (year-month) and read in large (many GB) files using sc.textFile with predefined Schema
   - Initially tried to use df.read.json, but was unable due to too-large memory requirements
3. Clean, simple tokenization and stop word removal
4. Write preprocessed year-month dataframe back to S3
"""

import os
import json
import configparser
from nltk.corpus import stopwords
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, from_unixtime, size

stopwords = set(stopwords.words('english'))

infilepath = 's3a://bkrull-insight-bucket/RC/RC_{year}-{month:02d}'
outfilepath = 's3a://bkrull-insight-bucket/tokenize/{year}-{month:02d}'

schema = StructType([
    StructField('archived', BooleanType()),
    StructField('author', StringType()),
    StructField('author_flair_css_class', StringType()),
    StructField('author_flair_text', StringType()),
    StructField('body', StringType()),
    StructField('controversiality', IntegerType()),
    StructField('created_utc', StringType()),
    StructField('distinguished', StringType()),
    StructField('downs', IntegerType()),
    StructField('edited', StringType()),
    StructField('gilded', IntegerType()),
    StructField('id', StringType()),
    StructField('link_id', StringType()),
    StructField('name', StringType()),
    StructField('parent_id', StringType()),
    StructField('retrieved_on', IntegerType()),
    StructField('score', IntegerType()),
    StructField('score_hidden', BooleanType()),
    StructField('subreddit', StringType()),
    StructField('subreddit_id', StringType()),
    StructField('ups', IntegerType())
])
udf_tokenize = udf(lambda x: \
    [word \
        for word in x.lower().split() \
        if (len(word) > 3 and \
            word.isalpha() and \
            word not in stopwords)],
    ArrayType(StringType()))

columns_to_drop = [
    'archived', 'author_flair_css_class', 'controversiality', \
    'author_flair_text', 'distinguished', 'downs', \
    'edited', 'gilded', 'id', 'link_id', 'name', 'parent_id', \
    'removal_reason', 'retrieved_on', 'score_hidden', 'subreddit_id', 'ups'
]

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

    dburl = 'jdbc:postgresql://{ip}:{port}/{database}'.format(
        ip=dbip, port=dbport, database=dbname)

    os.environ['PYSPARK_SUBMIT_ARGS'] = \
            "--packages org.apache.hadoop:hadoop-aws:2.7.1 pyspark-shell"

    spark = SparkSession.builder \
            .appName("S3 READ TEST") \
            .config("spark.executor.cores", "6") \
            .config("spark.executor.memory", "6gb") \
     .config("spark.sql.session.timeZone", "America/Los_Angeles") \
            .getOrCreate()

    sc = spark.sparkContext

    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.awsAccessKeyId", access_id)
    hadoop_conf.set("fs.s3a.awsSecretAccessKey", access_key)

    for year in range(2008, 2016):
        for month in range(1, 13):
            print 'Reading {year}: {month:02d}'.format(year=year, month=month)
            rdd = sc.textFile(infilepath.format(year=year, month=month))
            df = rdd.map(json.loads).toDF(schema=schema).persist()

            df = df.drop(*columns_to_drop) \
                 .filter(df.score > 10) \
                 .filter(df.body != '[deleted]')

            df = df.withColumn('created_utc',
                               df.created_utc.cast(IntegerType()))
            df = df.withColumn('created_utc',
                               from_unixtime(
                                   df.created_utc, format='yyyy-MM-dd'))

            df = df.withColumn('body', udf_tokenize('body')) \
                 .filter(size('body') != 0)

            print 'Writing {year}: {month}'.format(year=year, month=month)
            df.write.json(outfilepath.format(year=year, month=month))

            spark.catalog.clearCache()
