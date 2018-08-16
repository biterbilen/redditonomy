#! /usr/bin/env python
import os
import configparser
import datetime
import argparse
from dateutil.parser import parse
from nltk.corpus import stopwords
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_json, to_date, lit, datediff, udf, struct
from pyspark.sql.types import *
from pyspark.ml.feature import CountVectorizer, StopWordsRemover
from pyspark.ml import Pipeline
from pyspark.ml.clustering import LDA, LDAModel
from itertools import chain

def indices_to_terms(vocabulary):
    def indices_to_terms(xs, weights):
        terms = [(vocabulary[int(x)], weights[i]) for i, x in enumerate(xs)]
        this_score = sorted(terms, key=lambda x: x[1], reverse=True)
        top_terms, _ = zip(*this_score)
        top_terms = top_terms[:5]
        return top_terms
    return udf(indices_to_terms, ArrayType(StringType()))


if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read(os.path.expanduser('~/.aws/credentials'))

    dbuser = config.get('db', 'user')
    dbpwd = config.get('db', 'password')
    dbip= config.get('db', 'ip')
    dbport = config.get('db', 'port')
    dbname = config.get('db', 'database')
    access_id = config.get('aws', 'aws_access_key_id') 
    access_key = config.get('aws', 'aws_secret_access_key')

    dburl = 'jdbc:postgresql://{ip}:{port}/{database}'.format(ip=dbip,
                                                              port=dbport,
                                                              database=dbname)

    access_id = config.get('aws', 'aws_access_key_id') 
    access_key = config.get('aws', 'aws_secret_access_key')

    parser = argparse.ArgumentParser()
    parser.add_argument('subreddit', help='name of subreddit')
    parser.add_argument('week', help='date of beginning of week')
    args = parser.parse_args()

    subreddit = args.subreddit
    week = args.week
    week_end = parse(week) + datetime.timedelta(days=7)
    week_end = week_end.isoformat()
    year = week.split('-')[0]
    month = int(week.split('-')[1])

    with open('/home/ubuntu/insight/src/spark/extra_stopwords.txt') as f:
        extra_stopwords = [
            unicode(line.strip(), 'utf-8') for line in f.readlines()]

    spark = SparkSession.builder \
            .appName('Count Vectorization') \
            .config('spark.task.maxFailures', '20') \
            .config('spark.executor.cores', '6') \
            .config('spark.executor.memory', '6gb') \
	        .config('spark.dynamicAllocation.enabled', False) \
     	    .config('spark.sql.session.timeZone', 'America/Los_Angeles') \
            .getOrCreate()

    sc = spark.sparkContext

    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    hadoop_conf.set('fs.s3a.awsAccessKeyId', access_id)
    hadoop_conf.set('fs.s3a.awsSecretAccessKey', access_key)

    infilepath = 's3a://bkrull-insight-bucket/tokenize/{year}-{month:02d}'
    outfilepath = 's3a://bkrull-insight-bucket/results/{subreddit}/{week}'
    cols = ['subreddit', 'date', 'results']
    
    remover = StopWordsRemover(inputCol='body', outputCol='filtered')
    stopwords = remover.getStopWords()
    stopwords.extend(extra_stopwords)
    remover.setStopWords(stopwords).setCaseSensitive(True)

    cv = CountVectorizer(inputCol='filtered', outputCol='features', minDF=1.0)
    lda = LDA(k=5, maxIter=10, optimizer='online')
    pipeline = Pipeline(stages=[remover, cv, lda])

    df = spark.read.json(infilepath.format(year=year, month=month))
    subreddit_df = df.filter(df.subreddit == subreddit)
    tokens = subreddit_df.filter(df.created_utc.between(lit(week), lit(week_end))) \
             .select('body')
    
    num_docs = tokens.count()
    if num_docs >= 1:
        model = pipeline.fit(tokens)

        cvmodel = model.stages[1]
        vocabulary = cvmodel.vocabulary
        vocab_size = len(vocabulary)

        topics = model.stages[-1].describeTopics()   
        topics = topics.withColumn(
            'terms',
            indices_to_terms(vocabulary)(topics.termIndices,
            topics.termWeights)
        )
        
        scores = topics.select('terms').rdd.flatMap(lambda list: list).collect()
        
        val = Row(date=week, results=scores, subreddit=subreddit, \
                  vocab_size=vocab_size, num_docs=num_docs)
        line = (subreddit, week, val)
        week_df = spark.createDataFrame([line], cols)

        week_df.printSchema()
        week_df = week_df.withColumn(
            'date',
            to_date(week_df.date))
        week_df = week_df.withColumn(
            'results',
            to_json(week_df.results).cast(StringType()))

        week_df.write.jdbc(
            dburl, 'newresults',
            mode='append',
            properties={'user': dbuser, 'password': dbpwd}
        )
