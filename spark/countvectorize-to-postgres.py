#! /usr/bin/env python
import os
import configparser
import datetime
from nltk.corpus import stopwords
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_json, to_date, lit, datediff, udf, struct
from pyspark.sql.types import *
from pyspark.ml.feature import CountVectorizer, StopWordsRemover
from pyspark.ml import Pipeline
from pyspark.ml.clustering import LDA, LDAModel
from itertools import chain

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

def indices_to_terms(vocabulary):
    def indices_to_terms(xs):
        return [vocabulary[int(x)] for x in xs]
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

    infilepath = 's3a://bkrull-insight-bucket/tokenize/{}-*'
    outfilepath = 's3a://bkrull-insight-bucket/cv/{subreddit}-{week}'
    df = spark.read.json(infilepath.format(2008))

    subreddits = df.select('subreddit').distinct().collect()
    weeks = get_week_list()
    cols = ['subreddit', 'date', 'results']
    print subreddits
    for row in subreddits:
    	print '-------------------{}----------------'.format(row.subreddit)
    	subreddit_df = df.filter(df.subreddit == row.subreddit).cache()
        for i, week in enumerate(weeks):
            print '========={}========'.format(week[0])
            tokens = df.filter(df.created_utc.between(lit(week[0]), lit(week[1]))) \
                     .select('body')

    	    num_docs = tokens.count()
            if num_docs < 1:
                continue
    
            remover = StopWordsRemover(inputCol='body', outputCol='filtered')
            cv = CountVectorizer(inputCol='filtered', outputCol='features', minDF=1.0)
            lda = LDA(k=5, maxIter=10, optimizer='online')
    	    pipeline = Pipeline(stages=[remover, cv, lda])
                
            # Fit the pipeline to training documents.
            model = pipeline.fit(tokens)
    
            cvmodel = model.stages[1]
            vocabulary = cvmodel.vocabulary
    	    vocab_size = len(vocabulary)

            topics = model.stages[-1].describeTopics()   
            topics = topics.withColumn(
                'terms',
                indices_to_terms(vocabulary)(topics.termIndices)
            )
            
            results = topics.withColumn(
                'results',
                struct(topics.terms, topics.termWeights)) \
                .select('results') \
                .rdd.flatMap(lambda list: list).collect()

            scores = []
            for topic in results:
                scores.append([
                    (topic.terms[i], topic.termWeights[i]) for i in range(10)])
            
            val = Row(date=week[0], results=scores, subreddit=row.subreddit, \
                      vocab_size=vocab_size, num_docs=num_docs)
            line = (row.subreddit, week[0], val)
            week_df = spark.createDataFrame([line], cols)

            week_df.printSchema()
            week_df = week_df.withColumn(
                'date',
                to_date(week_df.date))
            week_df = week_df.withColumn(
                'results',
                to_json(week_df.results).cast(StringType()))

            week_df.show(1)

            week_df.write.jdbc(
                dburl, 'newresults',
                mode='append',
                properties={'user': dbuser, 'password': dbpwd}
            )

