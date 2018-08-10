#! /usr/bin/env python
import os
import configparser
import datetime
from nltk.corpus import stopwords
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_date, lit, datediff, udf
from pyspark.sql.types import *
from pyspark.ml.feature import CountVectorizer
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

def make_results(subreddit, date, vocab_size, num_docs, terms_and_scores):
     def make_results(subreddit, date, vocab_size, num_docs, terms_and_scores):
         """ expecting 3 columns in this order: date, subreddit, countvector"""

         terms_and_scores = ['hello', 'banana']
	 jsonresult = {
	     "date": date,
	     "results": terms_and_scores,
	     "subreddit": subreddit,
	     "vocab_size": vocab_size,
	     "num_docs": num_docs
	 }
	 return str(jsonresult)

     return udf(make_results, StringType())

def indices_to_terms(vocabulary, scores):
    def indices_to_terms(xs):
        return [(vocabulary[int(x)], scores[int(x)]) for x in xs]
    return udf(indices_to_terms, ArrayType(ArrayType()))

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
    cols = ['subreddit', 'week', 'tokens', 'num_docs', 'vocab_size']

    for row in subreddits:
	print row.subreddit
	subreddit_df = df.filter(df.subreddit == row.subreddit).cache()
        for i, week in enumerate(weeks):
            tokens = df.filter(df.created_utc.between(lit(week[0]), lit(week[1]))) \
                     .select('body') \
                     .rdd.flatMap(lambda list: chain(*list)).collect()
        
	    numdocs = len(tokens)

	    line = (row.subreddit, week[0], tokens, numdocs, 0)
	    week_df = spark.createDataFrame([line], cols)

            cv = CountVectorizer(inputCol='tokens', outputCol='features', minDF=1.0)
            lda = LDA(k=10, seed=1, maxIter=20, optimizer='online')
	    pipeline = Pipeline(stages=[cv, lda])
            
            # Fit the pipeline to training documents.
            model = pipeline.fit(week_df)

	    prediction = model.transform(week_df)
	    vocab = cv.fit(week_df)
	    vocab_size = len(vocab.vocabulary)
            prediction = prediction.withColumn(
				'terms_and_scores',
	        		indices_to_terms(
				    vocab.vocabulary,
				    prediction.topicDistribution)('features'))

	    prediction.printSchema()
	    prediction.show()
#            prediction = prediction.withColumn(
#				'results',
#	        		make_results(
#				    prediction.subreddit,
#				    prediction.week,
#				    prediction.vocab_size,
#				    prediction.num_docs,
#				    prediction.terms_and_scores
#				))
#
#            prediction.select('subreddit', 'week', 'results').write.jdbc(
# 		dburl, 'results',
#                mode='append',
#                properties={'user': dbuser, 'password': dbpwd}
#            )
	    break
	    if i >10:  break
        break
    
