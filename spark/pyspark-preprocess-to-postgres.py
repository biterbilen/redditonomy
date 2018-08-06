#! /usr/bin/env python

import os
import configparser
from nltk.corpus import stopwords
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, from_unixtime, size

def build_vocabulary():
    """
    val termCounts: Array[(String, Long)] = \
        tokenized.flatMap(_.map(_ -> 1L)).reduceByKey(_ + _).collect().sortBy(-_._2)
    val numStopwords = 20
    val vocabArray: Array[String] = \
        termCounts.takeRight(termCounts.size - numStopwords).map(_._1)
    val vocab = Map[String, Int] = vocabArray.zipwithIndex.toMap
    """
    return vocab_array
   
def build_term_count_vectors():
    """
    val documents: RDD[(Long, Vector)] = \
	tokenized.zipWithIndex.map { case (tokens, id) =>
	    val counts = new mutable.HashMap[Int, Double]()
	    tokens.foreach { term =>
		if (vocab.contains(term)) {
		    val idx = vocab(term)
		    counts(idx) = counts.getOrElse(idx, 0.0) + 1.0
		}
	    }
	    (id, Vectors.sparse(vocab.size, counts.toSeq))
	}
    """
    return term_count_vectors


if __name__ == '__main__':
    config = configparser.ConfigParser()

    config.read(os.path.expanduser('~/.aws/credentials'))
    access_id = config.get('aws', 'aws_access_key_id') 
    access_key = config.get('aws', 'aws_secret_access_key')

    os.environ['PYSPARK_SUBMIT_ARGS'] = \
            "--packages org.apache.hadoop:hadoop-aws:2.7.1 pyspark-shell"

    spark = SparkSession.builder \
            .appName("S3 READ TEST") \
            .config("spark.executor.cores", "6") \
            .config("spark.executor.memory", "4gb") \
	    .config("spark.sql.session.timeZone", "America/Los_Angeles") \
            .getOrCreate()

    sc=spark.sparkContext

    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.awsAccessKeyId", access_id)
    hadoop_conf.set("fs.s3a.awsSecretAccessKey", access_key)

    base = 's3a://bkrull-insight-bucket/RC/RC_'
    year = 2008
    df = spark.read.json([base+'{}-{:02d}'.format(year, month) 
			  for month in range(12, 13)])

    stopwords = set(stopwords.words('english'))
    columns_to_drop = [
	'archived', 'author_flair_css_class', \
	'author_flair_text', 'distinguished', 'downs', \
	'edited', 'gilded', 'id', 'link_id', 'name', 'parent_id', \
	'removal_reason', 'retrieved_on', 'score_hidden', 'subreddit_id', 'ups'
    ]
    df = df.drop(*columns_to_drop) \
	 .filter(df.score > 10) \
	 .filter(df.body != '[deleted]')

    df = df.withColumn('created_utc', df.created_utc.cast(IntegerType()))
    df = df.withColumn('created_utc', 
	 	       from_unixtime(df.created_utc, format='yyyy-MM-dd'))

    df.show(10)

    udf_tokenize = udf(lambda x: \
	[word \
	 for word in x.lower().split() \
	 if (len(word) > 3 and \
 	     word.isalpha() and \
	     word not in stopwords)], 
	ArrayType(StringType()))

    df = df.withColumn('body', udf_tokenize('body')) \
	 .filter(size('body') != 0)

    df.show(10)

    dbuser = config.get('db', 'user')
    dbpwd = config.get('db', 'password')
    dbip= config.get('db', 'ip')
    dbport = config.get('db', 'port')
    dbname = config.get('db', 'database')

    dburl = 'jdbc:postgresql://{ip}:{port}/{database}'.format(ip=dbip,
                                                              port=dbport,
                                                              database=dbname)
    df.write.jdbc(dburl, 'corpus',
		  mode='overwrite',
                  properties={'user': dbuser, 'password': dbpwd}
    )

