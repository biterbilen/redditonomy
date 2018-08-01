# Redditonomy
_A Lambda Architecture Pipeline for Developing Taxonomies_

## Purpose
According to Alexa.com, Reddit is the 17th most active website globally. Given
Reddit's (sub)community-based structure, there are interesting patterns of
behavior that emerge in each individual community. Being able to automatically
extract topics from these communities is highly desirable in order to both
understand and target

A _taxonomy_ represents the underlying structure or themes within a body of
text. This structure can be obtained by feeding tokenized raw text into a
machine learning model such as WordNet that is able to classify tokens into
particular categories.

Redditonomy is a full-stack application that allows a user to find popular
subreddits and look at the main themes or taxonomies that emerge in those
subcultures as a function of time.

## Data
Reddit comments are available as bz2 compressed files through
[archive.org](https://archive.org) broken down by year from its inception in
2007 to 2015.

The volume of comments has increased over the years from ~100MBs in 2007 to
~30GBs per year. The data is a list of documents containing key, value pairs
such as:

Key | Value Type
----| ----------
parent_id | str
created_utc | int (utc)
controversiality | int
distinguished | null
subreddit_id | str
id | str
downs | int
score | int
author | str
body | str
gilded | int
author_flair_text | str
subreddit | str
name | str

- ~100GB of comments for batch model processing
- Simulated stream of ~500 comments per second (2011 rate)

## Relevant Technologies and Concepts
S3, Kafka, Spark, Spark-streaming, Cassandra, Machine-learning, WordNet, AirFlow

## Proposed Architecture 
- Raw bz2 sections of yearly data stored on S3.
- Kafka-python will emulate a stream of producers
- Kafka will ingest the data and produce subreddit topics
- Spark-streaming will ingest the simulated streaming data
- HDFS will be used to store longer-term data
- Spark will batch process longer-term data
- Cassandra will store analytical data
- Flask will query and present the data
<img src="./img/architecture.png" width="400px"/>

## Stretch Goals
Allow user to search for keywords or tags using ElasticSearch.

## Discovering Topics in a Corpus

One of the more widely used statistical methods for determing the important
concepts in a set of documents is called latent dirichlet allocation (LDA).
SparkMLib has an implementation of LDA that is able to use two different types
of optimization algorithms: expectation-maximization (EM) and online variational
Bayes (online).

[Example LDA gist](https://gist.github.com/jkbradley/ab8ae22a8282b2c8ce33)
[Spark LDA Doc](http://spark.apache.org/docs/2.1.2/api/java/org/apache/spark/mllib/clustering/LDA.html)
[Blog about LDA implementation using GraphX](https://databricks.com/blog/2015/03/25/topic-modeling-with-lda-mllib-meets-graphx.html)
