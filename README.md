# /r/edditonomy
_a spark.ml architecture for developing taxonomies_

## Business Case
According to Alexa.com, Reddit is the 17th most active website globally. Given
Reddit's (sub)community-based structure, there are interesting patterns of
behavior that emerge in each individual community. 

The underlying structure or themes within a body of text is called a _taxonomy_.
This structure can be obtained by feeding tokenized raw text into a machine
learning model such as latent dirichlet allocation (LDA) that is able to
classify tokens into particular categories.

The main value in such a platform is being able to automatically extract topics
from these communities yielding highly desirable targeting abilities for ads and
a deeper understanding of specific verticals.

/r/edditonomy is a full-stack application that allows a user to find popular
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

- 0.9TB of comments for batch model processing

## Relevant Technologies and Concepts
S3, Spark, SparkML, PostgreSQL, Machine-learning, AirFlow, Redis

## Proposed Architecture 
- Raw sections of yearly data stored on S3.
- Spark will batch ingest the comment data and store preprocessed results in PostgreSQL
- Batch SparkML jobs will run builds for weekly data and store results in PostgresQL 
- Redis cache will sit on top of PostgreSQL database to limit direct db interaction
- Batch jobs will be mediated by AirFlow
- Flask will query and present the data
<img src="./img/architecture.png" width="400px"/>

## Discovering Topics in a Corpus

One of the more widely used statistical methods for determing the important
concepts in a set of documents is called latent dirichlet allocation (LDA).
SparkMLib has an implementation of LDA that is able to use two different types
of optimization algorithms: expectation-maximization (EM) and online variational
Bayes (online).
