# Redditonomy
A Lambda Architecture Pipeline for Developing Taxonomies

## Purpose
When dealing with a corpus of text, it is often important to extract
underlying concepts or themes. 

## Relevant Technologies
S3, Kafka, Spark, Spark-streaming, Cassandra, Plotly/dash

## Proposed Architecture 
- BZ2 compressed sections of yearly data will be stored on S3.
- Kafka will ingest the data and produce publications
- Spark-streaming will ingest the simulated streaming data
- HDFS will be used to store longer-term data
- Spark will batch process longer-term data
- Cassandra will store analytics-type data
- Plotly/dash front-end

## Data
Reddit comments are available through [archive.org](https://archive.org) broken
down by year. The volume of comments has increased over the years from ~100MBs
in 2007 to ~30GBs per year. Streaming data will be emulated by 
