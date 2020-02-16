# project_data_lake

## Overview 

In this project, I build a data lake hosted on S3 using Spark for a music stremaing app called Sparkify. To do so, I build an ETL pipeline that extracts raw data - logs of user activity and metadata on songs in the app - that reside in S3 buckets, processes it using Spark, and loads the transformed data onto the data lake as a set of dimensional tables following the STAR schema.

* `data`: Contains examples of the raw data.
* `etl.py`: Reads data from S3, processes it and writes them to the data lake.
* `.env`: Contains AWS credentials. Not pushed to the repo. 

## Quick Start

1. Create your .env file with the following structure:

```
AWS_ACCESS_KEY_ID= <insert access key>
AWS_SECRET_ACCESS_KEY= <insert secret access key>
```

## Tech Stack
* pyspark
* boto3
* dotenv
* AWS
