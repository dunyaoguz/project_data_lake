# project_data_lake

## Overview 

In this project, I set up a data lake hosted on S3 using Spark for a music stremaing app called Sparkify. To do so, I build an ETL pipeline that extracts raw data - logs of user activity and metadata on songs in the app - that reside in S3 buckets, processes it using Spark, and loads the transformed data onto the data lake as a set of dimensional tables following the STAR schema.

* `data`: Contains examples of the raw data.
* `etl.py`: Reads data from S3, processes it and writes them to the data lake.
* `.env`: Contains AWS credentials. Not pushed to the repo. 

## Quick Start

1. Obtain your AWS credentials from console. 

2. Install dependencies.

```
pip install boto3
pip install pyspark
pip install python-dotenv
```

3. Create your .env file with the following structure:

```
AWS_ACCESS_KEY_ID= <insert access key>
AWS_SECRET_ACCESS_KEY= <insert secret access key>
DATA_LAKE_NAME= <insert a name for your data lake> 
```
4. Run the following commands.

``` 
cd project_data_lake
python etl.py
```
## Example Query

Find the percentage of male and female users:

```
user_df = spark.read.parquet('user_data.parquet')
user_df.registerTempTable("user_data")

gender_paid_counts = spark.sql(
    """
    SELECT 
        COUNT(*) GENDER_COUNT,
        gender, 
        ROUND(AVG(CASE WHEN level = 'paid' THEN 1 ELSE 0 END), 2) PAID_PERCENT
    FROM user_data
    GROUP BY 2
    """).collect()
t = PrettyTable(['GENDER', 'GENDER_COUNT', 'PAID_PERCENT'])
for row in gender_paid_counts:
    t.add_row([row.gender, row.GENDER_COUNT, row.PAID_PERCENT])
print(t)
```
```
+--------+--------------+--------------+
| GENDER | GENDER_COUNT | PAID_PERCENT |
+--------+--------------+--------------+
|   F    |      60      |     0.25     |
|   M    |      44      |     0.16     |
+--------+--------------+--------------+
```

## Tech Stack
* pyspark
* boto3
* dotenv
* AWS 
