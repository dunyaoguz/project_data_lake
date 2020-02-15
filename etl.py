from datetime import datetime
from dotenv import load_dotenv, find_dotenv
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import boto3

load_dotenv()

KEY = os.environ['AWS_ACCESS_KEY_ID']
SECRET = os.environ['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Create a spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def create_s3_bucket(bucket_name):
    """
    Creates an s3 bucket with input name.
    """
    s3 = boto3.client('s3',
                      region_name='us-west-2',
                      aws_access_key_id=KEY,
                      aws_secret_access_key=SECRET)
    s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint':'us-west-2'})

create_s3_bucket('dunyas-lake-2')

def process_song_data(spark, input_data, output_data):
    """
    Extract data on songs and artists from the source s3 bucket and load onto the target s3 bucket.
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, 'songs.parquet'), mode='overwrite', partitionBy=['year', 'artist_id'])
    print('Created the songs table.')

    # extract columns to create artists table
    artists_table = df.select('artist_id',
                              F.col('artist_name').alias('name'),
                              F.col('artist_location').alias('location'),
                              F.col('artist_latitude').alis('latitude'),
                              F.col('artist_longitude').alias('longitude')) \
                              .dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')
    print('Created the artists table.')


def process_log_data(spark, input_data, output_data):
    """
    Extract data on users, time and songplays from the event logs in the source s3 bucket and load onto the target s3 bucket.
    """
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.select(F.col('userId').alias('user_id'),
                            F.col('firstName').alias('first_name'),
                            F.col('lastName').alias('last_name'),
                            'gender',
                            'level') \
                    .dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')
    print('Created the users table.')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: str(int(int(ts)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: str(datetime.fromtimestamp(int(ts) / 1000)))
    df = df.withColumn('start_time', get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select('start_time',
                           F.hour('start_time').alias('hour'),
                           F.dayofmonth('start_time').alias('day'),
                           F.weekofyear('start_time').alias('week'),
                           F.month('start_time').alias('month'),
                           F.year('start_time').alias('year')) \
                   .dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, 'time.parquet'), mode='overwrite', partitionBy=['year', 'month'])
    print('Created the time table.')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs_table.parquet'))

    # extract columns from joined song and log datasets to create songplays table
    df = df.join(song_df, song_df.title == df.song)
    songplays_table = df.select('start_time',
                                F.col('userId').alias('user_id'),
                                'level',
                                'song_id',
                                'artist_id',
                                F.col('sessionId').alias('session_id'),
                                'location',
                                F.col('userAgent').alias('user_agent'))
    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, 'songplays.parquet'), mode='overwrite', partitionBy=['year', 'month'])
    print('Created the songplays table.')


def main():
    create_s3_bucket('dunyas-lake')
    spark = create_spark_session()

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dunyas-lake/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
