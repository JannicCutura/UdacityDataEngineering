import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


def create_spark_session():
    spark = (SparkSession
        .builder
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
        .getOrCreate())
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)
    songs_table = (df.filter("NOT song_id IS NULL")
                   .select(["song_id", "title", "artist_id", "year", "duration"]))

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + 'songs_table/')

    # extract columns to create artists table
    artists_table =  (df.filter("NOT artist_id IS NULL")
                   .select(["artist_id", "artist_name", "artist_location",
                            "artist_latitude", "artist_longitude"]))

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists_table/')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = f"{input_data}/log_data"

    # read log data file
    df =

    # filter by actions for song plays
    df =

    # extract columns for users table
    artists_table =

    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df =

    # create datetime column from original timestamp column
    get_datetime = udf()
    df =

    # extract columns to create time table
    time_table =

    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df =

    # extract columns from joined song and log datasets to create songplays table
    songplays_table =

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def etl(output_data='s3a://udacity-data-engineering-nd/data_lakes_with_spark/'):
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"


    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


etl()