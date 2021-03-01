import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, from_unixtime
from pyspark.sql.types import LongType, FloatType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Process data from songs dataset:
        - Creates songs data file
        - Create artists data file
        Arguments:
            spark {object}:   Spark session
            input_data {str}: S3 bucket url of input data
            output_data {str}: S3 bucket url of output data
        Returns:
            N/A
    """
    # get filepath to song data file    
    song_data = input_data+'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.load(song_data, format='json')

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.save(output_data+'songs.parquet',format="parquet",header=True)
    
    # extract columns to create artists table
    artists_table = df.select("artist_id",col("artist_name").alias("name"),
                              col("artist_location").alias("location"),
                              col("artist_latitude").alias("latitude"),
                              col("artist_latitude").alias("longitude")).distinct()
    
    # write artists table to parquet files
    artists_table.write.save(output_data+'artists.parquet',format="parquet",header=True)


def process_log_data(spark, input_data, output_data):
    """ Process data from logs dataset:
        - Creates time data file
        - Creates users data file
        - Creates songplays data file
        Arguments:
            spark {object}:   Spark session
            input_data {str}: S3 bucket url of input data
            output_data {str}: S3 bucket url of output data
        Returns:
            N/A
    """
    # get filepath to log data file
    log_data = input_data+'log_data/*/*/*.json'

    # read log data file
    df = spark.read.load(log_data, format='json')
    
    # filter by actions for song plays
    df = df.where(df.page=="NextSong")

    # extract columns for users table
    users_table = df.select(col("userId").alias("user_id"),
                            col("firstName").alias("first_name"), 
                            col("lastName").alias("last_name"),
                            "gender", "level").distinct()
    
    # write users table to parquet files
    users_table.write.save(output_data+'users.parquet',format="parquet",header=True)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : x/1000, FloatType() )
    df = df.filter(df.ts.isNotNull()).withColumn('ts', col('ts').cast(FloatType()))
    df = df.withColumn('ts', get_timestamp('ts').cast(LongType()))
    df = df.withColumn('ts',from_unixtime('ts'))

    
    # extract columns to create time table
    df = df.withColumn('month', month('ts')).withColumn('year', year('ts'))
    df = df.withColumn('hour', hour('ts')).withColumn('day', dayofmonth('ts'))
    df = df.withColumn('weekday', dayofweek('ts')).withColumn('week', weekofyear('ts'))
    time_table = df.select(col('ts').alias('start_time'), 
                           'hour','day','week', 'month', 'year', 'weekday')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.save(output_data+'time.parquet',format="parquet",header=True)

    # read in song data to use for songplays table
    song_df = spark.read.load(output_data+'songs.parquet', format='parquet')

    # extract columns from joined song and log datasets to create songplays table
    df = df.withColumnRenamed('song', 'title')
    songplays_table = df.join(song_df,'title').select(col("ts").alias("start_time"),
                                                col("userId").alias("user_id"), "level",
                                                "song_id", "artist_id",
                                                col("sessionId").alias("session_id"),
                                                "location", col("userAgent").alias("user_agent"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.save(output_data+'songplays.parquet',format="parquet",header=True)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://<YOUR_BUCKET>/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
