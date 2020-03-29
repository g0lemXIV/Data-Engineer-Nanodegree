import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import monotonically_increasing_id as mi



config = configparser.ConfigParser()
config.read('dl.cfg')
# Add acces key to aws
os.environ['AWS_ACCESS_KEY_ID']=config['ROLE']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['ROLE']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    # helper function which create spark session
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def count_duplcates(df, by):
    """Function which calculcate percentage of
    reduced size
    Parameters
    ----------
    df: pyspark.sql.dataframe.DataFrame
        pyspark frame
    by: list
        list of columns which consider when drop 
    """
    temp = df.count()
    df = df.dropDuplicates(by)
    reduce_by = round(1 - (df.count()/temp), 2) * 100
    return (df, reduce_by)

def process_song_data(spark, input_data, output_data):
    """Function which process song data
    it read json files from input data, transform it
    and save into output s3 bucket
    Parameters
    ----------
    spark: pyspark.sql.dataframe.DataFrame
        spark engine
    input_data: str
        path to input data, should be called from dl.cfg
    output_data: str
        path to output data, should be called from dl.cfg
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, config['DATA']['SONG_DATA'])
    
    # read song data file
    df = spark.read.json(song_data)
    print('Data has been loaded successfully!')
    # extract columns to create songs table
    songs_table = df.select('song_id',
                            'title',
                            'artist_id',
                            'year', 
                            'duration')
    # drop duplicates if exist
    print('Drop duplicates from song table')
    songs_table, reduce = count_duplcates(songs_table, ['song_id', 'artist_id'])
    print('Song table reduced by {}%'.format(reduce))
    # write songs table to parquet files partitioned by year and artist
    out_path = os.path.join(output_data, 'songs.parquet')
    print('Write songs table to parquet into {}'.format(out_path))
    songs_table.write.partitionBy("year", "artist_id").parquet(out_path, compression='gzip',
                              mode='overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id',
                              'artist_name',
                              'artist_location',
                              'artist_latitude',
                              'artist_longitude')
    
    # write artists table to parquet files
    out_path = os.path.join(output_data, 'artist.parquet')
    print('Write artists table to parquet into {}'.format(out_path))
    artists_table.write.parquet(out_path, compression='gzip',
                              mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """Function which process log user data
    it read json files from input data, transform it
    and save into output s3 bucket
    Parameters
    ----------
    spark: pyspark.sql.dataframe.DataFrame
        spark engine
    input_data: str
        path to input data, should be called from dl.cfg
    output_data: str
        path to output data, should be called from dl.cfg
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, config['DATA']['LOG_DATA'])

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("""page = 'NextSong'""")

    # extract columns for users table    
    user_table = df.select('userId',
                           'firstName',
                           'lastName',
                           'gender',
                           'level')

    print('Drop duplicates from user table')
    user_table, reduce = count_duplcates(user_table, ['userId'])
    print('User table reduced by {}%'.format(reduce))
    # write users table to parquet files
    out_path = os.path.join(output_data, 'user.parquet')
    print('Write users table to parquet into {}'.format(out_path))
    user_table.write.parquet(out_path, compression='gzip',
                             mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x/1000.0)),
                        returnType=T.TimestampType())
    df = df.withColumn("ts", get_timestamp('ts'))
    
    # extract columns to create time table
    time_table = df.select(col('ts').alias('timestamp'),
                           hour('ts').alias('hour'),
                           dayofmonth('ts').alias('day'),
                           weekofyear('ts').alias('week'),
                           month('ts').alias('month'),
                           year('ts').alias('year'),
                           dayofweek('ts').alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    out_path = os.path.join(output_data, 'time.parquet')
    print('Write time table to parquet into {}'.format(out_path))
    time_table.write.partitionBy("year", "month").parquet(out_path,
                                                          compression='gzip',
                                                          mode='overwrite')

    # read in song data to use for songplays table
    out_path = os.path.join(output_data, 'songs.parquet')
    print('Read song table to DataFrame from {}'.format(out_path))
    song_df = spark.read.parquet(out_path)

    # get condition how to join tables
    cond = [df.song == song_df.title]
    # extract columns from joined song and log datasets to create songplays table
    songplays_table = song_df.join(df, cond)
    songplays_table = songplays_table.select('ts', 
                                             'userId',
                                             'level',
                                             'song_id',
                                             'artist_id',
                                             'sessionId',
                                             'location',
                                             'userAgent'
                                             )
    # Add id for songplay
    songplays_table = songplays_table.withColumn("songplay_id", mi())
    songplays_table = songplays_table.withColumn("month", month('ts'))
    songplays_table = songplays_table.withColumn("year", year('ts'))

    # write songplays table to parquet files partitioned by year and month
    out_path = os.path.join(output_data, 'songplays.parquet')
    print('Write time table to parquet into {}'.format(out_path))
    songplays_table.write.partitionBy("month", "year").parquet(out_path,
                                  compression='gzip',
                                  mode='overwrite')


def main():
    """Create spark session with AWS creditentials,
    run all process function and save transformed files into s3.
    """
    spark = create_spark_session()
    input_data = config['DATA']['INPUT_PATH']
    output_data = config['DATA']['OUTPUT_PATH']
    print('WARNING! WE ARE USING PYTHON FOR ALL MANIPULATION WHICH COSE SLOW PROCESSING \
           IMPLEMENTATION SHOULD BE CHANGE TO SPARK SQL')
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
