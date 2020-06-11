# coding=utf-8
"""ETL of podcasts data etl process"""
import time
from datetime import datetime
import argparse
import string
import json
import os
import pandas as pd
# other helper functions
import config as cfg
import itunes_wrapper as itunes
import clean_data as clean
from rss_wrapper import main as xmlparser
import aws_utils as aws
from load_tables import main as staging
from create_tables import main as creating
# aws sdk
import boto3


def load_authors_names(csv_name: str = 'kaggle_podcasts.csv', test: bool =False) -> list:
    """Load kaggle dataset and extract unique authors.

    Parameters
    ----------
    csv_name : str
        Name of file with podcasts (should be paste in data/raw).
    test : bool
        For test purpose, data can be split to chunk of 100 rows.

    Returns
    -------
    list
        Returns list of unique authors.

    """
    # load kaggle podcast data
    try:
        data_path = os.path.join(os.getenv('RAW_DATA'), csv_name)
        names = pd.read_csv(data_path, usecols=['Name'])
        if test is True:
            names = names.sample(100)
    except FileNotFoundError:
        print('Csv not found in {}'.format(data_path))
    names = names[names['Name'].str.contains('[A-Za-z]')]
    names = names.Name.unique()
    # clean names
    table = str.maketrans('', '', string.punctuation)
    stripped = [w.translate(table) for w in names]
    authors = [word.lower() for word in stripped]
    return authors


def itunes_collect(authors: list, save_data: bool = True,
                   save_feed: bool = True) -> list:
    """Itunes data parser. It can take long time to grab all data with no
    developer account. Normally Apple block requests greater then 20 per min.

    Parameters
    ----------
    authors : list
        List of string of authors name.
    save_data : bool
        If True, save data into JSON_DATA path.
    save_feed : bool
        If True, all feed urls were saved to txt file.

    Returns
    -------
    list
        List of list with unique feed urls.

    """
    # create Apple ITunes instance
    chunk = len(authors) //10
    apple = itunes.AppleSearch()
    apple_path = os.getenv('JSON_DATA')
    feed_list = list()
    # search for podcaster in loop
    print("Starting request to Itunes, is can be take up to 4 hours")
    for i, _ in enumerate(authors):
        if (i % chunk) == 0:
            print("{}/{} authors scan".format(i+1, len(authors)))
        try:
            subfolder_name = authors[i][0]
            sub_apple_path = itunes.make_dir(apple_path, subfolder_name)
        except IndexError:
            sub_apple_path = itunes.make_dir(apple_path, 'None')
        apple.search(authors[i])
        apple.get_data(save=save_data, podcast_path=sub_apple_path)
        # add feed url to feed list
        feed_list.extend(apple.get_feed())
        if i % 20 == 0:
            time.sleep(60)
    print("Finished at: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    # clean feed list
    feed_list = list(set(feed_list))
    feed_list = list(filter(None, feed_list))
    if save_feed is True:
        feed_path = os.path.join(cfg.DATA_PATH, 'feed_url.txt')
        with open(feed_path, 'w') as text_file:
            for feed in feed_list:
                text_file.write("%s\n" % feed)
        print('Sucefully saved feed to {}'.format(feed_path))
    return feed_list


def rss_collect(feed_list: list) -> None:
    """Parse RSS fedd (WARING parser use all cpus).

    Parameters
    ----------
    feed_list : list
        list of feed url to request on.

    Returns
    -------
    None
    """
    xmlparser(feed_list, os.getenv('XML_DATA'))
    pass


def data_processing(kaggle_csv_name: str = "kaggle_podcasts.csv") -> dict:
    """Process all data with clean_data.

    Parameters
    ----------
    kaggle_csv_name : str
        path to kaggle data csv

    Returns
    -------
    dict
        Returns dict with same name as S3 subfolder,

    """
    # read data paths
    data_path = os.getenv('RAW_DATA')
    xml_files = os.getenv('XML_DATA')
    apple_path = os.getenv('JSON_DATA')
    kaggle_path = os.path.join(data_path, kaggle_csv_name)
    podcast_csvs = [os.path.join(xml_files, file) for file in os.listdir(xml_files) if 'podcast' in file]
    shows_csvs = [os.path.join(xml_files, file) for file in os.listdir(xml_files) if 'shows' in file]
    # process data
    podcasts_df = clean.merge_podcasts(kaggle_path, apple_path, podcast_csvs)
    episodes_df = clean.merge_episodes(shows_csvs)

    return {"episodes_table": episodes_df, "podcasts_table": podcasts_df}


def save_preprocess_data(df: pd.DataFrame, alias: str, chunk_num: int, save_dir: str) -> str:
    """Save preprocess data in small chunk.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe to save
    alias : str
        alias to data.
    chunk_num : int
        number of chunk to split data
    save_dir : str
        saving_dir, it should be different for podcasts and episodes

    Returns
    -------
    str
        Path to local saved file (need for s3)

    """
    paths = clean.save_data(df=df, counts=chunk_num, save_dir=save_dir,
                            alias=alias)
    return paths

def load_to_s3(file_path: str, s3_pth: str, bucket_name: str) -> None:
    """Load data to S3.

    Parameters
    ----------
    file_path : str
        Path to local data chunk
    s3_pth : str
        s3 name.
    bucket_name : str
        bucket name

    Returns
    -------
    None

    """
    json_arr = ''
    s3_resource = boto3.resource('s3')
    chunk = pd.read_csv(file_path)
    # for sure
    chunk.fillna('Empty', inplace=True)
    for row in chunk.to_dict(orient='r'):
        json_arr += json.dumps(row)
    s3_resource.Object(bucket_name, s3_pth).put(Body=json_arr)


def s3_to_redshift() -> None:
    """Load data from S3 to Redshift. First step is to create tables, second
    staging and inserting all data.
    """
    creating()
    staging()

def main(test: bool = True):
    """Main function to ETL pipeline.
    Steps are common:
    1. Load authors
    2. Load AppleITunes and XML datasets
    3. Process datasets
    4. Save datasets into S3
    5. Inserting data to Redshift.

    Parameters
    ----------
    test : bool
        For test use True.
    """
    print("Start processing kaggle data")
    authors = load_authors_names(test=test)
    print("Start collecting itunes data")
    feed_list = itunes_collect(authors)
    print("Start collecting rrs feed")
    rss_collect(feed_list)
    print("Start data processing")
    data_dict = data_processing()
    print("Start saving preprocess data")
    # Itereate over dictionary of data and save it to S3
    for name, df in data_dict.items():
        max_ = df.shape[0] // 1000
        chunk = max(max_, 10)
        print(name)
        if name == 'episodes_table':
            dir_ = os.getenv('PREPROCESS_EPISODES')
        elif name == 'podcasts_table':
            dir_ = os.getenv('PREPROCESS_PODCASTS')
        paths = save_preprocess_data(df, name, chunk_num=chunk, save_dir=dir_)
        for i, path in enumerate(paths):
            new_name = '{}/{}_{}.json'.format(name, name, i)
            print(new_name)
            load_to_s3(path, s3_pth=new_name,
                        bucket_name=os.getenv('BUCKET_NAME'))
        # Start Staging and Inserting
        print("Starting S3 to Redshift loader")
        s3_to_redshift()
        print("Sucefully make data analitics tables")


if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(description='Test podcast etl pipeline')

    parser.add_argument('--test', action="use for test",
                        default=False)
    args = parser.parse_args()
    main(test=args.test)
