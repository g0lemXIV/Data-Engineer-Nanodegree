# coding=utf-8
"""Cleaning and merging datasets"""
import numpy as np
import pandas as pd
import warnings
import json
import os
import glob
import configparser
import os

warnings.filterwarnings('ignore')


def read_data(filepath: str) -> pd.DataFrame:
    """read data from json files.

    Parameters
    ----------
    filepath : str
        Path to json files.

    Returns
    -------
    pd.DataFrame
        Returns pandas dataframe with all object files.

    """
    # get all files matching extension from directory
    json_list = list()
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            try:
                with open(f, 'r') as j:
                    contents = json.loads(j.read())
                    json_list.append(contents)
            except Exception as e:
                print(e)
        df = pd.DataFrame(json_list)
    return df


# kaggle dataframe cleaning
def clean_kaggle_df(data_path: str) -> pd.DataFrame:
    """Clean kaggle podcasts data with pandas.

    Parameters
    ----------
    data_path : str
        data path to raw kaggle_data_podcasts (must be csv).

    Returns
    -------
    pd.DataFrame
        Returns clean pandas with ratings, genre, name and description

    """
    podcast_kaggle_df = pd.read_csv(data_path, index_col='index')
    # check if data had all names
    names_count = podcast_kaggle_df['Name'].isnull().sum()
    if names_count == 0:
        print('All names exist in kaggle podcast dataset')
    else:
        print('{} names not exist or read not properly'.format(names_count))
    # check description and change characters to lower
    podcast_kaggle_df['Description'] = podcast_kaggle_df.Description.str.lower()
    # replace all non alphabet characters
    podcast_kaggle_df['Description'] = podcast_kaggle_df['Description'].str.replace('\W', ' ')
    description = podcast_kaggle_df['Description'].isnull().sum()
    if names_count == 0:
        print('All Descriptions exist in kaggle podcast dataset')
    else:
        print('{} Description(s) not exist or read not properly'.format(names_count))
    # Change Not Found values to 0 and set dtype to numeric
    rates = ['Rating', 'Rating_Volume']
    podcast_kaggle_df[rates] = podcast_kaggle_df[rates].replace('Not Found',  0)
    podcast_kaggle_df[rates] = podcast_kaggle_df[rates].replace('Not Found',  0)
    podcast_kaggle_df[rates] = podcast_kaggle_df[rates].apply(pd.to_numeric)

    # generate random ranking for spotify (for project purpose only)
    podcast_kaggle_df['Spotify_ranking'] = np.random.uniform(4, 6, podcast_kaggle_df.shape[0])

    return podcast_kaggle_df


# Apple data cleaning
def apple_data_cleaning(path: str) -> pd.DataFrame:
    """Apple data cleaning function.

    Parameters
    ----------
    path : str
        path to direcotry where json object was stored.

    Returns
    -------
    pd.DataFrame
        Returns clean pandas with more data about artist and feed url

    """
    apple_data = read_data(path)
    # define columns which is nesseserly to use
    columns = ['feedUrl', 'country', 'collectionName', 'trackId', 'primaryGenreName', 'currency']
    apple_data = apple_data[columns]
    apple_data.rename(columns={'feedUrl': 'feed_url'}, inplace=True)
    # change all country code to uppercase
    apple_data['country'] = apple_data['country'].str.upper()
    # check if all trackId exist
    trackid_count = apple_data['trackId'].isnull().sum()
    if trackid_count == 0:
        print('All trackId exist in apple iTunes data')
    else:
        print('{} trackId not exist or read not properly'.format(trackid_count))
    # drop podcasts which don't have feed url
    temp = apple_data.shape[0]
    apple_data.feed_url.dropna(inplace=True)
    print('Delate {} podcasts without feed url'.format(int(apple_data.shape[0] - temp)))
    return apple_data


# XML Parser data cleaning for podcasts
def clean_podcasts_xml(path: list) -> pd.DataFrame:
    """RSS feed cleaner from xml files saved as csv.

    Parameters
    ----------
    path : list
        list of paths to chunks of xml data

    Returns
    -------
    pd.DataFrame
        Returns data about author like language, name, email.

    """
    xml_podcasts = pd.concat([pd.read_csv(file) for file in path])
    # get columns which are needed
    podcast_col = ['language', 'owner_name', 'owner_email', 'podcast_count', 'feed_url']
    xml_podcasts = xml_podcasts[podcast_col]
    # clean language code
    xml_podcasts['language'] = xml_podcasts.language.str.replace('[^a-zA-Z0-9]', '').str.lower().str.split('-', expand=True).loc[:, 0].str[:2]
    # check email adress
    mask = xml_podcasts.owner_email.str.contains('@').fillna(False)
    print('Found {} podcast(s) with wrong or without email'.format(xml_podcasts[~mask].shape[0]))
    print('Dropping all podcasters without emails')
    xml_podcasts = xml_podcasts[mask]
    xml_podcasts['owner_email'] = xml_podcasts.owner_email.str.split(' ').str[0]
    # first check if feed is correctly if not drop data
    mask  = xml_podcasts.podcast_count.astype(str).str.isdigit()
    # drop all podcasters without episodes
    xml_podcasts = xml_podcasts[mask]
    xml_podcasts['podcast_count'] = xml_podcasts['podcast_count'].apply(pd.to_numeric)
    temp = xml_podcasts.shape[0]
    xml_podcasts = xml_podcasts[xml_podcasts.podcast_count > 0]
    print('Dropping podcasts without episodes: {}'.format(temp - xml_podcasts.shape[0]))
    return xml_podcasts


# XML Parser data cleaning for episodes
def clean_episodes_xml(path: list) -> pd.DataFrame:
    """RSS feed cleaner for episodes

    Parameters
    ----------
    path : list
        list of paths to chunks of xml data

    Returns
    -------
    pd.DataFrame
        Returns data about episodes like description, title, published date etc.

    """
    col_ = ['comments_url', 'description', 'cc', 'guid', 'file_size',
           'duration_itunes', 'published_date', 'title', 'feed_url']
    if type(path) == list:
        xml_shows = pd.concat([pd.read_csv(file) for file in path])
    else:
        xml_shows = pd.read_csv(path)
    # get only unique ows
    xml_shows = xml_shows[col_]
    print(f"Length before drop duplicates {xml_shows.shape[0]}")
    xml_shows.drop_duplicates(inplace=True)
    print(f"Length after drop duplicates {xml_shows.shape[0]}")
    # check if www in coments if not add Empty
    xml_shows['comments_url'].fillna('Empty', inplace=True)
    xml_shows[~xml_shows['comments_url'].str.contains('www')]['comments_url'] = 'Empty'
    # clean file_size data
    xml_shows.file_size.astype(str).replace(r'\D+', '', regex=True, inplace=True)
    xml_shows['file_size'] = pd.to_numeric(xml_shows.file_size, errors='coerce')
    xml_shows['file_size'] = xml_shows.file_size.fillna(0).astype(float) / (2**10 * 2**10)
    xml_shows['file_size'] = pd.to_numeric(xml_shows.file_size, errors='coerce')
    xml_shows['file_size'] = xml_shows['file_size'].round(2)
    # fill cc as open
    xml_shows['cc'].fillna('Open', inplace=True)
    # clean description
    xml_shows['description'] = xml_shows.description.astype(str).str.replace(r'<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});',
                                                                             '', regex=True)
    xml_shows[xml_shows.description.str.len() < 5] = 'Empty'
    # set itunes duration to full minutes
    mask_time = xml_shows.duration_itunes.str.contains(':', na=False)
    xml_shows[mask_time]['duration_itunes'] = '0'
    xml_shows['duration_itunes'] = xml_shows.duration_itunes.replace(r'[^\D]', '')
    xml_shows['duration_itunes'] = xml_shows.duration_itunes.dropna().str.split(':').str[0]
    xml_shows['duration_itunes'] = pd.to_numeric(xml_shows.duration_itunes, errors='coerce')
    xml_shows['duration_itunes'].fillna(0, inplace=True)
    # try to clean datetime
    try:
        xml_shows['published_date'] = xml_shows.published_date.str.extract(r'(\d{2}.\w{3}.\d{4})')
    except Exception as e:
        print(e)
    # drop duplicates second time
    xml_shows.drop_duplicates(inplace=True)
    return xml_shows


def merge_podcasts(kaggle_path: str, apple_path: list, podcast_csvs: list) -> pd.DataFrame:
    """Merge podcasts data from 3 source: kaggle, appleItunes and rss feed.

    Parameters
    ----------
    kaggle_path : str
        path to kaggle podcast dataset
    apple_path : list
        list of paths to chunks of json data
    podcast_csvs : list
        list of paths to chunks of xml data saved in csv format

    Returns
    -------
    pd.DataFrame
        Dataframe with all clean podcasts from source

    """
    # clean kaggle podcasts
    podcasts_kaggle = clean_kaggle_df(kaggle_path)
    # clean apple iTunes podcasts
    apple_df = apple_data_cleaning(apple_path)
    # clean xml podcasts
    xml_podcasts = clean_podcasts_xml(podcast_csvs)
    # merge data
    all_data = podcasts_kaggle.merge(apple_df, left_on='Name', right_on='collectionName').merge(xml_podcasts, on='feed_url')
    all_data.drop_duplicates(inplace=True)
    print("Shape of podcasts DataFrame: {}".format(all_data.shape))
    all_data['authorID'] = all_data.index
    # change columns name to same as redshift style
    all_data.columns = [col.lower() for col in all_data.columns]
    return all_data


def merge_episodes(shows_csvs: list) -> pd.DataFrame:
    """Merge episodes data from chunks of csv from xml parser.

    Parameters
    ----------
    shows_csvs : list
        List of paths to chunks of csv

    Returns
    -------
    pd.DataFrame
        Returns clean episodes dataframe (frame could be very large)

    """
    episodes_paths = list()
    episodes_df = pd.concat([clean_episodes_xml(pth) for pth in shows_csvs], axis=0)
    # prevent Nan or Empty values
    episodes_df.dropna(inplace=True)
    episodes_df = episodes_df[episodes_df.feed_url != 'Empty']
    # fill index as guid
    episodes_df['guid'] = episodes_df.index
    print("Shape of episodes DataFrame: {}".format(episodes_df.drop_duplicates().shape))
    return episodes_df


def save_data(df: pd.DataFrame, save_dir: str,
              alias: str, counts: int = 1) -> list:
    paths_list = list()
    split_list = np.array_split(df, counts)
    print("Saving data")
    for i, chunk in enumerate(split_list):
        print("Saving chunk {}/{}".format(i+1, counts))
        name = os.path.join(save_dir, f"{alias}_{i}.csv")
        chunk.to_csv(name, index=False)
        paths_list.append(name)
    return paths_list


def main():
    # save podcasts into local directory
    podcasts_df = merge_podcasts()
    podcasts_df.to_csv('./data/podcasts_new.csv', index=False)
    # save episodes into local directory
    merge_episodes()


if __name__ == '__main__':
    main()
