
# for multiprocessing data
import os
import sys
import argparse
import requests
import concurrent.futures
import multiprocessing
import pandas as pd
# podcasts xml parser
from pyPodcastParser.Podcast import Podcast

sys.getrecursionlimit()
sys.setrecursionlimit(50000)  # change recurion limits


def concurent_parser(feed_url_list: list) -> dict:
    """Parse data with pyPodcastParser more:
    https://github.com/mr-rigden/pyPodcastParser

    Parameters
    ----------
    feed_url_list : list
        list of feed urls to parse.

    Returns
    -------
    dict
        {"podcast_list": podcasts_df, "show_list": shows_list,
         "wrong_feed": wrong_feed}

    """
    shows_list = list()
    podcasts_df = list()

    wrong_feed = list()

    for i, feed in enumerate(feed_url_list):
        if i % 500 == 0:
            to_end = (i / len(feed_url_list)) * 100
            print("Progress: {}%".format(round(to_end, 2)))
        more_podcast_data = dict()
        try:
            response = requests.get(feed)
            podcast = Podcast(response.content)
            # add more podcast data
            more_podcast_data['description'] = podcast.description
            more_podcast_data['categories'] = podcast.categories
            more_podcast_data['language'] = podcast.language
            more_podcast_data['link'] = podcast.link
            more_podcast_data['owner_name'] = podcast.owner_name
            more_podcast_data['owner_email'] = podcast.owner_email
            more_podcast_data['podcast_count'] = len(podcast.items)
            more_podcast_data['feed_url'] = feed
            podcasts_df.append(more_podcast_data)
            # information about podcasts
            for episode in podcast.items:
                default_show_dict = dict()
                default_show_dict['author'] = episode.author
                default_show_dict['comments_url'] = episode.comments
                default_show_dict['description'] = episode.description
                default_show_dict['cc'] = episode.creative_commons
                default_show_dict['guid'] = episode.guid
                default_show_dict['file_size'] = episode.enclosure_length
                default_show_dict['duration_itunes'] = episode.itunes_duration
                default_show_dict['link'] = episode.link
                default_show_dict['published_date'] = episode.published_date
                default_show_dict['title'] = episode.title
                default_show_dict['feed_url'] = feed
                shows_list.append(default_show_dict)
        except Exception as e:
            print(e)
            print("Wrong feed, save feed into list")
            wrong_feed.append(feed)
    return {"podcast_list": podcasts_df, "show_list": shows_list,
            "wrong_feed": wrong_feed}


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def main(lines: list, save_dir: str) -> None:
    """Process url feed with all CPU's than save it into chunks.

    Parameters
    ----------
    lines : list
        list of all feed urls.
    save_dir : str
        directory where save chunks of parsed rss.

    Returns
    -------
    None

    """
    # defuault settings
    cpu_num = multiprocessing.cpu_count()
    len_feed = len(lines) // cpu_num
    s = chunks(lines, len_feed)
    # make list for results
    results = list()
    parser_feed = list()

    with concurrent.futures.ProcessPoolExecutor() as executor:
        for i in range(cpu_num):
            res = executor.submit(concurent_parser, next(s))
            results.append(res)
        for i, f in enumerate(concurrent.futures.as_completed(results)):
            parser_result = f.result()
            try:
                podcast_df_name = os.path.join(save_dir, 'podcast_table_{}.csv'.format(i))
                shows_name = os.path.join(save_dir, 'shows_table_{}.csv'.format(i))
                print('Saving podcast into {}'.format(podcast_df_name))
                pd.DataFrame(parser_result['podcast_list']).to_csv(podcast_df_name)
                print('Saving shows into {}'.format(shows_name))
                pd.DataFrame(parser_result['show_list']).to_csv(shows_name, )
                parser_feed.append(parser_result['wrong_feed'])
            except Exception as e:
                print(e)

if __name__ == "__main__":
    # argument parser_result
    parser = argparse.ArgumentParser(description='Create xml parser')
    parser.add_argument('--path', metavar='path', required=True,
                        help='the path to feed_url as txt file')
    parser.add_argument('--save_dir', metavar='path', required=True,
                        help='path to directory where save parse files')
    args = parser.parse_args()
    with open(args.path) as f:
        lines = f.read().split('\n')
    main(lines, args.save_dir)
