# coding=utf-8

"""
This is an appleITunes podcasts wrapper

It will search podcasts by name, because the basic version of itunes has
limitations to 20 queries per minute, this script can take about 2-3 hours.
"""
import json
import os
import time
import requests


def make_dir(path: str, char: str) -> str:
    """make directory in local path which depends on first character of
    the podcast name.

    Parameters
    ----------
    path : str
        Description of parameter `path`.
    char : str
        First char of podcast name.

    Returns
    -------
    str
        Return path to created directory.

    """
    char = char.upper()
    f_path = os.path.join(path, char)
    if not os.path.exists(f_path):
        os.makedirs(f_path)
    return f_path


class AppleSearch():
    """AppleITunes search wrapper.

    Parameters
    ----------
    entity : str
        enitity for search to (in out case podcasts)
    aws_creditentials : type
        amazon aws_creditentials (not implemented yet)

    Attributes
    ----------
    entity
    aws_creditentials
    """

    def __init__(self, entity: str = 'podcast',
                 aws_creditentials = None):
        self.entity = 'podcast'
        self.aws_creditentials = aws_creditentials

    def search(self, query: str) -> dict:
        """Search method to search for podcaster names.

        Parameters
        ----------
        query : str
            query to search in appleItunes api, it should be podcaster name..
        """
        query = "https://itunes.apple.com/search?term={0}&entity={1}".format(query, self.entity)
        try:
            req = requests.get(query).json()
            if req['resultCount'] > 0:
                self.data = req
        except Exception as e:
            print(e)
            print(query)
            self.data = None
            pass

    def get_data(self, return_data: bool = True, save: bool = True,
                 podcast_path: str = None) -> dict:
        """Short summary.

        Parameters
        ----------
        usecol: list
            use parameter to return specific value from request
        return_data : bool
            If True, return data.
        save : bool
            if True, save data int default localization.
        podcast_path : str
            If not None save data in this path.

        Returns
        -------
        dict
            Dictionary with all data about podcaster, if podcaster not exists,
            method returns none. Only if return_data is True.

        """
        if self.data:
            if save is True:
                # iterate over all results
                for res in range(self.data["resultCount"]):
                    name = "{}.json".format(self.data["results"][res]["collectionId"])
                    if podcast_path is not None:
                        path = os.path.join(podcast_path, name)
                    else:
                        path = os.path.join(os.environ['JSON_DATA'], name)
                    with open(path, 'w') as f:
                        json.dump(self.data["results"][res], f)
            if return_data is True:
                return self.data
        else:
            pass

    def get_feed(self) -> str:
        # Get feed url from request.
        feed = list()
        for res in range(self.data['resultCount']):
            try:
                print("feed: ", self.data["results"][res]['feedUrl'])
                feed.append(self.data["results"][res]['feedUrl'])
            except KeyError:
                pass
            return feed
