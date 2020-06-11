# -*- coding: utf-8 -*-
"""Config file which add parameters to enviroment variables"""
from pathlib import Path
import configparser
import os


# load enviroment variables
project_dir = str(Path(__file__).resolve().parents[1])
print(project_dir)
DATA_PATH = os.path.join(project_dir, 'data')
config = configparser.ConfigParser()
config.read(os.path.join(project_dir, 'dwh.cfg'))
print(config.keys())
# create paths to data directories
PATHS = config['LOCAL']
print(PATHS)
RAW = os.path.join(DATA_PATH, PATHS['RAW_DATA'])
JSON_DATA = os.path.join(DATA_PATH, PATHS['JSON_DATA'])
XML_DATA = os.path.join(DATA_PATH, PATHS['XML_DATA'])
PREPROCESS_PODCASTS = os.path.join(DATA_PATH, PATHS['PREPROCESS_EPISODES'])
PREPROCESS_EPISODES = os.path.join(DATA_PATH, PATHS['PREPROCESS_EPISODES'])

# load data paths to path enviroment
os.environ['RAW_DATA'] = RAW
os.environ['JSON_DATA'] = JSON_DATA
os.environ['XML_DATA'] = XML_DATA
os.environ['PREPROCESS_EPISODES'] = PREPROCESS_EPISODES
os.environ['PREPROCESS_PODCASTS'] = PREPROCESS_PODCASTS

# load aws access keys to Environment
os.environ['AWS_ACCESS_KEY_ID'] = config['IAM_ROLE']['ACC_KEY']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['IAM_ROLE']['SEC_KEY']

# load s3 path to Environment
os.environ['BUCKET_NAME'] = config['S3']['BUCKET_NAME']
os.environ['S3_PODCASTS'] = config['S3']['PODCASTS']
os.environ['S3_EPISODES'] = config['S3']['EPISODES']
os.environ['S3_KAGGLE'] = config['S3']['KAGGLE']

# load region
os.environ['S3_REGION'] = config['REGION']['GZIP']

print(os.environ['RAW_DATA'])
