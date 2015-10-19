__author__ = 'Jordi Vilaplana'

import pymongo
from pymongo import MongoClient
import json
import logging

logging.basicConfig(
    filename='emovix_twitter_detectlang.log',
    level=logging.WARNING,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%d-%m-%y %H:%M')

# Configuration parameters
detectlanguage_api_key = ""
database_name = ""

client = None
db = None

if __name__ == '__main__':
    logging.debug('emovix_twitter_detectlang.py starting ...')

    # Load configuration
    with open('config.json', 'r') as f:
        config = json.load(f)
        detectlanguage_api_key = config['detectlanguage_api_key']
        database_name = config['database_name']

    client = MongoClient('mongodb://localhost:27017/')
    db = client[database_name]

    while True:
        try:
            twitterStatus = db.twitterStatus.find_one({ "lang": "es", "language_detections": { "$exists": False } })
            print twitterStatus



            break
        except Exception as e:
            # Oh well, reconnect and keep trucking
            logging.error(e.__class__)
            logging.error(e)
            continue
        except KeyboardInterrupt:
            break
