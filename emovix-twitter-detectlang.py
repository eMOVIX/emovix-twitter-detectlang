__author__ = 'Jordi Vilaplana'

from pymongo import MongoClient
import detectlanguage
import json
import logging
import time

logging.basicConfig(
    filename='emovix_twitter_detectlang.log',
    level=logging.DEBUG,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%d-%m-%y %H:%M')

# Configuration parameters
detectlanguage_api_key = ""
database_host = ""
database_name = ""

client = None
db = None

if __name__ == '__main__':
    logging.debug('emovix_twitter_detectlang.py starting ...')

    # Load configuration
    with open('config.json', 'r') as f:
        config = json.load(f)
        detectlanguage_api_key = config['detectlanguage_api_key']
        database_host = config['database_host']
        database_name = config['database_name']

    client = MongoClient('mongodb://' + database_host + ':27017/')
    db = client[database_name]

    detectlanguage.configuration.api_key = detectlanguage_api_key

    while True:
        try:
            if detectlanguage.user_status()['requests'] >= detectlanguage.user_status()['daily_requests_limit']:
                logging.debug("Number of requests over daily limit.")
                time.sleep(60)

            twitterStatus = db.twitterStatus.find_one({ "language_detections": { "$exists": False } })
            text = twitterStatus['text'].encode('utf-8')
            result = detectlanguage.detect(text)

            if len(result) == 0:
                result = {}
                result['source'] = 'detectlanguage'
                twitterStatus['language_detections'] = []
            else:
                result = result[0]
                result['source'] = 'detectlanguage'

                twitterStatus['language_detections'] = []
                twitterStatus['language_detections'].append(result)

            db.twitterStatus.update( { "_id": twitterStatus['_id']}, twitterStatus, upsert=True)

        except Exception as e:
            # Oh well, just keep going
            logging.error(e.__class__)
            logging.error(e)
            continue
        except KeyboardInterrupt:
            break
