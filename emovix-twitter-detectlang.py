__author__ = 'Jordi Vilaplana'

from pymongo import MongoClient
import detectlanguage
import json
import logging
import time

logging.basicConfig(
    filename='emovix_twitter_detectlang.log',
    level=logging.WARNING,
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

            statuses = db.twitterStatus.find({ "language_detections": { "$exists": False } })

            if statuses:
                count = 0
                batch_request = []
                batch_status = []
                for twitterStatus in statuses:
                    if count >= 20:
                        logging.debug("Processing batch ...")
                        detections = detectlanguage.detect(batch_request)

                        if len(detections) != 20:
                            logging.error("ABNORMAL NUMBER OF LANGUAGE DETECTIONS: " + str(len(detections)))
                            break

                        count = 0
                        for detection in detections:
                            detection[0]['source'] = 'detectlanguage'
                            batch_status[count]['language_detections'] = []
                            batch_status[count]['language_detections'].append(detection[0])
                            db.twitterStatus.update( { "_id": batch_status[count]['_id']}, twitterStatus, upsert=True)
                            count += 1

                        count = 0
                        batch_request = []
                        batch_ids = []

                    text = twitterStatus['text'].encode('utf-8')
                    batch_request.append(text)
                    batch_status.append(twitterStatus)
                    count += 1

            break

        except Exception as e:
            # Oh well, just keep going
            logging.error(e.__class__)
            logging.error(e)
            continue
        except KeyboardInterrupt:
            break
