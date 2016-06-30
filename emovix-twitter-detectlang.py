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
twitterStatusCol = ""

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
        twitterStatusCol = config['source_box'] + "_twitterStatus"

    client = MongoClient('mongodb://' + database_host + ':27017/')
    db = client[database_name]

    detectlanguage.configuration.api_key = detectlanguage_api_key

    while True:
        try:
            if detectlanguage.user_status()['requests'] >= detectlanguage.user_status()['daily_requests_limit']:
                logging.debug("Number of requests over daily limit.")
                time.sleep(60)

            statuses = db[twitterStatusCol].find({ "language_detections.language": { "$exists": False } })

            if statuses:
                count = 0
                batch_request = []
                batch_status = []
                for twitterStatus in statuses:
                    if count >= 500:
                        logging.debug("Processing batch ...")
                        detections = detectlanguage.detect(batch_request)

                        if len(detections) != 500:
                            logging.error("ABNORMAL NUMBER OF LANGUAGE DETECTIONS: " + str(len(detections)))
                            break

                        count = 0
                        for detection in detections:
                            if len(detection) == 0:
                                detection = {}
                                detection['source'] = 'detectlanguage'
                                detection['language'] = ''
                                batch_status[count]['language_detections'] = []
                                batch_status[count]['language_detections'].append(detection)
                            else:
                                detection[0]['source'] = 'detectlanguage'
                                batch_status[count]['language_detections'] = []
                                batch_status[count]['language_detections'].append(detection[0])

                            db[twitterStatusCol].update( { "_id": batch_status[count]['_id']}, batch_status[count], upsert=True)
                            count += 1

                        count = 0
                        batch_request = []
                        batch_status = []

                    text = twitterStatus['text'].encode('utf-8')
                    batch_request.append(text)
                    batch_status.append(twitterStatus)
                    count += 1

        except Exception as e:
            # Oh well, just keep going
            logging.error(e.__class__)
            logging.error(e)
            continue
        except KeyboardInterrupt:
            break
