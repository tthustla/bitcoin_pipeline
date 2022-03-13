"""
Producer app writing single Bitcoin price record at a time to a Kinesis Data Stream using
the PutRecord API of the Python SDK.

price_timestamp from the Nomcis API response is used as the partition key which ensures
price records will be equally distributed across the shard of the stream.
"""
import json
import logging
import sys
import time

import boto3
import requests
import os
from dotenv import load_dotenv

load_dotenv()

key = os.getenv("NOMICS_KEY")
url = f"https://api.nomics.com/v1/currencies/ticker?key={key}&ids=BTC&interval=1h&per-page=100&page=1"
logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',datefmt='%Y-%m-%d %H:%M:%S',level=logging.INFO)

def main(args):
    logging.info('Starting PutRecord Producer')

    stream_name = args[1]
    logging.info(stream_name)
    kinesis = boto3.client('kinesis', region_name='us-east-1')

    while True:
        response = requests.get(url)
        if response.status_code == 200:
            parsed = json.loads(response.content)
            logging.info(f'Retrieved {parsed[0]}')
            try:
                # execute single PutRecord request
                kinesis_resp = kinesis.put_record(StreamName=stream_name,
                                            Data=json.dumps(parsed[0]).encode('utf-8'),
                                            PartitionKey=parsed[0]['price_timestamp'])
                logging.info(f"Produced Record {kinesis_resp['SequenceNumber']} to Shard {kinesis_resp['ShardId']}")
            except Exception as e:
                logging.error({
                    'message': 'Error producing record',
                    'error': str(e),
                    'record': parsed[0]
                })
        else:
            logging.warning(f'{response} Failed to retrieve data from Nomics API')
        time.sleep(10)

if __name__ == '__main__':
    main(sys.argv)