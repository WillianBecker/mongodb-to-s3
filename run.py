# -*- coding: utf-8 -*-
"""run.py: Get data from MongoDB and sends to AWS S3.
author: Willian Eduardo Becker
date: 13-12-2018
"""
import boto3
import copy
import csv
import threading
import sys
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import numpy as np

# S3 PARAMETERS
bucket_name_destination = "YOUR_AWS_BUCKET"
awspath = "YOUR_AWS_PATH"
access_key = "YOUR_AWS_ACCESS_KEY"
secret_access_key = "YOUR_AWS_SECRET_KEY"

# MONGO PARAMETERS
mongo_host = "YOUR_MONGO_HOST"
mongo_port = "YOUR_MONGO_PORT"  # change to int type
mongo_database = "YOUR_MONGO_DATABASE"
mongo_collection = "YOUR_MONGO_COLLECTION"
mongo_username = "YOUR_MONGO_USERNAME"
mongo_password = "YOUR_MONGO_PASSWORD"


class ProgressPercentage(object):
    # used to monitoring S3 uploading process
    def __init__(self, filename):
        self._filename = filename
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        with self._lock:
            self._seen_so_far += bytes_amount
            sys.stdout.write(
                "\r%s --> %s bytes transferred" % (
                    self._filename, self._seen_so_far))
            sys.stdout.flush()


def send_file_to_s3(filename, bucket_name_destination, awspath):
    print "Step 3 of 3: Sending file to S3 ..."
    s3 = boto3.client('s3',
                      aws_access_key_id=access_key,
                      aws_secret_access_key=secret_access_key)
    key = awspath + filename
    s3.upload_file(filename, bucket_name_destination, key,
                   Callback=ProgressPercentage(filename))

    return


def create_csv_file(result):
    filename = datetime.now().strftime("%Y-%m-%d.csv")
    result.to_csv(filename, index=False, sep=';', encoding='utf-8')

    return filename


def transform_mongo_data(docs):
    print "Step 2 of 3: Transforming Mongo data ..."
    columns = ['id', 'description', 'count']
    df = pd.DataFrame(columns=columns)

    for doc in docs:  # more data operations may be added
        doc['id'] = sorted(doc['id'], key=lambda k: k['data'])
        mongo_data = pd.DataFrame([doc])
        mongo_data = mongo_data.fillna("")
        df = pd.concat([df, mongo_data], sort=False)

    return df


def get_mongo_data():
    print "Step 1 of 3: Getting Mongo data ..."
    client = MongoClient(mongo_host, port=mongo_port, username=mongo_username,
                         password=mongo_password)
    db = client[mongo_database]
    collection = db.mongo_collection
    cursor = collection.find({})

    documents = []
    for document in cursor:
        documents.append(document)

    return documents


def run():
    mongo_data = get_mongo_data()
    result = transform_mongo_data(mongo_data)
    filename = create_csv_file(result)
    send_file_to_s3(filename, bucket_name_destination, awspath)


if __name__ == "__main__":
    print "Starting time: {}".format(datetime.now())
    run()
    print "\n Finished time: {}".format(datetime.now())
