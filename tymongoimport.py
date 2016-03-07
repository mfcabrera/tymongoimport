#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''tymongoimport
 (Almost) a drop-in replacement for Mongoimport

Usage:
  tymongoimport [--batchSize=<bs>] [--upsert | --find-replace] [--host=<host:port>] [--database=<db>] [--collection=<col>]
  tymongoimport [--batchSize=<bs>] [--upsert | --find-replace] [--host=<host:port>] [--database=<db>] [--collection=<col>]  (--user=<user> --password=<passwd>)
  tymongoimport [--batchSize=<bs>] [--upsert | --find-replace] [--host=<host:port>]  [--database=<db>] [--collection=<col>] (--user=<user> --password=<passwd> --authenticationDatabase=<authdb>)
  tymongoimport --help
  tymongoimport --version

Options:
  --help     Show this screen.
  --version     Show version.
  --upsert      Use Upsert
  --db
  --batchSize=<bs>  the batch size [default: 1000]
  --host=<host:port>  mongo instance  [default: localhost]
  --user=<user>  the user with write access to the database
  --password=<passwd>  the password for authentication
  --authenticationDatabase=<authdb>  the database to perform the authentication to  [default: admin]
   --database=<db>  the target database  [default: test]
'''

from __future__ import print_function, unicode_literals

import logging
import sys
from sys import stdin

import pymongo
from bson.json_util import loads
from docopt import docopt
from pymongo import ReplaceOne, InsertOne
from pymongo.errors import BulkWriteError
from collections import Counter

__version__ = "0.1.0"
__author__ = "Miguel Cabrera"
__license__ = "MIT"


class MongoBulkUpdater(object):

    INSERT, UPSERT, FIND_REPLACE = range(3)

    def __init__(self, db, collection_name, insert_mode=INSERT):
        self.db = db
        self.collection = db.get_collection(collection_name)
        self.insert_mode = insert_mode
        self.find_replace = False

        if self.insert_mode == MongoBulkUpdater.UPSERT:
            logging.info("Working in upsert mode")
        elif self.insert_mode == MongoBulkUpdater.FIND_REPLACE:
            logging.info("Working in find-replace mode")
        else:
            logging.info("Working in working in normal insertion mode")

    def process_batch(self, batch):
        # bulk_op = self.collection.bulk_write()

        operations = []
        for line in batch:
            document = loads(line)
            if self.insert_mode == MongoBulkUpdater.FIND_REPLACE:
                ex = self.collection.find({'_id': document['_id']}).limit(1).count(with_limit_and_skip=True)
                if ex > 0:  # Document exists
                    _id = document['_id']
                    del document['_id']
                    operations.append(ReplaceOne({"_id": _id}, document))
                else:
                    operations.append(InsertOne(document))
            elif self.insert_mode == MongoBulkUpdater.UPSERT:
                _id = document['_id']
                del document['_id']
                operations.append(ReplaceOne({"_id": _id}, document, upsert=True))
            else:
                operations.append(InsertOne(document))
        result = self.collection.bulk_write(operations, ordered=False)
        return result

    @classmethod
    def insertion_mode_from_args(cls, args):
        if args['--find-replace']:
            return cls.FIND_REPLACE
        elif args['--upsert']:
            return cls.UPSERT
        else:
            return cls.INSERT


def handle_bulk_errors(bwe):
    logging.error("Failed inserting task {}".format(bwe))
    logging.error("Server answer  with code {}".format(bwe.code))
    logging.error(
        "nModified: {} nUpserted: {} nMatched: {}".format(
            bwe.details['nModified'],
            bwe.details['nUpserted'],
            bwe.details['nMatched'],
        )
    )
    for i, o in enumerate(bwe.details['writeErrors']):
        del o['op']
        logging.error(o)
        if i > 10:
            logging.error("Too many errors. Truncating")
            break



def main():
    '''Main entry point for the tymongoimport CLI.'''

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    args = docopt(__doc__, version=__version__)
    logging.info(args)

    # Extract parameters
    db_name = args['--database']
    host = args['--host']
    collection = args["--collection"]
    batchSize = int(args["--batchSize"])

    # Insertion modes
    insert_mode = MongoBulkUpdater.insertion_mode_from_args(args)

    log_str = "Connectiong to '{}' writing to database: '{}' in collection '{}'"
    logging.info(log_str.format(host, db_name, collection))

    client = pymongo.MongoClient(host=host)
    db = client[db_name]

    writer = MongoBulkUpdater(db, collection, insert_mode)

    stats = Counter()
    buffer = []

    def do_bulk_insert(buffer):
        try:
            r = writer.process_batch(buffer)
            stats['inserted'] += r.inserted_count
            stats['modified'] += r.modified_count
            stats['upserted'] += r.upserted_count

            logging.info("Upserted/Inserted so far {} documents".format(stats['inserted'] + stats['upserted'] + stats['modified']))
        except BulkWriteError as bwe:
            handle_bulk_errors(bwe)
            exit(-1)


    for i, line in enumerate(stdin):
        buffer.append(line)
        if len(buffer) % batchSize == 0:
            do_bulk_insert(buffer)
            buffer = []

    if len(buffer) > 0:
        do_bulk_insert(buffer)  # process the rest

if __name__ == '__main__':
    main()
