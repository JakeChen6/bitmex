#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 17 17:14:45 2020

@author: zhishe
"""

import logging
import json
import traceback

from pymongo import MongoClient

from mongo_config import USER_NAME, PASSWORD, CLUSTER_NAME, DB_NAME

DIR = '/Users/zhishe/myProjects/bitmex'

MONGODB_URL = f"mongodb+srv://{USER_NAME}:{PASSWORD}@{CLUSTER_NAME}-cgkkq.mongodb.net/{DB_NAME}?retryWrites=true&w=majority"


def write_to_db(keys, queue):
    """
    keys : dict
        table : collection name in MongoDB

    queue : queue.Queue
        the communication channel. Data to write into MongoDB is read from queue.

    """
    db_writer = DB_Writer(keys)
    db_writer.start()

    while True:
        timestamp, message = queue.get()
        if 'action' in message and message['action'] == 'terminate':  # signal to terminate
            break
        else:
            db_writer.write(timestamp, message)

    db_writer.close()


class DB_Writer:
    def __init__(self, keys):
        """
        keys : dict
            table : collection name in MongoDB
        """
        self.keys = keys
        self.logger = setup_logger()

    def start(self):
        """
        Connect to MongoDB Atlas.
        """
        self.logger.debug('Connecting to MongoDB...')
        self.client = MongoClient(MONGODB_URL)
        self.db = self.client[DB_NAME]
        self.logger.debug('Connected to MongoDB.')

    def write(self, timestamp, message):
        """
        Write data to MongoDB.
        """
        self.logger.debug(json.dumps(message))

        # action is used to differentiate between snapshot and real-time update data

        # 'partial' - full table image - snapshot
        # 'insert'  - new row - real-time update
        # 'update'  - update row - real-time update
        # 'delete'  - delete row - real-time update

        table = message.get('table')
        action = message.get('action')  
        data = {'timestamp': timestamp, 'action': action, 'data': message['data']}

        collection_name = self.keys.get(table)
        
        try:
            self.db[collection_name].insert_one(data)
        except:
            self.logger.error(traceback.format_exc())

    def close(self):
        """
        Disconnect from MongoDB.
        """
        self.logger.debug('Disconnecting from MongoDB...')
        self.client.close()
        self.logger.debug('Disconnected from MongoDB.')


def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter("\n\n%(asctime)s - %(name)s.%(funcName)s - %(levelname)s - %(message)s")

    # write DEBUG level logs to a file
    fh = logging.FileHandler(DIR + '/mongodb.log')
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)

    return logger
