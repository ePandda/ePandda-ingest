#
# Mongo Class for handling data import from ingesters
# by Mike Benowitz
#

# import database tools
from pymongo import MongoClient
import pymongo
from pymongo.errors import BulkWriteError

# data tools
import json

# sys tools
from subprocess import Popen, PIPE, call

class mongoImport:
    def __init__(self):
        self.config = json.load(open('./config.json'))
        self.client = MongoClient("mongodb://" + self.config['mongodb_user'] + ":" + self.config['mongodb_password'] + "@" + self.config['mongodb_host'])
        self.idigbio = self.client[self.config['idigbio_db']]
        self.pbdb = self.client[self.config['pbdb_db']]
        self.endpoints = self.client[self.config['endpoints_db']]

    def checkIDBCollectionStatus(self, collectionKey, modifiedDate, collectionSize):
        # Status flags
        # new = This is a new collection
        # modified = This collection has been modified since the last ingest
        # static = This collection has not changed since last ingest
        collCollection = self.idigbio_2.collectionStatus

        collStatus = collCollection.find_one({'collectionKey': collectionKey})
        if not collStatus:
            return 'new'
        elif collStatus['modifiedDate'] == modifiedDate and collstatus['collectionSize'] == collectionSize:
            return 'modified'
        else:
            return 'static'

    def iDBFullImport(self, occurrenceFile):
        out, err = Popen(['mongoimport', '-h', self.config['mongodb_host'], '-u', self.config['mongodb_user'], '-p', self.config['mongodb_password'], '--authenticationDatabase', 'admin', '-d', self.config['idigbio_db'], '-c', self.config['idigbio_coll'], '--type', 'csv', '--file', occurrenceFile, '--headerline'], stdin=PIPE, stdout=PIPE, stderr=PIPE).communicate()
        if err:
            print(err)
            return False
        else:
            print(out)
            return True

    def iDBPartialImport(self, occurrenceFile):
        #TODO best way to quickly parse and check for existence/contents of rows
        return False
