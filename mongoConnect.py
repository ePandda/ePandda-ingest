#
# Mongo Class for handling data import from ingesters
# by Mike Benowitz
#

# import database tools
from pymongo import MongoClient
import pymongo
from pymongo.errors import BulkWriteError, InvalidOperation

# data tools
import json
import csv

# sys tools
from subprocess import Popen, PIPE, call
import logging

# helper module
from helpers import ingestHelpers

class mongoConnect:
    def __init__(self):
        self.config = json.load(open('./config.json'))
        self.client = MongoClient("mongodb://" + self.config['mongodb_user'] + ":" + self.config['mongodb_password'] + "@" + self.config['mongodb_host'])
        self.idigbio = self.client[self.config['idigbio_db']]
        self.pbdb = self.client[self.config['pbdb_db']]
        self.endpoints = self.client[self.config['endpoints_db']]
        self.logger = logging.getLogger("ingest.mongoConnection")

    def checkIDBCollectionStatus(self, collectionKey, modifiedDate, collectionSize):
        # Status flags
        # new = This is a new collection
        # modified = This collection has been modified since the last ingest
        # static = This collection has not changed since last ingest
        collCollection = self.idigbio.collectionStatus

        collStatus = collCollection.find_one({'collection': collectionKey})
        if not collStatus:
            return 'new'
        elif collStatus['modifiedDate'] != modifiedDate:
            return 'modified'
        else:
            return 'static'

    def iDBFullImport(self, occurrenceFile, collectionKey, collectionModified):
        importCall = Popen(['mongoimport', '--host', self.config['mongodb_host'], '-u', self.config['mongodb_user'], '-p', self.config['mongodb_password'], '--authenticationDatabase', 'admin', '-d', self.config['idigbio_db'], '-c', self.config['idigbio_coll'], '--type', 'csv', '--file', occurrenceFile, '--headerline'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
        out, err = importCall.communicate()
        if importCall.returncode != 0:
            self.logger.error("mongoimport failed with error: " + err)
            return False
        else:
            self.logger.info("mongoimport success! " + out)
        collCollection = self.idigbio.collectionStatus
        updateStatus = collCollection.update({'collection': collectionKey}, {'$set': {'collection': collectionKey, 'modifiedDate': collectionModified}}, upsert=True)
        if collCollection:
            self.logger.debug("Added/updated collection entry in collectionStatus for " + collectionKey)
        else:
            self.logger.warning("Failed to update this record in collectionStatus: " + collectionKey)
        return True

    def iDBPartialImport(self, occurrenceFile, collectionKey, collectionModified):
        occCollection = self.idigbio[self.config['idigbio_db']]
        with open(occurrenceFile, 'rb') as occCSV:
            occReader = csv.reader(occCSV)
            uuids = []
            occHashes = {}
            rowCount = 0
            for row in occReader:
                idbUUID = row['idigbio:uuid']
                occHash = ingestHelpers.getMd5Hash(row)
                occHashes[idbUUID] = {'hash': occHash, 'row': rowCount}
                uuids.append(idbUUID)
                rowCount += 1
            occRecs = occCollection.find({'idigbio:uuid': {'$in': uuids}}, {'_id': 0})
            bulk = occCollection.initialize_unordered_bulk_op()
            for occ in occRecs:
                checkUUID = occ['idigbio:uuid']
                if checkUUID in occHashes:
                    checkHash = ingestHelpers.getMd5Hash(occ)
                    if occHashes[checkUUID]['hash'] != checkHash:
                        bulk.find({'idigbio:uuid': checkUUID}).upsert().update({'$set': occReader[occHashes[checkUUID]['row']]})
                else:
                    bulk.insert(occReader[occHashes[checkUUID]['row']])

            try:
                bulk_results = bulk.execute()
            except BulkWriteError as bwe:
                self.logger.error("Partial import bulk failure!")
                self.logger.error(bwe.details)
                return False
            except InvalidOperation as io:
                self.logger.warning("There were no records to update")
                return False
            return bulk_results

        return False
