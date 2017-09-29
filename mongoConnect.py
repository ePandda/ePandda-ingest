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
        importCall = Popen(['mongoimport', '--host', self.config['mongodb_host'], '-u', self.config['mongodb_user'], '-p', self.config['mongodb_password'], '--authenticationDatabase', 'admin', '-d', self.config['idigbio_db'], '-c', self.config['idigbio_coll'], '--type', 'csv', '--file', occurrenceFile, '--headerline', '--mode', 'upsert', '--upsertFields', 'idigbio:uuid'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
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

    def pbdbIngestTmpCollections(self, csvFiles):
        for csvFile in csvFiles:
            collectionName = 'tmp_' + csvFile[:-4]
            importCall = Popen(['mongoimport', '--host', self.config['mongodb_host'], '-u', self.config['mongodb_user'], '-p', self.config['mongodb_password'], '--authenticationDatabase', 'admin', '-d', self.config['pbdb_db'], '-c', collectionName, '--type', 'csv', '--file', csvFile, '--headerline', '--drop'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
            out, err = importCall.communicate()
            if importCall.returncode != 0:
                self.logger.error("mongoimport failed with error: " + err)
                return False
            else:
                self.logger.info("mongoimport success! " + out)
                return True

    def pbdbMergeTmpCollections(self, occurrence, collection, reference):
        self.logger.debug("Getting unique collection_no values from occurrences")
        occurrenceCollection = self.pbdb[occurrence]
        collectionCollection = self.pbdb[collection]
        referenceCollection = self.pbdb[reference]

        # Creating a few basic indexes speeds this process up
        occurrenceCollection.create_index("collection_no")
        occurrenceCollection.create_index("reference_no")
        collectionCollection.create_index("collection_no")
        referenceCollection.create_index("reference_no")

        self.logger.info("Merging occurrences and collections")
        collectionNos = occurrenceCollection.distinct('collection_no')
        for collectionNo in collectionNos:
            collectionData = collectionCollection.find_one({'collection_no': collectionNo})
            if not collectionData:
                self.logger.error("Could not find collection_no: " + str(collectionNo))
            self.logger.debug("Adding collection data for collection_no: " + str(collectionNo))
            occurrenceCollection.update_many({'collection_no': collection_no}, {'$addToSet': {'coll_refs': collectionData}})

        self.logger.info("Merging occurrences and references")
        referenceNos = occurrenceCollection.distinct('reference_no')
        for referenceNo in referenceNos:
            referenceData = referenceCollection.find_one({'reference_no': referenceNo})
            if not referenceData:
                self.logger.error("Could not find reference_no: " + str(referenceNo))
            self.logger.debug("Adding collection data for collection_no: " + str(referenceNo))
            occurrenceCollection.update_many({'reference_no': reference_no}, {'$addToSet': {'occ_refs': referenceData}})

        return True

    def pbdbMergeNewData(self, tmp_occurrence):
        self.logger.info("Merging new PaleoBio data")

        self.logger.debug("Exporting contents of temporary collection")
        exportCall = Popen(['mongoexport', '--host', self.config['mongodb_host'], '-u', self.config['mongodb_user'], '-p', self.config['mongodb_password'], '--authenticationDatabase', 'admin', '-d', self.config['pbdb_db'], '-c', tmp_occurrence, '--type', 'json', '-o', 'tmp_occurrence.json'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
        out, err = exportCall.communicate()
        if exportCall.returncode != 0:
            self.logger.error("mongoexport failed with error: " + err)
            return False
        else:
            self.logger.debug("Successfully exported temp mongo collection! " + out)

        self.logger.debug("Importing new contents of temporary collection with upsert")
        importCall = Popen(['mongoimport', '--host', self.config['mongodb_host'], '-u', self.config['mongodb_user'], '-p', self.config['mongodb_password'], '--authenticationDatabase', 'admin', '-d', self.config['pbdb_db'], '-c', self.config['pbdb_coll'], '--type', 'json', '--file', 'tmp_occurrence.json', '--mode', 'upsert', '--upsertFields', 'occurrence_no'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
        out, err = importCall.communicate()
        if importCall.returncode != 0:
            self.logger.error("mongoimport failed with error: " + err)
            return False
        else:
            self.logger.info("mongoimport success! " + out)
            return True
        return mergeResult


    # This method is going to be deprecated as unecessary!
    # TODO DELETE once confirmed that we can just use upsert on records
    def generalPartialImport(self, reader, sourceType, idField, mongoCollection):
        for row in reader:
            recordId = row[idField]
            rowHash = ingestHelpers.getMd5Hash(row)
            recordHashes[recordId] = {'hash': rowHash, 'row': rowCount}
            recordIds.append(recordId)
            rowCount += 1
            if rowCount % 500 == 0:
                self.logger.info("Batch importing 500 records")
                bulkCount += 1
                mongoRecords = mongoCollection.find({idField: {'$in': recordIds}}, {'_id': 0})
                bulk = mongoCollection.initialize_unordered_bulk_op()
                for record in mongoRecords:
                    checkRecordId = record[idField]
                    if checkRecordId in recordHashes:
                        checkHash = ingestHelpers.getMd5Hash(record)
                        if recordHashes[checkRecordId]['hash'] != checkHash:
                            bulk.find({idField: checkUUID}).upsert().update({'$set': reader[recordHashes[checkRecordId]['row']]})
                    else:
                        bulk.insert(occReader[recordHashes[checkRecordId]['row']])
                try:
                    bulk_results = bulk.execute()
                    self.logger.debug(bulk_results)
                except BulkWriteError as bwe:
                    self.logger.error("Partial import bulk failure in Batch" + str(bulkCount) + "!")
                    self.logger.error(bwe.details)
                    importError = True
                except InvalidOperation as io:
                    self.logger.info("There were no records to update in Batch " + str(bulkCount))
        if rowCount % 500 != 0:
            bulkCount += 1
            try:
                bulk_results = bulk.execute()
                self.logger.debug(bulk_results)
            except BulkWriteError as bwe:
                self.logger.error("Partial import bulk failure in Batch" + str(bulkCount) + "!")
                self.logger.error(bwe.details)
                importError = True
            except InvalidOperation as io:
                self.logger.info("There were no records to update in Batch " + str(bulkCount))

        return importError
