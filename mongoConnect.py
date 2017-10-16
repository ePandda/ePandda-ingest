#
# Mongo Class for handling data import from ingesters
# by Mike Benowitz
#

# import database tools
from pymongo import MongoClient
import pymongo
from pymongo.errors import BulkWriteError, InvalidOperation
from bson import ObjectId

# data tools
import json
import csv

# sys tools
from subprocess import Popen, PIPE, call
import logging
import datetime

# helper module
from helpers import ingestHelpers

class mongoConnect:
    def __init__(self):
        self.config = json.load(open('./config.json'))
        self.client = MongoClient("mongodb://" + self.config['mongodb_user'] + ":" + self.config['mongodb_password'] + "@" + self.config['mongodb_host'])
        self.idigbio = self.client[self.config['idigbio_db']]
        self.pbdb = self.client[self.config['pbdb_db']]
        self.ingestLog = self.client[self.config['log_db']]
        self.endpoints = self.client[self.config['endpoints_db']]
        self.logger = logging.getLogger("ingest.mongoConnection")

    def closeConnection(self):
        try:
            self.client.close()
            return True
        except:
            self.logger.error("Couldn't close mongo connection")
            return False

    def checkIDBCollectionStatus(self, collectionKey, modifiedDate):
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

    def iDBPartialImport(self, occurrenceFile, collectionKey, collectionModified, fileType):
        importCall = Popen(['mongoimport', '--host', self.config['mongodb_host'], '-u', self.config['mongodb_user'], '-p', self.config['mongodb_password'], '--authenticationDatabase', 'admin', '-d', self.config['idigbio_db'], '-c', self.config['idigbio_coll'], '--type', fileType, '--file', occurrenceFile, '--headerline', '--mode', 'upsert', '--upsertFields', 'idigbio:uuid'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
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
            # Checking for duplicate headers
            duplicateHeaders = ingestHelpers.csvDuplicateHeaderCheck(csvFile)
            if duplicateHeaders:
                self.logger.debug(duplicateHeaders)
                renameStatus = ingestHelpers.csvRenameDuplicateHeaders(csvFile, duplicateHeaders)
            collectionName = 'tmp_' + csvFile[:-4]
            importArgs = ['mongoimport', '--host', self.config['mongodb_host'], '-u', self.config['mongodb_user'], '-p', self.config['mongodb_password'], '--authenticationDatabase', 'admin', '-d', self.config['pbdb_db'], '-c', collectionName, '--type', 'csv', '--file', csvFile, '--headerline']
            if collectionName == 'tmp_occurrence':
                importArgs.append('--drop')
            	self.logger.debug("Dropping existing records in " + collectionName)
	    elif collectionName == 'tmp_reference':
                importArgs.extend(['--mode', 'upsert', '--upsertFields', 'reference_no'])
            elif collectionName == 'tmp_collection':
                importArgs.extend(['--mode', 'upsert', '--upsertFields', 'collection_no'])
            importCall = Popen(importArgs, stdin=PIPE, stdout=PIPE, stderr=PIPE)
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
            collectionData.pop("_id", None) # Remove ObjectID field
            if not collectionData:
                self.logger.error("Could not find collection_no: " + str(collectionNo))
                continue
            self.logger.debug("Adding collection data for collection_no: " + str(collectionNo))
            occurrenceCollection.update_many({'collection_no': collectionNo}, {'$addToSet': {'coll_refs': collectionData}})

        self.logger.info("Merging occurrences and references")
        referenceNos = occurrenceCollection.distinct('reference_no')
        for referenceNo in referenceNos:
            referenceData = referenceCollection.find_one({'reference_no': referenceNo})
            referenceData.pop("_id", None) # Remove ObjectID field
            if not referenceData:
                self.logger.error("Could not find reference_no: " + str(referenceNo))
                continue
            self.logger.debug("Adding collection data for collection_no: " + str(referenceNo))
            occurrenceCollection.update_many({'reference_no': referenceNo}, {'$addToSet': {'occ_refs': referenceData}})

        return True

    def pbdbMergeNewData(self, tmp_occurrence):
        self.logger.info("Merging new PaleoBio data")

        self.logger.debug("Exporting contents of temporary collection")
        exportCall = Popen(['mongoexport', '--host', self.config['mongodb_host'], '-u', self.config['mongodb_user'], '-p', self.config['mongodb_password'], '--authenticationDatabase', 'admin', '-d', self.config['pbdb_db'], '-c', 'tmp_occurrence', '--type', 'json', '-o', 'tmp_occurrence.json'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
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
        return True

    def createIngestLog(self, sources):
        ingests = self.ingestLog[self.config['ingest_collection']]
        ingestRecord = ingests.insert_one({'ingestDate': datetime.datetime.utcnow(), 'ingestSources': sources, 'status': 'STARTED'})
        ingestId = ingestRecord.inserted_id
        return ingestId

    def addRunTime(self, ingestID, timeString):
        ingests = self.ingestLog[self.config['ingest_collection']]
        ingestResult = ingests.update_one({'_id': ingestID}, {'$set': {'runTime': timeString, 'status': 'COMPLETE'}})
        if ingestResult.modified_count == 1:
            self.logger.debug("Added run time to mongo ingest log")
            return True
        else:
            self.logger.warning("Could not add time to mongo ingest log!")
            return False

    def addToIngestCount(self, ingestID, source, recordCount):
        ingests = self.ingestLog[self.config['ingest_collection']]
        ingestResult = ingests.update_one({'_id': ingestID}, {'$inc': {source+'_updated_records': recordCount}})
        if ingestResult.modified_count == 1:
            self.logger.debug("Added import count to ingest log")
            return True
        else:
            self.logger.warning("Could not add import count to ingest log!")
            return False

    def getCollectionCount(self, source):
        sourceDB = self.client[self.config[source+'_db']]
        sourceCollection = sourceDB[self.config[source+'_coll']]
        totalCount = sourceCollection.find({}).count()
        return totalCount

    def addLogCount(self, ingestID, source):
        ingests = self.ingestLog[self.config['ingest_collection']]
        totalCount = self.getCollectionCount(source)
        self.logger.info(str(totalCount) + " Records in " + source)
        ingestResult = ingests.update_one({'_id': ingestID}, {'$set': {source+"_total_records": totalCount}})
        if ingestResult.modified_count == 1:
            self.logger.debug("Added total count to ingest log")
            return totalCount
        else:
            self.logger.warning("Could not add total count to ingest log!")
            return False

    def indexTest(self, db, collection, indexes):
        self.logger.debug("Checking indexes for " + collection)
        collectionName = self.config[collection]
        testCollection = self.client[db][collectionName]
        print testCollection
        existingIndexes = testCollection.index_information()
        indexChecklist = []
        missingIndexes = []
        for indexID in existingIndexes:
            indexName = existingIndexes[indexID]['key'][0][0]
            self.logger.debug("Adding record for index " + indexName)
            indexChecklist.append(indexName)
        for index in indexes:
            if index not in indexChecklist:
                missingIndexes.append(index)
        if missingIndexes:
            return missingIndexes
        return True

    def createIndex(self, db, collection, indexes):
        self.logger.info("Creating missing indexes for " + collection)
        collectionName = self.config[collection]
        targetCollection = self.client[db][collectionName]
        for index in indexes:
            try:
                targetCollection.create_index(index)
                self.logger.info("Created index " + index + " on " + collection)
            except:
                self.logger.error("Failed to create index " + index + " on " + collection)
                return False
        return True

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

    def getSentinelCount(self, source):
        sourceDB = self.client[self.config[source+'_db']]
        sentinelCollection = sourceDB['sentinels']
        totalCount = sentinelCollection.find({}).count()
        return totalCount

    def addSentinels(self, source, totalCount, existingSentinels):
        sourceDB = self.client[self.config[source+'_db']]
        sourceCollection = sourceDB[self.config[source+'_coll']]
        sentinelCollection = sourceDB['sentinels']

        # Calculating no. of sentinels to add
        sentinelMax = int(totalCount * self.config['sentinel_ratio'])
        newSentinels = sentinelMax - existingSentinels
        sentinelInterval = totalCount / sentinelMax
        self.logger.debug("setting sentinel interval to " + str(sentinelInterval) + " for max " + str(newSentinels) + " sentinals")
        try:
            lastSentinel = sentinelCollection.find_one({}).sort([('_id', -1)])
            lastSentinelID = lastSentinel['_id']
        except AttributeError:
            lastSentinelID = ObjectId('000000000000000000000000')
        sentinelCount = 0
        bulk = sentinelCollection.initialize_unordered_bulk_op()
        while sentinelCount < newSentinels:
            sentinelCursor = sourceCollection.find({'_id': {'$gt': lastSentinelID}}).skip(sentinelInterval).limit(1)
            try:
                newSentinel = sentinelCursor.next()
            except StopIteration:
                sentinelCount += 1
                continue
            bulk.find({'_id': newSentinel['_id']}).upsert().update({'$set': newSentinel})
            lastSentinelID = newSentinel['_id']
            sentinelCount += 1
            if sentinelCount % 250 == 0:
                try:
                    bulk_results = bulk.execute()
                    self.logger.debug(bulk_results)
                except BulkWriteError as bwe:
                    self.logger.error("Partial import bulk failure for these Sentinels")
                    self.logger.error(bwe.details)
                    importError = True
                except InvalidOperation as io:
                    self.logger.info("There were no records to update in Sentinels")
                bulk = sentinelCollection.initialize_unordered_bulk_op()

        if sentinelCount % 250 != 0:
            try:
                bulk_results = bulk.execute()
                self.logger.debug(bulk_results)
            except BulkWriteError as bwe:
                self.logger.error("Partial import bulk failure for these Sentinels")
                self.logger.error(bwe.details)
                importError = True
            except InvalidOperation as io:
                self.logger.info("There were no records to update in Sentinels")


        self.logger.info(str(sentinelCount) + " new sentinel records created")
        if sentinelCount + existingSentinels >= sentinelMax:
            return True
        else:
            return False

    def verifySentinels(self, source):
        sourceDB = self.client[self.config[source+'_db']]
        sourceCollection = sourceDB[self.config[source+'_coll']]
        sentinelCollection = sourceDB['sentinels']

        sentinels = sentinelCollection.find({})
        self.logger.info("Checking sentinels for " + source)
        modifiedSentinels = staticSentinels = missingSentinels = sentinelBatch = sentinelCount = 0
        for sentinel in sentinels:
            if sentinelCount % 100 == 0:
                sentinelBatch += 1
                self.logger.info("Processing Sentinel Batch " + str(sentinelBatch))
            sentinelCount += 1
            sourceRecord = sourceCollection.find_one({'_id': sentinel['_id']})
            if not sourceRecord:
                missingSentinels += 1
                self.logger.warning("document " + str(sourceRecord['_id']) + " is missing")
                continue
            recordChanged = ingestHelpers.compareDocuments(sourceRecord, sentinel)
            if recordChanged is True:
                modifiedSentinels += 1
                self.logger.warning("document " + str(sourceRecord['_id']) + " has changed")
                continue

            staticSentinels += 1

        self.logger.info(str(staticSentinels) + " Sentinels Unchanged / " + str(modifiedSentinels) + " Sentinels Modified / " + str(missingSentinels) + " Sentinels Missing")
        return staticSentinels, modifiedSentinels, missingSentinels
