#
# Mongo Class for handling data import from ingesters
# by Mike Benowitz
#

# import database tools
from pymongo import MongoClient
import pymongo
from pymongo.errors import BulkWriteError, InvalidOperation
from bson import ObjectId
import pandas as pd
# ElasticSearch too now
from elasticsearch import Elasticsearch, helpers

# data tools
import json
import csv

# sys tools
from subprocess import Popen, PIPE, call
import shutil
import logging
import datetime
import time
import math
import os
import fnmatch

# helper module
from helpers import ingestHelpers

class multiConnect:
    def __init__(self):
        self.config = json.load(open('./config.json'))
        self.client = MongoClient("mongodb://" + self.config['mongodb_user'] + ":" + self.config['mongodb_password'] + "@" + self.config['mongodb_host'])
        self.elastic = self.config['elastic_host']
        self.esClient = Elasticsearch(self.elastic)
        self.idigbio = self.config['idigbio_endpoint']
        self.pbdb = self.config['pbdb_endpoint']
        self.idigbio_db = self.client[self.config['idigbio_db']]
        self.pbdb_db = self.client[self.config['pbdb_db']]
        self.indexMapping = self.config['elastic_mapping']
        self.ingestLog = self.client[self.config['log_db']]
        self.endpoints = self.client[self.config['endpoints_db']]
        self.logger = logging.getLogger("ingest.multiConnection")
        self.logstash = self.config['logstash_path']

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
        collCollection = self.idigbio_db.collectionStatus

        collStatus = collCollection.find_one({'collection': collectionKey})
        if not collStatus:
            return 'new'
        elif collStatus['modifiedDate'] != modifiedDate:
            return 'modified'
        else:
            return 'static'

    def iDBFullImport(self, occurrenceFile, collectionKey, collectionModified):
        self.logger.debug("Formating GeoPoints for " + occurrenceFile)
        ingestHelpers.idbCleanSpreadsheet(occurrenceFile)
        for file in os.listdir('.'):
            if fnmatch.fnmatch(file, 'occurrence_*.csv'):
                self.logger.debug("Importing collection " + file + "into Elastic")
                tmpIN = open(file)
                importCall = Popen([self.logstash, '-f', 'idigbio_logstash.conf', '--path.settings', '/etc/logstash'], stdin=tmpIN, stdout=PIPE, stderr=PIPE)
                out, err = importCall.communicate()
                if importCall.returncode != 0:
                    self.logger.error("elastic import failed with error: " + err)
                    return False
                else:
                    self.logger.info("elastic import success! " + out)
                    os.remove(file)
        collCollection = self.idigbio_db.collectionStatus
        updateStatus = collCollection.update({'collection': collectionKey}, {'$set': {'collection': collectionKey, 'modifiedDate': collectionModified}}, upsert=True)
        if collCollection:
            self.logger.debug("Added/updated collection entry in collectionStatus for " + collectionKey)
        else:
            self.logger.warning("Failed to update this record in collectionStatus: " + collectionKey)
        return True


    def iDBPartialImport(self, occurrenceFile, collectionKey, collectionModified, fileType):
        self.logger.debug("Formating GeoPoints for " + occurrenceFile)
        ingestHelpers.idbCleanSpreadsheet(occurrenceFile)
        self.logger.debug("Importing collection " + occurrenceFile + " into Elastic")
        tmpIN = open(occurrenceFile)
        importCall = Popen([self.logstash, '-f', 'idigbio_logstash.conf', '--path.settings', '/etc/logstash'], stdin=tmpIN, stdout=PIPE, stderr=PIPE)
        out, err = importCall.communicate()
        if importCall.returncode != 0:
            self.logger.error("elastic import failed with error: " + err)
            return False
        else:
            self.logger.info("elastic import success! " + out)
        collCollection = self.idigbio_db.collectionStatus
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
            importArgs = ['mongoimport', '--host', self.config['mongodb_host'], '-u', self.config['mongodb_user'], '-p', self.config['mongodb_password'], '--authenticationDatabase', 'admin', '-d', self.config['pbdb_db'], '-c', collectionName, '--numInsertionWorkers', '4', '--type', 'csv', '--file', csvFile, '--headerline']
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
        occurrenceCollection = self.pbdb_db[occurrence]
        collectionCollection = self.pbdb_db[collection]
        referenceCollection = self.pbdb_db[reference]

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
                continue
            collectionData.pop("_id", None) # Remove ObjectID field
            self.logger.debug("Adding collection data for collection_no: " + str(collectionNo))
            occurrenceCollection.update_many({'collection_no': collectionNo}, {'$addToSet': {'coll_refs': collectionData}})

        self.logger.info("Merging occurrences and references")
        referenceNos = occurrenceCollection.distinct('reference_no')
        for referenceNo in referenceNos:
            referenceData = referenceCollection.find_one({'reference_no': referenceNo})
            if not referenceData:
                self.logger.error("Could not find reference_no: " + str(referenceNo))
                continue
            referenceData.pop("_id", None) # Remove ObjectID field
            self.logger.debug("Adding reference data for reference: " + str(referenceNo))
            occurrenceCollection.update_many({'reference_no': referenceNo}, {'$addToSet': {'occ_refs': referenceData}})

        return True

    def pbdbMergeNewData(self, tmp_occurrence):
        self.logger.info("Merging new PaleoBio data")

        self.logger.debug("Exporting contents of temporary collection")
        exportCall = Popen(['mongoexport', '--host', self.config['mongodb_host'], '-u', self.config['mongodb_user'], '-p', self.config['mongodb_password'], '--authenticationDatabase', 'admin', '-d', self.config['pbdb_db'], '-c', 'tmp_occurrence', '--type', 'csv', '-o', 'tmp_occurrence.csv', '--fieldFile', 'sources/pbdbFields.txt'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
        out, err = exportCall.communicate()
        if exportCall.returncode != 0:
            self.logger.error("mongoexport failed with error: " + err)
            return False
        else:
            self.logger.debug("Successfully exported temp mongo collection! " + out)

        self.logger.debug("Formating GeoPoints for " + tmp_occurrence)
        ingestHelpers.pbdbCleanGeoPoints(tmp_occurrence)
        self.logger.debug("Importing collection " + tmp_occurrence + " into Elastic")
        tmpIN = open(tmp_occurrence)
        importCall = Popen([self.logstash, '-f', 'pbdb_logstash.conf', '--path.settings', '/etc/logstash'], stdin=tmpIN, stdout=PIPE, stderr=PIPE)
        out, err = importCall.communicate()
        if importCall.returncode != 0:
            self.logger.error("elasticsearch_loader failed with error: " + err)
            return False
        else:
            self.logger.info("elasticsearch_loader success! " + out)
            return True
        return True

    def getCollectionCounts(self, docType, collectionField):
        setAggregation = {
            "size": 0,
            "query":{
                "type": {
                    "value": docType
                }
            },
            "aggs": {
                "sets":{
                    "terms": {
                        "field": collectionField,
                        "size": 2000
                    }
                }
            }
        }
        recordSets = self.esClient.search(index=docType, body=setAggregation)
        collectionCounts = []
        if not recordSets:
            return False
        for recordSet in recordSets['aggregations']['sets']:
            setCount = recordSet['doc_count']
            if not setCount:
                self.loggger("Couldn't find " + docType + " recordset " + recordSet + " for counting")
                return False
            collectionCounts.append((recordSet, setCount))
        return collectionCounts

    def checkAndDeleteRecords(self, groupID, recordIDs, idField, docType):
        resultTop = totalResults = offset = 0
        epanddaIDs = set()
        while resultTop <= totalResults:
            setSearch = {
                "_source": False,
                "size": 1000,
                "from": offset,
                "query":{
                    "term": {
                        idField: groupID
                    },
                    "type": {
                        "value": docType
                    }
                }
            }
            setIDs = self.esClient.search(index=docType, query=setSearch)
            for specimen in setIDs['hits']['hits']:
                epanddaIDs.add(specimen['_id'])
            if totalResults == 0:
                totalResults = setIDs['hits']['total']
            resultTop += 1000
            offset += 1000
        self.logger.debug("Comparing source and local sets for " + setID)
        deleteSpecimens = list(sourceIDs ^ epanddaIDs)
        if len(deleteSpecimens) > 0:
            self.logger.info("Found " + str(len(deleteSpecimens)) + " deleted specimens in ePandda. Removing")
            self.logger.debug(deleteSpecimens)
        else:
            self.logger.debug("Didn't find any duplicate specimens. Check recordset " + setID + " in " + docType)

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
        fullResultQuery = {"size": 0, "query": {"match_all": {}}}
        countResults = self.esClient.search(index=source, doc_type=source, body=fullResultQuery)
        totalCount = countResults['hits']['total']
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

    def getSentinelCount(self, source):
        sourceDB = self.client[self.config[source+'_db']]
        sentinelCollection = sourceDB['sentinels']
        totalCount = sentinelCollection.find({}).count()
        return totalCount

    def addSentinels(self, source, totalCount, existingSentinels):
        # Calculating no. of sentinels to add
        sourceDB = self.client[self.config[source+'_db']]
        sentinelCollection = sourceDB['sentinels']
        sentinelMax = int(math.ceil(totalCount * self.config['sentinel_ratio']))
        newSentinels = sentinelMax - existingSentinels
        sentinelInterval = totalCount / sentinelMax
        self.logger.debug("setting sentinel interval to " + str(sentinelInterval) + " for max " + str(newSentinels) + " sentinals")

        sentinelCount = 0
        createdSentinels = []
        bulk = sentinelCollection.initialize_unordered_bulk_op()
        randomSentinelQuery = {
            "size": 25,
            "query": {
                "function_score": {
                    "query": {"match_all": {}},
                    "random_score": {}
                }
            }
        }
        sentinelLoop = True
        while sentinelLoop:
            addSentinels = self.esClient.search(body=randomSentinelQuery, index=source, doc_type=source)
            for newSentinel in addSentinels['hits']['hits']:
                if newSentinel['_id'] in createdSentinels:
                    self.logger.debug("Sentinel already added, skipping")
                    continue
                bulk.find({'_id': newSentinel['_id']}).upsert().update({'$set': newSentinel['_source']})
                createdSentinels.append(newSentinel['_id'])
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
                if sentinelCount > newSentinels:
                    sentinelLoop = False
                    break

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
            sentinelQuery = {
                "term":{
                    "size": 1,
                    "_id": sentinel['_id']
                }
            }
            sourceRecords = self.esClient.search(index=source, body=sentinelQuery)
            sourceRecord = sourceRecords['hits']['hits'][0]['_source']
            if not sourceRecord:
                missingSentinels += 1
                self.logger.warning("document " + str(sentinel['_id']) + " is missing")
                continue
            recordChanged = ingestHelpers.compareDocuments(sourceRecord, sentinel)
            if recordChanged is True:
                modifiedSentinels += 1
                self.logger.warning("document " + str(sentinel['_id']) + " has changed")
                continue

            staticSentinels += 1

        self.logger.info(str(staticSentinels) + " Sentinels Unchanged / " + str(modifiedSentinels) + " Sentinels Modified / " + str(missingSentinels) + " Sentinels Missing")
        return staticSentinels, modifiedSentinels, missingSentinels

    def deleteDuplicates(self, source):
        sourceDB = self.client[self.config[source+'_db']]
        sourceCollection = sourceDB[self.config[source+'_coll']]
        sourceIDField = self.config[source+'_idField']
        modifiedField = self.config[source+'_modifiedField']
        self.logger.info("Checking " + source + " for duplicates")
        duplicateQuery = {
            "size": 0,
            "aggs": {
                "duplicateCount": {
                    "terms":{
                        "field": "_id",
                        "min_doc_count": 2
                    },
                    "aggs":{
                        "duplicateDocuments": {
                            "top_hits": {}
                        }
                    }
                }
            }
        }
        duplicates = self.esClient.search(index=source, doc_type=source, body=duplicateQuery)
        if duplicates['hits']['total'] == 0:
            self.logger.info("No duplicates found")
            return None
        deleteCount = 0
        self.logger.info("Removing  " + str(len(duplicates)) + " from " + source)
        for dupes in duplicates['aggregrations']['duplicateCount']['buckets']:
            duplicateArray = [dupe['_source'] for dupe in dupes['duplicateDocuments']['hits']['hits']]
            self.logger.info("De-duplicating record: " + dupe['key'])
            duplicateArray.sort(key=lambda x: (datetime.datetime.strptime(x[modifiedField][0:10], "%Y-%m-%d"), len(x)))
            bestRecord = duplicateArray.pop()
            self.logger.debug("Best record is " + str(bestRecord['_id']))
            for record in duplicateArray:
                self.logger.debug('Deleting ' + str(record[sourceIDField]))
                deleteResult = self.esClient.delete(index=source, doc_type=source, id=record[sourceIDField])
                if deleteResult['result'] != 'deleted':
                    self.logger.error("FAILED TO DELETE " + record[sourceIDField])
                self.logger.info("Deleted duplicate " + record[sourceIDField])
                deleteCount += 1
        return deleteCount
