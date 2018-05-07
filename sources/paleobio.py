#
# Class for PaleoBio ingest
#

# Data parsing
import json
import pandas as pd

# Data harvesting/gathering
from unidecode import unidecode
import urllib2
import requests

# System tools
import traceback
import os
import os.path
import shutil
import logging

# local stuff
import multiConnect
from helpers import ingestHelpers
from helpers import testHelpers

class paleobio:
    def __init__(self, test, fullRefresh, ingestLog):
        self.config = json.load(open('./config.json'))
        self.source = "pbdb"
        self.logger = logging.getLogger("ingest.paleobio")
        ingestInterval = self.config['pbdb_ingest_interval'] + 'd'
        if test:
            ingestInterval = '24h'
        elif fullRefresh:
            ingestInterval = '1900'
        self.occurrenceURL = 'https://paleobiodb.org/data1.2/occs/list.csv?all_records&show=full&occs_modified_after=' + ingestInterval
        self.collectionURL = 'https://paleobiodb.org/data1.2/colls/list.csv?all_records&show=full&colls_modified_after=' + ingestInterval
        self.referenceURL = 'https://paleobiodb.org/data1.2/refs/list.csv?all_records&show=both&refs_modified_after=' + ingestInterval
        self.recordCountURL = 'https://paleobiodb.org/data1.2/occs/list.json?all_records&rowcount&limit=1'
        self.collectionCountURL = 'https://paleobiodb.org/data1.2/colls/single.json?coll_id='
        self.occurrenceDownloadURL = 'https://paleobiodb.org/data1.2/occs/list.json?coll_id='
        self.ingestLog = ingestLog
        self.tests = testHelpers.epanddaTests(None, None)

    # This is the main component of the ingester, and relies on a few different
    # helpers. But most of this code is specific to PaleoBio
    def runIngest(self, dry=False, test=False):
        # Should this be a dry or test run?
        dryRun = dry
        testRun = test
        # Download source PBDB spreadsheets
        self.logger.info("Starting download from PaleoBio")
        downloadResults = self.downloadFromPBDB()
        if downloadResults is False:
            self.logger.error("A download failed! Ingest halted")
            return False
        self.logger.info("Completed paleobio download")

        # open a mongo connection
        multiConn = multiConnect.multiConnect()
        downloadedFiles = ['occurrence.csv', 'collection.csv', 'reference.csv']
        # Ingest records into temporary mongo collections for easier merging
        self.logger.info("Creating ingest collections")
        tmpCollectionResults = multiConn.pbdbIngestTmpCollections(downloadedFiles)
        if tmpCollectionResults is False:
            self.logger.error("Could not create all necessary mongo collections. Halting")
            return False
        self.logger.info("Created PaleoBio temporary collections")

        # Get the count of records being imported and store it in the ingest log
        recordCount = ingestHelpers.csvCountRows('occurrence.csv')
        recordCountResult = multiConn.addToIngestCount(self.ingestLog, self.source, recordCount, 0)
        if recordCountResult is False:
            self.logger.error("Could not log record count. Check validity carefully!")

        for csvFile in downloadedFiles:
            os.remove(csvFile)
            self.logger.debug("Deleted source file: " + csvFile)
        # Merge collections and references into occurrence collection
        self.logger.info("Merging temporary collections")
        mergeResult = multiConn.pbdbMergeTmpCollections('tmp_occurrence', 'tmp_collection', 'tmp_reference')
        if mergeResult is False:
            self.logger.error("Could not merge PaleoBio collections. Halting")
            return False
        self.logger.info("Created merged dataset")

        # Merge new data into main pbdb collection
        ingestResult = multiConn.pbdbMergeNewData('tmp_occurrence.csv')
        if ingestResult is False:
            self.logger.error("There was an error ingesting new records. Halting and please review the log")
            return False
        os.remove('tmp_occurrence.csv')

        # Create sentinels on the ingested data
        sentinelStatus = self.tests.createSentinels(['pbdb'])
        if sentinelStatus is False:
            self.logger.error("Sentinal Creation Failure for PBDB")
            return False

        return True

    def downloadFromPBDB(self):
        for downloadURL in [('occurrence', self.occurrenceURL), ('collection', self.collectionURL), ('reference', self.referenceURL)]:
            self.logger.debug("Downloading " + downloadURL[0] + " from PaleoBio")
            sourceCSV = urllib2.urlopen(downloadURL[1]).read()
            with open(downloadURL[0]+".csv", "wb") as csvFile:
                csvFile.write(sourceCSV)
            # Verify that file exists
            if os.path.isfile(downloadURL[0]+".csv"):
                self.logger.info("Successfully downloaded " + downloadURL[0])
            else:
                self.logger.error("Failed to download " + downloadURL[0])
                return False
        return True

    def getRecordCount(self):
        self.logger.debug("Checking full PBDB record Count")
        resp = requests.get(self.recordCountURL)
        if resp.status_code == 200:
            recordCountBody = resp.json()
            if 'records_found' in recordCountBody:
                recordCount = recordCountBody['records_found']
                return recordCount
        return None

    def deleteCheck(self):
        self.logger.info("Checking PBDB for deleted records")

        # open a mongo connection
        multiConn = multiConnect.multiConnect()
        pbdbCollections = multiConn.getCollectionCounts('pbdb', 'occurrence_no')
        multiConn.closeConnection()
        if not pbdbCollections:
            self.logger.error("Could not load collections sets. Check logs for error")
            return False
        for collection in pbdbCollections:
            collectionID = collection[0]
            collectionCount = collection[1]
            self.logger.debug("Checking counts of " + collectionID)
            apiResponse = requests.get(self.collectionCountURL+collectionID)
            if apiResponse.status_code == 200:
                collectionCountBody = apiResponse.json()
                if 'records' in collectionCountBody:
                    record = collectionCountBody['records'][0]
                    occurrenceCount = record['noc']
                    if collectionCount == occurrenceCount:
                        self.logger.debug(collectionID + " matches source record count")
                    elif occurrenceCount > collectionCount:
                        self.logger.warning(collectionID + " is missing records from source")
                    else:
                        self.logger.info(collectionID + " contains deleted records, deleting now")
                        deleteResult = self.deleteRecords(collectionID)
                else:
                    self.logger.error("Could not load recordset from pbdb: " + collectionID)
            else:
                self.logger.error("Requests error " + str(apiResponse.status_code) + "for: " + collectionID)

    def deleteRecords(self, collectionID):
        self.logger.info("Deleting records from " + collectionID)
        occurrenceIds = set()
        apiResponse = requests.get(self.occurrenceDownloadURL+collectionID)
        if apiResponse.status_code == 200:
            collOccurrences = apiResponse.json()
            if 'records' in collOccurrences:
                for occurrence in collOccurrences['records']:
                    occurrenceIds.add(occurrence['oid'][4:])

        # open a mongo connection
        multiConn = multiConnect.multiConnect()
        idbRecordSets = multiConn.checkAndDeleteRecords(collectionID, occurrenceIds, 'collection_no', 'pbdb')
        multiConn.closeConnection()
