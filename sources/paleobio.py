#
# Class for PaleoBio ingest
#

# Data parsing
import json
import pandas as pd

# Data harvesting/gathering
from unidecode import unidecode
import urllib2

# System tools
import traceback
import os
import os.path
import shutil
import logging

# local stuff
import mongoConnect

class paleobio:
    def __init__(self, test):
        self.config = json.load(open('./config.json'))
        self.source = "paleobio"
        self.logger = logging.getLogger("ingest.paleobio")
        ingestInterval = self.config['pbdb_ingest_interval']
        if test:
            ingestInterval = '24h'
        self.occurrenceURL = 'https://paleobiodb.org/data1.2/occs/list.csv?all_records&show=full&occs_modified_after=' + ingestInterval
        self.collectionURL = 'https://paleobiodb.org/data1.2/colls/list.csv?all_records&show=full&colls_modified_after=' + ingestInterval
        self.referenceURL = 'https://paleobiodb.org/data1.2/refs/list.csv?all_records&show=both&refs_modified_after=' + ingestInterval

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
        mongoConn = mongoConnect.mongoConnect()
        downloadedFiles = ['occurrence.csv', 'collection.csv', 'reference.csv']
        # Ingest records into temporary mongo collections for easier merging
        self.logger.info("Creating ingest collections")
        tmpCollectionResults = mongoConn.pbdbIngestTmpCollections(downloadedFiles)
        if tmpCollectionResults is False:
            self.logger.error("Could not create all necessary mongo collections. Halting")
            return False
        self.logger.info("Created PaleoBio temporary collections")

        for csvFile in downloadedFiles:
            os.remove(csvFile)
            self.logger.debug("Deleted source file: " + csvFile)
        # Merge collections and references into occurrence collection
        self.logger.info("Merging temporary collections")
        mergeResult = mongoConn.pbdbMergeTmpCollections('tmp_occurrence', 'tmp_collection', 'tmp_reference')
        if mergeResult is False:
            self.logger.error("Could not merge PaleoBio collections. Halting")
            return False
        self.logger.info("Created merged dataset")

        # Merge new data into main pbdb collection
        ingestResult = mongoConn.pbdbMergeNewData('tmp_occurrece')
        if ingestResult is False:
            self.logger.error("There was an error ingesting new records. Halting and please review the log")
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
