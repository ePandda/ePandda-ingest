#
# Methods for testing validity of ePandda ingest process and resulting records
#

# Core modules
import logging
import time
import sys
import json

# Local modules
import mongoConnect

class epanddaTests:
    def __init__(self, idb, pbdb):
        self.config = json.load(open('./config.json'))
        self.logger = logging.getLogger("test.main")
        self.sources = {
            "idigbio": idb,
            "pbdb": pbdb
        }

    def checkIndexes(self, fullRefresh, importStatus):
        indexes = self.config['test_indexes']
        mongoConn = mongoConnect.mongoConnect()
        for indexCheck in indexes:
            database = indexCheck['db']
            collection = indexCheck['collection']
            verifyIndexes = indexCheck['indexes']
            indexTestResult = mongoConn.indexTest(database, collection, verifyIndexes)

            if indexTestResult is True:
                if indexCheck['dropForFull'] is False:
                    self.logger.info(collection + " has all necessary indexes")
                elif fullRefresh is True:
                    self.logger.info("Dropping indexes on " + collection + " for full import ")
                    indexDropResult = mongoConn.deleteIndexes(database, collection, verifyIndexes)
                    return indexDropResult
                else:
                    self.logger.info("Retaining necessary indexes for partial import")
            elif indexCheck['dropForFull'] is False:
                self.logger.warning(collection + " is missing the following indexes. They will now be created")
                self.logger.warning(indexTestResult)
                indexResult = mongoConn.createIndexes(database, collection, indexTestResult)
                if indexResult is False:
                    self.logger.warning(collection + " failed index test. Exit")
                    return False
            else:
                if importStatus is 'post':
                    self.logger.info(collection + " has not been indexed. Indexing now following full import")
                    indexResult = mongoConn.createIndexes(database, collection, indexTestResult)
                    if indexResult is False:
                        self.logger.warning(collection + " failed index test. Exit")
                        return False
                elif fullRefresh is False:
                    self.logger.info("Need indexes for partial import. Indexing now.")
                    indexResult = mongoConn.createIndexes(database, collection, indexTestResult)
                    if indexResult is False:
                        self.logger.warning(collection + " failed index test. Exit")
                        return False
                else:
                    self.logger.info(collection + " is not indexed, which is necessary for the full import, proceeding")
        return True

    def checkCounts(self, sources, fullCounts):
        for source in sources:
            sourceInstance = self.sources[source]
            totalCount = sourceInstance.getRecordCount()
            if totalCount:
                epanddaSourceTotal = fullCounts[sourceInstance.source]
                if epanddaSourceTotal == totalCount:
                    self.logger.info("COUNTS MATCH EXACTLY")
                elif epanddaSourceTotal > totalCount:
                    countDiff = epanddaSourceTotal - totalCount
                    self.logger.warning("ePandda has more records than " + sourceInstance.source + " by " + str(countDiff) + ". Review for possible duplicates")
                else:
                    countDiff = totalCount - epanddaSourceTotal
                    percentShared = epanddaSourceTotal / totalCount
                    if percentShared < 0.95:
                        self.logger.info("ePandda differs from " + sourceInstance.source + " by " + str(countDiff) + ". Less than 5%")
                    else:
                        self.logger.warning("ePandda differs from " + sourceInstance.source + " by " + str(countDiff) + ". MORE than 5%")
            else:
                self.logger.warning("Could not get count from " + sourceInstance.source + "Check records for validity")

    def createSentinels(self, sources):
        sentinelRatio = self.config['sentinel_ratio']
        mongoConn = mongoConnect.mongoConnect()
        for source in sources:
            totalCount = mongoConn.getCollectionCount(source)
	    if not totalCount:
		self.logger.info("New import, no sentinals can exist yet!")
		continue
            sentinelCount = mongoConn.getSentinelCount(source)
            if sentinelCount / totalCount >= sentinelRatio:
                self.logger.info("Sentinel Collection exists for " + source)
            else:
                self.logger.warning("Insuficient sentinals for " + source + " Adding new sentinels")
                sentinalCreationResult = mongoConn.addSentinels(source, totalCount, sentinelCount)
                if sentinalCreationResult is True:
                    self.logger.info("Successfully created new sentinels")
                    return True
                else:
                    self.logger.error("Could not create sufficient sentinels, Check database!")
                    return False


    def checkSentinels(self, sources):
        mongoConn = mongoConnect.mongoConnect()
        errorReport = False
        for source in sources:
            sentinelCount = mongoConn.getSentinelCount(source)
            static, modified, missing = mongoConn.verifySentinels(source)
            if missing > 0 or modified > (sentinelCount/10):
                self.logger.error("Potential Issue with " + source + " flagged from sentinels")
                errorReport = True
        return errorReport
