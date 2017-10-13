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
    def __init__(self):
        self.config = json.load(open('./config.json'))
        self.logger = logging.getLogger("test.main")

    def checkIndexes(self):
        indexes = self.config['test_indexes']
        mongoConn = mongoConnect.mongoConnect()
        for indexCheck in indexes:
            database = indexCheck['db']
            collection = indexCheck['collection']
            verifyIndexes = indexCheck['indexes']
            indexTestResult = mongoConn.indexTest(database, collection, verifyIndexes)

            if indexTestResult is True:
                self.logger.info(collection + " has all necessary indexes")
            else:
                self.logger.warning(collection + " is missing the following indexes. They will now be created")
                self.logger.warning(indexTestResult)
                indexResult = mongoConn.createIndexes(db, collection, indexTestResult)
                if indexResult is False:
                    self.logger.warning(collection + " failed index test. Exit")
                    return False
        return True

    def checkCounts(self, sources, fullCounts):
        for source in sources:
            totalCount = source.getRecordCount()
            if totalCount:
                epanddaSourceTotal = fullCounts[source.source]
                if epanddaSourceTotal == totalCount:
                    self.logger.info("COUNTS MATCH EXACTLY")
                elif epanddaSourceTotal > totalCount:
                    countDiff = epanddaSourceTotal - totalCount
                    self.logger.warning("ePandda has more records than " + source.source + " by " + str(countDiff) + ". Review for possible duplicates")
                else:
                    countDiff = totalCount - epanddaSourceTotal
                    percentShared = epanddaSourceTotal / totalCount
                    if percentShared < 0.95:
                        self.logger.info("ePandda differs from " + source.source + " by " + str(countDiff) + ". Less than 5%")
                    else:
                        self.logger.warning("ePandda differs from " + source.source + " by " + str(countDiff) + ". MORE than 5%")
            else:
                self.logger.warning("Could not get count from " + source.source + "Check records for validity")
