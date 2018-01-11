#
# Methods for testing validity of ePandda ingest process and resulting records
#

# Core modules
import logging
import time
import sys
import json

# Local modules
import multiConnect

class epanddaTests:
    def __init__(self, idb, pbdb):
        self.config = json.load(open('./config.json'))
        self.logger = logging.getLogger("test.main")
        self.sources = {
            "idigbio": idb,
            "pbdb": pbdb
        }

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
        multiConn = multiConnect.multiConnect()
        for source in sources:
            totalCount = multiConn.getCollectionCount(source)
	    if not totalCount:
		self.logger.info("New import, no sentinals can exist yet!")
		continue
            sentinelCount = multiConn.getSentinelCount(source)
            if sentinelCount / totalCount >= sentinelRatio:
                self.logger.info("Sentinel Collection exists for " + source)
            else:
                self.logger.warning("Insuficient sentinals for " + source + " Adding new sentinels")
                sentinalCreationResult = multiConn.addSentinels(source, totalCount, sentinelCount)
                if sentinalCreationResult is True:
                    self.logger.info("Successfully created new sentinels")
                    return True
                else:
                    self.logger.error("Could not create sufficient sentinels, Check database!")
                    return False


    def checkSentinels(self, sources):
        multiConn = multiConnect.multiConnect()
        errorReport = False
        for source in sources:
            sentinelCount = multiConn.getSentinelCount(source)
            static, modified, missing = multiConn.verifySentinels(source)
            if missing > 0 or modified > (sentinelCount/10):
                self.logger.error("Potential Issue with " + source + " flagged from sentinels")
                errorReport = True
        return errorReport

    def checkAndRemoveDuplicates(self, sources):
        self.logger.info("Checking for duplicate records")
        multiConn = multiConnect.multiConnect()
        for source in sources:
            removedDuplicates = multiConn.deleteDuplicates(source)
            if removedDuplicates is None:
                self.logger.info("No duplicates found in " + source)
            else:
                self.logger.info(str(removedDuplicates) + " duplicate records deleted from " + source)
            return removedDuplicates
