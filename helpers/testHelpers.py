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
        indexes = self.config['verify_indexes']
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
