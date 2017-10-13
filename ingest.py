#
# Script to Ingest records to ePandda
# Currently can ingest:
# iDigBio
# PaleoBioDB
#
# by Michael Benowitz
# Whirl-i-Gig Inc.
#

# core dependencies
import sys
import json
import datetime
import argparse
import logging
import time

# import/ingest dependencies
import urllib2
import requests
from unidecode import unidecode

# Source modules that can be ingested
from sources import idigbio
from sources import paleobio

# Helper functions for managing ingest
from helpers import ingestHelpers
from helpers import logHelpers
from helpers import testHelpers

def main():
    # Start the process timer and create ingest_run mongo entry
    startTime = time.time()
    startDateTime = time.strftime("%Y/%m/%d@%H:%M:%s")


    # Get the parameters provided in the args
    parser = ingestHelpers.createParser()
    args = parser.parse_args()
    ingestSources = args.sources
    dryRun = args.dryRun
    testRun = args.test
    logLevel = args.logLevel
    fullRefresh = args.fullRefresh

    # Create the logs
    logger = logHelpers.createLog('ingest', logLevel, '_ingest')
    testLogger = logHelpers.createLog('test', logLevel, '_tests')
    logger.info("Starting ePandda ingest")
    # Source classes
    idb = idigbio.idigbio(testRun, fullRefresh)
    pbdb = paleobio.paleobio(testRun, fullRefresh)
    sourceNames = ingestHelpers.getSourceNames([idb, pbdb])

    try:
        for ingestSource in ingestSources:
            if ingestSource not in sourceNames:
                logger.error("Invalid source provided: " + ingestSource)
                raise
    except:
        print "An invalid source database was provided. The following databases"\
        "are available for ingest: "
        for source in sourceNames:
            print source
        sys.exit(0)

    # Create log entry in ingest collection
    ingestID = logHelpers.createMongoLog(startDateTime, ingestSources)

    # Create test instance
    tests = testHelpers.epanddaTests()

    for ingestSource in ingestSources:
        ingester = sourceNames[ingestSource]

        # Check indexes and create if necessary
        indexStatus = tests.checkIndexes()
        if indexStatus is False:
            logger.error("Index Creation Failure")
            sys.exit(3)
        logger.info("Starting import for: " + ingestSource)
        outcome = ingester.runIngest(dry=dryRun, test=testRun)
        if outcome is False:
            logger.error("Import of " + ingestSource + " failed! Review full log")
        else:
            logger.info("Import of " + ingestSource + " successful!")

    endTime = time.time()
    ingestLogStatus = logHelpers.logRunTime(ingestID, startTime, endTime)
    if ingestLogStatus == False:
        logger.error("Failed to update mongo ingest log. CHECK FOR ERRORS!")
    logger.info("Ingest Complete")

if __name__ == '__main__':
    main()
