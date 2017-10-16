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

    # Get the parameters provided in the args
    parser = ingestHelpers.createParser()
    args = parser.parse_args()
    ingestSources = args.sources
    dryRun = args.dryRun
    testRun = args.test
    logLevel = args.logLevel
    fullRefresh = args.fullRefresh

    # Create log entry in ingest collection
    ingestID = logHelpers.createMongoLog(ingestSources)

    # Create the logs
    logger, coreLogFile = logHelpers.createLog('ingest', logLevel, '_ingest')
    testLogger, testLogFile = logHelpers.createLog('test', logLevel, '_tests')
    logger.info("Starting ePandda ingest")
    # Source classes
    idb = idigbio.idigbio(testRun, fullRefresh, ingestID)
    pbdb = paleobio.paleobio(testRun, fullRefresh, ingestID)
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

    # Create test instance
    tests = testHelpers.epanddaTests()

    # Check indexes and create if necessary
    indexStatus = tests.checkIndexes()

    for ingestSource in ingestSources:
        ingester = sourceNames[ingestSource]

        if indexStatus is False:
            logger.error("Index Creation Failure")
            sys.exit(3)
        logger.info("Starting import for: " + ingestSource)
        outcome = ingester.runIngest(dry=dryRun, test=testRun)
        if outcome is False:
            logger.error("Import of " + ingestSource + " failed! Review full log")
            logHelpers.emailLogAndStatus('ERROR', logger.baseFilename, testLogger.baseFilename, ['michael@whirl-i-gig.com, mwbenowitz@gmail.com'])
        else:
            logger.info("Import of " + ingestSource + " successful!")

    # Log the current number of records in ePandda
    addFullCounts = logHelpers.addFullCounts(ingestID, ingestSources)

    # Test for existence/well form-edness of sentinel records
    

    # Check full counts against APIs of source providers
    tests.checkCounts([idb, pbdb], addFullCounts)


    endTime = time.time()
    ingestLogStatus = logHelpers.logRunTime(ingestID, startTime, endTime)
    if ingestLogStatus == False:
        logger.error("Failed to update mongo ingest log. CHECK FOR ERRORS!")
    logHelpers.emailLogAndStatus('FINAL STATUS', coreLogFile, testLogFile, ['michael@whirl-i-gig.com, mwbenowitz@gmail.com'])
    logger.info("Ingest Complete")

if __name__ == '__main__':
    main()
