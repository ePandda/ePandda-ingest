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
    # Source classes. Add new classes here
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
    tests = testHelpers.epanddaTests(idb, pbdb)

    # Check indexes and create if necessary
    indexStatus = tests.checkIndexes(fullRefresh, 'pre')
    if indexStatus is False:
        logger.error("Index Creation Failure")
        logHelpers.emailLogAndStatus('TEST ERROR', coreLogFile, testLogFile)
        sys.exit(3)

    # Check for sentinels and add if necessary
    sentinelStatus = tests.createSentinels(ingestSources)
    if sentinelStatus is False:
        logger.error("Sentinal Creation Failure")
        logHelpers.emailLogAndStatus('TEST ERROR', coreLogFile, testLogFile)
        sys.exit(4)

    #
    # MAIN BODY RUN THE INGESTS
    #
    for ingestSource in ingestSources:
        ingester = sourceNames[ingestSource]
        logger.info("Starting import for: " + ingestSource)
        outcome = ingester.runIngest(dry=dryRun, test=testRun)
        if outcome is False:
            logger.error("Import of " + ingestSource + " failed! Review full log")
            logHelpers.emailLogAndStatus('INGEST ERROR', logger.baseFilename, testLogger.baseFilename)
            sys.exit(5)
        else:
            logger.info("Import of " + ingestSource + " successful!")

    # If this was a full import, create indexes on collections 
    indexCreationResult = testHelpers.checkIndexes(fullRefresh, 'post')

    # Log the current number of records in ePandda
    addFullCounts = logHelpers.addFullCounts(ingestID, ingestSources)

    # Test for existence/well form-edness of sentinel records
    sentinelErrorStatus = tests.checkSentinels(ingestSources)
    if sentinelErrorStatus is True:
        logger.error("Sentinels Failed to Verify, check logs")
        logHelpers.emailLogAndStatus('SENTINEL ERROR', coreLogFile, testLogFile)
        sys.exit(6)

    # Check full counts against APIs of source providers
    tests.checkCounts(ingestSources, addFullCounts)


    endTime = time.time()
    ingestLogStatus = logHelpers.logRunTime(ingestID, startTime, endTime)
    if ingestLogStatus == False:
        logger.error("Failed to update mongo ingest log. CHECK FOR ERRORS!")
    logHelpers.emailLogAndStatus('SUCCESS', coreLogFile, testLogFile)
    logger.info("Ingest Complete")

if __name__ == '__main__':
    main()
