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

def main():

    # Get the parameters provided in the args
    parser = ingestHelpers.createParser()
    args = parser.parse_args()
    ingestSources = args.sources
    dryRun = args.dryRun
    testRun = args.test
    logLevel = args.logLevel
    fullRefresh = args.fullRefresh

    # Create the log
    logger = logHelpers.createLog('ingest', logLevel)
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

    for ingestSource in ingestSources:
        ingester = sourceNames[ingestSource]
        logger.info("Starting import for: " + ingestSource)
        outcome = ingester.runIngest(dry=dryRun, test=testRun)
        if outcome is False:
            logger.error("Import of " + ingestSource + " failed! Review full log")
        else:
            logger.info("Import of " + ingestSource + " successful!")

    logger.info("Ingest Complete")

if __name__ == '__main__':
    main()
