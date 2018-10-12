#
# Helpers for ePandda ingest process
#
import sys
import os
import re
import shutil
from importlib import import_module
import argparse
import logging
import json
import hashlib
import pandas as pd
from tempfile import NamedTemporaryFile
import csv

# Create the helper logger
logger = logging.getLogger('ingest.helpers')

def getSourceNames(sources):
    sourceNames = {}
    for source in sources:
        sourceNames[source.source] = source
    return sourceNames

def createParser():
    parser = argparse.ArgumentParser(description="Import data from a range of sources into ePandda")
    parser.add_argument('-s', '--sources', nargs='+', help='REQUIRED. A list of sources to import data from', required=True)
    parser.add_argument('-d', '--dryRun', action='store_true', help="Don't import only download & inspect collections")
    parser.add_argument('-t', '--test', action='store_true', help="Only import a subset of records for db testing")
    parser.add_argument('-l', '--logLevel', help="Set the level of message to be logged. Options: DEBUG|INFO|WARNING|ERROR")
    parser.add_argument('-F', '--fullRefresh', action='store_true', help="Set ingest to overwrite all ePandda records and download new records from providers")
    parser.add_argument('-D', '--removeDeleted', action='store_true', help="Set to run checks for deleted records from ingest sources and remove them from ePandda")
    return parser

def getMd5Hash(dict):
    # This calculates the hash of a python dict
    # We dump to json so we can sort the keys, ensuring that we get the same hash
    # for the same data
    md5 = hashlib.md5(json.dumps(dict, sort_keys=True)).hexdigest()
    return md5

# This detects if two records in Elastic are the same
# It does so by removing any fields that are globally unique and then
# comparing all of the data fields
# If the calculated hash is the same they are the same
def compareDocuments(source, sentinel):
    for doc in [source, sentinel]:
        for field in ['_id', '@version', '@timestamp', 'type', 'host']:
            if '_id' in doc:
                doc.pop('_id', None)
        for ref in ['coll_refs', 'occ_refs']:
            if ref in doc:
                for i in range(len(doc[ref])):
                    if '_id' in doc[ref][i]:
                        doc[ref][i].pop('_id', None)
    sourceHash = getMd5Hash(source)
    sentinelHash = getMd5Hash(sentinel)
    if sourceHash != sentinelHash:
        return True

    return False

# This provides the iDigBio spreadsheet standardization methods
def idbCleanSpreadsheet(occurrenceFile):
    tempCSV = open("occurrence_0.csv", "wb")
    csv.field_size_limit(sys.maxsize)
    with open(occurrenceFile, 'rb') as csvFile:
        reader = csv.reader(csvFile)
        writer = csv.writer(tempCSV)
        fileCount = 0
        rowCount = 0
        header = True
        for row in reader:
            if header:
                # Get the column numbers for important columns that we'll be parsing
                geoCell = row.index('idigbio:geoPoint')
                flagCell = row.index('idigbio:flags')
                idigbioDate = row.index('idigbio:eventDate')
                modifiedDate = row.index('idigbio:dateModified')
                dwcDate = row.index('dwc:eventDate')
                # Add rows to store split dates
                row.append("dwc:eventDateEarly")
                row.append("dwc:eventDateLate")
                header = False
                writer.writerow(row)
                continue
            # Convert JSON georeference to plain lat,lng pair that can be
            # understood by Elastic's georef index
            if row[geoCell]:
                tmpGeo = json.loads(row[geoCell])
                geoStr = str(tmpGeo['lat']) + ',' + str(tmpGeo['lon'])
                row[geoCell] = geoStr
            # These two take datetime strings and extract only dates, which is what
            # we care about
            if row[idigbioDate]:
                onlyDateMatch = re.match("([0-9\-]+)T.*", row[idigbioDate])
                if onlyDateMatch:
                    onlyDate = onlyDateMatch.group(1)
                    row[idigbioDate] = onlyDate
            if row[modifiedDate]:
                modDateMatch = re.match("([0-9\-]+)T.*", row[idigbioDate])
                if modDateMatch:
                    modDate = modDateMatch.group(1)
                    row[modifiedDate] = modDate
            # Split the single field date into two separate fields
            if row[dwcDate]:
                if re.search("-[0-9]{1}", row[dwcDate]):
                    newDate = re.sub(r"-([0-9]{1})", r"-0\1", row[dwcDate])
                dwcArray = row[dwcDate].split('/')
                if len(dwcArray) > 1:
                    dwcEarly = dwcArray[0]
                    dwcLate = dwcArray[1]
                    row.append(dwcEarly)
                    row.append(dwcLate)
                else:
                    row.append(row[dwcDate])
                    row.append(row[dwcDate])
            if row[flagCell] is None:
            	row[flagCell] = []
            rowCount += 1
            print row
            writer.writerow(row)
            if rowCount % 5000000 == 0:
                tempCSV.close()
                fileCount += 1
                tempCSV = open("occurrence_" + str(fileCount) + ".csv", "wb")
                writer = csv.writer(tempCSV)

    tempCSV.close()

# This provides the PBDB data standardization methods
def pbdbCleanGeoPoints(occurrenceFile):
    tempCSV = open("tmp_pbdb.csv", "wb")
    csv.field_size_limit(sys.maxsize)
    with open(occurrenceFile, 'rb') as csvFile, tempCSV:
        reader = csv.reader(csvFile)
        writer = csv.writer(tempCSV)
        header = True
        for row in reader:
            if header:
            	rowLen = len(row)
                # Get column numbers for important roles
                latCell = row.index('lat')
                lngCell = row.index('lng')
                pLatCell = row.index('paleolat')
                pLngCell = row.index('paleolng')
                # For PBDB we will be merging some fields
                row.append('geoPoint')
                row.append('paleoGeoPoint')
                # Some field names include .0., which Logstash can't handle
                # Replace them with a simple dash, which is what users expect
                # anyway
                for cell in row:

                	if '.0.' in cell:
                		newCell = cell.replace('.0.', '-')
                		cellIndex = row.index(cell)
                		row[cellIndex] = newCell
                print(row)
                header = False
            else:
                # combine lat, lng into a single value
            	if row[latCell] and row[lngCell]:
            	    row.append(str(row[latCell]) + ',' + str(row[lngCell]))
            	else:
            		row.append('')
                # combine paleo lat, lng
            	if row[pLatCell] and row[pLngCell]:
                	row.append(str(row[pLatCell]) + ',' + str(row[pLngCell]))
                else:
            		row.append('')
            writer.writerow(row)
    shutil.move("tmp_pbdb.csv", occurrenceFile)
    tempCSV.close()

# This deduplicate header names
# It will just return any duplicates that are found
def csvDuplicateHeaderCheck(csvFile):
    occurrenceHeader = pd.read_csv(csvFile, sep=",", nrows=1)
    occurrenceHeadList = list(occurrenceHeader.columns.values)
    logger.debug(occurrenceHeadList)
    duplicateHeaders = []
    for header in occurrenceHeadList:
        if '.' in header:
            logger.debug("Found invalid header: " + header)
            duplicateHeaders.append(header[:-2])
        if occurrenceHeadList.count(header) > 1:
            logger.debug("Found duplicate header: " + header)
            if header in duplicateHeaders:
                continue
            duplicateHeaders.append(header)
    return duplicateHeaders

# This will rename any duplicate headers that were found
def csvRenameDuplicateHeaders(csvFileName, duplicateHeaders):
    logger.info("Removing duplicate header values from " + csvFileName)
    tempfile = NamedTemporaryFile(delete=False)
    with open(csvFileName, 'rb') as csvFile, tempfile:
        reader = csv.reader(csvFile)
        writer = csv.writer(tempfile)
        rowCount = 0
        for row in reader:
            if rowCount == 0:
                for duplicate in duplicateHeaders:
                    dupCount = 0
                    for i in range(len(row)):
                        if row[i] == duplicate:
                            logger.debug("Replacing bad header: " + duplicate)
                            dupCount += 1
                            row[i] = duplicate + str(dupCount)
            writer.writerow(row)
            rowCount += 1
    shutil.move(tempfile.name, csvFileName)
    return True

def csvCountRows(csvFileName):
    with open(csvFileName, 'rb') as csvFile:
        rowCount = sum(1 for row in csvFile)
    rowCount -= 1 # Accounts for header row
    return rowCount
