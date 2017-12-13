#
# Helpers for ePandda ingest process
#
import os
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

def compareDocuments(source, sentinel):
    for doc in [source, sentinel]:
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

def idbCleanGeoPoints(occurrenceFile):
    tempCSV = NamedTemporaryFile(delete=False)
    with open(occurrenceFile, 'rb') as csvFile, tempCSV:
        reader = csv.reader(csvFile)
        writer = csv.writer(tempCSV)
        header = True
        for row in reader:
            if header:
                geoCell = row.index('idigbio:geoPoint')
                header = False
                continue
            if row[geoCell]:
                row[geoCell] = json.loads(row[geoCell])
            writer.writerow(row)
    shutil.move(tempCSV.name, occurrenceFile)

def pbdbCleanGeoPoints(occurrenceFile):
    tempJSON = NamedTemporaryFile(delete=False)
    with open(occurrenceFile, 'rb') as jsonFile:
        reader = json.load(jsonFile)
        for row in reader:
            row.pop("_id")
            if row['lat'] and row['lng']:
                row['geoPoint'] = [row['lat'], row['lng']]
            if row['paleolat'] and row['paleolng']:
                row['paleoGeoPoint'] = [row['paleolat'], row['paleolng']]
        json.dump(reader, tempJSON)
    shutil.move(tempJSON.name, occurrenceFile)

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
