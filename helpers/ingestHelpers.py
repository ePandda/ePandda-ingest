#
# Helpers for ePandda ingest process
#
import os
from importlib import import_module
import argparse
import json
import hashlib

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
    return parser

def getMd5Hash(dict):
    # This calculates the hash of a python dict
    # We dump to json so we can sort the keys, ensuring that we get the same hash
    # for the same data
    md5 = hashlib.md5(json.dumps(dict, sort_keys=True)).hexdigest()
    return md5
