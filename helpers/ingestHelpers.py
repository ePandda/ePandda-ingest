#
# Helpers for ePandda ingest process
#
import os
from importlib import import_module
import argparse

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
    return parser
