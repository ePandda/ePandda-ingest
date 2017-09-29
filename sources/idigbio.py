#
# Class for iDigBio ingest
#

# Data parsing
from bs4 import BeautifulSoup
import json
import pandas as pd

# Data harvesting/gathering
from unidecode import unidecode
import urllib2
import requests
import zipfile

# System tools
import traceback
import os
import shutil
import logging

# local stuff
import mongoConnect

class idigbio:
    def __init__(self):
        self.source = "idigbio"
        self.ingestURL = "http://s.idigbio.org/idigbio-static-downloads?max-keys=10000000"
        self.collectionRoot = "http://s.idigbio.org/idigbio-static-downloads/"
        self.logger = logging.getLogger("ingest.idigbio")

    # This is the main component of the ingester, and relies on a few different
    # helpers. But most of this code is specific to iDigBio
    def runIngest(self, dry=False, test=False):
        # Should this be a dry or test run?
        dryRun = dry
        testRun = test

        # Get and parse iDigBios XML digest of all of their component collections
        endpoints = urllib2.urlopen(self.ingestURL).read()
    	endpointXML = BeautifulSoup(endpoints, 'lxml')

        try:
            truncated = endpointXML.find('istruncated') # We don't want any truncated data!
            if truncated == True:
                self.logger.error("iDigBio truncated dataset exception")
                raise
        except:
            print "Please adjust the URL in the idigbio.py __init__ function it is returning truncated results"
            sys.exit(1)

        # open a mongo connection
        mongoConn = mongoConnect.mongoConnect()

    	# Iterate through everything we get back
    	for collection in endpointXML.find_all('contents'):
            collectionKey = collection.key.string
            collectionModified = collection.lastmodified.string
            collectionSize = collection.size.string
            # Skip these collections
            if '.eml' in collectionKey or 'idigbio' in collectionKey or '.png' in collectionKey:
                self.logger.info("This idigbio collection cannot be imported: " + collectionKey)
                continue

            # Check if this collection a) exists and b) was modified since last import
            # If not exists import it straight, if it does replace matching docs in mongo
            # Returns 3 possible flags: new | modified | static
            # TODO only update specific fields? Or just overwrite in mongo?
            self.logger.info("Checking status of collection " + collectionKey)
            collectionStatus = mongoConn.checkIDBCollectionStatus(collectionKey, collectionModified, collectionSize)
            if collectionStatus == 'static':
                self.logger.debug("Skipping collection " + collectionKey + " no changes since last ingest")
                continue

    		# Download & unzip the zip file!
            collectionDir = self.downloadCollection(self.collectionRoot, collectionKey)
            if not collectionDir:
                continue

            # Check that we got a decent CSV/TXT file in that unzipped directory
            # This spot checks 'core' fields from each of the main indexes we create
            # If there they're it means that its a well formed record
            occurrenceFile = self.checkCollection(collectionDir)
            if not occurrenceFile:
                continue

            # TODO Image check and merge

            # If the collection has validated, then either import the full collection or
            # import the updated specimens
            if collectionStatus == 'new' and dryRun is False:
                self.logger.info("Doing full import of " + collectionKey)
                fullImportResult = mongoConn.iDBFullImport(occurrenceFile, collectionKey, collectionModified)
                if fullImportResult is False:
                    self.logger.error("Import of " + collectionKey + " Failed")
                    continue
                self.logger.info("Imported " + collectonKey)
            elif collectionStatus == 'modified' and dryRun is False:
                self.logger.info("Doing partial import of " + collectionKey)
                partialImportResult = mongoConn.iDBPartialImport(occurrenceFile, collectionKey, collectionModified)
                if partialImportResult is False:
                    self.logger.error("There were at least some errors during import of " + collectionKey)
                    print "Imported with at least some errors"
                    continue
                else:
                    self.logger.info("Updated records in " + collectionKey)

    		# Once we're done delete the ZIP and directory and move on to the next!
            self.logger.debug("Deleting " + collectionKey + " & " + collectionDir)
            os.remove(collectionKey)
            shutil.rmtree(collectionDir)

    def downloadCollection(self, collectionRoot, collectionKey):
        self.logger.debug("Downloading collection " + collectionKey)
        sourceColl = urllib2.urlopen(collectionRoot + collectionKey).read()
        try:
            with open(collectionKey, 'wb') as zip_file:
                zip_file.write(sourceColl)
            # Unzip the zip file!
            collectionDir = collectionKey[:-4]
            with zipfile.ZipFile(collectionKey, 'r') as unzip:
                unzip.extractall(collectionDir)
        except zipfile.BadZipfile:
            self.logger.error("This file cannot be unzipped! Manually check for validity: " + collectionKey)
            return None
        return collectionDir

    def checkCollection(self, collectionDir):
        self.logger.debug("Checking collection directory" + collectionDir)
        dirContents = os.listdir(collectionDir)
        validFile = False
        for collFile in dirContents:
            if collFile in ['occurrence.txt', 'occurrence.csv']:
                self.logger.info("Found valid " + collFile + " in " + collectionDir)
                occurrenceFile = collectionDir + '/' + collFile
                validFile = True
                break
        if validFile is False:
            self.logger.error("No occurrence file found. Check this collection for valid content: " + collectionDir)
            return None
        occurrenceHeader = pd.read_csv(occurrenceFile, sep=",", nrows=1)
        occurrenceHeadList = list(occurrenceHeader.columns.values)
        headerChecklist = ['idigbio:uuid', 'idigbio:institutionName', 'dwc:genus', 'dwc:specificEpithet', 'dwc:country', 'dwc:stateProvince', 'dwc:earliestAgeOrLowestStage', 'dwc:latestAgeOrHighestStage', 'dwc:formation']
        for check in headerChecklist:
            if check not in occurrenceHeadList:
                self.logger.error(occurrenceFile + "is not a valid CSV or TXT. Check source collection for validity")
                self.logger.debug("Header list for invalid file: " + str(occurrenceHeadList))
                os.remove(collectionKey)
                return None
        return occurrenceFile
