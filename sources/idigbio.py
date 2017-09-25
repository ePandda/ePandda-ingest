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
import zipfile

# System tools
import traceback
import os
import shutil

# local stuff
import mongoConnect

class idigbio:
    def __init__(self):
        self.source = "idigbio"
        self.ingestURL = "http://s.idigbio.org/idigbio-static-downloads?max-keys=10000000"
        self.collectionRoot = "http://s.idigbio.org/idigbio-static-downloads/"

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
                raise
        except:
            print "Please adjust the URL in the idigbio.py __init__ function it is returning truncated results"
            sys.exit(1)

        # open a mongo connection
        mongoConnect = mongoConnect()

    	# Iterate through everything we get back
    	for collection in endpointXML.find_all('contents'):
            collectionKey = collection.key.string
            collectionModified = collection.lastmodified.string
            colleztionSize = collection.size.string
            # Skip these collections
            if '.eml' in collectionKey or 'idigbio' in collectionKey:
                continue

            # Check if this collection a) exists and b) was modified since last import
            # If not exists import it straight, if it does replace matching docs in mongo
            # Returns 3 possible flags: new | modified | static
            # TODO only update specific fields? Or just overwrite in mongo?
            collectionStatus = checkIDBCollectionStatus(collectionKey, collectionModified, collectionSize)

            if collectionStatus == 'static':
                continue

    		# Download & unzip the zip file!
    		collection = urllib2.urlopen(self.collectionRoot + collectionKey).read()
            collectionDir = unzipCollection(collectionKey, collection)
            if not collectionDir:
                continue

            # Check that we got a decent CSV/TXT file in that unzipped directory
            # This spot checks 'core' fields from each of the main indexes we create
            # If there they're it means that its a well formed record
            occurrenceFile = checkCollection(collectionDir)
            if not occurrenceFile:
                continue

            # TODO Image check and merge

            # If the collection has validated, then either import the full collection or
            # import the updated specimens
            if collectionStatus == 'new' and dryRun is False:
                fullImportResult = mongoConnect.iDBFullImport(occurrenceFile)
                if not fullImportResult:
                    print "FAILED IMPORT"
                    continue
            elif collectionStatus == 'modified' and dryRun is False:
                partialImportResult = mongoConnect.iDBPartialImport(occurrenceFile)

    		# Once we're done delete the ZIP and directory and move on to the next!
    		os.remove(collection_key)
    		shutil.rmtree(collection_dir)

    def unzipCollection(collectionKey, collection):
        try:
            with open(collectionKey, 'wb') as zip_file:
                zip_file.write(collection)
            # Unzip the zip file!
            collectionDir = collectionKey[:-4]
            with zipfile.ZipFile(collectionKey, 'r') as unzip:
                unzip.extractall(collectionDir)
        except zipfile.BadZipfile:
            # TODO Make this output to a log file
            print "THIS IS A BAD ZIP FILE. LEAVING IT HERE FOR FURTHER INSPECTION"
            print collectionKey
            return None

        return collectionDir

    def checkCollection(collectionDir):
        dirContents = os.listdir(collectionDir)
        for collFile in dirContents:
            if collFile in ['occurrence.txt', 'occurrence.csv']:
                occurrenceFile = collectionDir + '/' + collFile
                break
            # TODO Make this output to a log file
            print("THERE IS NO OCCURRENCE FILE! WE'RE LEAVING IT HERE FOR FUTURE INSPECTION")
            return None
        occurrenceHeader = pd.read_csv(occurrenceFile, sep=",", nrows=1)
        occurrenceHeadList = list(occurrenceHeader.columns.values)
        headerChecklist = ['idigbio:uuid', 'idigbio:institutionName', 'dwc:genus', 'dwc:specificEpithet', 'dwc:country', 'dwc:stateProvince', 'dwc:earliestAgeOrLowestStage', 'dwc:latestAgeOrHighestStage', 'dwc:formation']
        for check in headerChecklist:
            if check not in occurrenceHeadList:
                print occurrenceHeadList
                print "THIS IS A BAD FILE. WE'RE LEAVING IT HERE FOR FUTURE INSPECTION"
                os.remove(collectionKey)
                return None
        return occurrenceFile
