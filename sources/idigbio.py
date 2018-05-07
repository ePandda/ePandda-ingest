#
# Class for iDigBio ingest
#

# Data parsing
from bs4 import BeautifulSoup
import json
import pandas as pd
import csv

# Data harvesting/gathering
from unidecode import unidecode
import urllib2
import requests
from requests.exceptions import ReadTimeout, ConnectionError
import zipfile
import zlib

# System tools
import sys
import traceback
import os
import shutil
import logging
from datetime import datetime, timedelta
import time
import re

# local modules
import multiConnect
from helpers import ingestHelpers
from helpers import testHelpers

class idigbio:
    def __init__(self, test, fullRefresh, ingestLog):
        self.config = json.load(open('./config.json'))
        self.source = "idigbio"
        self.fullRefresh = fullRefresh
        self.ingestURL = "http://s.idigbio.org/idigbio-static-downloads?max-keys=10000000"
        self.collectionRoot = "http://s.idigbio.org/idigbio-static-downloads/"
        self.refreshInterval = self.config['idigbio_ingest_interval']
        if test:
            self.refreshInterval = 1
        self.refreshDate = datetime.today() - timedelta(days=int(self.refreshInterval))
        self.refreshFrom = self.refreshDate.strftime('%Y-%m-%d')
        #self.refreshURL = 'https://api.idigbio.org/v2/download/?rq={"datemodified":{"type":"range","gte":"' + self.refreshFrom + '"}}'
        self.refreshDownloadURL = "http://s.idigbio.org/idigbio-downloads/"
        self.recordCountURL = "https://search.idigbio.org/v2/summary/count/records/"
        self.deleteCheckRoot = "https://search.idigbio.org/v2/summary/stats/api?recordset="
        self.apiDownloadRoot = "https://api.idigbio.org/v2/download/?rq="
        self.logger = logging.getLogger("ingest.idigbio")
        self.testLogger = logging.getLogger("test.idigbio")
        self.ingestLog = ingestLog
        self.tests = testHelpers.epanddaTests(None, None)

    # This is the main component of the ingester, and relies on a few different
    # helpers. But most of this code is specific to iDigBio
    def runIngest(self, dry=False, test=False):
        # Should this be a dry or test run?
        dryRun = dry
        testRun = test
        # Check the type of import that should be run
        if self.fullRefresh:
            ingestResult = self.runFullIngest()
        else:
            ingestResult = self.runPartialIngest(self.refreshInterval, self.refreshFrom)

        # create Sentinel records for new records
        sentinelStatus = self.tests.createSentinels(['idigbio'])
        if sentinelStatus is False:
            self.logger.error("Sentinal Creation Failure for IDB")
            return False
        return True

    def runPartialIngest(self, refreshInterval, refreshFrom):
        self.logger.info("Starting ingest of idigbio records modified in past " + str(refreshInterval) + " days")

        # open a mongo connection
        multiConn = multiConnect.multiConnect()
        collectionName = 'iDigBio_ingest_' + str(refreshFrom)
        # Check if the collection has already been updated for this date
        #refreshStatus = multiConn.checkIDBCollectionStatus(collectionName, refreshFrom)
        #if refreshStatus == 'static':
        #    self.logger.info("An ingest has already been run from this date")
        #    return False

        # Query the iDigBio API for modified records
        startDate = self.refreshDate
        endDate = datetime.today()
        if startDate < (datetime.today() - timedelta(days=7)):
        	endDate = startDate + timedelta(days=7)
        while(startDate < datetime.today()):
            dateChunkURL = 'https://api.idigbio.org/v2/download/?rq={"datemodified":{"type":"range","gte":"' + startDate.strftime('%Y-%m-%d') + '", "lte":"'+endDate.strftime('%Y-%m-%d')+'"}}'
            occurrenceFile, mediaFile, collectionKey = self.idbAPIDownload(dateChunkURL)
            if not occurrenceFile:
                return False

			# Get the count of records being imported and store it in the ingest log
            recordCount = ingestHelpers.csvCountRows(occurrenceFile)
            mediaCount = 0
            if mediaFile:
            	mediaCount = ingestHelpers.csvCountRows(mediaFile)
            recordCountResult = multiConn.addToIngestCount(self.ingestLog, self.source, recordCount, mediaCount)
            if recordCountResult is False:
                self.logger.error("Could not log record count. Check validity carefully!")

			# Download and ingest the created iDigBio file
            ingestResult = multiConn.iDBPartialImport(occurrenceFile, mediaFile, collectionName, self.refreshFrom, 'csv')
            if ingestResult is False:
                self.logger.error("There were at least some errors during import of " + collectionKey)
                print "Imported with at least some errors"
            else:
                self.logger.info("Updated records in " + collectionKey)
            
            self.logger.debug("Deleting " + collectionKey)
            os.remove(collectionKey)
            shutil.rmtree(collectionKey[:-4])
            startDate = endDate + timedelta(days=1)
            endDate = endDate + timedelta(days=8)
            if(endDate > datetime.today()):
                endDate = datetime.today()
			
        return True

    def runFullIngest(self):
        self.logger.info("Starting complete iDigBio Ingest")
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
        multiConn = multiConnect.multiConnect()

        mostRecentDumpDate = datetime.strptime("01/01/1900", "%m/%d/%Y")
        mostRecentKey = mostRecentModified = mostRecentSize = ''
    	# Iterate through everything we get back for the most recent full dump
    	for collection in endpointXML.find_all('contents'):
            collectionKey = collection.key.string
            collectionModified = collection.lastmodified.string
            collectionSize = collection.size.string
            # Skip these collections
            if 'idigbio' not in collectionKey:
                self.logger.info("Skipping this partial collection: " + collectionKey)
                continue

            # Store collection key of the most recent full dump
            self.logger.info("Checking status of collection " + collectionKey)
            dateGroup = re.search(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', collectionKey)
            if dateGroup:
            	dumpDate = datetime.strptime(dateGroup.group(), "%Y-%m-%d")
            	if dumpDate > mostRecentDumpDate:
                	mostRecentDumpDate = dumpDate
                	mostRecentKey = collectionKey
                	mostRecentModified = collection.lastmodified.string
                	mostRecentSize = collection.size.string

        collectionKey = mostRecentKey
        collectionModified = mostRecentModified
        collectionSize = mostRecentSize

        # Download & unzip the zip file!
        collectionDir = self.downloadCollection(self.collectionRoot, collectionKey)
        if not collectionDir:
            return False

        # Check that we got a decent CSV/TXT file in that unzipped directory
        # This spot checks 'core' fields from each of the main indexes we create
        # If there they're it means that its a well formed record
        occurrenceFile, mediaFile = self.checkCollection(collectionDir)
        if not occurrenceFile:
            return False

        # Get the count of records being imported and store it in the ingest log
        recordCount = ingestHelpers.csvCountRows(occurrenceFile)
        mediaCount = 0
        if mediaFile:
        	mediaCount = ingestHelpers.csvCountRows(mediaFile)
        recordCountResult = multiConn.addToIngestCount(self.ingestLog, self.source, recordCount, mediaCount)
        if recordCountResult is False:
            self.logger.error("Could not log record count. Check validity carefully!")

        # TODO Image check and merge

        # If the collection has validated, then either import the full collection or
        # import the updated specimens
        self.logger.info("Doing full import of " + collectionKey)
        fullImportResult = multiConn.iDBFullImport(occurrenceFile, mediaFile, collectionKey, collectionModified)
        if fullImportResult is False:
            self.logger.error("Import of " + collectionKey + " Failed")
            self.logger.info("Imported " + collectionKey)

            self.logger.debug("Deleting " + collectionKey + " & " + collectionDir)
            os.remove(collectionKey)
            shutil.rmtree(collectionDir)

            # Run partial import for updates since most recent dump
            today = datetime.today()
            dayCount = (today - mostRecentDumpDate).days
            partialResult = self.runPartialIngest(dayCount, mostRecentDumpDate)

        return True

    def idbAPIDownload(self, requestURL):
        # Generate the request to iDigBio for records changed in the specified range
        modifiedStatusURL = self.generateIDBRecordRequest(requestURL)
        if modifiedStatusURL is False:
            self.logger.error("Could not generate iDigBio update request")
            return False

        # Wait for the download file to be generated and get the download link
        idbDownloadURL = self.getIDBDownloadURL(modifiedStatusURL)
        if idbDownloadURL is False:
            self.logger.error("Could not get iDigBio downloadURL")
            return False

        # Get the collectionKey for the downloaded collection
        collectionMatch = re.search('\/([a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}.zip)$', idbDownloadURL)
        if collectionMatch:
            collectionKey = collectionMatch.group(1)

        # Download & unzip the zip file!
        collectionDir = self.downloadCollection(self.refreshDownloadURL, collectionKey)
        if not collectionDir:
            return False

        # Check that we got a decent CSV/TXT file in that unzipped directory
        # This spot checks 'core' fields from each of the main indexes we create
        # If there they're it means that its a well formed record
        occurrenceFile, mediaFile = self.checkCollection(collectionDir)
        if not occurrenceFile:
            return False

        return occurrenceFile, mediaFile, collectionKey

    def generateIDBRecordRequest(self, requestURL):
        try:
            recordRequest = requests.get(requestURL, timeout=10)
        except (ConnectionError, ReadTimeout) as e:
            self.logger.error("Could not access iDigBio API")
            return False

        if recordRequest.status_code == 200:
            recordStatus = recordRequest.json()
            if "status_url" in recordStatus:
                return recordStatus["status_url"]
        return False

    def getIDBDownloadURL(self, statusURL):
        self.logger.info("Getting iDigBio download from: " + statusURL)
        while True:
            self.logger.debug("Checking status of iDigBio partial download")
            try:
                requestStatus = requests.get(statusURL, timeout=10)
            except (ConnectionError, ReadTimeout) as e:
                self.logger.error("Could not access iDigBio API. Retrying")
                i = 0
                while(i < 3):
                	try:
                		requestStatus = requests.get(statusURL, timeout=10)
                		break
            		except (ConnectionError, ReadTimeout) as e:
            			self.logger.error("Retry attempt " + str(i) + " Failed")
            		i += 1
            		time.sleep(15)
            	if i == 2:
            		self.logger.error("Could not access iDigBio API. All retry attempts failed")
                	return False

            if requestStatus.status_code == 200:
                downloadStatus = requestStatus.json()
                if "task_status" in downloadStatus:
                    if downloadStatus['task_status'] == "SUCCESS":
                        return downloadStatus['download_url']
                    elif downloadStatus['task_status'] == "FAILURE":
                        return False
            else:
                self.logger.error("iDigBio download link failed")
                return False
            time.sleep(30)

        return False



    def downloadCollection(self, collectionRoot, collectionKey):
        self.logger.debug("Downloading collection " + collectionKey)
        if os.path.isfile(collectionKey):
        	self.logger.info(collectionKey + " is already downloaded! Skipping step")
        	zf = open(collectionKey, 'rb')
        else:
		sourceColl = requests.get(collectionRoot + collectionKey, stream=True)
		zWrite = open(collectionKey, 'wb')
		for chunk in sourceColl.iter_content(chunk_size=1024):
			if chunk:
				zWrite.write(chunk)
		zWrite.close()
		zf = open(collectionKey, 'rb')
	self.logger.debug("Download Complete. Unziping source file " + collectionKey)	
	# Unzip the zip file!
	collectionDir = collectionKey[:-4]
	zFile = zipfile.ZipFile(zf)
	zFile.extractall(collectionDir)	
	
	zFile.close()
	zf.close()

        return collectionDir

    def checkCollection(self, collectionDir):
        self.logger.debug("Checking collection directory" + collectionDir)
        dirContents = os.listdir(collectionDir)
        validFile = False
        mediaFile = None
        for collFile in dirContents:
            if collFile in ['occurrence.txt', 'occurrence.csv']:
            	if validFile == False:
                	self.logger.info("Found valid " + collFile + " in " + collectionDir)
                	occurrenceFile = collectionDir + '/' + collFile
                	validFile = True
            if collFile in ['multimedia.txt', 'multimedia.csv']:
            	if mediaFile == None:
            		self.logger.info("Found valid multimedia " + collFile + " in " + collectionDir)
            		mediaFile = collectionDir + '/' + collFile
        if validFile is False:
            self.logger.error("No occurrence file found. Check this collection for valid content: " + collectionDir)
            return None
        if mediaFile is None:
        	self.logger.error("No multimedia file found. No immediate issue but verify that no media exists")
        	
        occurrenceHeader = pd.read_csv(occurrenceFile, sep=",", nrows=1)
        occurrenceHeadList = list(occurrenceHeader.columns.values)
        headerChecklist = ['idigbio:uuid', 'idigbio:institutionName', 'dwc:genus', 'dwc:specificEpithet', 'dwc:country', 'dwc:stateProvince', 'dwc:earliestAgeOrLowestStage', 'dwc:latestAgeOrHighestStage', 'dwc:formation']
        for check in headerChecklist:
            if check not in occurrenceHeadList:
                self.logger.error(occurrenceFile + " is not a valid CSV or TXT. Check source collection for validity")
                self.logger.debug("Header list for invalid file: " + str(occurrenceHeadList))
                return None
        duplicateHeaders = ingestHelpers.csvDuplicateHeaderCheck(occurrenceFile)
        if duplicateHeaders:
            ingestHelpers.csvRenameDuplicateHeaders(occurrenceFile, duplicateHeaders)
        if mediaFile:
        	mediaHeader = pd.read_csv(mediaFile, sep=",", nrows=1)
        	mediaHeadList = list(mediaHeader.columns.values)
        	mediaChecklist = ['idigbio:uuid', 'ac:accessURI', 'idigbio:etag']
        	for mCheck in mediaChecklist:
        		if mCheck not in mediaHeadList:
        			self.logger.error(mediaFile + " is not a valid CSV or TXT multimedia file. Check Source for validity")
        			mediaFile = None
        
        return occurrenceFile, mediaFile

    def getRecordCount(self):
        self.logger.debug("Checking full PBDB record Count")
        resp = requests.get(self.recordCountURL)
        if resp.status_code == 200:
            recordCountBody = resp.json()
            if 'itemCount' in recordCountBody:
                recordCount = recordCountBody['itemCount']
                return recordCount
        return None

    def deleteCheck(self):
        self.logger.info("Checking for deleted records")

        # open a mongo connection
        multiConn = multiConnect.multiConnect()
        idbRecordSets = multiConn.getCollectionCounts('idigbio', 'idigbio:recordset')
        multiConn.closeConnection()
        if not idbRecordSets:
            self.logger.error("Could not load record sets. Check logs for error")
            return False
        for recordSet in idbRecordSets:
            setID = recordSet[0]
            setCount = recordSet[1]
            self.logger.debug("Checking counts of " + setID)
            apiResponse = requests.get(self.deleteCheckRoot+setID)
            mostRecentSnapshot = datetime(1, 1, 1)
            mostRecentSnapString = None
            if apiResponse.status_code == 200:
                setCountBody = apiResponse.json()
                if 'dates' in setCountBody:
                    for snapDate in setCountBody['dates']:
                        snapshotDate = datetime.strptime(snapDate, "%Y-%m-%d")
                        if snapshotDate > mostRecentSnapshot:
                            mostRecentSnapshot = snapshotDate
                            mostRecentSnapString = snapDate

                    self.logger.debug("Got most recent date " + mostRecentSnapString)
                    latestCount = setCountBody['dates'][mostRecentSnapString][setID]['records']

                    if latestCount == setCount:
                        self.logger.debug(setID + " matches source record count")
                    elif latestCount > setCount:
                        self.logger.warning(setID + " is missing records from source")
                    else:
                        self.logger.info(setID + " contains deleted records, deleting now")
                        deleteResult = self.deleteRecords(setID)
                else:
                    self.logger.error("Could not load recordset from idigbio: " + setID)
            else:
                self.logger.error("Requests error " + str(apiResponse.status_code) + "for: " + setID)

    def deleteRecords(self, setID):
        specimenUUIDs = set()
        downloadURL = self.apiDownloadRoot+'{"recordset": ' + setID + '}'
        # Download the relevant collection from the API
        occurrenceFile = self.idbAPIDownload(self.refreshURL)
        with open(occurrenceFile, 'rb') as csvFile:
            specimenReader = csv.reader(csvFile)
            for specimen in specimenReader:
                specimenUUIDs.add(specimen['idigbio:uuid'])

        # open a mongo connection
        multiConn = multiConnect.multiConnect()
        idbRecordSets = multiConn.checkAndDeleteRecords(setID, specimenUUIDs, 'idigbio:recordset', 'idigbio')


        multiConn.closeConnection()
