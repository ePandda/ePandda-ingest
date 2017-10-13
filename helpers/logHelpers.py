#
# Class for logining status/errors from the ingest
#

# Core python modules
import logging
import time

# local modules
import mongoConnect

def createLog(module, level, fileSuffix):
    logger = logging.getLogger(module)
    if level:
        checkLevel = level.lower()
    else:
        checkLevel = 'warning'
    levels = {'debug': logging.DEBUG, 'info': logging.INFO, 'warning': logging.WARNING, 'error': logging.ERROR, 'critical': logging.CRITICAL}
    today = time.strftime("%Y_%m_%d")
    loggerFile = './logs/' + today + fileSuffix + ".log"
    fileLog = logging.FileHandler(loggerFile)
    conLog = logging.StreamHandler()
    if checkLevel in levels:
        logger.setLevel(levels[checkLevel])
        fileLog.setLevel(levels[checkLevel])
        conLog.setLevel(levels[checkLevel])
    else:
        fileLog.setLevel(levels['warning'])
        conLog.setLevel(levels['warning'])
    formatter = logging.Formatter('%(asctime)s_%(name)s_%(levelname)s: %(message)s')
    fileLog.setFormatter(formatter)
    conLog.setFormatter(formatter)

    logger.addHandler(fileLog)
    logger.addHandler(conLog)
    return logger

def createMongoLog(sources):
    # open a mongo connection
    mongoConn = mongoConnect.mongoConnect()
    ingestLogID = mongoConn.createIngestLog(sources)
    mongoConn.closeConnection()
    return ingestLogID

def addFullCounts(ingestID, sources):
    # open a mongo connection
    mongoConn = mongoConnect.mongoConnect()
    countsResult = {}
    for source in sources:
        countStatus = mongoConn.addLogCount(ingestID, source)
        if countStatus is False:
            countsResult[source] = None
            continue
        countsResult[source] = countStatus
    return countsResult



def logRunTime(ingestID, startTime, endTime):
    logger = logging.getLogger('ingest.log')
    logger.debug("Start time " + str(startTime))
    logger.debug("Start time " + str(endTime))
    totalTime = endTime - startTime
    minutes, seconds = divmod(totalTime, 60)
    hours, minutes = divmod(minutes, 60)
    timeString = "%d:%02d:%02d" % (hours, minutes, seconds)
    # open a mongo connection
    mongoConn = mongoConnect.mongoConnect()
    ingestLogComplete = mongoConn.addRunTime(ingestID, timeString)
    mongoConn.closeConnection()
    return ingestLogComplete
