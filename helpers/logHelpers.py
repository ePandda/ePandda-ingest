#
# Class for logining status/errors from the ingest
#

# Core python modules
import logging
import time
import json

# Email modules
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.mime.text import MIMEText
from email import Encoders

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
    return logger, loggerFile

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

def emailLogAndStatus(status, logFile, testLogFile, recipients):
    config = json.load(open('./config.json'))
    logger = logging.getLogger('ingest.mail')
    smtpConfig = config['smtp']
    msg = MIMEMultipart()
    msg["Subject"] = "ePandda Ingest results from " + time.strftime("%Y/%m/%d")
    msg["From"] = "michael@whirl-i-gig.com"
    msg["To"] = ', '.join(recipients)

    msg.attach(MIMEText(status + "\nCheck the attached log files for a summary of the most recent run of the ePandda ingest service"))

    for log in [logFile, testLogFile]:
        part = MIMEBase('application', "octect-stream")
        part.set_payload(open(log, "rb").read())
        Encoders.encode_base64(part)

        part.add_header('Content-Disposition', 'attachment; filename="' + log + '"')
        msg.attach(part)
    try:
        s = smtplib.SMTP(smtpConfig['server'], smtpConfig['port'])
        s.login(smtpConfig['user'], smtpConfig['password'])
    except:
        logger.error("Failed to connect to the mail server! Check config options")

    try:
        s.sendmail("michael@whirl-i-gig.com", recipients, msg.as_string())
    except:
        logger.error("Failed to send email!! Check config/local mail folder")
