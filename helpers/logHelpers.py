#
# Class for logining status/errors from the ingest
#

# Uses the main pythn logging module

import logging
import time

def createLog(module, level):
    logger = logging.getLogger(module)
    if level:
        checkLevel = level.lower()
    else:
        checkLevel = 'warning'
    levels = {'debug': logging.DEBUG, 'info': logging.INFO, 'warning': logging.WARNING, 'error': logging.ERROR, 'critical': logging.CRITICAL}
    today = time.strftime("%Y_%m_%d")
    loggerFile = './logs/'+today+"_ingest.log"
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
