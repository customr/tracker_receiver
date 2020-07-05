import logging
import sys
from logging.handlers import RotatingFileHandler

PATH = "tracker_receiver/src/logs/history.log"
LEVEL = logging.DEBUG
frt = logging.Formatter('%(levelname)s :: %(message)s %(asctime)s\n'+'-'*15)

logger = logging.getLogger()
logger.setLevel(LEVEL)

handler = RotatingFileHandler(PATH, mode='a', maxBytes=10*1024*1024)
handler.setFormatter(frt)
logger.addHandler(handler)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(frt)
logger.addHandler(handler)


logger.info('RECEIVER START\n\n\n\n')