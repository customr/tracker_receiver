import logging
import sys
from logging.handlers import RotatingFileHandler

PATH = "tracker_receiver/src/logs/"
LEVEL = logging.DEBUG
frt = logging.Formatter('[%(asctime)s] %(levelname)s :: %(message)s\n'+'-'*15)

logger = logging.getLogger()
logger.setLevel(LEVEL)

handler = RotatingFileHandler(PATH+'history.log', mode='a', maxBytes=1024*1024*5)
handler.setLevel(logging.INFO)
handler.setFormatter(frt)
logger.addHandler(handler)

handler = RotatingFileHandler(PATH+'debug.log', mode='a', maxBytes=1024*1024*5)
handler.setLevel(logging.DEBUG)
handler.setFormatter(frt)
logger.addHandler(handler)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(frt)
logger.addHandler(handler)


logger.info('RECEIVER START\n\n\n\n')