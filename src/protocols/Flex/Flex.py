import os
import struct
import binascii
import socket
import threading

from src.utils import *
from src.db_worker import *
from src.logs.log_config import logger


class Flex:

	BASE_PATH = 'tracker_receiver/src/protocols/Flex/'
	NAME = 'Flex'
	TRACKERS = set()

	def __init__(self, sock, addr, model):
		self.sock = sock
		self.addr = addr

		self.lock = threading.Lock()
		self.stop = False
	

	def start(self):
		pass
		