import os
import socket
import threading

from json import load

from src.logs.log_config import logger

class TrackerServer:
	def __init__(self, protocol, server, model=''):
		self.ip, self.port = server
		self.model = model
		self.sock = socket.socket()
		self.sock.bind((self.ip, int(self.port)))
		self.sock.listen(1024)

		if self.ip=='': self.ip = 'ANY'
		logger.info(f'Сервер для {protocol.NAME}{model} запущен - [{self.ip}:{self.port}]\n')
		
		listen_th = threading.Thread(target=self.connecter, args=(protocol, model))
		listen_th.start()


	def connecter(self, protocol, model):
		while True:
			conn, addr = self.sock.accept()
			conn.settimeout(60)
			new = protocol(conn, addr, model)
			threading.Thread(target=protocol.start, args=(new, )).start()