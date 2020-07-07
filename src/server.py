import os
import socket
import threading

from json import load

from src.logs.log_config import logger

class TrackerServer:
	def __init__(self, protocol):
		p = os.path.join('tracker_receiver/src/', 'servers.json')
		with open(p, 'r') as s:
			servers = load(s)

		self.ip, self.port = servers[protocol.NAME.lower()].split(':')
		self.sock = socket.socket()
		self.sock.bind((self.ip, int(self.port)))
		self.sock.listen(1024)

		if self.ip=='': self.ip = 'ANY'
		logger.info(f'Сервер для {protocol.NAME} запущен - [{self.ip}:{self.port}]\n')
		
		listen_th = threading.Thread(target=self.connecter, args=(protocol,))
		listen_th.start()


	def connecter(self, protocol):
		while True:
			conn, addr = self.sock.accept()
			protocol(conn, addr)
			threading.Thread(target=protocol.start).start()