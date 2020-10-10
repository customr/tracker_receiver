import os
import socket
import threading

from json import load

from src.logs.log_config import logger

class TrackerServer:
	def __init__(self, protocol, server, model=''):
		self.ip, self.port = server.split(':')
		self.model = model
		self.sock = socket.socket()
		self.sock.bind((self.ip, int(self.port)))
		self.sock.listen(1024)

		if self.ip=='': self.ip = 'ANY'
		logger.info(f'Сервер для {protocol.NAME}{model} запущен - [{self.ip}:{self.port}]\n')
		
		listen_th = threading.Thread(target=self.connecter, args=(protocol, model))
		listen_th.start()


	def connecter(self, protocol, model):
		trackers = dict()
		while True:
			conn, addr = self.sock.accept()
			conn.settimeout(60)
			if addr[0] in trackers.keys():
				if trackers[addr[0]] in protocol.TRACKERS:
					trackers[addr[0]].join()
					del(trackers[addr[0]])

				if model:
					new = protocol(conn, addr, model)
				else:
					new = protocol(conn, addr)
				
				th = threading.Thread(target=protocol.start, args=(new, ))
				th.start()
				trackers.update({addr[0]: th})

			else:
				if model:
					new = protocol(conn, addr, model)
				else:
					new = protocol(conn, addr)

				th = threading.Thread(target=protocol.start, args=(new, ))
				th.start()
				trackers.update({addr[0]: th})