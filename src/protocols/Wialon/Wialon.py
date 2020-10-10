import os
import struct
import binascii
import socket
import threading

from datetime import datetime
from time import time, sleep
from json import load, loads, dumps

from src.utils import *
from src.db_worker import *
from src.db_connect import CONN
from src.logs.log_config import logger


class Wialon:

	BASE_PATH = 'tracker_receiver/src/protocols/Wialon/'
	NAME = 'Wialon'
	TRACKERS = set()

	BLOCK_TYPES = {
		1: 's',
		2: 'b',
		3: 'i',
		4: 'd',
		5: 'l',
		6: 'img'
	}

	def __init__(self, sock, addr):
		self.sock = sock
		self.addr = addr
		self.stop = True
		self.success = False


	def start(self):
		logger.info(f'Wialon подключен [{self.addr[0]}:{self.addr[1]}]')
		Wialon.TRACKERS.add(self)
		self.db = pymysql.connect(**CONN)

		self.stop = False
		self.handle_packet()


	def handle_packet(self):
		if self.stop:
			self.stop = False

		self.assign = None
		while not self.stop:
			try:
				packet = binascii.hexlify(self.sock.recv(4096))
				self.success = True

			except Exception:
				self.sock.close()
				self.stop = True
				self.success = False
				Wialon.TRACKERS.remove(self)
				logger.debug(f'[Wialon] отключен [{self.addr[0]}:{self.addr[1]}]')
				break

			if len(packet)<8:
				if packet==b'\xff' or packet==b'' or packet==b'ff':
					continue

				else:
					logger.error(f'[Wialon] непонятный пакет: {packet}')
					self.sock.close()
					self.stop = True
					Wialon.TRACKERS.remove(self)
					logger.debug(f'[Wialon {self.imei} отключен [{self.addr[0]}:{self.addr[1]}]')
					break

			logger.debug(f'[Wialon] получен пакет:\n{packet}\n')
			packet_size = len(packet)
			packet, packet_header = self.handle_header(packet)

			if not self.assign:
				self.assign = get_configuration(self.db, self.NAME, packet_header['uid'])

			if packet_header['packet_size'] != (packet_size-7)//2:
				logger.error(f"[Wialon] размер принятого пакета не сходится {packet_header['packet_size']}!={(packet_size-6)/2}")
				self.success = False

			packet_data = self.handle_data(packet)
			self.data, insert = self.prepare_data(packet_header, packet_data)

			if insert:
				count = insert_geo(self.db, self.data, debug=True)

				if count==0:
					self.success = False
					logger.error(f"[Wialon] {packet_header['uid']} запись не удалось записать в бд")

			if self.success:
				self.sock.send(b'11')
				logger.info(f"Wialon {packet_header['uid']} принято 1/1 запись")
			else:
				self.sock.send(b'')
				logger.warning(f"Wialon {packet_header['uid']} пакет не был принят")


	def handle_header(self, packet):
		packet, packet_size = extract_uint(packet, '<')
		packet, uid = extract_str(packet, packet.index(b'00')//2, '>')
		packet, timestamp = extract_uint(packet[2:])
		packet, mask = extract_int(packet)
		data = dict(packet_size=packet_size, uid=uid, timestamp=timestamp, mask=mask)
		logger.debug(f'[Wialon] header data: {data}')
		return packet, data


	def handle_data(self, packet):
		packet_data = {}
		while len(packet)!=0 and self.success:
			packet, data = self.handle_block(packet)
			packet_data.update(data)

		logger.debug(f'[Wialon] main data: {packet_data}')
		return packet_data


	def handle_block(self, packet):
		packet, header = extract_ushort(packet)
		if header!=0x0BBB:
			self.success = False
			logger.error(f'[Wialon] Непонятный блок пакета {packet} [{self.addr[0]}:{self.addr[1]}]\n')

		packet, block_size = extract_uint(packet)
		packet, block_attribute = extract_ubyte(packet)
		packet, block_type = extract_ubyte(packet)

		block_type = Wialon.BLOCK_TYPES[block_type]
		name_length = packet.index(b'00')//2

		packet, block_name = extract_str(packet, name_length)
		block_name = block_name.decode('ascii')
		packet = packet[2:]

		if block_type=='s':
			data_length = packet.index(b'00')//2
			packet, block_data = extract_str(packet, data_length)
			block_data = block_data.decode('ascii')
			packet = packet[2:]

		elif block_type=='b':
			packet, block_data = self.handle_posinfo(packet)

		elif block_type=='i':
			packet, block_data = extract_int(packet)

		elif block_type=='d':
			packet, block_data = extract_double(packet, '<')

		elif block_type=='l':
			packet, block_data = extract_long(packet)
		
		elif block_type=='img':
			self.success = False
			return packet, {}

		else:
			logger.error(f"Wialon неизвестный тип блока {block_type}")
			self.success = False

		block_data = {block_name:block_data}
		logger.debug(f'[Wialon] Блок {block_name}={block_data}')
		return packet, block_data


	def handle_posinfo(self, packet):
		packet, lat = extract_double(packet, '<')
		packet, lon = extract_double(packet, '<')
		packet, height = extract_double(packet, '<')
		packet, speed = extract_ushort(packet)
		packet, direction = extract_ushort(packet)
		packet, sat_num = extract_ubyte(packet)
		data = dict(lon=lon,lat=lat,height=height,speed=speed,direction=direction,sat_num=sat_num)
		return packet, data


	def assign_data(self, data):
		new_data = {}
		for k,v in data.items():
			if self.assign.get(k, None):
				ikey = self.assign[k]
				if '*' in ikey:
					spl = ikey.split('*')
					ikey, k = spl[0], spl[1]
					v = round(v*float(k), 4)

				new_data.update({ikey:v})

			else:
				new_data.update({k:v})

		return new_data


	def prepare_data(self, header, data):
		data = self.assign_data(data)

		insert = 0
		if not data.get('ignition', None):
			data['ignition'] = 0

		if not data.get('sensor', None):
			data['sensor'] = 0

		reserve = {}
		for k,v in data.items():
			if k!='posinfo':
				reserve.update({k:v})

		reserve = str(reserve).replace("'", '"')
		reserve = reserve.replace(' ', '')[1:-1]

		if data.get('posinfo'):
			insert = 1
			data = {
				"imei": header['uid'],
				"lat": float('{:.6f}'.format(data['posinfo']['lat'])),
				"lon": float('{:.6f}'.format(data['posinfo']['lon'])),
				"datetime": datetime.utcfromtimestamp(header['timestamp']),
				"type": 0,
				"speed": data['posinfo']['speed'],
				"direction": data['posinfo']['direction'],
				"bat": 0,
				"fuel": 0,
				"ignition": data['ignition'],
				"sensor": data['sensor'],
				"reserve": reserve,
				"ts": datetime.utcfromtimestamp(int(time()))
			}

		logger.debug(f'[Wialon] final data {data}')
		return data, insert


