import os
import struct
import binascii
import socket
import threading
import datetime

from json import load

from src.utils import *
from src.logs.log_config import logger


class TeltonikaServer:
	def __init__(self):
		p = os.path.join('src/', 'servers.json')
		with open(p, 'r') as s:
			protocol = load(s)

		self.ip, self.port = protocol['teltonika'].split(':')
		self.sock = socket.socket()
		self.sock.bind((self.ip, int(self.port)))
		self.sock.listen(1024)

		logger.info(f'Сервер для Teltonika запущен - [{self.ip}:{self.port}]\n')
		
		listen_th = threading.Thread(target=self.connecter)
		listen_th.start()


	def connecter(self):
		while True:
			conn, addr = self.sock.accept()
			Teltonika(conn, addr)


class Teltonika:

	BASE_PATH = 'src/protocols/Teltonika/'

	def __init__(self, sock, addr):
		self.sock = sock
		self.addr = addr

		self.imei = self.handle_imei()
		logger.info(f'Teltonika {self.imei} connected [{addr}]\n')

		self.decoder = self.get_decoder()
		packet, self.data = self.handle_data()

		packet, NumOfData_2 = extract_ubyte(packet)
		packet, crc16 = extract_uint(packet)

		if len(packet)!=0:
			logger.error(f'[Teltonika] дешифрованы не все данные:\n{packet}\n')

		assign = load(open(self.BASE_PATH+f'configurations/{self.imei}.json', 'r'))
		
		for n, rec in enumerate(self.data):
			iodata = {}
			for name, x in assign.items():
				value = 0
				if name in rec['iodata'].keys():
					value = rec['iodata'][name]

				iodata.update({x: value})

			logger.info(f"[Teltonika] Record #{n+1} AVL IO Data преобразована:\n{iodata}\n")
			rec.update({"iodata": iodata, "imei": int(self.imei)})

		logger.debug(f'[Teltonika] result:\n{self.data}\n')
		logger.info(f'Teltonika {self.imei} received {len(self.data)} records')


	def get_decoder(self):
		listdir = os.listdir(self.BASE_PATH+'configurations')

		with open(self.BASE_PATH+'configurations/trackers.json', 'r') as fd:
			trackers = load(fd)
			model = None

			if self.imei in trackers.keys():
				model = trackers[self.imei]


		if model:
			decoder = load(open(self.BASE_PATH+f'avl_ids/{model}.json', 'r'))
			logger.debug(f"[Teltonika] для {self.imei} выбрана конфигурация {model}\n")
			return decoder

		else:
			logger.critical(f"Teltonika для imei {self.imei} AVL IDs не найдены\n")
			raise ValueError('Unknown tracker')


	def handle_imei(self):
		#packet = binascii.hexlify(self.sock.recv(34))
		packet = b'000F333536333037303432343431303133'
		length = int(packet[:4], 16)
		packet = unpack_from_bytes(f'{length}s', packet[4:])[0]
		#self.sock.send(struct.pack('!B', 1))

		return packet.decode('ascii')


	def handle_data(self):
		all_data = []
		#packet = binascii.hexlify(self.sock.recv(4096))
		packet = b'000000000000005F10020000016BDBC7833000000000000000000000000000000000000B05040200010000030002000B00270042563A00000000016BDBC7871800000000000000000000000000000000000B05040200010000030002000B00260042563A00000200005FB3'
		logger.debug(f'[Teltonika] получен пакет с данными:\n{packet}\n')
		packet, _ = extract_int(packet) #preamble zero bytes
		packet, data_len = extract_uint(packet)
		packet, self.codec = extract_ubyte(packet)
		packet, count = extract_ubyte(packet)
		
		codec_func = None

		#codec 8
		if self.codec==8:
			codec_func = self.codec_8

		#codec 8 extended
		elif self.codec==142:
			codec_func = self.codec_8

		#codec 16
		elif self.codec==16:
			codec_func = self.codec_16

		else:
			logger.critical(f"Teltonika неизвестный кодек {self.codec}")
			raise ValueError('Unknown codec')

		for rec in range(count):
			data = {'ts': datetime.datetime.now().strftime(DATETIME_FORMAT)}
			packet, codecdata = codec_func(packet)
			data.update(codecdata)
			all_data.append(data)
			logger.debug(f"[Teltonika] #{len(all_data)}:\n{data}\n")

		#self.sock.send(struct.pack("!I", len(all_data)))
		#self.sock.close()

		return packet, all_data


	def codec_8(self, packet):
		logger.debug(f'[Teltonika] CODEC {self.codec} AVL Data packet:\n{packet}\n')
		packet, timestamp = extract(packet, 8)
		timestamp = int(b'0x'+timestamp, 16)
		timestamp /= 1000
		packet, _ = extract_ubyte(packet) #priority
		packet, lon = extract_int(packet)
		lon /= 10000000
		packet, lat = extract_int(packet)
		lat /= 10000000
		packet, alt = extract_ushort(packet)
		packet, dr = extract_ushort(packet)
		packet, sat_num = extract_ubyte(packet)
		packet, speed = extract_ushort(packet)

		dt = datetime.datetime.utcfromtimestamp(timestamp)
		dt = dt.strftime(DATETIME_FORMAT)
		data = {
			"datetime": dt,
			"lon": lon,
			"lat": lat,
			"alt": alt,
			"direction": dr,
			"sat_num": sat_num,
			"speed": speed
		}

		logger.debug(f'[Teltonika] AVL Data обработана:\n{data}\n')

		if self.codec==8:
			packet, EventIO = extract_ubyte(packet)
			packet, NumOfIO = extract_ubyte(packet)

		elif self.codec==142:
			packet, EventIO = extract_ushort(packet)
			packet, NumOfIO = extract_ushort(packet)

		elif self.codec==16:
			packet, EventIO = extract_ushort(packet)
			packet, Generation_type = extract_ubyte(packet)
			packet, NumOfIO = extract_ubyte(packet)
		else:
			logger.critical(f"Teltonika неизвестный кодек {self.codec}\n")
			raise ValueError('Unknown codec')

		packet, iodata = self.handle_io(packet)
		data.update({"iodata": iodata})

		logger.debug(f'[Teltonika] AVL IO Data обработана:\n{iodata}\n')

		return packet, data


	def codec_16(self, packet):
		return self.codec_8(packet)


	def handle_io(self, packet):
		data = {}

		for extract_func in [extract_byte, extract_short, extract_int, extract_longlong]:
			if self.codec==8 or self.codec==16:
				packet, count = extract_ubyte(packet)
			elif self.codec==142:
				packet, count = extract_ushort(packet)
			else:
				logger.critical(f"Teltonika неизвестный кодек {self.codec}\n")
				raise ValueError('Unknown codec')

			iodata = {}
			for _ in range(count):
				if self.codec==8:
					packet, io_id = extract_ubyte(packet)

				elif self.codec==142 or self.codec==16:
					packet, io_id = extract_ushort(packet)
					
				else:
					logger.critical(f"Teltonika неизвестный кодек {self.codec}\n")
					raise ValueError('Unknown codec')

				packet, io_val = extract_func(packet)

				if str(io_id) not in self.decoder.keys():
					logger.error(f'[Teltonika] Неизвестный AVL IO ID {io_id}\n')

				else:
					iodata.update({self.decoder[str(io_id)]: io_val})

			data.update(iodata)
		

		if self.codec==142:
			packet, count = extract_ushort(packet)

			iodata = {}
			for _ in range(count):
				packet, io_id = extract_ushort(packet)
				packet, length = extract_ushort(packet)
				packet, io_val = extract_x(packet, 'q', length)

				if str(io_id) not in self.decoder.keys():
					logger.error(f'[Teltonika] Неизвестный AVL IO ID {io_id}\n')

				else:
					iodata.update({self.decoder[str(io_id)]: io_val})

			data.update(iodata)

		return packet, data


	def codec_12(self, command):
		pass

	def codec_13(self, command):
		pass


	def codec_14(self, command):
		pass