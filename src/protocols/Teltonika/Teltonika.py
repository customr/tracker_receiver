import os
import struct
import binascii
import socket
import threading
import datetime

from time import time, sleep
from json import load, loads

from src.utils import *
from src.db_worker import *
from src.logs.log_config import logger
from src.protocols.Teltonika.crc import crc16

ws_ip = '127.0.0.1'
ws_port = 5678
ws_addr = f"ws://{ws_ip}:{ws_port}/"

class Teltonika:

	BASE_PATH = 'tracker_receiver/src/protocols/Teltonika/'
	NAME = 'Teltonika'
	TRACKERS = set()

	def __init__(self, sock, addr, model):
		self.sock = sock
		self.addr = addr
		self.model = model
		self.command_response = {}

	
	def start(self):
		self.imei = self.handle_imei()
		logger.info(f'Teltonika{self.model} {self.imei} подключен [{self.addr[0]}:{self.addr[1]}]')
		self.TRACKERS.add(self)

		self.assign = get_configuration(self.imei, self.model)
		self.decoder = self.get_decoder(self.model)
		self.ign_v = get_ignition_v(self.imei)
		
		self.lock = threading.Lock()
		self.stop = False
		main_th = threading.Thread(target=self.handle_packet)
		main_th.start()


	def get_decoder(self, model):
		if model:
			decoder = load(open(self.BASE_PATH+f'avl_ids/{model.lower()}.json', 'r'))
			logger.debug(f"[Teltonika] для {self.imei} выбрана модель {model.lower()}\n")
			return decoder

		else:
			logger.critical(f"Teltonika для imei {self.imei} модель не найдена\n")
			raise ValueError('Unknown tracker')


	def handle_imei(self):
		try:
			packet = binascii.hexlify(self.sock.recv(34))
			length = int(packet[:4], 16)
			packet = unpack_from_bytes(f'{length}s', packet[4:])[0]
			imei = packet.decode('ascii')
			logger.debug(f'[Teltonika] imei получен {imei}\n')
			self.sock.send(struct.pack('!B', 1))
			logger.debug(f'[Teltonika] ответ на сообщение с imei отправлен\n')

		except Exception as e:
			self.sock.close()
			self.TRACKERS.remove(self)
			self.stop = True
			raise e

		return imei


	def handle_packet(self):
		while not self.stop:
			try:
				packet = binascii.hexlify(self.sock.recv(4096))
			except Exception:
				self.sock.close()
				self.stop = True
				self.TRACKERS.remove(self)
				break

			self.lock.acquire()

			if len(packet)<8:
				if packet==b'\xff' or packet==b'' or packet==b'ff':
					continue

				else:
					logger.error(f'[Teltonika] непонятный пакет: {packet}')
					self.sock.close()
					self.stop = True
					self.TRACKERS.remove(self)
					break

			logger.debug(f'[Teltonika] получен пакет:\n{packet}\n')
			try:
				packet, z = extract_int(packet) #preamble zero bytes
				assert z==0, 'Not teltonika packet'
				packet, data_len = extract_uint(packet)
				packet, self.codec = extract_ubyte(packet)
				packet, self.count = extract_ubyte(packet)
				logger.debug(f'[Teltonika] codec={self.codec} rec_count={self.count}\n')

			except Exception as e:
				with open('tracker_receiver/src/logs/errors.log', 'a') as fd:
					fd.write(f'Ошибка в распаковке {packet}\n{e}\n')
			

			if self.codec in (8, 142, 16):
				self.data = self.handle_data(packet)
				self.data = self.prepare_geo(self.data)
				count = insert_geo(self.data)
				logger.info(f'Teltonika{self.model} {self.imei} принято {count}/{len(self.data)} записей')
				self.sock.send(struct.pack("!I", count))

			elif self.codec in (12, 13, 14):
				result = self.handle_command(packet)
				resp = {"action":"response", "result": result}
				resp = json.dumps(resp)
				self.command_response = resp
				logger.debug(f'[Teltonika] ответ на команду принят\n{result}\n')

			else:
				logger.critical(f"Teltonika неизвестный кодек {self.codec}")
				raise ValueError('Unknown codec')

			self.lock.release()
			sleep(2)

		del(self)


	def send_command(self, codec, command):
		result = ''
		codec = int(codec)
		if codec==12:
			com_length = len(command)
			length = 8+com_length

		elif codec==14:
			com_length = 8+len(command)
			length = 8+com_length

		elif codec==13:
			result = 'Сервер не может отправлять команду по кодеку 13!'
			return result

		else:
			result = f'Неизвестный кодек - {codec}'
			return result

		packet = ''
		packet = add_int(packet, 0)
		packet = add_uint(packet, length)
		packet = add_ubyte(packet, codec)
		packet = add_ubyte(packet, 1)
		packet = add_ubyte(packet, 5)
		packet = add_uint(packet, com_length)

		if codec==14:
			packet = add_str(packet, self.imei.rjust(16, '0'))

		packet = add_str(packet, command)
		packet = add_ubyte(packet, 1)
		crc16_pack = struct.unpack(f'{len(packet[16:])//2}s', binascii.a2b_hex(packet[16:].encode('ascii')))[0]
		packet = add_uint(packet, crc16(crc16_pack))
		logger.debug(f'[Teltonika] командный пакет сформирован:\n{packet}\n')
		packet = pack(packet)
		self.sock.send(packet)
		logger.debug(f'[Teltonika] команда отправлена\n')


	def handle_data(self, packet):
		all_data = []
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

		for rec in range(self.count):
			data = {
				'imei': self.imei,
				'ts': datetime.datetime.utcfromtimestamp(int(time()))
			}

			packet, codecdata = codec_func(packet)
			data.update(codecdata)

			if 'voltage' in data['iodata'].keys():
				if self.ign_v is not None:
					if data['iodata']['voltage']>self.ign_v:
						data['iodata']['ignition'] = 1
					else:
						data['iodata']['ignition'] = 0

			all_data.append(data)
			logger.debug(f"[Teltonika] #{len(all_data)}:\n{data}\n")

		logger.debug(f'[Teltonika] data:\n{all_data}\n')
		return all_data


	def handle_command(self, packet):
		packet, _ = extract_ubyte(packet)
		packet, length = extract_uint(packet)

		if self.codec==14:
			packet, imei = extract_str(packet, 8)
			length -= 8

		elif self.codec==13:
			packet, ts = extract(packet, 8)
			ts = int(b'0x'+timestamp, 16)
			ts /= 1000

		packet, response = extract_str(packet, length)
		packet, _ = extract_ubyte(packet)
		packet, _ = extract_uint(packet)
		logger.debug(f'[Teltonika] пакет с ответом на команду распакован\n')
		return response.decode('ascii')


	def codec_8(self, packet):
		logger.debug(f'[Teltonika] CODEC {self.codec} AVL Data packet:\n{packet}\n')
		packet, timestamp = extract(packet, 8)
		timestamp = b'0x'+timestamp
		timestamp = int(timestamp, 16)
		timestamp /= 1000
		packet, _ = extract_ubyte(packet) #priority
		packet, lon = extract_int(packet)
		lon /= 10000000
		packet, lat = extract_int(packet)
		lat /= 10000000
		packet, alt = extract_ushort(packet)
		packet, dr = extract_ushort(packet)
		dr = dr//2
		packet, sat_num = extract_ubyte(packet)
		packet, speed = extract_ushort(packet)

		dt = datetime.datetime.utcfromtimestamp(timestamp)
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

		if EventIO==385:
			packet, iodata = self.handle_beacon(packet)
		else:
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
					if str(io_id) in self.assign.keys():
						iodata.update({self.assign[str(io_id)]: io_val})

					elif self.decoder[str(io_id)] in self.assign.keys():
						iodata.update({self.assign[self.decoder[str(io_id)]]: io_val})

					else:
						iodata.update({self.decoder[str(io_id)]: io_val})

			if 'voltage' in iodata.keys():
				iodata['voltage'] = iodata['voltage']/1000

			if 'Battery Voltage' in iodata.keys():
				iodata['Battery Voltage'] = iodata['Battery Voltage']/1000

			data.update(iodata)
		

		if self.codec==142:
			packet, count = extract_ushort(packet)

			iodata = {}
			for _ in range(count):
				packet, io_id = extract_ushort(packet)
				packet, length = extract_ushort(packet)

				if length>8:
					packet, io_val = extract(packet, length)
				else:
					packet, io_val = extract_x(packet, 'q', length)

				if str(io_id) not in self.decoder.keys():
					logger.error(f'[Teltonika] Неизвестный AVL IO ID {io_id}\n')

				else:
					if str(io_id) in self.assign.keys():
						iodata.update({self.assign[str(io_id)]: io_val})

					elif self.decoder[str(io_id)] in self.assign.keys():
						iodata.update({self.assign[self.decoder[str(io_id)]]: io_val})

					else:
						iodata.update({self.decoder[str(io_id)]: io_val})

			data.update(iodata)

		return packet, data


	def prepare_geo(self, records):
		all_geo = []
		for data in records:
			data['iodata'].update({"sat_num": data['sat_num']})
			reserve = str(data['iodata']).replace("'", '"')
			reserve = reserve[1:-1]

			geo = {
				'imei': data['imei'],
				'lat': float('{:.6f}'.format(data['lat'])),
				'lon': float('{:.6f}'.format(data['lon'])),
				'datetime': data['datetime'],
				'type': 0,
				'speed': data['speed'],
				'direction': data['direction'],
				'bat': 0,
				'fuel': 0,
				'ignition': data['iodata'].get('ignition', 0),
				'sensor': data['iodata'].get('sensor', 0),
				'reserve': reserve,
				'ts': data['ts']
			}

			all_geo.append(geo)

		return all_geo


	def handle_beacon(self, packet):
		packet, _ = extract_short(packet)
		packet, _ = extract_short(packet)
		packet, _ = extract_short(packet)
		packet, _ = extract_short(packet)
		packet, _ = extract_short(packet)
		packet, length = extract_short(packet)
		packet, beacon = extract(packet, length)

		return packet, {"Beacon": beacon}


	@staticmethod
	def get_tracker(imei):
		for t in Teltonika.TRACKERS:
			if str(t.imei)==str(imei):
				return t

		return None