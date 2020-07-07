import os
import struct
import binascii
import socket
import threading
import datetime

from time import time
from json import load

from src.utils import *
from src.db_worker import *
from src.logs.log_config import logger
from src.protocols.Teltonika.crc import crc16


class Teltonika:

	BASE_PATH = 'tracker_receiver/src/protocols/Teltonika/'
	NAME = 'Teltonika'

	def __init__(self, sock, addr):
		self.sock = sock
		self.addr = addr

		self.imei = self.handle_imei()
		logger.info(f'Teltonika {self.imei} подключен[{addr}]\n')

		self.assign, self.model = get_configuration_and_model(self.imei)
		self.decoder = self.get_decoder(self.model)
		self.ign_v = get_ignition_v(self.imei)
		
		self.lock = threading.Lock()
		waiter_th = threading.Thread(target=self.wait_command)
		main_th = threading.Thread(target=self.handle_packet)
		waiter_th.start()
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
		packet = binascii.hexlify(self.sock.recv(34))
		length = int(packet[:4], 16)
		packet = unpack_from_bytes(f'{length}s', packet[4:])[0]
		imei = packet.decode('ascii')
		logger.debug(f'[Teltonika] imei получен {imei}\n')
		self.sock.send(struct.pack('!B', 1))
		logger.debug(f'[Teltonika] ответ на сообщение с imei отправлен\n')
		return imei


	def handle_packet(self):
		while True:
			packet = binascii.hexlify(self.sock.recv(4096))
			self.lock.acquire()
			logger.debug(f'[Teltonika] получен пакет:\n{packet}\n')
			packet, _ = extract_int(packet) #preamble zero bytes
			packet, data_len = extract_uint(packet)
			packet, self.codec = extract_ubyte(packet)
			packet, self.count = extract_ubyte(packet)
		

			if self.codec in (8, 142, 16):
				self.data = self.handle_data(packet)
				self.data = self.prepare_geo(self.data)
				count = insert_geo(self.data)
				logger.info(f'Teltonika из них безошибочных: {count} записей')

			elif self.codec in (12, 13, 14):
				result = self.handle_command(packet)
				with open(self.BASE_PATH+'result.txt', 'w') as res:
					res.write(result)

				logger.info(f'[Teltonika] ответ на команду принят\n')

			else:
				logger.critical(f"Teltonika неизвестный кодек {self.codec}")
				raise ValueError('Unknown codec')

			self.lock.release()


	def wait_command(self):
		command_path = self.BASE_PATH+'command.txt'
		result_path =  self.BASE_PATH+'result.txt'
		error_c = 0
		while True:
			if os.path.getsize(command_path)>0:
				self.lock.acquire()
				try:
					command_fd = open(command_path, 'r')
					command_file = command_fd.read()
					imei, codec = command_file.split(' ')[:2]

					if imei!=self.imei: 
						self.lock.release()
						continue

					command = command_file.split('"')[1]
					msg = f'[Teltonika] команда дешифрована:\nimei={imei}\ncodec={codec}\ncommand={command}\n'
					logger.debug(msg)
					command_fd.close()
					open(command_path, 'w').close()
					self.send_command(imei, codec, command)
					error_c = 0

				except Exception as e:
					error_c += 1
					logger.error(f'Teltonika ошибка в обработке команды:\n{e}\nПопытка={error_c}\n')
					command_fd.close()
					
					if error_c>1:
						open(command_path, 'w').close()

					with open(result_path, 'w') as res:
						res.write(f'Ошибка в обработке команды:\n{e}\n')

				self.lock.release()


	def send_command(self, imei, codec, command):
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
			packet = add_str(packet, imei.rjust(16, '0'))

		packet = add_str(packet, command)
		packet = add_ubyte(packet, 1)
		crc16_pack = struct.unpack(f'{len(packet[16:])//2}s', binascii.a2b_hex(packet[16:].encode('ascii')))[0]
		packet = add_uint(packet, crc16(crc16_pack))
		logger.debug(f'[Teltonika] командный пакет сформирован:\n{packet}\n')
		packet = pack(packet)
		self.sock.send(packet)
		logger.debug(f'[Teltonika] команда отправлена\n')

		if result:
			with open(self.BASE_PATH+'result.txt', 'w') as res:
				res.write(result)


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

		# for n, rec in enumerate(all_data):
		# 	for name, x in self.assign.items():
		# 		if name in rec['iodata'].keys():
		# 			value = rec['iodata'][name]

		# 			if name=='External Voltage':
		# 				if self.ign_v is not None:
		# 					if value>self.ign_v:
		# 						value = 1
		# 					else:
		# 						value = 0

		# 					rec['iodata'].update({'ignition': value})
		# 					del(rec['iodata'][name])
		# 					continue

		# 			rec['iodata'].update({x: value})
		# 			del(rec['iodata'][name])

		# 	rec.update({"imei": int(self.imei)})
		# 	logger.debug(f"[Teltonika] Record #{n+1} AVL IO Data преобразована\n")

		logger.debug(f'[Teltonika] data:\n{all_data}\n')
		logger.info(f'Teltonika {self.imei} получено {len(all_data)} записей')
		self.sock.send(struct.pack("!I", len(all_data)))
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
			if not data['iodata'].get('ignition', None):
				data['iodata'].update({"ignition": 0})

			if not data['iodata'].get('sensor', None):
				data['iodata'].update({"sensor": 0})

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
				'ignition': data['iodata']['ignition'],
				'sensor': data['iodata']['sensor'],
				'reserve': reserve,
				'ts': data['ts']
			}

			all_geo.append(geo)

		return all_geo