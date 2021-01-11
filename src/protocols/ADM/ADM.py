import os
import struct
import binascii
import socket
import threading
import datetime

from time import time, sleep
from json import load, loads, dumps
from enum import Enum

from src.utils import *
from src.db_worker import *
from src.db_connect import CONN
from src.logs.log_config import logger

data_blocks = (
    ADM.data_acc,
    ADM.data_ain,
    ADM.data_din,
    ADM.data_fuel,
    ADM.data_can,
    ADM.data_odometr
)

class ADM:

    BASE_PATH = 'tracker_receiver/src/protocols/ADM/'
    NAME = 'ADM'
    TRACKERS = set()

    def __init__(self, sock, addr, model):
        self.sock = sock
        self.addr = addr
        self.model = model
        self.need_reply = False


    def start(self):
        ADM.TRACKERS.add(self)
        self.db = pymysql.connect(**CONN)

        self.imei = self.handle_imei()
        logger.info(f'{self.model} {self.imei} подключен [{self.addr[0]}:{self.addr[1]}]')

        self.assign = get_configuration(self.NAME, self.imei, self.model)
        self.ign_v = get_ignition_v(self.imei)

        self.lock = threading.Lock()
        self.stop = False
        self.data = {}
        self.handle_packet()


    def handle_imei(self):
        try:
            packet = binascii.hexlify(self.sock.recv(64))
            packet, _ = extract_ushort(packet)
            packet, _ = extract_ubyte(packet)
            packet, _ = extract_ubyte(packet)
            packet, imei = extract_str(packet, 15)
            packet, _ = extract_ubyte(packet)
            packet, need_reply = extract_ubyte(packet)

            if need_reply==2:
                self.need_reply = True

            logger.debug(f'[{self.model}] imei получен {imei}\n')
            # self.sock.send(struct.pack('!B', 1))
            # logger.debug(f'[ADM] ответ на сообщение с imei отправлен\n')

        except Exception as e:
            ADM.TRACKERS.remove(self)
            self.stop = True
            self.sock.close()
            raise e

        return imei.decode('ascii')


    def handle_packet(self):
        if self.stop:
            self.stop = False

        while not self.stop:
            self.msg_type = 0
            try:
                packet = binascii.hexlify(self.sock.recv(4))
                packet = binascii.hexlify(self.sock.recv(extract_ushort(packet)[1]))
            except Exception:
                self.sock.close()
                self.db.close()
                self.stop = True
                logger.info(f'{self.model} {self.imei} отключен [{self.addr[0]}:{self.addr[1]}]')
                break

            self.lock.acquire()
            logger.debug(f'[ADM{self.model}] {self.imei} принят пакет {packet}')

            all_data = []
            while len(packet)!=0:
                packet, data = self.data_main(packet)
                logger.debug(f'[ADM{self.model}] {self.imei} основные данные обработаны\n{d}')
                logger.debug(f'[ADM{self.model}] {self.imei} статус пакета {d["typ"]}')
                bits = list(map(int, str(bin(d['typ']))[2:][::-1]))[2:]
                for n, bit in enumerate(bits):
                    if bit:
                        before = packet[:]
                        packet, d = data_blocks[n](self, packet)
                        logger.debug(f'[ADM{self.model}] {self.imei} блок {data_blocks[n].__name__}\nПакет до: {before}\nПакет после: {packet}\nДанные: {d}')
                        data.update(d)

                data = self.rename_data(data)
                logger.debug(f'[ADM{self.model}] {self.imei} данные после переименования: {data}')
                data = prepare_geo(data)
                logger.debug(f'[ADM{self.model}] {self.imei} данные после обработки: {data}')
                all_data.append(data)

            count = insert_geo(all_data)
            logger.info(f'ADM{self.model} {self.imei} принято {count}/{len(all_data)} записей')
            if self.need_reply:
                self.sock.send(b'***'+str(count).encode('ascii')+b'*')


    def data_main(self, packet):
        packet, _ = extract_ushort(packet)
        packet, _ = extract_ubyte(packet)
        packet, typ = extract_ubyte(packet)
        packet, _ = extract_ubyte(packet)
        packet, pid = extract_ushort(packet)
        packet, status = extract_ushort(packet)
        packet, lat = extract_float(packet, '<')
        packet, lon = extract_float(packet, '<')
        packet, direction = extract_ushort(packet)
        packet, speed = extract_ushort(packet)
        packet, ACC = extract_ubyte(packet)
        packet, HEIGHT = extract_ushort(packet)
        packet, _ = extract_ubyte(packet)
        packet, sat_n = extract_ubyte(packet)
        packet, dt = extract_uint(packet)
        packet, V_POWER = extract_ushort(packet)
        packet, V_BATTERY = extract_ushort(packet)

        dt = datetime.datetime.utcfromtimestamp(dt)
        data = {key:value for key, value in locals().items() if key not in ['self', 'packet']}
        return packet, data


    def data_acc(self, packet):
        packet, VIB = extract_ubyte(packet)
        packet, VIB_COUNT = extract_ubyte(packet)
        packet, OUT = extract_ubyte(packet)
        packet, IN_ALARM = extract_ubyte(packet)

        data = {key:value for key, value in locals().items() if key not in ['self', 'packet']}
        return packet, data


    def data_ain(self, packet):
        packet, IN_A0 = extract_ushort(packet)
        packet, IN_A1 = extract_ushort(packet)
        packet, IN_A2 = extract_ushort(packet)
        packet, IN_A3 = extract_ushort(packet)
        packet, IN_A4 = extract_ushort(packet)
        packet, IN_A5 = extract_ushort(packet)

        data = {key:value for key, value in locals().items() if key not in ['self', 'packet']}
        return packet, data


    def data_din(self, packet):
        packet, IN_D0 = extract_uint(packet)
        packet, IN_D1 = extract_uint(packet)

        data = {key:value for key, value in locals().items() if key not in ['self', 'packet']}
        return packet, data


    def data_fuel(self, packet):
        packet, FUEL_LEVEL_0 = extract_ushort(packet)
        packet, FUEL_LEVEL_1 = extract_ushort(packet)
        packet, FUEL_LEVEL_2 = extract_ushort(packet)
        packet, TEMP_0 = extract_byte(packet)
        packet, TEMP_1 = extract_ubyte(packet)
        packet, TEMP_2 = extract_ubyte(packet)

        data = {key:value for key, value in locals().items() if key not in ['self', 'packet']}
        return packet, data


    def data_can(self, packet):
        return packet, {}


    def data_odometr(self, packet):
        packet, ODOMETR = extract_uint(packet)

        data = {key:value for key, value in locals().items() if key not in ['self', 'packet']}
        return packet, data


    def prepare_geo(self, data):
        ex_keys = ('lat', 'lon', 'speed', 'direction', 'dt', 'typ', 'pid')
        reserve = {k:v for k,v in data.items() if k not in ex_keys}
        reserve = str(reserve).replace("'", '"').replace(' ', '')[1:-1]

        geo = {
            'imei': self.imei,
            'lat': float('{:.3f}'.format(data['lat'])),
            'lon': float('{:.3f}'.format(data['lon'])),
            'datetime': data['dt'],
            'type': 0,
            'speed': data['speed'],
            'direction': data['direction'],
            'bat': 0,
            'fuel': 0,
            'ignition': data.get('ignition', 0),
            'sensor': data.get('sensor', 0),
            'reserve': reserve,
            'ts': datetime.datetime.utcfromtimestamp(int(time()))
        }

        return geo


    def rename_data(self, data):
        return data