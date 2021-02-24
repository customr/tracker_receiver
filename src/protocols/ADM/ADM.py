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


class ADM:

    BASE_PATH = 'tracker_receiver/src/protocols/ADM/'
    NAME = 'ADM'
    TRACKERS = set()

    DATA_BLOCKS = (
        'data_acc',
        'data_ain',
        'data_din',
        'data_fuel',
        'data_can',
        'data_odometr'
    )

    def __init__(self, sock, addr, model):
        self.sock = sock
        self.addr = addr
        self.model = model
        self.imei = ''


    def start(self):
        ADM.TRACKERS.add(self)
        self.db = pymysql.connect(**CONN)

        self.need_reply = 0
        self.imei = self.handle_imei()

        if not self.imei is None:
            logger.info(f'ADM{self.model} {self.imei} подключен [{self.addr[0]}:{self.addr[1]}]')

            self.assign = get_configuration(self.NAME, self.imei, self.model)
            self.ign_v = get_ignition_v(self.imei)

            self.stop = False
            self.data = {}
            self.command_response = {}

            try:
                self.handle_packet()
            except Exception as e:
                self.close()
                raise e


    def close(self):
        ADM.TRACKERS.remove(self)
        self.sock.close()
        self.db.close()
        self.stop = True
        logger.info(f'ADM{self.model} {self.imei} отключен [{self.addr[0]}:{self.addr[1]}]')


    def handle_imei(self):
        try:
            packet = binascii.hexlify(self.sock.recv(1024))
            logger.debug(f'[ADM{self.model}] пакет с imei: {packet}')
            packet, _ = extract_ushort(packet, '=')
            packet, _ = extract_ubyte(packet, '=')
            packet, _ = extract_ubyte(packet, '=')
            packet, imei = extract_str(packet, 15)

            if not imei.isdigit():
                ADM.TRACKERS.remove(self)
                self.sock.close()
                self.db.close()
                self.stop = True
                return None

            packet, _ = extract_ubyte(packet, '=')
            packet, need_reply = extract_ubyte(packet, '=')

            self.need_reply = need_reply
            logger.debug(f'[ADM{self.model}] imei получен {imei}\nneed reply={need_reply}\n')
            # self.sock.send(struct.pack('!B', 1))
            # logger.debug(f'[ADM] ответ на сообщение с imei отправлен\n')

        except Exception as e:
            self.close()
            raise e

        return imei.decode('ascii')


    def handle_packet(self):
        if self.stop:
            self.stop = False

        while not self.stop:
            self.msg_type = 0
            try:
                packet = binascii.hexlify(self.sock.recv(1024))
                if len(packet)==4:
                    packet = binascii.hexlify(self.sock.recv(1024))
                if len(packet)==0:
                    continue

            except Exception as e:
                self.close()
                break

            logger.debug(f'[ADM{self.model}] {self.imei} принят пакет {packet}')

            all_data = []
            while len(packet)>0:
                packet, data = self.data_main(packet)
                if packet is None:
                    break
                logger.debug(f'[ADM{self.model}] {self.imei} основные данные обработаны\n{data}')
                logger.debug(f'[ADM{self.model}] {self.imei} статус пакета {data["typ"]}')
                bits = list(map(int, str(bin(data['typ']))[2:][::-1]))[2:]
                for n, bit in enumerate(bits):
                    if bit:
                        before = packet[:]
                        packet, d = getattr(ADM, ADM.DATA_BLOCKS[n])(self, packet)
                        logger.debug(f'[ADM{self.model}] {self.imei} блок {ADM.DATA_BLOCKS[n]}\nПакет до: {before}\nПакет после: {packet}\nДанные: {d}')
                        data.update(d)

                data = self.rename_data(data)
                logger.debug(f'[ADM{self.model}] {self.imei} данные после переименования: {data}')
                data = self.prepare_geo(data)
                logger.debug(f'[ADM{self.model}] {self.imei} данные после обработки: {data}')
                all_data.append(data)

            if not packet is None:
                count = insert_geo(all_data)
                logger.info(f'ADM{self.model} {self.imei} принято {count}/{len(all_data)} записей')
                if self.need_reply==1:
                    pass
                elif self.need_reply==2:
                    self.sock.send(b'***'+str(count).encode('ascii')+b'*')


    def send_command(self, command):
        packet = ''
        packet = add_str(packet, command)
        packet = add_byte(packet, 0x0D)
        packet = add_byte(packet, 0x0A)
        logger.debug(f'[ADM{self.model}] {self.imei} командный пакет сформирован:\n{packet}\n')
        packet = pack(packet)
        self.sock.send(packet)
        logger.debug(f'[ADM{self.model}] {self.imei} команда {command} отправлена\n')


    def handle_command(self, packet):
        resp = str(binascii.a2b_hex(packet))[2:-1]
        return resp


    @staticmethod
    def get_tracker(imei):
        for t in ADM.TRACKERS:
            if str(t.imei)==str(imei):
                return t

        return None


    def data_main(self, packet):
        packet, _ = extract_ushort(packet, '=')
        packet, size = extract_ubyte(packet, '=')

        if size==0x84:
            try:
                logger.debug(f'[ADM{self.model}] {self.imei} принят ответ на команду {packet}')
                result = self.handle_command(packet)
            except Exception as e:
                result = "Ошибка на сервере: "+str(e)

                resp = {"action":"response", "result": result}
                self.command_response = dumps(resp)
                logger.debug(f'[ADM{self.model}] {self.imei} ошибка распаковки ответа на команду\n{result}\n')
                logger.info(str(e))
            else:
                resp = {"action":"response", "result": result}
                self.command_response = dumps(resp)
                logger.debug(f'[ADM{self.model}] {self.imei} ответ на команду принят\n{result}\n')

            return None, None

        packet, typ = extract_ubyte(packet, '=')
        packet, _ = extract_ubyte(packet, '=')
        packet, ID = extract_ushort(packet, '=')
        packet, status = extract_ushort(packet, '=')
        packet, lat = extract_float(packet, '<')
        packet, lon = extract_float(packet, '<')
        packet, direction = extract_ushort(packet, '=')
        packet, speed = extract_ushort(packet, '=')
        packet, ACC = extract_ubyte(packet, '=')
        packet, HEIGHT = extract_ushort(packet, '=')
        packet, HDOP = extract_ubyte(packet, '=')
        packet, SAT_N = extract_ubyte(packet, '=')
        packet, timestamp = extract_uint(packet, '=')
        packet, V_POWER = extract_ushort(packet, '=')
        packet, V_BATTERY = extract_ushort(packet, '=')

        direction /= 10
        speed /= 10
        ACC /= 10

        if direction > 360:
            direction = 0
        else:
            direction = (direction/2)%0xff

        SAT_N = (SAT_N&0x0f) + ((SAT_N&0xf0)>>4)
        dt = datetime.datetime.utcfromtimestamp(timestamp)
        data = {key:value for key, value in locals().items() if key not in ['self', 'packet']}
        return packet, data


    def data_acc(self, packet):
        packet, VIB = extract_ubyte(packet, '=')
        packet, VIB_COUNT = extract_ubyte(packet, '=')
        packet, OUT = extract_ubyte(packet, '=')
        packet, IN_ALARM = extract_ubyte(packet, '=')

        data = {key:value for key, value in locals().items() if key not in ['self', 'packet', 'IN_ALARM']}
        for i in range(4):
            data[f'IN_ALARM_{i}'] = int((IN_ALARM & (2**i)) > 0)

        return packet, data


    def data_ain(self, packet):
        packet, IN_A0 = extract_ushort(packet, '=')
        packet, IN_A1 = extract_ushort(packet, '=')
        packet, IN_A2 = extract_ushort(packet, '=')
        packet, IN_A3 = extract_ushort(packet, '=')
        packet, IN_A4 = extract_ushort(packet, '=')
        packet, IN_A5 = extract_ushort(packet, '=')

        data = {key:value for key, value in locals().items() if key not in ['self', 'packet']}
        return packet, data


    def data_din(self, packet):
        packet, IN_D0 = extract_uint(packet, '=')
        packet, IN_D1 = extract_uint(packet, '=')

        data = {key:value for key, value in locals().items() if key not in ['self', 'packet']}
        return packet, data


    def data_fuel(self, packet):
        packet, FUEL_LEVEL_0 = extract_ushort(packet, '=')
        packet, FUEL_LEVEL_1 = extract_ushort(packet, '=')
        packet, FUEL_LEVEL_2 = extract_ushort(packet, '=')
        packet, TEMP_0 = extract_byte(packet, '=')
        packet, TEMP_1 = extract_byte(packet, '=')
        packet, TEMP_2 = extract_byte(packet, '=')

        data = {key:value for key, value in locals().items() if key not in ['self', 'packet']}
        return packet, data


    def data_can(self, packet):
        can_packet, lenght = extract_ubyte(packet)
        can_packet = can_packet[:lenght*2-2]
        packet = packet[lenght*2:]

        data = {}
        while len(can_packet)>0:
            can_packet, can = extract_ubyte(can_packet, '=')
            tag = can&0x3f
            data_len = (can&0xc0)>>6
            if data_len==0:
                can_packet, value = extract_byte(can_packet, '<')
            elif data_len==1:
                can_packet, value = extract_short(can_packet, '<')
            elif data_len==2:
                can_packet, value = extract_int(can_packet, '<')
            elif data_len==3:
                can_packet, value = extract_longlong(can_packet, '<')
            else:
                value = 'error'

            data[f'CAN{tag}'] = value

        return packet, data


    def data_odometr(self, packet):
        packet, ODOMETR = extract_uint(packet, '=')

        data = {key:value for key, value in locals().items() if key not in ['self', 'packet']}
        return packet, data


    def prepare_geo(self, data):
        ex_keys = ('lat', 'lon', 'speed', 'direction', 'timestamp', 'dt', 'typ', 'size', 'ID', '_')
        reserve = {k:v for k,v in data.items() if k not in ex_keys}
        reserve = str(reserve)[1:-1].replace("'", '"').replace(' ', '')

        geo = {
            'imei': self.imei,
            'lat': float('{:.6f}'.format(data['lat'])),
            'lon': float('{:.6f}'.format(data['lon'])),
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
        new_data = {}
        for key, value in data.items():
            if key in self.assign.keys():
                ikey = self.assign[key]
                if '*' in ikey:
                    spl = ikey.split('*')
                    ikey, k = spl[0], spl[1]
                    new_val = round(value*float(k), 2)

                    if ikey=='temp' or ikey=='temp2':
                        new_val = round(new_val, 1)

                    new_data[ikey] = new_val
                    continue

                new_data[ikey] = value
                continue

            new_data[key] = value

        return new_data